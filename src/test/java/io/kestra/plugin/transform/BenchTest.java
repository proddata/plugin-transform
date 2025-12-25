package io.kestra.plugin.transform;

import com.amazon.ion.IonValue;
import com.amazon.ion.IonList;
import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ion.system.IonBinaryWriterBuilder;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.transform.ion.IonTypeName;
import io.kestra.plugin.transform.ion.IonValueUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.zip.GZIPOutputStream;

@KestraTest
@EnabledIfSystemProperty(named = "bench", matches = "true")
class BenchTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void runBenchmarks() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        List<Integer> recordCounts = parseRecordCounts();
        OutputFormat format = parseFormat();
        Compression compression = parseCompression();
        Path benchDir = Path.of("build", "bench");
        Files.createDirectories(benchDir);
        Path reportPath = benchDir.resolve("report.md");

        try (BufferedWriter writer = Files.newBufferedWriter(reportPath, StandardCharsets.UTF_8)) {
            writer.write("# Kestra transform benchmark\n\n");
            writer.write("- Records: " + formatCountList(recordCounts) + "\n");
            writer.write("- Format: " + format.name().toLowerCase(java.util.Locale.ROOT) + "\n");
            writer.write("- Compression: " + compression.label + "\n");
            writer.write("- Output: build/bench\n\n");
            writer.write("| Records | Input size | Generate (ms) | Read (ms) | Copy (ms) | Map (ms) | Unnest (ms) | Filter (ms) | Aggregate (ms) |\n");
            writer.write("| --- | --- | --- | --- | --- | --- | --- | --- | --- |\n");

            for (int recordCount : recordCounts) {
                Path inputPath = benchDir.resolve("input-" + recordCount + "." + format.fileExtension);
                GenerationResult generation = ensureIonFile(inputPath, recordCount, format);
                long inputBytes = generation.bytes();

                String inputUri = storeInput(runContext, inputPath);

                BenchResult read = timeTaskWithMemory(() -> runReadOnly(runContext, inputUri));
                BenchResult copy = timeTaskWithMemory(() -> runCopy(runContext, inputUri, compression));
                BenchResult map = timeTaskWithMemory(() -> runMap(runContext, inputUri));
                BenchResult unnest = timeTaskWithMemory(() -> runUnnest(runContext, inputUri));
                BenchResult filter = timeTaskWithMemory(() -> runFilter(runContext, inputUri));
                BenchResult aggregate = timeTaskWithMemory(() -> runAggregate(runContext, inputUri));
                writer.write("| " + formatCount(generation.records())
                    + " | " + formatSize(inputBytes)
                    + " | " + formatDuration(generation.durationMs())
                    + (generation.generated() ? "" : " (cached)")
                    + " | " + formatDuration(read.durationMs)
                    + " | " + formatDuration(copy.durationMs)
                    + " | " + formatDuration(map.durationMs)
                    + " | " + formatDuration(unnest.durationMs)
                    + " | " + formatDuration(filter.durationMs)
                    + " | " + formatDuration(aggregate.durationMs)
                    + " |\n");
                writer.flush();
            }
        }
    }

    private List<Integer> parseRecordCounts() {
        String raw = System.getProperty("bench.records");
        if (raw == null || raw.isBlank()) {
            return List.of(10_000, 100_000, 1_000_000);
        }
        String[] parts = raw.split(",");
        List<Integer> sizes = new ArrayList<>();
        for (String part : parts) {
            String trimmed = part.trim();
            if (!trimmed.isEmpty()) {
                sizes.add(Integer.parseInt(trimmed));
            }
        }
        return sizes;
    }

    private GenerationResult ensureIonFile(Path path, int recordCount, OutputFormat format) throws IOException {
        if (Files.exists(path)) {
            return new GenerationResult(false, 0L, Files.size(path), recordCount);
        }
        long start = System.nanoTime();
        int index = 0;
        int writtenRecords = 0;

        try (OutputStream fileStream = Files.newOutputStream(path);
             OutputStream outputStream = new java.io.BufferedOutputStream(fileStream);
             CountingOutputStream countingStream = new CountingOutputStream(outputStream)) {
            if (format == OutputFormat.BINARY) {
                java.io.ByteArrayOutputStream buffer = new java.io.ByteArrayOutputStream(64 * 1024);
                while (writtenRecords < recordCount) {
                    buffer.reset();
                    try (IonWriter writer = IonBinaryWriterBuilder.standard().build(buffer)) {
                        int batch = 0;
                        while (batch < 1000 && writtenRecords < recordCount) {
                            writeRecord(writer, index++, true);
                            batch++;
                            writtenRecords++;
                        }
                        writer.finish();
                    }
                    buffer.writeTo(countingStream);
                }
            } else {
                try (IonWriter writer = IonValueUtils.system().newTextWriter(countingStream)) {
                    while (writtenRecords < recordCount) {
                        writeRecord(writer, index++, false);
                        writtenRecords++;
                        countingStream.write('\n');
                        if (writtenRecords % 1000 == 0) {
                            writer.flush();
                        }
                    }
                    writer.finish();
                }
            }
        }

        long durationMs = Duration.ofNanos(System.nanoTime() - start).toMillis();
        long bytes = Files.size(path);
        return new GenerationResult(true, durationMs, bytes, writtenRecords);
    }

    private void writeRecord(IonWriter writer, int index, boolean binary) throws IOException {
        writer.stepIn(IonType.STRUCT);
        writer.setFieldName("customer_id");
        writer.writeString("c" + (index % 100));
        writer.setFieldName("country");
        writer.writeString("US");
        writer.setFieldName("total_spent");
        if (binary) {
            writer.writeInt(index % 100);
        } else {
            writer.writeDecimal(BigDecimal.valueOf(index % 100));
        }
        writer.setFieldName("active");
        writer.writeBool(index % 2 == 0);
        writer.setFieldName("items");
        writer.stepIn(IonType.LIST);
        writeItem(writer, "sku-" + index + "-a", 10, binary);
        writeItem(writer, "sku-" + index + "-b", 20, binary);
        writer.stepOut();
        writer.stepOut();
    }

    private void writeItem(IonWriter writer, String sku, int price, boolean binary) throws IOException {
        writer.stepIn(IonType.STRUCT);
        writer.setFieldName("sku");
        writer.writeString(sku);
        writer.setFieldName("price");
        if (binary) {
            writer.writeInt(price);
        } else {
            writer.writeDecimal(BigDecimal.valueOf(price));
        }
        writer.stepOut();
    }

    private String storeInput(RunContext runContext, Path path) throws IOException {
        String name = "bench-" + UUID.randomUUID() + ".ion";
        try (FileInputStream inputStream = new FileInputStream(path.toFile())) {
            return runContext.storage().putFile(inputStream, name).toString();
        }
    }

    private void runMap(RunContext runContext, String uri) throws Exception {
        io.kestra.plugin.transform.Map task = io.kestra.plugin.transform.Map.builder()
            .from(Property.ofValue(uri))
            .output(io.kestra.plugin.transform.Map.OutputMode.STORE)
            .fields(Map.of(
                "customer_id", io.kestra.plugin.transform.Map.FieldDefinition.builder().expr("customer_id").type(IonTypeName.STRING).build(),
                "total_spent", io.kestra.plugin.transform.Map.FieldDefinition.builder().expr("total_spent").type(IonTypeName.DECIMAL).build()
            ))
            .build();
        task.run(runContext);
    }

    private void runReadOnly(RunContext runContext, String uri) throws Exception {
        try (InputStream inputStream = runContext.storage().getFile(java.net.URI.create(uri))) {
            Iterator<IonValue> iterator = IonValueUtils.system().iterate(inputStream);
            while (iterator.hasNext()) {
                IonValue value = iterator.next();
                if (value instanceof IonList list) {
                    for (IonValue element : list) {
                        element.getType();
                    }
                } else {
                    value.getType();
                }
            }
        }
    }

    private void runCopy(RunContext runContext, String uri, Compression compression) throws Exception {
        try (InputStream inputStream = runContext.storage().getFile(java.net.URI.create(uri))) {
            Path outputPath = runContext.workingDir().createTempFile(".ion");
            try (OutputStream fileStream = java.nio.file.Files.newOutputStream(outputPath);
                 OutputStream baseStream = new java.io.BufferedOutputStream(fileStream);
                 OutputStream outputStream = wrapCompression(baseStream, compression);
                 IonWriter writer = IonValueUtils.system().newTextWriter(outputStream)) {
                Iterator<IonValue> iterator = IonValueUtils.system().iterate(inputStream);
                while (iterator.hasNext()) {
                    IonValue value = iterator.next();
                    if (value instanceof IonList list) {
                        for (IonValue element : list) {
                            element.writeTo(writer);
                            outputStream.write('\n');
                        }
                    } else {
                        value.writeTo(writer);
                        outputStream.write('\n');
                    }
                }
                writer.finish();
            }
        }
    }

    private void runUnnest(RunContext runContext, String uri) throws Exception {
        Unnest task = Unnest.builder()
            .from(Property.ofValue(uri))
            .output(Unnest.OutputMode.STORE)
            .path(Property.ofValue("items[]"))
            .as(Property.ofValue("item"))
            .build();
        task.run(runContext);
    }

    private void runFilter(RunContext runContext, String uri) throws Exception {
        Filter task = Filter.builder()
            .from(Property.ofValue(uri))
            .output(Filter.OutputMode.STORE)
            .where(Property.ofValue("active"))
            .build();
        task.run(runContext);
    }

    private void runAggregate(RunContext runContext, String uri) throws Exception {
        Aggregate task = Aggregate.builder()
            .from(Property.ofValue(uri))
            .output(Aggregate.OutputMode.STORE)
            .groupBy(Property.ofValue(List.of("customer_id")))
            .aggregates(Map.of(
                "order_count", Aggregate.AggregateDefinition.builder().expr("count()").type(IonTypeName.INT).build(),
                "total_spent", Aggregate.AggregateDefinition.builder().expr("sum(total_spent)").type(IonTypeName.DECIMAL).build()
            ))
            .build();
        task.run(runContext);
    }

    private long timeTask(ThrowingRunnable runnable) throws Exception {
        long start = System.nanoTime();
        runnable.run();
        return Duration.ofNanos(System.nanoTime() - start).toMillis();
    }

    private BenchResult timeTaskWithMemory(ThrowingRunnable runnable) throws Exception {
        long before = usedMemoryBytes();
        long start = System.nanoTime();
        runnable.run();
        long durationMs = Duration.ofNanos(System.nanoTime() - start).toMillis();
        long after = usedMemoryBytes();
        return new BenchResult(durationMs, before, after);
    }

    private long usedMemoryBytes() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }

    private record BenchResult(long durationMs, long beforeBytes, long afterBytes) {
    }

    private record GenerationResult(boolean generated, long durationMs, long bytes, long records) {
    }

    private String formatCountList(List<Integer> counts) {
        List<String> formatted = new ArrayList<>();
        for (Integer count : counts) {
            formatted.add(formatCount(count));
        }
        return formatted.toString();
    }

    private String formatCount(long count) {
        return String.format(java.util.Locale.ROOT, "%,d", count);
    }

    private String formatSize(long bytes) {
        double mib = bytes / (1024.0 * 1024.0);
        return String.format(java.util.Locale.ROOT, "%.2f MiB (%d bytes)", mib, bytes);
    }

    private String formatDuration(long durationMs) {
        return Long.toString(durationMs);
    }

    private OutputFormat parseFormat() {
        String raw = System.getProperty("bench.format");
        if (raw == null || raw.isBlank()) {
            return OutputFormat.TEXT;
        }
        if ("binary".equalsIgnoreCase(raw.trim())) {
            return OutputFormat.BINARY;
        }
        return OutputFormat.TEXT;
    }

    private Compression parseCompression() {
        String raw = System.getProperty("bench.compression");
        if (raw == null || raw.isBlank()) {
            return Compression.NONE;
        }
        if ("gzip".equalsIgnoreCase(raw.trim())) {
            return Compression.GZIP;
        }
        return Compression.NONE;
    }

    private enum OutputFormat {
        TEXT("ion"),
        BINARY("ionb");

        private final String fileExtension;

        OutputFormat(String fileExtension) {
            this.fileExtension = fileExtension;
        }
    }

    private enum Compression {
        NONE("none"),
        GZIP("gzip");

        private final String label;

        Compression(String label) {
            this.label = label;
        }
    }

    private OutputStream wrapCompression(OutputStream outputStream, Compression compression) throws IOException {
        if (compression == Compression.GZIP) {
            return new GZIPOutputStream(outputStream, true);
        }
        return outputStream;
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    private static final class CountingOutputStream extends OutputStream implements AutoCloseable {
        private final OutputStream delegate;
        private long count;

        private CountingOutputStream(OutputStream delegate) {
            this.delegate = delegate;
        }

        @Override
        public void write(int b) throws IOException {
            delegate.write(b);
            count++;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            delegate.write(b, off, len);
            count += len;
        }

        @Override
        public void flush() throws IOException {
            delegate.flush();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }

        private long getCount() {
            return count;
        }
    }
}
