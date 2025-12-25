package io.kestra.plugin.transform;

import com.amazon.ion.IonList;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@KestraTest
@EnabledIfSystemProperty(named = "bench", matches = "true")
class BenchTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void runBenchmarks() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        List<Integer> sizesMb = parseSizes();
        Path benchDir = Path.of("build", "bench");
        Files.createDirectories(benchDir);
        Path reportPath = benchDir.resolve("report.txt");

        try (BufferedWriter writer = Files.newBufferedWriter(reportPath, StandardCharsets.UTF_8)) {
            writer.write("Kestra transform benchmark\n");
            writer.write("Sizes (MB): " + sizesMb + "\n");
            writer.write("Output: build/bench\n\n");

            for (int sizeMb : sizesMb) {
                Path inputPath = benchDir.resolve("input-" + sizeMb + "mb.ion");
                GenerationResult generation = ensureIonFile(inputPath, sizeMb);
                long inputBytes = generation.bytes();

                String inputUri = storeInput(runContext, inputPath);

                writer.write("Input: " + inputPath.getFileName() + " (" + inputBytes + " bytes)\n");
                writer.write("Generate: " + generation.durationMs() + " ms"
                    + (generation.generated() ? "" : " (cached)") + "\n");

                BenchResult read = timeTaskWithMemory(() -> runReadOnly(runContext, inputUri));
                writer.write(formatResult("Read", read));
                writer.flush();

                BenchResult copy = timeTaskWithMemory(() -> runCopy(runContext, inputUri));
                writer.write(formatResult("Copy", copy));
                writer.flush();

                BenchResult map = timeTaskWithMemory(() -> runMap(runContext, inputUri));
                writer.write(formatResult("Map", map));
                writer.flush();

                BenchResult unnest = timeTaskWithMemory(() -> runUnnest(runContext, inputUri));
                writer.write(formatResult("Unnest", unnest));
                writer.flush();

                BenchResult filter = timeTaskWithMemory(() -> runFilter(runContext, inputUri));
                writer.write(formatResult("Filter", filter));
                writer.flush();

                BenchResult aggregate = timeTaskWithMemory(() -> runAggregate(runContext, inputUri));
                writer.write(formatResult("Aggregate", aggregate));
                writer.flush();
                writer.write("\n");
                writer.flush();
            }
        }
    }

    private List<Integer> parseSizes() {
        String raw = System.getProperty("bench.sizes");
        if (raw == null || raw.isBlank()) {
            return List.of(1, 10, 100, 1024);
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

    private GenerationResult ensureIonFile(Path path, int sizeMb) throws IOException {
        if (Files.exists(path)) {
            return new GenerationResult(false, 0L, Files.size(path));
        }
        long targetBytes = sizeMb * 1024L * 1024L;
        long start = System.nanoTime();
        int index = 0;

        try (OutputStream fileStream = Files.newOutputStream(path);
             OutputStream outputStream = new java.io.BufferedOutputStream(fileStream);
             CountingOutputStream countingStream = new CountingOutputStream(outputStream);
             IonWriter writer = IonValueUtils.system().newTextWriter(countingStream)) {
            while (countingStream.getCount() < targetBytes) {
                IonStruct record = createRecord(index++);
                record.writeTo(writer);
                countingStream.write('\n');
            }
            writer.finish();
        }

        long durationMs = Duration.ofNanos(System.nanoTime() - start).toMillis();
        return new GenerationResult(true, durationMs, Files.size(path));
    }

    private IonStruct createRecord(int index) {
        IonStruct record = IonValueUtils.system().newEmptyStruct();
        record.put("customer_id", IonValueUtils.system().newString("c" + (index % 100)));
        record.put("country", IonValueUtils.system().newString("US"));
        record.put("total_spent", IonValueUtils.system().newDecimal(index % 100));
        record.put("active", IonValueUtils.system().newBool(index % 2 == 0));

        IonList items = IonValueUtils.system().newEmptyList();
        items.add(createItem("sku-" + index + "-a", 10));
        items.add(createItem("sku-" + index + "-b", 20));
        record.put("items", items);
        return record;
    }

    private IonStruct createItem(String sku, int price) {
        IonStruct item = IonValueUtils.system().newEmptyStruct();
        item.put("sku", IonValueUtils.system().newString(sku));
        item.put("price", IonValueUtils.system().newDecimal(price));
        return item;
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

    private void runCopy(RunContext runContext, String uri) throws Exception {
        try (InputStream inputStream = runContext.storage().getFile(java.net.URI.create(uri))) {
            Path outputPath = runContext.workingDir().createTempFile(".ion");
            try (OutputStream fileStream = java.nio.file.Files.newOutputStream(outputPath);
                 OutputStream outputStream = new java.io.BufferedOutputStream(fileStream);
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

    private String formatResult(String name, BenchResult result) {
        long delta = result.afterBytes - result.beforeBytes;
        double deltaMiB = delta / (1024.0 * 1024.0);
        return name + ": " + result.durationMs + " ms"
            + " | mem_before=" + result.beforeBytes
            + " mem_after=" + result.afterBytes
            + " mem_delta=" + String.format(java.util.Locale.ROOT, "%.2f", deltaMiB) + " MiB\n";
    }

    private record BenchResult(long durationMs, long beforeBytes, long afterBytes) {
    }

    private record GenerationResult(boolean generated, long durationMs, long bytes) {
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
