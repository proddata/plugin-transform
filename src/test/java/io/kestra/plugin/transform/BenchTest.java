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
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
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
                long generationMs = generateIonFile(inputPath, sizeMb);
                long inputBytes = Files.size(inputPath);

                String inputUri = storeInput(runContext, inputPath);

                writer.write("Input: " + inputPath.getFileName() + " (" + inputBytes + " bytes)\n");
                writer.write("Generate: " + generationMs + " ms\n");

                BenchResult map = timeTaskWithMemory(() -> runMap(runContext, inputUri));
                writer.write(formatResult("Map", map));
                writer.flush();

                BenchResult unnest = timeTaskWithMemory(() -> runUnnest(runContext, inputUri));
                writer.write(formatResult("Unnest", unnest));
                writer.flush();

                BenchResult filter = timeTaskWithMemory(() -> runFilter(runContext, inputUri));
                writer.write(formatResult("Filter", filter));
                writer.flush();
                writer.write("Aggregate: skipped (in-memory grouping)\n");
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

    private long generateIonFile(Path path, int sizeMb) throws IOException {
        long targetBytes = sizeMb * 1024L * 1024L;
        long start = System.nanoTime();
        long written = 0L;
        int index = 0;

        try (OutputStream outputStream = Files.newOutputStream(path);
             IonWriter writer = IonValueUtils.system().newTextWriter(outputStream)) {
            while (written < targetBytes) {
                IonStruct record = createRecord(index++);
                record.writeTo(writer);
                writer.flush();
                outputStream.write('\n');
                outputStream.flush();
                written = Files.size(path);
            }
            writer.finish();
        }

        return Duration.ofNanos(System.nanoTime() - start).toMillis();
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

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }
}
