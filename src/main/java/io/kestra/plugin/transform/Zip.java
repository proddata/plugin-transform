package io.kestra.plugin.transform;

import com.amazon.ion.IonList;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.transform.ion.IonValueUtils;
import io.kestra.plugin.transform.util.TransformException;
import io.kestra.plugin.transform.util.TransformOptions;
import io.kestra.plugin.transform.util.TransformTaskSupport;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Zip records",
    description = "Merge two record streams by position (record i with record i)."
)
@Plugin(
    examples = {
        @io.kestra.core.models.annotations.Example(
            title = "Zip records from two sources",
            code = {
                "left: \"{{ outputs.left.records }}\"",
                "right: \"{{ outputs.right.records }}\"",
                "options:",
                "  onConflict: RIGHT"
            }
        )
    },
    metrics = {
        @Metric(name = "processed", type = Counter.TYPE),
        @Metric(name = "failed", type = Counter.TYPE),
        @Metric(name = "dropped", type = Counter.TYPE)
    }
)
public class Zip extends Task implements RunnableTask<Zip.Output> {
    @Schema(
        title = "Left records",
        description = "Ion list or struct to zip, or a storage URI pointing to an Ion file."
    )
    private Property<Object> left;

    @Schema(
        title = "Right records",
        description = "Ion list or struct to zip, or a storage URI pointing to an Ion file."
    )
    private Property<Object> right;

    @Schema(
        title = "Options",
        description = "Error handling and conflict behavior."
    )
    @Builder.Default
    private Options options = new Options();

    @Schema(
        title = "Output mode",
        description = "AUTO stores to internal storage when any input is a storage URI; otherwise it returns records."
    )
    @Builder.Default
    private OutputMode output = OutputMode.AUTO;

    @Override
    public Output run(RunContext runContext) throws Exception {
        TransformTaskSupport.ResolvedInput leftInput = TransformTaskSupport.resolveInput(runContext, left);
        TransformTaskSupport.ResolvedInput rightInput = TransformTaskSupport.resolveInput(runContext, right);

        OutputMode effectiveOutput = output == OutputMode.AUTO
            ? ((leftInput.fromStorage() || rightInput.fromStorage()) ? OutputMode.STORE : OutputMode.RECORDS)
            : output;

        StatsAccumulator stats = new StatsAccumulator();

        if (effectiveOutput == OutputMode.STORE && (leftInput.fromStorage() || rightInput.fromStorage())) {
            URI storedUri = zipStreamToStorage(runContext, leftInput, rightInput, stats);
            runContext.metric(Counter.of("processed", stats.processed))
                .metric(Counter.of("failed", stats.failed))
                .metric(Counter.of("dropped", stats.dropped));
            return Output.builder()
                .uri(storedUri.toString())
                .build();
        }

        List<IonStruct> leftRecords = TransformTaskSupport.normalizeRecords(resolveInMemory(runContext, leftInput));
        List<IonStruct> rightRecords = TransformTaskSupport.normalizeRecords(resolveInMemory(runContext, rightInput));
        if (leftRecords.size() != rightRecords.size()) {
            throw new TransformException("left and right inputs must have same length: left="
                + leftRecords.size() + ", right=" + rightRecords.size());
        }

        if (effectiveOutput == OutputMode.STORE) {
            URI storedUri = storeRecords(runContext, leftRecords, rightRecords, stats);
            runContext.metric(Counter.of("processed", stats.processed))
                .metric(Counter.of("failed", stats.failed))
                .metric(Counter.of("dropped", stats.dropped));
            return Output.builder()
                .uri(storedUri.toString())
                .build();
        }

        List<Object> rendered = zipToRecords(leftRecords, rightRecords, stats);
        runContext.metric(Counter.of("processed", stats.processed))
            .metric(Counter.of("failed", stats.failed))
            .metric(Counter.of("dropped", stats.dropped));
        return Output.builder()
            .records(rendered)
            .build();
    }

    private Object resolveInMemory(RunContext runContext, TransformTaskSupport.ResolvedInput resolvedInput) throws TransformException {
        if (!resolvedInput.fromStorage()) {
            return resolvedInput.value();
        }
        return TransformTaskSupport.loadIonFromStorage(runContext, resolvedInput.storageUri());
    }

    private List<Object> zipToRecords(List<IonStruct> leftRecords,
                                      List<IonStruct> rightRecords,
                                      StatsAccumulator stats) throws TransformException {
        List<Object> outputRecords = new ArrayList<>();
        for (int i = 0; i < leftRecords.size(); i++) {
            stats.processed++;
            IonStruct leftRecord = leftRecords.get(i);
            IonStruct rightRecord = rightRecords.get(i);
            try {
                IonStruct merged = mergeRecords(leftRecord, rightRecord);
                outputRecords.add(IonValueUtils.toJavaValue(merged));
            } catch (TransformException e) {
                stats.failed++;
                if (options.onError == TransformOptions.OnErrorMode.FAIL) {
                    throw e;
                }
                if (options.onError == TransformOptions.OnErrorMode.SKIP) {
                    stats.dropped++;
                }
            }
        }
        return outputRecords;
    }

    private URI storeRecords(RunContext runContext,
                             List<IonStruct> leftRecords,
                             List<IonStruct> rightRecords,
                             StatsAccumulator stats) throws TransformException {
        String name = "zip-" + UUID.randomUUID() + ".ion";
        try {
            java.nio.file.Path outputPath = runContext.workingDir().createTempFile(".ion");
            try (OutputStream outputStream = TransformTaskSupport.wrapCompression(
                TransformTaskSupport.bufferedOutput(outputPath));
                 IonWriter writer = IonValueUtils.system().newTextWriter(outputStream)) {
                for (int i = 0; i < leftRecords.size(); i++) {
                    stats.processed++;
                    IonStruct leftRecord = leftRecords.get(i);
                    IonStruct rightRecord = rightRecords.get(i);
                    try {
                        IonStruct merged = mergeRecords(leftRecord, rightRecord);
                        merged.writeTo(writer);
                        outputStream.write('\n');
                    } catch (TransformException e) {
                        stats.failed++;
                        if (options.onError == TransformOptions.OnErrorMode.FAIL) {
                            throw e;
                        }
                        if (options.onError == TransformOptions.OnErrorMode.SKIP) {
                            stats.dropped++;
                        }
                    }
                }
            }
            return runContext.storage().putFile(outputPath.toFile(), name);
        } catch (IOException e) {
            throw new TransformException("Unable to store zipped records", e);
        }
    }

    private URI zipStreamToStorage(RunContext runContext,
                                   TransformTaskSupport.ResolvedInput leftInput,
                                   TransformTaskSupport.ResolvedInput rightInput,
                                   StatsAccumulator stats) throws TransformException {
        String name = "zip-" + UUID.randomUUID() + ".ion";
        try (RecordCursor leftCursor = openCursor(runContext, leftInput);
             RecordCursor rightCursor = openCursor(runContext, rightInput)) {
            java.nio.file.Path outputPath = runContext.workingDir().createTempFile(".ion");
            try (OutputStream outputStream = TransformTaskSupport.wrapCompression(
                TransformTaskSupport.bufferedOutput(outputPath));
                 IonWriter writer = IonValueUtils.system().newTextWriter(outputStream)) {
                while (true) {
                    boolean leftHas = leftCursor.hasNext();
                    boolean rightHas = rightCursor.hasNext();
                    if (!leftHas && !rightHas) {
                        break;
                    }
                    if (leftHas != rightHas) {
                        throw new TransformException("left and right inputs must have same length");
                    }
                    stats.processed++;
                    try {
                        IonStruct leftRecord = leftCursor.nextStruct();
                        IonStruct rightRecord = rightCursor.nextStruct();
                        IonStruct merged = mergeRecords(leftRecord, rightRecord);
                        merged.writeTo(writer);
                        outputStream.write('\n');
                    } catch (TransformException e) {
                        stats.failed++;
                        if (options.onError == TransformOptions.OnErrorMode.FAIL) {
                            throw e;
                        }
                        if (options.onError == TransformOptions.OnErrorMode.SKIP) {
                            stats.dropped++;
                        }
                    }
                }
            }
            return runContext.storage().putFile(outputPath.toFile(), name);
        } catch (IOException e) {
            throw new TransformException("Unable to store zipped records", e);
        }
    }

    private IonStruct mergeRecords(IonStruct leftRecord, IonStruct rightRecord) throws TransformException {
        IonStruct merged = IonValueUtils.system().newEmptyStruct();
        for (IonValue value : leftRecord) {
            merged.put(value.getFieldName(), IonValueUtils.cloneValue(value));
        }
        for (IonValue value : rightRecord) {
            String fieldName = value.getFieldName();
            if (merged.get(fieldName) != null) {
                if (options.onConflict == ConflictMode.FAIL) {
                    throw new TransformException("Field conflict on '" + fieldName + "'");
                }
                if (options.onConflict == ConflictMode.LEFT) {
                    continue;
                }
            }
            merged.put(fieldName, IonValueUtils.cloneValue(value));
        }
        return merged;
    }

    private RecordCursor openCursor(RunContext runContext,
                                    TransformTaskSupport.ResolvedInput input) throws TransformException {
        if (!input.fromStorage()) {
            List<IonStruct> records = TransformTaskSupport.normalizeRecords(input.value());
            return RecordCursor.ofList(records);
        }
        try {
            InputStream inputStream = runContext.storage().getFile(input.storageUri());
            Iterator<IonValue> iterator = IonValueUtils.system().iterate(inputStream);
            if (!iterator.hasNext()) {
                return RecordCursor.ofIterator(List.<IonValue>of().iterator(), inputStream);
            }
            IonValue first = iterator.next();
            if (first instanceof IonList list) {
                if (iterator.hasNext()) {
                    throw new TransformException("Expected Ion list or newline-delimited structs, got mixed values");
                }
                return RecordCursor.ofIterator(list.iterator(), inputStream);
            }
            return RecordCursor.ofIterator(new PrependIterator(first, iterator), inputStream);
        } catch (IOException e) {
            throw new TransformException("Unable to read Ion file from storage: " + input.storageUri(), e);
        }
    }

    private static final class PrependIterator implements Iterator<IonValue> {
        private IonValue first;
        private final Iterator<IonValue> delegate;

        private PrependIterator(IonValue first, Iterator<IonValue> delegate) {
            this.first = first;
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext() {
            return first != null || delegate.hasNext();
        }

        @Override
        public IonValue next() {
            if (first != null) {
                IonValue value = first;
                first = null;
                return value;
            }
            return delegate.next();
        }
    }

    private static final class RecordCursor implements AutoCloseable {
        private final Iterator<IonValue> iterator;
        private final AutoCloseable closeable;

        private RecordCursor(Iterator<IonValue> iterator, AutoCloseable closeable) {
            this.iterator = iterator;
            this.closeable = closeable;
        }

        static RecordCursor ofList(List<IonStruct> records) {
            return new RecordCursor(new Iterator<IonValue>() {
                private final Iterator<IonStruct> delegate = records.iterator();

                @Override
                public boolean hasNext() {
                    return delegate.hasNext();
                }

                @Override
                public IonValue next() {
                    return delegate.next();
                }
            }, null);
        }

        static RecordCursor ofIterator(Iterator<IonValue> iterator, AutoCloseable closeable) {
            return new RecordCursor(iterator, closeable);
        }

        boolean hasNext() {
            return iterator.hasNext();
        }

        IonStruct nextStruct() throws TransformException {
            IonValue value = iterator.next();
            if (value instanceof IonStruct struct) {
                return struct;
            }
            throw new TransformException("Expected struct record, got " + (value == null ? "null" : value.getType()));
        }

        @Override
        public void close() throws IOException {
            if (closeable != null) {
                try {
                    closeable.close();
                } catch (Exception e) {
                    if (e instanceof IOException ioException) {
                        throw ioException;
                    }
                }
            }
        }
    }

    @Builder
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Options {
        @Builder.Default
        @Schema(title = "On error behavior")
        private TransformOptions.OnErrorMode onError = TransformOptions.OnErrorMode.FAIL;

        @Builder.Default
        @Schema(title = "On conflict behavior")
        private ConflictMode onConflict = ConflictMode.FAIL;
    }

    public enum ConflictMode {
        FAIL,
        LEFT,
        RIGHT
    }

    public enum OutputMode {
        AUTO,
        RECORDS,
        STORE
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Stored Ion file URI",
            description = "URI to the stored Ion file when output mode is STORE or AUTO resolves to STORE."
        )
        private final String uri;

        @Schema(
            title = "Zipped records",
            description = "JSON-safe records when output mode is RECORDS or AUTO resolves to RECORDS."
        )
        private final List<Object> records;
    }

    private static final class StatsAccumulator {
        private long processed;
        private long failed;
        private long dropped;
    }
}
