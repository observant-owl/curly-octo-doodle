package dev.savepointmigrator;

import org.apache.flink.api.common.BatchShuffleMode;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.state.api.OperatorIdentifier;
import org.apache.flink.state.api.OperatorTransformation;
import org.apache.flink.state.api.SavepointWriter;
import org.apache.flink.state.api.functions.KeyedStateBootstrapFunction;
import org.apache.flink.state.api.functions.KeyedStateReaderFunction;
import org.apache.flink.state.api.input.KeyedStateInputFormat;
import org.apache.flink.state.api.input.SourceBuilder;
import org.apache.flink.state.api.input.operator.KeyedStateReaderOperator;
import org.apache.flink.state.api.runtime.SavepointLoader;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * The migration job: for each rewritten operator, read every key's states, convert, and send them
 * to a writer that builds the operator anew. Untouched operators are copied by {@link SavepointWriter}.
 *
 * <p>Records are {@code (key, index into Op.writes, content serialized with that slot's serializer)},
 * {@code (key, Plan.EVENT_TIMERS / PROCESSING_TIMERS, 8-byte timestamp)} for a timer, or
 * {@code (key, Plan.NOTHING, empty)} for a key with no output, so no custom serializer is needed
 * for the shuffle.
 *
 * <p>Memory: BATCH with blocking exchanges (reading and writing never overlap), parallelism ≫
 * slots (only slots/parallelism of the state is restored at once), RocksDB (restored state lives
 * on disk).
 */
public final class MigrationJob {

    private MigrationJob() {}

    public static JobExecutionResult run(Plan plan, Function<Plan.Op, Writer> writers) throws Exception {
        Spec spec = plan.spec();
        StreamExecutionEnvironment env = environment(spec);
        CheckpointMetadata input = SavepointLoader.loadSavepointMetadata(spec.savepoint);
        SavepointWriter writer = SavepointWriter.fromExistingSavepoint(env, spec.savepoint, backend(spec));
        for (Plan.Op op : plan.ops()) {
            OperatorIdentifier id = OperatorIdentifier.forUidHash(op.hash());
            TypeInformation<Tuple3<Object, Integer, byte[]>> recordType =
                    new TupleTypeInfo<>(op.keyType(), Types.INT, PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO);
            DataStream<Tuple3<Object, Integer, byte[]>> records = read(env, spec, input, op, new Reader(op), recordType);
            KeySelector<Tuple3<Object, Integer, byte[]>, Object> byKey = r -> r.f0;
            writer.removeOperator(id);
            writer.withOperator(id, OperatorTransformation.bootstrapWith(records)
                    .setMaxParallelism(op.maxParallelism())
                    .keyBy(byKey, op.keyType())
                    .transform(writers.apply(op)));
        }
        writer.write(spec.output);
        return env.execute("savepoint-migrator " + spec.savepoint + " -> " + spec.output);
    }

    /** Reads one operator's keys with {@code function}, counting all its timers on the side. */
    static <T> DataStream<T> read(StreamExecutionEnvironment env, Spec spec, CheckpointMetadata savepoint, Plan.Op op,
            KeyedStateReaderFunction<Object, T> function, TypeInformation<T> type) throws IOException {
        return SourceBuilder.fromFormat(env, new KeyedStateInputFormat<>(
                operator(savepoint, op.hash()), backend(spec), configuration(spec), new TimerCountingReader<>(function, op), env.getConfig()), type);
    }

    /** The operator with this uid hash, or null. */
    static OperatorState operator(CheckpointMetadata savepoint, String hash) {
        return savepoint.getOperatorStates().stream()
                .filter(os -> os.getOperatorID().toHexString().equals(hash)).findFirst().orElse(null);
    }

    static StreamExecutionEnvironment environment(Spec spec) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration(spec));
        env.setParallelism(spec.parallelism);
        return env;
    }

    static Configuration configuration(Spec spec) {
        Configuration conf = new Configuration();
        conf.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        conf.set(ExecutionOptions.BATCH_SHUFFLE_MODE, BatchShuffleMode.ALL_EXCHANGES_BLOCKING);
        // The default (adaptive) batch scheduler picks the writers' parallelism from the data size
        // (1 for small inputs), so the "parallelism >> slots" memory bound would not hold.
        conf.set(JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.Default);
        conf.set(RestartStrategyOptions.RESTART_STRATEGY, "none"); // fail fast; a failed run writes no _metadata
        conf.set(TaskManagerOptions.NUM_TASK_SLOTS, spec.slots);
        if (spec.managedMemory != null) {
            conf.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse(spec.managedMemory));
        }
        if (spec.tmpDir != null) {
            conf.set(CoreOptions.TMP_DIRS, spec.tmpDir);
        }
        return conf;
    }

    static EmbeddedRocksDBStateBackend backend(Spec spec) {
        EmbeddedRocksDBStateBackend rocks = new EmbeddedRocksDBStateBackend(false);
        if (spec.tmpDir != null) {
            rocks.setDbStoragePath(spec.tmpDir);
        }
        return rocks;
    }

    /**
     * Flink's keyed reader, plus a count of all timers in the queue. Flink only visits keys that
     * have state, so timers of keys without state are never read; verify compares this count with
     * the timers that were read.
     */
    static final class TimerCountingReader<T> extends KeyedStateReaderOperator<Object, T> {
        private final String uid;

        TimerCountingReader(KeyedStateReaderFunction<Object, T> function, Plan.Op op) {
            super(function, op.keyType());
            this.uid = op.uid();
        }

        @Override
        public void open() throws Exception {
            super.open();
            InternalTimerService<VoidNamespace> timers = getInternalTimerService("user-timers");
            LongCounter event = function.getRuntimeContext().getLongCounter(Verifier.queueCounter(uid, Plan.EVENT_TIMERS));
            LongCounter processing = function.getRuntimeContext().getLongCounter(Verifier.queueCounter(uid, Plan.PROCESSING_TIMERS));
            timers.forEachEventTimeTimer((namespace, time) -> event.add(1));
            timers.forEachProcessingTimeTimer((namespace, time) -> processing.add(1));
        }
    }

    /** Reads all states of a key, converts, emits one record per non-empty output state. */
    static final class Reader extends KeyedStateReaderFunction<Object, Tuple3<Object, Integer, byte[]>> {
        private final Plan.Op op;
        private transient State[] states;
        private transient boolean[] used; // false: a dropped state, never read (saves deserializing it)
        private transient Verifier.Digest digest;
        private transient DataOutputSerializer buffer;
        private transient LongCounter[][] merges; // per write slot: kept, of those with the source too, converted

        Reader(Plan.Op op) {
            this.op = op;
        }

        @Override
        public void open(Configuration parameters) {
            // dropped states are still registered: Flink's reader visits only keys of registered states,
            // and a key with a dropped state may also have timers
            states = op.reads().stream().map(s -> s.register(getRuntimeContext())).toArray(State[]::new);
            used = new boolean[states.length];
            for (Plan.Slot w : op.writes()) {
                used[w.read()] = true;
                if (w.existing() >= 0) {
                    used[w.existing()] = true;
                }
            }
            digest = new Verifier.Digest(getRuntimeContext(), op); // the "expected" side of verify
            buffer = new DataOutputSerializer(4096);
            merges = new LongCounter[op.writes().size()][];
            for (int w = 0; w < merges.length; w++) {
                if (op.writes().get(w).existing() >= 0) {
                    merges[w] = new LongCounter[Verifier.MERGE_COUNTS.size()];
                    for (int i = 0; i < merges[w].length; i++) {
                        merges[w][i] = getRuntimeContext().getLongCounter(
                                Verifier.mergeCounter(op.uid(), op.writes().get(w).name(), i));
                    }
                }
            }
        }

        @Override
        public void readKey(Object key, Context ctx, Collector<Tuple3<Object, Integer, byte[]>> out) throws Exception {
            List<Plan.Slot> reads = op.reads();
            Object[] values = new Object[reads.size()];
            for (int i = 0; i < values.length; i++) {
                values[i] = used[i] ? reads.get(i).read(states[i]) : null;
            }
            digest.startKey(key);
            boolean emitted = false;
            for (int w = 0; w < op.writes().size(); w++) {
                Plan.Slot slot = op.writes().get(w);
                Object content = values[slot.read()];
                Object existing = slot.existing() < 0 ? null : values[slot.existing()];
                if (existing != null) { // merge: target-wins
                    merges[w][0].add(1);
                    merges[w][1].add(content == null ? 0 : 1);
                    content = existing;
                } else if (content != null && slot.conversion() != null) {
                    if (merges[w] != null) {
                        merges[w][2].add(1);
                    }
                    try {
                        content = slot.conversion().convert(content);
                    } catch (Exception e) {
                        throw new IllegalStateException("operator '" + op.uid() + "', key " + key
                                + ", state '" + reads.get(slot.read()).name() + "': " + e, e);
                    }
                }
                if (slot.absent(content)) {
                    continue;
                }
                digest.add(slot, content);
                buffer.clear();
                slot.serializer().serialize(content, buffer);
                out.collect(Tuple3.of(key, w, buffer.getCopyOfBuffer()));
                emitted = true;
            }
            Set<Long> event = ctx.registeredEventTimeTimers(), processing = ctx.registeredProcessingTimeTimers();
            timers(key, Plan.EVENT_TIMERS, event, out);
            timers(key, Plan.PROCESSING_TIMERS, processing, out);
            if (!emitted && event.isEmpty() && processing.isEmpty()) {
                // Flink's reader takes a key without output as the end of the split: the keys after it would be lost
                out.collect(Tuple3.of(key, Plan.NOTHING, new byte[0]));
            }
            digest.endKey();
        }

        private void timers(Object key, int slot, Set<Long> times, Collector<Tuple3<Object, Integer, byte[]>> out)
                throws IOException {
            digest.addTimers(slot, times);
            for (long t : times) {
                buffer.clear();
                buffer.writeLong(t);
                out.collect(Tuple3.of(key, slot, buffer.getCopyOfBuffer()));
            }
        }
    }

    /** Writes the records of one operator into its new keyed state and timers. Tests override {@link #write} and {@link #timer}. */
    public static class Writer extends KeyedStateBootstrapFunction<Object, Tuple3<Object, Integer, byte[]>> {
        protected final Plan.Op op;
        private transient State[] states;
        private transient DataInputDeserializer in;

        public Writer(Plan.Op op) {
            this.op = op;
        }

        @Override
        public void open(OpenContext openContext) {
            states = op.writes().stream().map(s -> s.register(getRuntimeContext())).toArray(State[]::new);
            in = new DataInputDeserializer();
        }

        @Override
        public void processElement(Tuple3<Object, Integer, byte[]> r, Context ctx) throws Exception {
            if (r.f1 == Plan.NOTHING) {
                return;
            }
            in.setBuffer(r.f2);
            if (r.f1 < 0) {
                timer(r.f0, r.f1, in.readLong(), ctx.timerService());
            } else {
                write(r.f0, r.f1, op.writes().get(r.f1).serializer().deserialize(in));
            }
        }

        protected void timer(Object key, int slot, long time, TimerService timers) {
            if (slot == Plan.EVENT_TIMERS) {
                timers.registerEventTimeTimer(time);
            } else {
                timers.registerProcessingTimeTimer(time);
            }
        }

        protected void write(Object key, int slot, Object content) throws Exception {
            op.writes().get(slot).write(states[slot], content);
        }
    }
}
