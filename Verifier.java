package dev.savepointmigrator;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.state.api.functions.KeyedStateReaderFunction;
import org.apache.flink.state.api.runtime.SavepointLoader;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Proves that what the migration job produced (after conversion) is exactly what is in the output.
 *
 * <p>Per rewritten state: number of keys, number of entries and an order-independent digest (sum
 * of MD5s). The migration reader computes the expected side while it runs; a second job reads the
 * output and computes the actual side. Map entries are hashed one by one (MapState has no order),
 * lists as a whole (order matters), values as their bytes. Also checks that the output has the
 * same operators as the input. Timers are entries of the pseudo-states "timers(event)" and
 * "timers(processing)"; both sides also count all timers in the queue, which catches keys that have
 * only timers (Flink never visits those). It does not re-check the conversion code itself (that's tested)
 * nor serializer compatibility with the job (restoring the job is the check for that).
 */
public final class Verifier {

    private static final String PREFIX = "digest:";
    private static final String QUEUE = "queue:";
    private static final String MERGE = "merge:";
    /** What the report says about a merged target (merge: target-wins), in the reader's counter order. */
    static final List<String> MERGE_COUNTS = List.of("kept", "kept with the source too", "converted");

    private Verifier() {}

    public record Report(List<String> failures, List<String> summary) {
        public boolean ok() {
            return failures.isEmpty();
        }

        public String text() {
            StringBuilder b = new StringBuilder(ok() ? "VERIFY OK\n" : "VERIFY FAILED - do not use this savepoint\n");
            failures.forEach(f -> b.append("  FAIL ").append(f).append('\n'));
            summary.forEach(s -> b.append("  ").append(s).append('\n'));
            return b.toString();
        }
    }

    public static Report verify(Plan plan, JobExecutionResult migration) throws Exception {
        List<String> failures = new ArrayList<>();
        Set<String> in = operatorIds(plan.spec().savepoint);
        Set<String> out = operatorIds(plan.spec().output);
        if (!in.equals(out)) {
            failures.add("operators differ: input " + in + ", output " + out);
        }
        JobExecutionResult output = readOutput(plan);
        Map<String, long[]> expected = sums(migration);
        Map<String, long[]> actual = sums(output);
        checkTimerQueues(migration, expected, "input", failures);
        checkTimerQueues(output, actual, "output", failures);
        Map<String, List<String>> summary = new TreeMap<>();
        Set<String> metrics = new TreeSet<>(expected.keySet());
        metrics.addAll(actual.keySet());
        for (String metric : metrics) {
            long[] e = expected.getOrDefault(metric, new long[4]);
            long[] a = actual.getOrDefault(metric, new long[4]);
            if (!Arrays.equals(e, a)) {
                failures.add(metric + ": expected keys=" + e[0] + " entries=" + e[1] + ", found keys=" + a[0]
                        + " entries=" + a[1] + (e[0] == a[0] && e[1] == a[1] ? " (same counts, different content)" : ""));
            }
            String[] uidAndState = metric.split(" ", 2);
            summary.computeIfAbsent(uidAndState[0], u -> new ArrayList<>())
                    .add(uidAndState[1] + "=" + (uidAndState[1].equals("keys") ? e[0] : e[1]));
        }
        migration.getAllAccumulatorResults().forEach((name, value) -> {
            if (name.startsWith(MERGE)) {
                String[] uidAndText = name.substring(MERGE.length()).split(" ", 2);
                summary.computeIfAbsent(uidAndText[0], u -> new ArrayList<>()).add(uidAndText[1] + "=" + value);
            }
        });
        return new Report(failures, summary.entrySet().stream()
                .map(s -> s.getKey() + ": " + String.join(", ", s.getValue()))
                .collect(Collectors.toList()));
    }

    /** Accumulator for a merged target's keys, e.g. "merge:op countsV2 kept"; {@code i} indexes {@link #MERGE_COUNTS}. */
    static String mergeCounter(String uid, String state, int i) {
        return MERGE + uid + " " + state + " " + MERGE_COUNTS.get(i);
    }

    /** Accumulator for all timers in an operator's queue, including those of keys without state. */
    static String queueCounter(String uid, int slot) {
        return QUEUE + uid + " " + Plan.timersName(slot);
    }

    /** Timers on keys without state are never read; then the queue has more timers than were read. */
    private static void checkTimerQueues(JobExecutionResult job, Map<String, long[]> read, String side, List<String> failures) {
        job.getAllAccumulatorResults().forEach((name, value) -> {
            if (name.startsWith(QUEUE)) {
                String metric = name.substring(QUEUE.length());
                long onKeys = read.getOrDefault(metric, new long[4])[1];
                if ((Long) value != onKeys) {
                    failures.add(metric + ": the " + side + " has " + value + " timers, but only " + onKeys
                            + " are on keys with state. Keys that only have timers are not supported");
                }
            }
        });
    }

    private static Set<String> operatorIds(String savepoint) throws IOException {
        return SavepointLoader.loadSavepointMetadata(savepoint).getOperatorStates().stream()
                .filter(OperatorState::hasSubtaskStates)
                .map(os -> os.getOperatorID().toHexString())
                .collect(Collectors.toCollection(TreeSet::new));
    }

    private static JobExecutionResult readOutput(Plan plan) throws Exception {
        StreamExecutionEnvironment env = MigrationJob.environment(plan.spec());
        CheckpointMetadata output = SavepointLoader.loadSavepointMetadata(plan.spec().output);
        for (Plan.Op op : plan.ops()) {
            MigrationJob.read(env, plan.spec(), output, op, new OutputReader(op), Types.INT).sinkTo(new DiscardingSink<>());
        }
        return env.execute("savepoint-migrator verify " + plan.spec().output);
    }

    /** "uid state" → {keys, entries, h1, h2} from the accumulators of a job. */
    private static Map<String, long[]> sums(JobExecutionResult result) {
        Map<String, long[]> out = new HashMap<>();
        result.getAllAccumulatorResults().forEach((name, value) -> {
            if (name.startsWith(PREFIX)) {
                int sep = name.lastIndexOf('#');
                long[] s = out.computeIfAbsent(name.substring(PREFIX.length(), sep), m -> new long[4]);
                s[Integer.parseInt(name.substring(sep + 1))] = (Long) value;
            }
        });
        return out;
    }

    /** The "actual" side: reads the output's written states. */
    static final class OutputReader extends KeyedStateReaderFunction<Object, Integer> {
        private final Plan.Op op;
        private transient State[] states;
        private transient Digest digest;

        OutputReader(Plan.Op op) {
            this.op = op;
        }

        @Override
        public void open(Configuration parameters) {
            states = op.writes().stream().map(s -> s.register(getRuntimeContext())).toArray(State[]::new);
            digest = new Digest(getRuntimeContext(), op);
        }

        @Override
        public void readKey(Object key, Context ctx, Collector<Integer> out) throws Exception {
            digest.startKey(key);
            for (int w = 0; w < states.length; w++) {
                Object content = op.writes().get(w).read(states[w]);
                if (content != null) {
                    digest.add(op.writes().get(w), content);
                }
            }
            digest.addTimers(Plan.EVENT_TIMERS, ctx.registeredEventTimeTimers());
            digest.addTimers(Plan.PROCESSING_TIMERS, ctx.registeredProcessingTimeTimers());
            digest.endKey();
            out.collect(1); // a key that emits nothing ends the split early (Flink's KeyedStateInputFormat)
        }
    }

    /** Sums per "uid state" into Flink LongCounters (summing wraps around, which is fine for a digest). */
    static final class Digest {
        private final RuntimeContext ctx;
        private final String uid;
        private final TypeSerializer<Object> keySerializer;
        private final Map<String, LongCounter[]> counters = new HashMap<>();
        private final DataOutputSerializer buf = new DataOutputSerializer(4096);
        private final MessageDigest md5;
        private byte[] key;
        private boolean keyHasState;

        Digest(RuntimeContext ctx, Plan.Op op) {
            this.ctx = ctx;
            this.uid = op.uid();
            this.keySerializer = ctx.createSerializer(op.keyType());
            try {
                this.md5 = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException(e);
            }
        }

        void startKey(Object k) throws IOException {
            buf.clear();
            keySerializer.serialize(k, buf);
            key = buf.getCopyOfBuffer();
            keyHasState = false;
        }

        void endKey() {
            if (keyHasState) {
                add("keys", 1, 0, 0);
            }
        }

        @SuppressWarnings("unchecked")
        void add(Plan.Slot slot, Object content) throws IOException {
            keyHasState = true;
            if (slot.kind() == StateDescriptor.Type.MAP) {
                MapSerializer<Object, Object> m = (MapSerializer<Object, Object>) (TypeSerializer<?>) slot.serializer();
                for (Map.Entry<Object, Object> e : ((Map<Object, Object>) content).entrySet()) {
                    buf.clear();
                    m.getKeySerializer().serialize(e.getKey(), buf);
                    buf.writeBoolean(e.getValue() == null);
                    if (e.getValue() != null) {
                        m.getValueSerializer().serialize(e.getValue(), buf);
                    }
                    add(slot.name(), 0, 1, buf.length());
                }
                add(slot.name(), 1, 0, 0);
            } else {
                buf.clear();
                slot.serializer().serialize(content, buf);
                add(slot.name(), 1, content instanceof List ? ((List<?>) content).size() : 1, buf.length());
            }
        }

        /** Each timer is an entry (hashed one by one: their order is not kept). */
        void addTimers(int slot, Set<Long> times) throws IOException {
            for (long t : times) {
                buf.clear();
                buf.writeLong(t);
                add(Plan.timersName(slot), 0, 1, buf.length());
            }
            if (!times.isEmpty()) {
                add(Plan.timersName(slot), 1, 0, 0);
            }
        }

        /** Adds MD5(key, first {@code len} bytes of buf). */
        private void add(String state, long keys, long entries, int len) {
            md5.update(key);
            md5.update(buf.getSharedBuffer(), 0, len);
            ByteBuffer h = ByteBuffer.wrap(md5.digest());
            LongCounter[] c = counters.computeIfAbsent(state, s -> {
                LongCounter[] n = new LongCounter[4];
                for (int i = 0; i < 4; i++) {
                    n[i] = ctx.getLongCounter(PREFIX + uid + " " + s + "#" + i);
                }
                return n;
            });
            c[0].add(keys);
            c[1].add(entries);
            c[2].add(h.getLong());
            c[3].add(h.getLong());
        }
    }
}
