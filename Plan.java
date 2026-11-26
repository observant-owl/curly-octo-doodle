package dev.savepointmigrator;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** What {@link Planner} decided: which operators are rewritten and how. */
public record Plan(Spec spec, List<Op> ops) {

    /** Write "slots" for the timers of the user-timers service; a record's content is the timestamp. */
    public static final int EVENT_TIMERS = -1;
    public static final int PROCESSING_TIMERS = -2;
    /** Marks a key without output; the writer ignores it (see MigrationJob.Reader). */
    public static final int NOTHING = -3;

    /** Name of a timer slot in verify's metrics and in the report. */
    public static String timersName(int slot) {
        return slot == EVENT_TIMERS ? "timers(event)" : "timers(processing)";
    }

    /**
     * One rewritten operator. {@code reads}: every keyed state it has. {@code writes}: the states
     * that are not a source (passed through) plus the converted targets.
     */
    public record Op(
            String uid,
            String hash,
            int maxParallelism,
            TypeInformation<Object> keyType,
            List<Slot> reads,
            List<Slot> writes)
            implements Serializable {}

    /**
     * A keyed state. {@code serializer} is the one the backend keeps for it (the value serializer,
     * a ListSerializer for LIST, a MapSerializer for MAP); content travels as value / List / Map.
     * For writes, {@code read} is the index of the source in {@code reads}, {@code conversion}
     * is null for pass-through, and {@code existing} is the index of the target in {@code reads}
     * if it is merged into (merge: target-wins), else -1.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public record Slot(
            String name, StateDescriptor.Type kind, TypeSerializer<Object> serializer, int read, Conversion conversion,
            int existing)
            implements Serializable {

        public State register(RuntimeContext ctx) {
            TypeSerializer s = serializer.duplicate();
            return switch (kind) {
                case VALUE -> ctx.getState(new ValueStateDescriptor<>(name, s));
                case LIST -> ctx.getListState(new ListStateDescriptor<>(name, ((ListSerializer) s).getElementSerializer()));
                default -> {
                    MapSerializer m = (MapSerializer) s;
                    yield ctx.getMapState(new MapStateDescriptor<>(name, m.getKeySerializer(), m.getValueSerializer()));
                }
            };
        }

        /** The current key's content, or null if there is none. */
        public Object read(State state) throws Exception {
            Object v = switch (kind) {
                case VALUE -> ((ValueState<Object>) state).value();
                case LIST -> {
                    List<Object> l = new ArrayList<>();
                    Iterable<Object> it = ((ListState<Object>) state).get();
                    if (it != null) {
                        it.forEach(l::add);
                    }
                    yield l;
                }
                default -> {
                    Map<Object, Object> m = new HashMap<>();
                    for (Map.Entry<Object, Object> e : ((MapState<Object, Object>) state).entries()) {
                        m.put(e.getKey(), e.getValue());
                    }
                    yield m;
                }
            };
            return absent(v) ? null : v;
        }

        public void write(State state, Object content) throws Exception {
            switch (kind) {
                case VALUE -> ((ValueState<Object>) state).update(content);
                case LIST -> ((ListState<Object>) state).update((List<Object>) content);
                default -> ((MapState<Object, Object>) state).putAll((Map<Object, Object>) content);
            }
        }

        /** Null, or an empty List/Map for LIST/MAP (they can't tell empty from unset). A VALUE's empty collection is kept. */
        public boolean absent(Object v) {
            return v == null || kind != StateDescriptor.Type.VALUE
                    && (v instanceof Map<?, ?> m ? m.isEmpty() : ((Collection<?>) v).isEmpty());
        }
    }
}
