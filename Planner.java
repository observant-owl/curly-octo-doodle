package dev.savepointmigrator;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.StateDescriptor.Type;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.formats.avro.typeutils.AvroTypeInfo;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.KeyGroupsSavepointStateHandle;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot.CommonOptionsKeys;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot.CommonSerializerKeys;
import org.apache.flink.state.api.OperatorIdentifier;
import org.apache.flink.state.api.runtime.SavepointLoader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Checks a spec against the savepoint and builds the {@link Plan}. Only refuses what would
 * otherwise be lost <em>silently</em>; mistakes that fail loudly later (wrong key type, wrong
 * target type, broken class names) are left to Flink. All problems are reported together.
 */
public final class Planner {

    /** The spec can't be applied; nothing was written. */
    public static final class Refused extends RuntimeException {
        Refused(List<String> problems) {
            super(problems.size() + " problem(s):\n  - " + String.join("\n  - ", problems));
        }
    }

    private static final Map<String, TypeInformation<?>> ALIASES = Map.of(
            "STRING", Types.STRING, "LONG", Types.LONG, "INT", Types.INT,
            "BOOLEAN", Types.BOOLEAN, "DOUBLE", Types.DOUBLE, "FLOAT", Types.FLOAT);

    /** The queues of the timer service "user-timers" (KeyedProcessFunction & co.). */
    private static final Set<String> USER_TIMERS = Set.of("_timer_state/event_user-timers", "_timer_state/processing_user-timers");

    private Planner() {}

    public static Plan plan(Spec spec) throws IOException {
        List<String> errors = new ArrayList<>();
        Path out = new Path(spec.output);
        if (out.getFileSystem().exists(out)) { // also catches output == input
            errors.add("output " + spec.output + " already exists; the tool never overwrites");
        }
        CheckpointMetadata savepoint = SavepointLoader.loadSavepointMetadata(spec.savepoint);
        Map<String, List<Spec.Rule>> rulesByUid = new LinkedHashMap<>();
        for (Spec.Rule r : spec.rules) {
            rulesByUid.computeIfAbsent(r.uid, u -> new ArrayList<>()).add(r);
        }
        List<Plan.Op> ops = new ArrayList<>();
        for (Map.Entry<String, List<Spec.Rule>> e : rulesByUid.entrySet()) {
            OperatorState os = MigrationJob.operator(savepoint, OperatorIdentifier.forUid(e.getKey()).getOperatorId().toHexString());
            if (os == null) {
                errors.add("operator '" + e.getKey() + "' is not in the savepoint (typo in the uid?)");
                continue;
            }
            Plan.Op op = planOperator(e.getKey(), os, e.getValue(), errors);
            if (op != null) {
                ops.add(op);
            }
        }
        if (!errors.isEmpty()) {
            throw new Refused(errors);
        }
        return new Plan(spec, ops);
    }

    private static Plan.Op planOperator(String uid, OperatorState os, List<Spec.Rule> rules, List<String> errors)
            throws IOException {
        String where = "operator '" + uid + "'";
        int errorsBefore = errors.size();
        Map<String, StateMetaInfoSnapshot> states = readHeaders(where, os, errors);
        if (states == null) {
            return null;
        }
        List<Plan.Slot> reads = readSlots(where, states, errors);
        Set<Integer> dropped = new HashSet<>();
        List<Plan.Slot> converted = convertedSlots(where, rules, states.keySet(), reads, dropped, errors);
        TypeInformation<Object> keyType = keyType(where, rules, errors);
        if (errors.size() > errorsBefore) {
            return null;
        }
        return new Plan.Op(uid, os.getOperatorID().toHexString(), os.getMaxParallelism(), keyType,
                reads, writeSlots(reads, converted, dropped));
    }

    /**
     * The metadata of the operator's keyed states (and timer queues) by name, from the savepoint headers.
     * Null if the operator can't be rebuilt from its keyed state alone; anything else would be dropped.
     */
    private static Map<String, StateMetaInfoSnapshot> readHeaders(String where, OperatorState os, List<String> errors)
            throws IOException {
        Map<String, StateMetaInfoSnapshot> states = new TreeMap<>();
        for (OperatorSubtaskState st : os.getStates()) {
            if (st.getManagedOperatorState().hasState() || st.getRawOperatorState().hasState()
                    || st.getRawKeyedState().hasState()) {
                errors.add(where + " has operator (non-keyed) or raw state; it can't be rewritten");
                return null;
            }
            for (KeyedStateHandle h : st.getManagedKeyedState()) {
                // a native or heap-format handle would be misread by the canonical (RocksDB full) restore
                if (!(h instanceof KeyGroupsSavepointStateHandle)) {
                    errors.add(where + ": not a canonical savepoint (" + h.getClass().getSimpleName()
                            + "); take it with 'flink stop --type canonical'");
                    return null;
                }
                // read only the header: state names, kinds, serializer snapshots
                KeyedBackendSerializationProxy<?> header = new KeyedBackendSerializationProxy<>(Planner.class.getClassLoader());
                try (FSDataInputStream in = ((KeyGroupsSavepointStateHandle) h).openInputStream()) {
                    header.read(new DataInputViewStreamWrapper(in));
                }
                header.getStateMetaInfoSnapshots().forEach(m -> states.putIfAbsent(m.getName(), m));
            }
        }
        return states;
    }

    /** One read slot per keyed state; refuses timer services other than user-timers and unsupported states. */
    @SuppressWarnings("unchecked")
    private static List<Plan.Slot> readSlots(String where, Map<String, StateMetaInfoSnapshot> states, List<String> errors) {
        List<Plan.Slot> reads = new ArrayList<>();
        for (StateMetaInfoSnapshot m : states.values()) {
            if (m.getBackendStateType() == StateMetaInfoSnapshot.BackendStateType.PRIORITY_QUEUE) {
                // timer queues: only the user-timers service is carried over (MigrationJob.Reader / Writer)
                if (!USER_TIMERS.contains(m.getName())) {
                    errors.add(where + ": has timer service '" + m.getName() + "'; its timers would be lost; not supported");
                }
                continue;
            }
            String kind = m.getOption(CommonOptionsKeys.KEYED_STATE_TYPE);
            if (kind == null || !Set.of("VALUE", "LIST", "MAP").contains(kind)) { // Set.of(..).contains(null) throws
                errors.add(where + ": state '" + m.getName() + "' is " + kind + " state; only VALUE/LIST/MAP are supported");
                continue;
            }
            if (!(m.getTypeSerializerSnapshot(CommonSerializerKeys.NAMESPACE_SERIALIZER).restoreSerializer()
                    instanceof VoidNamespaceSerializer)) {
                errors.add(where + ": state '" + m.getName() + "' is namespaced (window state); not supported");
                continue;
            }
            TypeSerializer<Object> serializer = (TypeSerializer<Object>)
                    m.getTypeSerializerSnapshot(CommonSerializerKeys.VALUE_SERIALIZER).restoreSerializer();
            reads.add(new Plan.Slot(m.getName(), Type.valueOf(kind), serializer, -1, null, -1));
        }
        return reads;
    }

    /**
     * One write slot per converting rule, reading its source state (and, with merge, the existing target).
     * The sources of {@code drop} rules go into {@code dropped} and get no slot.
     */
    private static List<Plan.Slot> convertedSlots(String where, List<Spec.Rule> rules, Set<String> existingStates,
            List<Plan.Slot> reads, Set<Integer> dropped, List<String> errors) {
        List<String> names = reads.stream().map(Plan.Slot::name).toList();
        Set<String> targets = new HashSet<>();
        List<Plan.Slot> converted = new ArrayList<>();
        for (Spec.Rule r : rules) {
            String rw = where + ", state '" + r.state + "'";
            int src = names.indexOf(r.state);
            if (r.drop) {
                if (r.convert != null || r.merge != null || r.target != null && (r.target.name != null || !r.target.types.isEmpty())) {
                    errors.add(rw + ": 'drop: true' takes no 'convert', 'merge' or 'target'");
                } else if (src < 0) {
                    errors.add(rw + ": no such state. States: " + names);
                } else {
                    dropped.add(src);
                }
                continue;
            }
            if (r.convert == null || r.target == null || r.target.name == null) {
                errors.add(rw + ": 'convert' and 'target.name' are required");
            } else if (r.merge != null && !r.merge.equals("target-wins")) {
                errors.add(rw + ": unknown merge '" + r.merge + "'; the only one is 'target-wins'");
            } else if (src < 0) {
                errors.add(rw + ": no such state. States: " + names);
            } else if (reads.get(src).kind() != Type.VALUE) {
                errors.add(rw + ": is a " + reads.get(src).kind() + " state; only ValueStates are converted");
            } else if (!targets.add(r.target.name) || existingStates.contains(r.target.name) && r.merge == null) {
                errors.add(rw + ": target '" + r.target.name + "' already exists; the target must be a new state"
                        + " (or use 'merge: target-wins')");
            } else {
                try {
                    Conversion c = Conversion.byName(r.convert);
                    TypeSerializer<Object> serializer = targetSerializer(c.targetKind(), r.target.types);
                    int existing = names.indexOf(r.target.name);
                    if (existing >= 0 && !mergeable(reads.get(existing), c.targetKind(), serializer)) {
                        errors.add(rw + ": can't merge into '" + r.target.name + "': it is a " + reads.get(existing).kind()
                                + " state with other types than " + c.targetKind() + " " + r.target.types);
                        continue;
                    }
                    converted.add(new Plan.Slot(r.target.name, c.targetKind(), serializer, src, c, existing));
                } catch (Exception e) {
                    errors.add(rw + ": " + e);
                }
            }
        }
        for (Plan.Slot c : converted) {
            if (dropped.contains(c.read()) || dropped.contains(c.existing())) {
                errors.add(where + ": state '" + reads.get(dropped.contains(c.read()) ? c.read() : c.existing()).name()
                        + "' is dropped, but another rule converts or merges it");
            }
        }
        return converted;
    }

    /**
     * The states kept as they are, then the converted ones. Sources and merged targets are written only via
     * {@code converted}; dropped states not at all.
     */
    private static List<Plan.Slot> writeSlots(List<Plan.Slot> reads, List<Plan.Slot> converted, Set<Integer> dropped) {
        Set<Integer> replaced = new HashSet<>(dropped);
        for (Plan.Slot c : converted) {
            replaced.add(c.read());
            if (c.existing() >= 0) {
                replaced.add(c.existing());
            }
        }
        List<Plan.Slot> writes = new ArrayList<>();
        for (int i = 0; i < reads.size(); i++) {
            if (!replaced.contains(i)) {
                Plan.Slot s = reads.get(i);
                writes.add(new Plan.Slot(s.name(), s.kind(), s.serializer(), i, null, -1));
            }
        }
        writes.addAll(converted);
        return writes;
    }

    /** The operator's key type; every rule of the operator must give the same one. */
    @SuppressWarnings("unchecked")
    private static TypeInformation<Object> keyType(String where, List<Spec.Rule> rules, List<String> errors) {
        if (rules.stream().map(r -> r.key).distinct().count() > 1) {
            errors.add(where + ": rules disagree on the key type; it is the operator's, so give the same 'key' in each");
        }
        String key = rules.get(0).key;
        if (key == null) {
            errors.add(where + ": 'key' is empty; give the operator's key type (default STRING)");
            return null;
        }
        try {
            return (TypeInformation<Object>) type(key);
        } catch (ClassNotFoundException e) {
            errors.add(where + ": key type: " + e);
            return null;
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes", "deprecation"})
    static TypeSerializer<Object> targetSerializer(Type kind, Map<String, String> types) throws ClassNotFoundException {
        List<String> names = kind == Type.MAP ? List.of("key", "value") : kind == Type.LIST ? List.of("element") : List.of("value");
        List<TypeSerializer> s = new ArrayList<>();
        for (String n : names) {
            if (types == null || types.get(n) == null) {
                throw new IllegalArgumentException("target.types." + n + " is missing (" + kind + " needs " + names + ")");
            }
            TypeInformation<?> t = type(types.get(n));
            if (usesKryo(t)) {
                throw new IllegalArgumentException("target type " + t + " would still use Kryo. "
                        + "Use a class Flink serializes natively (basic type, Avro, POJO)");
            }
            s.add(t.createSerializer(new ExecutionConfig().getSerializerConfig()));
        }
        return kind == Type.MAP ? new MapSerializer(s.get(0), s.get(1)) : kind == Type.LIST ? new ListSerializer(s.get(0)) : s.get(0);
    }

    /** The existing state has the target's kind, and its entries can be written with the target's serializer as they are. */
    private static boolean mergeable(Plan.Slot existing, Type kind, TypeSerializer<Object> target) {
        return existing.kind() == kind && target.snapshotConfiguration()
                .resolveSchemaCompatibility(existing.serializer().snapshotConfiguration()).isCompatibleAsIs();
    }

    /** Type string → TypeInformation, like the job's {@code TypeInformation.of(X.class)}. No generics. */
    static TypeInformation<?> type(String name) throws ClassNotFoundException {
        TypeInformation<?> alias = ALIASES.get(name.trim().toUpperCase(Locale.ROOT));
        return alias != null ? alias
                : TypeInformation.of(Class.forName(name.trim(), false, Planner.class.getClassLoader()));
    }

    static boolean usesKryo(TypeInformation<?> t) {
        if (t instanceof GenericTypeInfo) {
            return true;
        }
        if (t instanceof ObjectArrayTypeInfo<?, ?> a) {
            return usesKryo(a.getComponentInfo());
        }
        if (t instanceof ListTypeInfo<?> l) {
            return usesKryo(l.getElementTypeInfo());
        }
        if (t instanceof MapTypeInfo<?, ?> m) {
            return usesKryo(m.getKeyTypeInfo()) || usesKryo(m.getValueTypeInfo());
        }
        if (t instanceof CompositeType && !(t instanceof AvroTypeInfo)) { // POJO fields; Avro uses its own serializer
            CompositeType<?> c = (CompositeType<?>) t;
            for (int i = 0; i < c.getArity(); i++) {
                if (usesKryo(c.getTypeAt(i))) {
                    return true;
                }
            }
        }
        return false;
    }
}
