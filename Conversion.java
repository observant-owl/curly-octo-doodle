package dev.savepointmigrator;

import org.apache.flink.api.common.state.StateDescriptor;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Turns the value of a source {@code ValueState} (one key) into the content of the target state.
 * Return a {@link Map} for a MAP target, a {@link List} for a LIST target, the value for a VALUE
 * target. Null (or an empty Map/List for MAP/LIST) means "no state for this key". See EXTENDING.md.
 */
public interface Conversion extends Serializable {

    StateDescriptor.Type targetKind();

    /** @param value the source value of the current key, never null */
    Object convert(Object value) throws Exception;

    static Conversion byName(String name) throws ReflectiveOperationException {
        return switch (name) {
            case "map-to-mapstate" -> new Builtin(StateDescriptor.Type.MAP, Map.class);
            case "list-to-liststate" -> new Builtin(StateDescriptor.Type.LIST, List.class);
            // a custom conversion from the job jar (or any jar on the classpath)
            default -> (Conversion) Class.forName(name, true, Conversion.class.getClassLoader())
                    .getDeclaredConstructor().newInstance();
        };
    }

    /** {@code ValueState<Map<K,V>>} → {@code MapState<K,V>} and {@code ValueState<List<T>>} → {@code ListState<T>}, as they are. */
    record Builtin(StateDescriptor.Type targetKind, Class<?> shape) implements Conversion {
        @Override
        public Object convert(Object value) {
            if (!shape.isInstance(value)) {
                throw new IllegalArgumentException("expected a " + shape.getName() + ", found " + value.getClass().getName());
            }
            return value;
        }
    }
}
