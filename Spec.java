package dev.savepointmigrator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** The YAML spec. Unknown keys are an error. See README.md. */
public class Spec {

    public String savepoint;
    public String output;
    /** Reader/writer parallelism. Higher = smaller pieces of state restored at once. */
    public int parallelism = 32;
    /** Task slots = how many pieces run at the same time. */
    public int slots = 2;
    /** Managed (RocksDB) memory of the local cluster, e.g. "1g". Flink's default if unset. */
    public String managedMemory;
    /** Directory for RocksDB files; default java.io.tmpdir. Needs about the state size free. */
    public String tmpDir;
    public List<Rule> rules = new ArrayList<>();

    /**
     * Convert state {@code state} of operator {@code uid} into the new state {@code target}, or with
     * {@code drop: true} just remove it. The source is removed either way.
     */
    public static class Rule {
        public String uid;
        /** The operator's key type, as in its keyBy. Only simple types/classes, no generics (Tuple2 etc.). */
        public String key = "STRING";
        public String state;
        /** map-to-mapstate | list-to-liststate | fully qualified class name of a {@link Conversion}. */
        public String convert;
        /** "target-wins": the target may exist already; where a key has it, it is kept and the source dropped. */
        public String merge;
        /** Remove {@code state} without a target: it is neither read nor written. No convert/merge/target. */
        public boolean drop;
        public Target target = new Target();
    }

    public static class Target {
        public String name;
        /** MAP: key + value; LIST: element; VALUE: value. */
        public Map<String, String> types = new LinkedHashMap<>();
    }

    public static Spec read(File file) throws IOException {
        return new ObjectMapper(new YAMLFactory()).readValue(file, Spec.class);
    }
}
