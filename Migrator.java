package dev.savepointmigrator;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import java.io.File;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;

/**
 * {@code java -cp savepoint-migrator-local.jar:your-job.jar dev.savepointmigrator.Migrator --spec spec.yaml [--dry-run]}
 *
 * <p>plan → migrate → verify. The report goes next to the output as {@code _VERIFY_OK.txt} or
 * {@code _VERIFY_FAILED.txt}. If the job fails, Flink never writes the output's {@code _metadata},
 * so nothing restorable is left behind; delete the directory and run again. Use an output only
 * if it has {@code _VERIFY_OK.txt}.
 *
 * <p>Exit codes: 0 migrated and verified, 1 verify failed, 2 refused (nothing written), 3 error.
 */
public final class Migrator {

    public static final String REPORT_OK = "_VERIFY_OK.txt";
    public static final String REPORT_FAILED = "_VERIFY_FAILED.txt";

    private Migrator() {}

    public static Verifier.Report migrate(Spec spec) throws Exception {
        return migrate(spec, MigrationJob.Writer::new);
    }

    /** @param writers tests pass faulty writers to prove verify catches them */
    static Verifier.Report migrate(Spec spec, Function<Plan.Op, MigrationJob.Writer> writers) throws Exception {
        Plan plan = Planner.plan(spec);
        Verifier.Report report = Verifier.verify(plan, MigrationJob.run(plan, writers));
        Path file = new Path(spec.output, report.ok() ? REPORT_OK : REPORT_FAILED);
        try (OutputStream o = file.getFileSystem().create(file, FileSystem.WriteMode.NO_OVERWRITE)) {
            o.write(report.text().getBytes(StandardCharsets.UTF_8));
        }
        return report;
    }

    public static void main(String[] args) {
        boolean dryRun = args.length == 3 && args[2].equals("--dry-run");
        if (args.length != 2 && !dryRun || !args[0].equals("--spec")) {
            System.err.println("usage: Migrator --spec <spec.yaml> [--dry-run]");
            System.exit(3);
        }
        try {
            Spec spec = Spec.read(new File(args[1]));
            if (dryRun) {
                for (Plan.Op op : Planner.plan(spec).ops()) {
                    System.out.println("operator '" + op.uid() + "' (key " + op.keyType() + ") is rewritten with:");
                    op.writes().forEach(w -> System.out.println("  " + w.name() + " [" + w.kind() + "] "
                            + (w.conversion() == null ? "unchanged" : "from '" + op.reads().get(w.read()).name()
                                    + "' via " + w.conversion().getClass().getSimpleName())));
                }
                System.out.println("Plan OK. Nothing written.");
                System.exit(0);
            }
            Verifier.Report report = migrate(spec);
            System.out.print(report.text());
            System.exit(report.ok() ? 0 : 1);
        } catch (Planner.Refused e) {
            System.err.println("REFUSED: " + e.getMessage() + "\nNothing was written.");
            System.exit(2);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(3);
        }
    }
}
