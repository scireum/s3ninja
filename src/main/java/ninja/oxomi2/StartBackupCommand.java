package ninja.oxomi2;

import sirius.kernel.async.Async;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.web.health.console.Command;

import javax.annotation.Nonnull;

@Register
public class StartBackupCommand implements Command {

    @Part
    private BackupAgent backupAgent;

    @Override
    public void execute(Output output, String... strings) throws Exception {
        if (!backupAgent.isRunning()) {
            backupAgent.setRunning(true);
            output.line("Backup started...");

            Async.defaultExecutor().start(backupAgent).execute();
        } else {
            output.line("Backup is already or still running.");
        }
    }

    @Override
    public String getDescription() {
        return "start all backup agents";
    }

    @Nonnull
    @Override
    public String getName() {
        return "startBackup";
    }
}
