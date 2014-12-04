package ninja.oxomi2;

import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.web.health.console.Command;

import javax.annotation.Nonnull;

@Register
public class StopBackupCommand implements Command {

    @Part
    private BackupAgent backupAgent;

    @Override
    public void execute(Output output, String... strings) throws Exception {
        backupAgent.setRunning(false);

        output.line("Backup stopped...");
    }

    @Override
    public String getDescription() {
        return "stop all backup agents";
    }

    @Nonnull
    @Override
    public String getName() {
        return "stopBackup";
    }
}
