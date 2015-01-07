package ninja.oxomi2;

import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.web.health.console.Command;

import javax.annotation.Nonnull;

@Register
public class StatusBackupCommand implements Command {

    @Part
    private OX2BackupAgent backupAgent;

    @Override
    public void execute(Output output, String... strings) throws Exception {
        final long bytesTotal = backupAgent.getBytesTransferred();
        final int filesTotal = backupAgent.getFilesTransferred();

        if (backupAgent.isRunning()) {
            output.line("Backup Agent is running.");
        } else {
            output.line("Backup Agent is not running.");
        }
        output.line("Since the last startup, a total of " + filesTotal + " Files containing " + bytesTotal + " bytes have been backed up.");
    }

    @Override
    public String getDescription() {
        return "Displays a status of the running backup agents";
    }

    @Nonnull
    @Override
    public String getName() {
        return "backupStatus";
    }
}
