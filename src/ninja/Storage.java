package ninja;

import com.google.common.collect.Lists;
import sirius.kernel.Sirius;
import sirius.kernel.di.std.ConfigValue;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Exceptions;
import sirius.kernel.health.Log;
import sirius.kernel.nls.NLS;

import java.io.File;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Andreas Haufler (aha@scireum.de)
 * @since 2013/08
 */
@Register(classes = Storage.class)
public class Storage {

    private File baseDir;
    protected static final Log LOG = Log.get("storage");

    @ConfigValue("storage.awsAccessKey")
    private String awsAccessKey;

    @ConfigValue("storage.awsSecretKey")
    private String awsSecretKey;

    protected File getBaseDir() {
        baseDir = getBaseDirUnchecked();

        if (!baseDir.exists()) {
            throw Exceptions.handle()
                            .to(LOG)
                            .withSystemErrorMessage("Basedir '%s' does not exist!", baseDir.getAbsolutePath())
                            .handle();
        }
        if (!baseDir.isDirectory()) {
            throw Exceptions.handle()
                            .to(LOG)
                            .withSystemErrorMessage("Basedir '%s' is not a directory!", baseDir.getAbsolutePath())
                            .handle();
        }

        return baseDir;
    }

    private File getBaseDirUnchecked() {
        if (baseDir == null) {
            baseDir = new File(Sirius.getConfig().getString("storage.baseDir"));
        }

        return baseDir;
    }

    public String getBasePath() {
        StringBuilder sb = new StringBuilder(getBaseDirUnchecked().getAbsolutePath());
        if (!getBaseDirUnchecked().exists()) {
            sb.append(" (non-existend!)");
        } else if (!getBaseDirUnchecked().isDirectory()) {
            sb.append(" (no directory!)");
        } else {
            sb.append(" (Free: " + NLS.formatSize(getBaseDir().getFreeSpace()) + ")");
        }

        return sb.toString();
    }

    public List<Bucket> getBuckets() {
        List<Bucket> result = Lists.newArrayList();
        for (File file : getBaseDir().listFiles()) {
            if (file.isDirectory()) {
                result.add(new Bucket(file));
            }
        }

        return result;
    }

    public Bucket getBucket(String bucket) {
        if (bucket.contains("..") || bucket.contains("/") || bucket.contains("\\")) {
            throw Exceptions.createHandled()
                            .withSystemErrorMessage(
                                    "Invalid bucket name: %s. A bucket name must not contain '..' '/' or '\\'",
                                    bucket)
                            .handle();
        }
        return new Bucket(new File(getBaseDir(), bucket));
    }

    public String getAwsAccessKey() {
        return awsAccessKey;
    }

    public String getAwsSecretKey() {
        return awsSecretKey;
    }
}
