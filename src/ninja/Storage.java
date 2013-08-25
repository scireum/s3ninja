package ninja;

import com.google.common.collect.Lists;
import sirius.kernel.Sirius;
import sirius.kernel.cache.Cache;
import sirius.kernel.cache.CacheManager;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Exceptions;
import sirius.kernel.health.Log;

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

    protected File getBaseDir() {
        if (baseDir == null) {
            baseDir = new File(Sirius.getConfig().getString("storage.baseDir"));
        }

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
}
