/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

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
 * Storage service which takes care of organizing buckets on disk.
 */
@Register(classes = Storage.class)
public class Storage {

    private File baseDir;
    protected static final Log LOG = Log.get("storage");

    @ConfigValue("storage.awsAccessKey")
    private String awsAccessKey;

    @ConfigValue("storage.awsSecretKey")
    private String awsSecretKey;

    @ConfigValue("storage.autocreateBuckets")
    private boolean autocreateBuckets;

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
            if (Sirius.isStartedAsTest()) {
                baseDir = new File(System.getProperty("java.io.tmpdir"), "s3ninja_test");
                baseDir.mkdirs();
            } else {
                baseDir = new File(Sirius.getConfig().getString("storage.baseDir"));
            }
        }

        return baseDir;
    }

    /**
     * Returns the base directory as string.
     *
     * @return a string containing the path of the base directory. Will contain additional infos, if the path is
     * not usable
     */
    public String getBasePath() {
        StringBuilder sb = new StringBuilder(getBaseDirUnchecked().getAbsolutePath());
        if (!getBaseDirUnchecked().exists()) {
            sb.append(" (non-existent!)");
        } else if (!getBaseDirUnchecked().isDirectory()) {
            sb.append(" (no directory!)");
        } else {
            sb.append(" (Free: " + NLS.formatSize(getBaseDir().getFreeSpace()) + ")");
        }

        return sb.toString();
    }

    /**
     * Enumerates all known buckets.
     *
     * @return a list of all known buckets
     */
    public List<Bucket> getBuckets() {
        List<Bucket> result = Lists.newArrayList();
        for (File file : getBaseDir().listFiles()) {
            if (file.isDirectory()) {
                result.add(new Bucket(file));
            }
        }

        return result;
    }

    /**
     * Returns a bucket with the given name
     *
     * @param bucket the name of the bucket to fetch. Must not contain .. or / or \
     * @return the bucket with the given id. Might not exist, but will never be <tt>null</tt>
     */
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

    /**
     * Returns the used AWS access key.
     *
     * @return the AWS access key
     */
    public String getAwsAccessKey() {
        return awsAccessKey;
    }

    /**
     * Returns the AWS secret key used to verify hashes.
     *
     * @return the AWS secret key
     */
    public String getAwsSecretKey() {
        return awsSecretKey;
    }

    /**
     * Determines if buckets should be automatically created.
     *
     * @return <tt>true</tt> if buckets can be auto-created upon the first request
     */
    public boolean isAutocreateBuckets() {
        return autocreateBuckets;
    }
}
