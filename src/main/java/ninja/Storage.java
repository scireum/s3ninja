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
import sirius.kernel.commons.Strings;
import sirius.kernel.di.std.ConfigValue;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Exceptions;
import sirius.kernel.health.Log;
import sirius.kernel.nls.NLS;

import javax.annotation.Nonnull;
import java.io.File;
import java.util.List;
import java.util.Objects;

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

    private File getBaseDir() {
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
                baseDir = new File(Sirius.getSettings().getString("storage.baseDir"));
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
            sb.append(" (Free: ").append(NLS.formatSize(getBaseDir().getFreeSpace())).append(")");
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
        for (File file : Objects.requireNonNull(getBaseDir().listFiles())) {
            if (file.isDirectory() && Bucket.isValidName(file.getName())) {
                result.add(new Bucket(file));
            }
        }

        result.sort((bucket1, bucket2) -> bucket1.getName().compareToIgnoreCase(bucket2.getName()));

        return result;
    }

    /**
     * Returns a bucket with the given name.
     * <p>
     * The method never returns <b>null</b>, but {@link Bucket#exists()} may return <b>false</b>.
     * <p>
     * Make sure that the name passes {@link Bucket#isValidName(String)} by meeting the naming restrictions documented
     * there.
     *
     * @param name the name of the bucket to fetch
     * @return the bucket with the given name
     */
    @Nonnull
    public Bucket getBucket(String name) {
        if (!Bucket.isValidName(name)) {
            throw Exceptions.createHandled()
                            .withSystemErrorMessage(
                                    "Bucket name \"%s\" does not adhere to the rules. [https://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html]",
                                    name)
                            .handle();
        }

        // following the current rules, "ui" is no valid bucket name after all; however, should Amazon change the rules,
        // this check may apply again
        if (Strings.areEqual(name, "ui")) {
            throw Exceptions.createHandled()
                            .withSystemErrorMessage("Bucket name \"%s\" is reserved for internal use.", name)
                            .handle();
        }

        return new Bucket(new File(getBaseDir(), name));
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
     * @return <b>true</b> if buckets can be auto-created upon the first request, <b>false</b> else
     */
    public boolean isAutocreateBuckets() {
        return autocreateBuckets;
    }
}
