/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package ninja;

import sirius.kernel.health.Exceptions;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Objects;

/**
 * Provides static helpers for migrating bucket data.
 */
public class BucketMigrator {

    protected static final int MOST_RECENT_VERSION = 2;

    private BucketMigrator() {
    }

    /**
     * Migrates a bucket folder to the most recent version.
     *
     * @param bucket the bucket to migrate
     */
    protected static void migrateBucket(Bucket bucket) {
        if (bucket.getVersion() <= 1) {
            migrateBucketVersion1To2(bucket);
        }

        // further incremental updates go here one day

        // write the most recent version marker
        bucket.setVersion(MOST_RECENT_VERSION);
        bucket.writeVersion();
    }

    /**
     * Migrates a bucket folder from version 1 to 2.
     *
     * @param bucket the bucket to migrate
     */
    private static void migrateBucketVersion1To2(Bucket bucket) {
        migratePublicMarkerVersion1To2(bucket);

        for (File object : Objects.requireNonNull(bucket.getFolder().listFiles(bucket::filterObjects))) {
            migrateObjectVersion1To2(bucket, object);
        }
    }

    /**
     * Migrates the legacy public marker file <tt>__ninja_public</tt> to <tt>$public</tt>.
     *
     * @param bucket the bucket to migrate
     */
    private static void migratePublicMarkerVersion1To2(Bucket bucket) {
        try {
            File legacyPublicMarker = new File(bucket.getFolder(), "__ninja_public");
            if (legacyPublicMarker.exists() && !bucket.getPublicMarker().exists()) {
                Files.move(legacyPublicMarker.toPath(), bucket.getPublicMarker().toPath());
            } else if (legacyPublicMarker.exists()) {
                Files.delete(legacyPublicMarker.toPath());
            }
        } catch (IOException e) {
            throw Exceptions.handle(Storage.LOG, e);
        }
    }

    /**
     * Migrates a legacy object along with its properties.
     * <p>
     * The legacy file name is considered as-is and URL-encoded for general UTF-8 support. The properties file is
     * prefixed with <tt>$</tt>, avoiding name clashes with other object files (where <tt>$</tt> would be encoded).
     *
     * @param bucket       the bucket to migrate
     * @param legacyObject the legacy object to migrate
     */
    private static void migrateObjectVersion1To2(Bucket bucket, File legacyObject) {
        File legacyProperties = new File(bucket.getFolder(), "__ninja_" + legacyObject.getName() + ".properties");

        try {
            File object = new File(bucket.getFolder(), StoredObject.encodeKey(legacyObject.getName()));
            File properties = new File(bucket.getFolder(), "$" + object.getName() + ".properties");

            if (!object.exists()) {
                Files.move(legacyObject.toPath(), object.toPath());
            } else if (!object.equals(legacyObject)) {
                Files.delete(legacyObject.toPath());
            }

            if (legacyProperties.exists()) {
                if (!properties.exists()) {
                    Files.move(legacyProperties.toPath(), properties.toPath());
                } else if (!properties.equals(legacyProperties)) {
                    Files.delete(legacyProperties.toPath());
                }
            }
        } catch (Exception e) {
            throw Exceptions.handle(Storage.LOG, e);
        }
    }
}
