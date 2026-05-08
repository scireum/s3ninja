/*
 * Made with all the love in the world
 * by scireum in Stuttgart, Germany
 *
 * Copyright by scireum GmbH
 * https://www.scireum.de - info@scireum.de
 */

package ninja;

import sirius.kernel.commons.Hasher;
import sirius.kernel.commons.Strings;
import sirius.kernel.health.Counter;
import sirius.kernel.xml.XMLStructuredOutput;

import javax.annotation.Nullable;
import java.io.File;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * Visits all files in the buckets directory and outputs their metadata to an {@link XMLStructuredOutput}.
 */
class ListFileTreeVisitor extends SimpleFileVisitor<Path> {

    private static final String CONTINUATION_TOKEN_SALT = "panta-rhei";

    private final Counter objectCount;
    private final XMLStructuredOutput output;
    private final int limit;
    private final String marker;
    private final String prefix;
    private final String continuationToken;
    private String nextContinuationToken;
    private final boolean useLimit;
    private final boolean usePrefix;
    private final boolean useContinuationToken;
    private boolean markerReached;
    private boolean continuationTokenReached;

    // Supressed warning "Null pointers should not be dereferenced"
    // as prefix can't be null when replacing as the usePrefix acts as a guard.
    @SuppressWarnings("squid:S2259")
    protected ListFileTreeVisitor(XMLStructuredOutput output,
                                  int limit,
                                  @Nullable String marker,
                                  @Nullable String prefix,
                                  @Nullable String continuationToken) {
        this.output = output;
        this.limit = limit;
        this.marker = marker;
        this.prefix = prefix;
        this.continuationToken = continuationToken;

        if (Strings.isFilled(continuationToken) && Strings.isFilled(marker)) {
            throw new IllegalArgumentException("Continuation token and marker are mutually exclusive.");
        }

        objectCount = new Counter();

        useLimit = limit > 0;
        usePrefix = Strings.isFilled(prefix);
        useContinuationToken = Strings.isFilled(continuationToken);

        markerReached = Strings.isEmpty(marker);
        continuationTokenReached = !useContinuationToken;
    }

    @Override
    public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) {
        File file = path.toFile();
        String name = StoredObject.decodeKey(file.getName());

        if (!file.isFile() || file.getName().startsWith("$")) {
            return FileVisitResult.CONTINUE;
        }

        if (!markerReached) {
            if (marker.equals(name)) {
                markerReached = true;
            }
            return FileVisitResult.CONTINUE;
        }

        if (useContinuationToken
            && !continuationTokenReached
            && continuationToken.equals(computeContinuationToken(name))) {
            continuationTokenReached = true;
        }

        StoredObject object = new StoredObject(file);
        if (useLimit && (!usePrefix || name.startsWith(prefix)) && continuationTokenReached) {
            long numObjects = objectCount.inc();
            if (numObjects > limit) {
                touchNextContinuationToken(name);
                return FileVisitResult.TERMINATE;
            }

            output.beginObject("Contents");
            output.property("Key", object.getKey());
            output.property("LastModified", S3Dispatcher.ISO8601_INSTANT.format(object.getLastModifiedInstant()));
            output.property("Size", object.getSizeBytes());
            output.property("StorageClass", "STANDARD");
            output.property("ETag", getETag(file));
            output.endObject();
        }

        return FileVisitResult.CONTINUE;
    }

    private String getETag(File file) {
        return Hasher.md5().hashFile(file).toHexString();
    }

    /**
     * Sets the next continuation token based on the given name if it is not already set.
     *
     * @param name the name of the object to compute the continuation token for
     */
    private void touchNextContinuationToken(String name) {
        if (nextContinuationToken == null) {
            nextContinuationToken = computeContinuationToken(name);
        }
    }

    /**
     * Computes a continuation token based on the given name that somewhat resembles the S3 behavior. However, it is
     * not known how S3 computes the continuation token, so this is a best effort approach.
     *
     * @param name the name of the object to compute the continuation token for
     * @return an obfuscated continuation token based on the name
     */
    private String computeContinuationToken(String name) {
        return Hasher.sha512().hash(CONTINUATION_TOKEN_SALT).hash(name).toBase64String();
    }

    public long getCount() {
        return objectCount.getCount();
    }

    public String getNextContinuationToken() {
        return nextContinuationToken;
    }
}
