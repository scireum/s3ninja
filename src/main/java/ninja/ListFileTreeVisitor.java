/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package ninja;

import sirius.kernel.commons.Hasher;
import sirius.kernel.commons.Strings;
import sirius.kernel.health.Counter;
import sirius.kernel.xml.XMLStructuredOutput;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * Visits all files in the buckets directory and outputs their metadata to an {@link XMLStructuredOutput}.
 */
class ListFileTreeVisitor extends SimpleFileVisitor<Path> {

    private final Counter objectCount;
    private final XMLStructuredOutput output;
    private final int limit;
    private final String marker;
    private final String prefix;
    private final boolean useLimit;
    private final boolean usePrefix;
    private boolean markerReached;

    // Supressed warning "Null pointers should not be dereferenced"
    // as prefix can't be null when replacing as the usePrefix acts as a guard.
    @SuppressWarnings("squid:S2259")
    protected ListFileTreeVisitor(XMLStructuredOutput output,
                                  int limit,
                                  @Nullable String marker,
                                  @Nullable String prefix) {
        this.output = output;
        this.limit = limit;
        this.marker = marker;
        this.prefix = prefix;
        objectCount = new Counter();
        useLimit = limit > 0;
        usePrefix = Strings.isFilled(prefix);
        markerReached = Strings.isEmpty(marker);
    }

    @Override
    public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
        File file = path.toFile();
        String name = StoredObject.decodeKey(file.getName());

        if (!file.isFile() || file.getName().startsWith("$")) {
            return FileVisitResult.CONTINUE;
        }
        if (!markerReached) {
            if (marker.equals(name)) {
                markerReached = true;
            }
        } else {
            StoredObject object = new StoredObject(file);
            if (useLimit && (!usePrefix || name.startsWith(prefix))) {
                long numObjects = objectCount.inc();
                if (numObjects <= limit) {
                    output.beginObject("Contents");
                    output.property("Key", object.getKey());
                    output.property("LastModified",
                                    S3Dispatcher.ISO8601_INSTANT.format(object.getLastModifiedInstant()));
                    output.property("Size", object.getSizeBytes());
                    output.property("StorageClass", "STANDARD");
                    output.property("ETag", getETag(file));
                    output.endObject();
                } else {
                    return FileVisitResult.TERMINATE;
                }
            }
        }
        return FileVisitResult.CONTINUE;
    }

    private String getETag(File file) {
        return Hasher.md5().hashFile(file).toHexString();
    }

    public long getCount() {
        return objectCount.getCount();
    }
}
