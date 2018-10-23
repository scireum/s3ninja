/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package ninja;

import com.google.common.hash.Hashing;
import sirius.kernel.commons.Strings;
import sirius.kernel.health.Counter;
import sirius.kernel.health.Exceptions;
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

    private Counter objectCount;
    private XMLStructuredOutput output;
    private int limit;
    private String marker;
    private String prefix;
    private boolean useLimit;
    private boolean usePrefix;
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
        if (usePrefix) {
            this.prefix = prefix.replace('/', '_');
        }
        markerReached = Strings.isEmpty(marker);
    }

    @Override
    public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
        File file = path.toFile();
        String name = file.getName();

        if (!file.isFile() || name.startsWith("__")) {
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
                    output.property("Key", file.getName());
                    output.property("LastModified",
                            S3Dispatcher.iso8601DateFormat.print(object.getFile().lastModified()));
                    output.property("Last-Modified",
                            S3Dispatcher.iso8601DateFormat.print(object.getFile().lastModified()));
                    output.property("Size", file.length());
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
        try {
            return com.google.common.io.Files.hash(file, Hashing.md5()).toString();
        } catch (IOException e) {
            Exceptions.ignore(e);
        }
        return null;
    }

    public long getCount() {
        return objectCount.getCount();
    }
}
