/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package ninja;

import com.google.common.collect.Lists;
import com.google.common.hash.Hashing;
import sirius.kernel.cache.Cache;
import sirius.kernel.cache.CacheManager;
import sirius.kernel.cache.ValueComputer;
import sirius.kernel.commons.Strings;
import sirius.kernel.health.Counter;
import sirius.kernel.health.Exceptions;
import sirius.kernel.xml.Attribute;
import sirius.kernel.xml.XMLStructuredOutput;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;

/**
 * Represents a bucket.
 * <p>
 * Internally a bucket is just a directory within the base directory.
 * </p>
 */
public class Bucket {

    private File file;
    private static Cache<String, Boolean> publicAccessCache = CacheManager.createCache("public-bucket-access");

    /**
     * Creates a new bucket based on the given directory.
     *
     * @param file the directory which stores the contents of the bucket.
     */
    public Bucket(File file) {
        this.file = file;
    }

    /**
     * Returns the name of the bucket.
     *
     * @return the name of the bucket
     */
    public String getName() {
        return file.getName();
    }

    /**
     * Deletes the bucket and all of its contents.
     */
    public void delete() {
        for (File child : file.listFiles()) {
            child.delete();
        }
        file.delete();
    }

    /**
     * Creates the bucket.
     * <p>
     * If the underlying directory already exists, nothing happens.
     * </p>
     */
    public void create() {
        if (!file.exists()) {
            file.mkdirs();
        }
    }

    /**
     * Returns a list of all stored objects
     *
     * @return a list of all objects in the bucket.
     */
    public List<StoredObject> getObjects() {
        List<StoredObject> result = Lists.newArrayList();
        for (File child : file.listFiles()) {
            if (child.isFile() && !child.getName().startsWith("__")) {
                result.add(new StoredObject(child));
            }
        }

        return result;
    }

    /**
     * Returns a list of at most the provided number of stored objects
     *
     * @param output the xml structured output the list of objects should be written to
     * @param limit  controls the maximum number of objects returned
     * @param marker the key to start with when listing objects in a bucket
     * @param prefix limits the response to keys that begin with the specified prefix
     */
    public void outputObjects(XMLStructuredOutput output, int limit, @Nullable String marker, @Nullable String prefix) {
        ListFileTreeVisitor visitor = new ListFileTreeVisitor(output, limit, marker, prefix);

        output.beginOutput("ListBucketResult", Attribute.set("xmlns", "http://s3.amazonaws.com/doc/2006-03-01/"));
        output.property("Name", getName());
        output.property("MaxKeys", limit);
        output.property("Marker", marker);
        output.property("Prefix", prefix);
        try {
            Files.walkFileTree(file.toPath(), visitor);
        } catch (IOException e) {
            Exceptions.handle(e);
        }
        output.property("IsTruncated", limit > 0 && visitor.getCount() > limit);
        output.endOutput();
    }

    /**
     * Visits all files in the buckets directory and outputs their metadata to an {@link XMLStructuredOutput}.
     */
    private static class ListFileTreeVisitor extends SimpleFileVisitor<Path> {

        Counter objectCount;
        XMLStructuredOutput output;
        int limit;
        String marker;
        String prefix;
        boolean useLimit;
        boolean usePrefix;
        boolean markerReached;

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
                if (!usePrefix || name.startsWith(prefix)) {
                    if (useLimit) {
                        long numObjects = objectCount.inc();
                        if (numObjects <= limit) {
                            output.beginObject("Contents");
                            output.property("Key", file.getName());
                            output.property("LastModified",
                                            S3Controller.ISO_INSTANT.format(object.getLastModifiedInstant()));
                            output.property("Size", file.length());
                            output.property("StorageClass", "STANDARD");

                            String etag = null;
                            try {
                                etag = com.google.common.io.Files.hash(file, Hashing.md5()).toString();
                            } catch (IOException e) {
                                Exceptions.ignore(e);
                            }
                            output.property("ETag", etag);
                            output.endObject();
                        } else {
                            return FileVisitResult.TERMINATE;
                        }
                    }
                }
            }
            return FileVisitResult.CONTINUE;
        }

        public long getCount() {
            return objectCount.getCount();
        }
    }

    /**
     * Determines if the bucket is private or public accessible
     *
     * @return <tt>true</tt> if the bucket is public accessible, <tt>false</tt> otherwise
     */
    public boolean isPrivate() {
        return !publicAccessCache.get(getName(), new ValueComputer<String, Boolean>() {
            @Nullable
            @Override
            public Boolean compute(@Nonnull String key) {
                return getPublicMarkerFile().exists();
            }
        });
    }

    private File getPublicMarkerFile() {
        return new File(file, "__ninja_public");
    }

    /**
     * Marks the bucket as private accessible.
     */
    public void makePrivate() {
        if (getPublicMarkerFile().exists()) {
            getPublicMarkerFile().delete();
            publicAccessCache.put(getName(), false);
        }
    }

    /**
     * Marks the bucket as public accessible.
     */
    public void makePublic() {
        if (!getPublicMarkerFile().exists()) {
            try {
                new FileOutputStream(getPublicMarkerFile()).close();
            } catch (IOException e) {
                throw Exceptions.handle(Storage.LOG, e);
            }
        }
        publicAccessCache.put(getName(), true);
    }

    /**
     * Returns the underlying directory as File.
     *
     * @return a <tt>File</tt> representing the underlying directory
     */
    public File getFile() {
        return file;
    }

    /**
     * Determines if the bucket exists.
     *
     * @return <tt>true</tt> if the bucket exists, <tt>false</tt> otherwise
     */
    public boolean exists() {
        return file.exists();
    }

    /**
     * Returns the child object with the given id.
     *
     * @param id the name of the requested child object. Must not contain .. / or \
     * @return the object with the given id, might not exist, but is always non null
     */
    public StoredObject getObject(String id) {
        if (id.contains("..") || id.contains("/") || id.contains("\\")) {
            throw Exceptions.createHandled()
                            .withSystemErrorMessage(
                                    "Invalid object name: %s. A object name must not contain '..' '/' or '\\'",
                                    id)
                            .handle();
        }
        return new StoredObject(new File(file, id));
    }
}
