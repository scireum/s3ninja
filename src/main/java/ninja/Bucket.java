/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package ninja;

import com.google.common.collect.Lists;
import sirius.kernel.cache.Cache;
import sirius.kernel.cache.CacheManager;
import sirius.kernel.health.Exceptions;
import sirius.kernel.xml.Attribute;
import sirius.kernel.xml.XMLStructuredOutput;
import sirius.web.controller.Page;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Represents a bucket.
 * <p>
 * Internally a bucket is just a directory within the base directory.
 * </p>
 */
public class Bucket {

    private static final int PAGE_SIZE = 25;

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
     *
     * @return true if all files of the bucket and the bucket itself was deleted successfully, false otherwise.
     */
    public boolean delete() {
        boolean deleted = false;
        for (File child : file.listFiles()) {
            deleted = child.delete() || deleted;
        }
        deleted = file.delete() || deleted;
        return deleted;
    }

    /**
     * Creates the bucket.
     * <p>
     * If the underlying directory already exists, nothing happens.
     *
     * @return true if the folder for the bucket was created successfully if it was missing before.
     */
    public boolean create() {
        return !file.exists() && file.mkdirs();
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
     * Determines if the bucket is private or public accessible
     *
     * @return <tt>true</tt> if the bucket is public accessible, <tt>false</tt> otherwise
     */
    public boolean isPrivate() {
        return !publicAccessCache.get(getName(), key -> getPublicMarkerFile().exists());
    }

    private File getPublicMarkerFile() {
        return new File(file, "__ninja_public");
    }

    /**
     * Marks the bucket as private accessible.
     */
    public void makePrivate() {
        if (getPublicMarkerFile().exists()) {
            if (getPublicMarkerFile().delete()) {
                publicAccessCache.put(getName(), false);
            } else {
                Storage.LOG.WARN("Failed to delete public marker for bucket %s - it remains public!", getName());
            }
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

    /**
     * Get a {@link Page} of {@link StoredObject}s, starting at <tt>start</tt> with a page size of {@value PAGE_SIZE}
     * items. With the <tt>query</tt> parameter you can filter for files containing the query in the file name.
     *
     * @param start where to start the page
     * @param query a search query
     * @return the {@link Page} for the given parameters
     */
    public Page<StoredObject> getPage(int start, String query) {
        List<StoredObject> files = getObjects(query);

        // because lists start at 0 and pages start at 1, the startIndex is start - 1.
        int startingIndex = start - 1;

        return new Page<StoredObject>().withStart(start)
                                       .withTotalItems(files.size())
                                       .withQuery(query)
                                       .withHasMore(startingIndex + PAGE_SIZE < files.size())
                                       .withItems(files.subList(startingIndex,
                                                                Math.min(startingIndex + PAGE_SIZE, files.size())));
    }

    /**
     * Get all files which file names contain the query. Leave the query empty to get all files.
     *
     * @param query The query to filter for
     * @return All files which contain the query
     */
    public List<StoredObject> getObjects(@Nonnull String query) {
        if (file.listFiles() == null) {
            return new ArrayList<>();
        }
        return Arrays.stream(file.listFiles())
                     .filter(currentFile -> currentFile.getName().contains(query)
                                            && currentFile.isFile()
                                            && !currentFile.getName().startsWith("__"))
                     .map(StoredObject::new)
                     .collect(Collectors.toList());
    }
}
