/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package ninja;

import sirius.kernel.cache.Cache;
import sirius.kernel.cache.CacheManager;
import sirius.kernel.commons.Limit;
import sirius.kernel.commons.Strings;
import sirius.kernel.health.Exceptions;
import sirius.kernel.xml.Attribute;
import sirius.kernel.xml.XMLStructuredOutput;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Represents a bucket.
 * <p>
 * Internally a bucket is just a directory within the base directory.
 * </p>
 */
public class Bucket {

    private final File file;
    private static final Cache<String, Boolean> publicAccessCache = CacheManager.createLocalCache("public-bucket-access");

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
            walkFileTreeOurWay(file.toPath(), visitor);
        } catch (IOException e) {
            Exceptions.handle(e);
        }
        output.property("IsTruncated", limit > 0 && visitor.getCount() > limit);
        output.endOutput();
    }

    /**
     * Very simplified stand-in for {@link Files#walkFileTree(Path, FileVisitor)} where we control the traversal order.
     *
     * @param path the start path.
     * @param visitor the visitor processing the files.
     * @throws IOException forwarded from nested I/O operations.
     */
    private static void walkFileTreeOurWay(Path path, FileVisitor<? super Path> visitor) throws IOException {
        if (!path.toFile().isDirectory()) {
            throw new IOException("Directory expected.");
        }

        try (Stream<Path> children = Files.list(path)) {
            children.sorted(Bucket::compareUtf8Binary)
                    .filter(p -> p.toFile().isFile())
                    .forEach(p -> {
                try {
                    BasicFileAttributes attrs = Files.readAttributes(p, BasicFileAttributes.class);
                    visitor.visitFile(p, attrs);
                } catch (IOException e) {
                    Exceptions.handle(e);
                }
            });
        }
    }

    private static int compareUtf8Binary(Path p1, Path p2) {
        String s1 = p1.getFileName().toString();
        String s2 = p2.getFileName().toString();

        byte[] b1 = s1.getBytes(StandardCharsets.UTF_8);
        byte[] b2 = s2.getBytes(StandardCharsets.UTF_8);

        // unless we upgrade to java 9+ offering Arrays.compare(...), we need to compare the arrays manually :(
        int length = Math.min(b1.length, b2.length);
        for (int i = 0; i < length; ++i) {
            if (b1[i] != b2[i]) {
                return Byte.compare(b1[i], b2[i]);
            }
        }
        return b1.length - b2.length;
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
     * Get <tt>size</tt> number of files, starting at <tt>start</tt>. Only get files containing the query. Leave the
     * query empty to get all files.
     *
     * @param query the query to filter for
     * @param limit the limit to apply
     * @return all files which contain the query
     */
    public List<StoredObject> getObjects(@Nonnull String query, Limit limit) {
        try (Stream<Path> stream = Files.list(file.toPath())) {
            return stream.sorted(Bucket::compareUtf8Binary)
                         .map(Path::toFile)
                         .filter(currentFile -> isMatchingObject(query, currentFile))
                         .filter(limit.asPredicate())
                         .map(StoredObject::new)
                         .collect(Collectors.toList());
        } catch (IOException e) {
            throw Exceptions.handle(e);
        }
    }

    private boolean isMatchingObject(@Nonnull String query, File currentFile) {
        return (Strings.isEmpty(query) || currentFile.getName().contains(query)) && currentFile.isFile() && !currentFile
                .getName()
                .startsWith("__");
    }

    /**
     * Count the files containing the query. Leave the query empty to count all files.
     *
     * @param query the query to filter for
     * @return the number of files in the bucket matching the query
     */
    public int countObjects(@Nonnull String query) {
        try (Stream<Path> stream = Files.list(file.toPath())) {
            return Math.toIntExact(stream.map(Path::toFile)
                                         .filter(currentFile -> isMatchingObject(query, currentFile))
                                         .count());
        } catch (IOException e) {
            throw Exceptions.handle(e);
        }
    }
}
