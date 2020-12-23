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
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Represents a bucket.
 * <p>
 * Internally a bucket is just a directory within the base directory.
 */
public class Bucket {

    /**
     * Enforces the <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html">official rules</a>
     * for bucket names:
     * <ul>
     *     <li>Between 3 and 63 characters</li>
     *     <li>Lowercase letters, numbers, dots, and hyphens</li>
     *     <li>First and last letter a number or a letter</li>
     * </ul>
     */
    private static final Pattern BUCKET_NAME_PATTERN = Pattern.compile("^[a-z\\d][a-z\\d\\-.]{1,61}[a-z\\d]$");

    /**
     * Matches IPv4 addresses roughly.
     */
    private static final Pattern IP_ADDRESS_PATTERN = Pattern.compile("^\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}$");

    private static final int MOST_RECENT_VERSION = 2;

    private final File folder;

    private final File versionMarker;

    private final File publicMarker;

    private static final Cache<String, Boolean> publicAccessCache = CacheManager.createLocalCache("public-bucket-access");

    /**
     * Creates a new bucket based on the given directory.
     *
     * @param folder the directory which stores the contents of the bucket.
     */
    public Bucket(File folder) {
        this.folder = folder;

        // set the public marker file
        this.publicMarker = new File(folder, "$public");

        // as last step, check the version, and migrate the bucket if necessary
        this.versionMarker = new File(folder, "$version");
        int version = getVersion();
        if (version < MOST_RECENT_VERSION) {
            migrateBucket(version);
        }
    }

    /**
     * Returns the name of the bucket.
     *
     * @return the name of the bucket
     */
    public String getName() {
        return folder.getName();
    }

    /**
     * Returns the encoded name of the bucket.
     *
     * @return the encoded name of the bucket
     */
    public String getEncodedName() {
        try {
            return URLEncoder.encode(getName(), StandardCharsets.UTF_8.toString());
        } catch (UnsupportedEncodingException e) {
            return getName();
        }
    }

    /**
     * Returns the underlying directory as {@link File}.
     *
     * @return a {@link File} representing the underlying directory
     */
    public File getFolder() {
        return folder;
    }

    /**
     * Determines if the bucket exists.
     *
     * @return <b>true</b> if the bucket exists, <b>false</b> else
     */
    public boolean exists() {
        return folder.exists();
    }

    /**
     * Creates the bucket.
     * <p>
     * If the underlying directory already exists, nothing happens.
     *
     * @return <b>true</b> if the folder for the bucket was created successfully and if it was missing before
     */
    public boolean create() {
        if (folder.exists() || !folder.mkdirs()) {
            return false;
        }

        // having successfully created the folder, write the version marker
        setVersion(MOST_RECENT_VERSION);
        return true;
    }

    /**
     * Deletes the bucket and all of its contents.
     *
     * @return true if all files of the bucket and the bucket itself was deleted successfully, false otherwise.
     */
    public boolean delete() {
        if (!folder.exists()) {
            return true;
        }

        boolean deleted = false;
        for (File child : Objects.requireNonNull(folder.listFiles())) {
            deleted = child.delete() || deleted;
        }
        deleted = folder.delete() || deleted;
        return deleted;
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
            walkFileTreeOurWay(folder.toPath(), visitor);
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
     * Determines if the bucket is only privately accessible, i.e. non-public.
     *
     * @return <b>true</b> if the bucket is only privately accessible, <b>false</b> else
     */
    public boolean isPrivate() {
        return !Boolean.TRUE.equals(publicAccessCache.get(getName(), key -> publicMarker.exists()));
    }

    /**
     * Marks the bucket as only privately accessible, i.e. non-public.
     */
    public void makePrivate() {
        if (publicMarker.exists()) {
            if (publicMarker.delete()) {
                publicAccessCache.put(getName(), false);
            } else {
                Storage.LOG.WARN("Failed to delete public marker for bucket %s - it remains public!", getName());
            }
        }
    }

    /**
     * Marks the bucket as publicly accessible.
     */
    public void makePublic() {
        if (!publicMarker.exists()) {
            try {
                new FileOutputStream(publicMarker).close();
            } catch (IOException e) {
                throw Exceptions.handle(Storage.LOG, e);
            }
        }
        publicAccessCache.put(getName(), true);
    }

    /**
     * Returns the object with the given key.
     * <p>
     * The method never returns <b>null</b>, but {@link StoredObject#exists()} may return <b>false</b>.
     * <p>
     * Make sure that the key passes {@link StoredObject#isValidKey(String)} by meeting the naming restrictions
     * documented there.
     *
     * @param key the key of the requested object
     * @return the object with the given key
     */
    @Nonnull
    public StoredObject getObject(String key) {
        if (!StoredObject.isValidKey(key)) {
            throw Exceptions.createHandled()
                            .withSystemErrorMessage(
                                    "Object key \"%s\" does not adhere to the rules. [https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html]",
                                    key)
                            .handle();
        }
        return new StoredObject(new File(folder, key));
    }

    /**
     * Returns a number of files meeting the given query, within the given indexing limits. Leave the query empty to
     * get all files.
     *
     * @param query the query to filter for
     * @param limit the limit to apply
     * @return all files meeting the query, restricted by the limit
     */
    public List<StoredObject> getObjects(@Nullable String query, Limit limit) {
        try (Stream<Path> stream = Files.list(folder.toPath())) {
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

    /**
     * Count the files containing the query. Leave the query empty to count all files.
     *
     * @param query the query to filter for
     * @return the number of files in the bucket matching the query
     */
    public int countObjects(@Nullable String query) {
        try (Stream<Path> stream = Files.list(folder.toPath())) {
            return Math.toIntExact(stream.map(Path::toFile)
                                         .filter(currentFile -> isMatchingObject(query, currentFile))
                                         .count());
        } catch (IOException e) {
            throw Exceptions.handle(e);
        }
    }

    private boolean isMatchingObject(@Nullable String query, File currentFile) {
        return (Strings.isEmpty(query) || currentFile.getName().contains(query)) && currentFile.isFile() && !currentFile
                .getName()
                .startsWith("__");
    }

    private int getVersion() {
        // non-existent buckets always have the most recent version
        if (!exists()) {
            return MOST_RECENT_VERSION;
        }

        // return the minimal version if the bucket exists, but without a version marker
        if (!versionMarker.exists()) {
            return 1;
        }

        try {
            // parse the version from the version marker file
            return Integer.parseInt(Strings.join(Files.readAllLines(versionMarker.toPath()), "\n").trim());
        } catch (IOException e) {
            throw Exceptions.handle(Storage.LOG, e);
        }
    }

    private void setVersion(int version) {
        // non-existent buckets always have the most recent version
        if (!exists()) {
            return;
        }

        try {
            // write the version into the version marker file
            Files.write(versionMarker.toPath(), Collections.singletonList(String.valueOf(version)));
        } catch (IOException e) {
            throw Exceptions.handle(Storage.LOG, e);
        }
    }

    /**
     * Migrates a bucket folder to the most recent version.
     *
     * @param fromVersion the version to migrate from.
     */
    private void migrateBucket(int fromVersion) {
        if (fromVersion <= 1) {
            try {
                // migrate public marker
                File legacyPublicMarker = new File(folder, "__ninja_public");
                if (legacyPublicMarker.exists() && !publicMarker.exists()) {
                    Files.move(legacyPublicMarker.toPath(), publicMarker.toPath());
                } else if (legacyPublicMarker.exists()) {
                    Files.delete(legacyPublicMarker.toPath());
                }
            } catch (IOException e) {
                throw Exceptions.handle(Storage.LOG, e);
            }

            // todo: migrate files and properties
        }

        // further incremental updates go here one day

        // write the most recent version marker
        setVersion(MOST_RECENT_VERSION);
    }

    /**
     * Checks whether the given string is valid for use as bucket name.
     * <p>
     * See the <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html">official naming
     * rules</a> for all requirements.
     *
     * @param name the name to check
     * @return <b>true</b> if the name is valid as bucket name, <b>false</b> else
     */
    public static boolean isValidName(@Nullable String name) {
        if (name == null || Strings.isEmpty(name.trim())) {
            return false;
        }

        // test the majority of simple requirements via a regex
        if (!BUCKET_NAME_PATTERN.matcher(name).matches()) {
            return false;
        }

        // make sure that it does not start with "xn--"
        if (name.startsWith("xn--")) {
            return false;
        }

        try {
            // make sure that the name is no valid IP address (the null check is pointless, it is just there to trigger
            // actual conversion after the regex has matched; if the parsing fails, we end up in the catch clause)
            if (IP_ADDRESS_PATTERN.matcher(name).matches() && InetAddress.getByName(name) != null) {
                return false;
            }
        } catch (Exception e) {
            // ignore this, we want the conversion to fail and thus to end up here
        }

        // reaching this point, the name is valid
        return true;
    }
}
