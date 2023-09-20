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
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
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

    private static final int MOST_RECENT_VERSION = BucketMigrator.MOST_RECENT_VERSION;

    private int version;

    private final File folder;

    private final File versionMarker;

    private final File publicMarker;

    private static final Cache<String, Boolean> publicAccessCache =
            CacheManager.createLocalCache("public-bucket-access");

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
        this.version = parseVersion();
        if (this.version < MOST_RECENT_VERSION) {
            BucketMigrator.migrateBucket(this);
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
        return Strings.urlEncode(getName());
    }

    /**
     * Returns the underlying directory as {@link File}.
     *
     * @return a {@link File} representing the underlying directory
     */
    public File getFolder() {
        return folder;
    }

    protected File getVersionMarker() {
        return versionMarker;
    }

    protected File getPublicMarker() {
        return publicMarker;
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
     * @return <b>true</b> if the folder for the bucket was created successfully or existed before, <b>false</b> else
     */
    public boolean create() {
        if (folder.exists()) {
            return true;
        }

        if (!folder.mkdirs()) {
            return false;
        }

        // having successfully created the folder, write the version marker
        writeVersion();
        return folder.isDirectory();
    }

    /**
     * Deletes the bucket and all of its contents.
     *
     * @return <b>true</b> if all files of the bucket and the bucket itself were deleted successfully, <b>false</b> else
     */
    public boolean delete() {
        if (!folder.exists()) {
            return true;
        }

        try {
            sirius.kernel.commons.Files.delete(folder.toPath());
            return true;
        } catch (IOException exception) {
            Exceptions.handle(Storage.LOG, exception);
            return false;
        }
    }

    /**
     * Sends a list of at most the provided number of stored objects using
     * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV1.html">V1</a> format.
     *
     * @param output the xml structured output the list of objects should be written to
     * @param limit  controls the maximum number of objects returned
     * @param marker the key to start with when listing objects in a bucket
     * @param prefix limits the response to keys that begin with the specified prefix
     */
    public void outputObjectsV1(XMLStructuredOutput output,
                                int limit,
                                @Nullable String marker,
                                @Nullable String prefix) {
        ListFileTreeVisitor visitor = new ListFileTreeVisitor(output, limit, marker, prefix);

        output.beginOutput("ListBucketResult", Attribute.set("xmlns", "http://s3.amazonaws.com/doc/2006-03-01/"));
        output.property("Name", getName());
        output.property("MaxKeys", limit);
        output.property("Marker", marker);
        output.property("Prefix", prefix);
        try {
            walkFileTreeOurWay(folder.toPath(), visitor);
        } catch (IOException exception) {
            throw Exceptions.handle(Storage.LOG, exception);
        }
        output.property("IsTruncated", limit > 0 && visitor.getCount() > limit);
        output.endOutput();
    }

    /**
     * Sends a list of at most the provided number of stored objects using
     * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html">V2</a> format.
     *
     * @param output the xml structured output the list of objects should be written to
     * @param limit  controls the maximum number of objects returned
     * @param marker the key to start with when listing objects in a bucket
     * @param prefix limits the response to keys that begin with the specified prefix
     */
    public void outputObjectsV2(XMLStructuredOutput output,
                                int limit,
                                @Nullable String marker,
                                @Nullable String prefix) {
        ListFileTreeVisitor visitor = new ListFileTreeVisitor(output, limit, marker, prefix);

        output.beginOutput("ListBucketResult", Attribute.set("xmlns", "http://s3.amazonaws.com/doc/2006-03-01/"));
        output.property("Name", getName());
        output.property("MaxKeys", limit);
        output.property("StartAfter", marker);
        output.property("Prefix", prefix);
        try {
            walkFileTreeOurWay(folder.toPath(), visitor);
        } catch (IOException exception) {
            throw Exceptions.handle(Storage.LOG, exception);
        }
        output.property("IsTruncated", limit > 0 && visitor.getCount() > limit);
        output.property("KeyCount", visitor.getCount());
        output.endOutput();
    }

    /**
     * Very simplified stand-in for {@link Files#walkFileTree(Path, FileVisitor)} where we control the traversal order.
     *
     * @param path    the start path.
     * @param visitor the visitor processing the files.
     * @throws IOException forwarded from nested I/O operations.
     */
    private void walkFileTreeOurWay(Path path, FileVisitor<? super Path> visitor) throws IOException {
        if (!path.toFile().isDirectory()) {
            throw new IOException("Directory expected.");
        }

        try (Stream<Path> children = Files.list(path)) {
            children.filter(childPath -> filterObjects(childPath.toFile()))
                    .sorted(Bucket::compareUtf8Binary)
                    .forEach(childPath -> {
                        try {
                            visitor.visitFile(childPath, Files.readAttributes(childPath, BasicFileAttributes.class));
                        } catch (IOException exception) {
                            throw Exceptions.handle(Storage.LOG, exception);
                        }
                    });
        }
    }

    private static int compareUtf8Binary(Path path1, Path path2) {
        String string1 = StoredObject.decodeKey(path1.getFileName().toString());
        String string2 = StoredObject.decodeKey(path2.getFileName().toString());

        byte[] bytes1 = string1.getBytes(StandardCharsets.UTF_8);
        byte[] bytes2 = string2.getBytes(StandardCharsets.UTF_8);

        // unless we upgrade to java 9+ offering Arrays.compare(...), we need to compare the arrays manually :(
        int length = Math.min(bytes1.length, bytes2.length);
        for (int index = 0; index < length; ++index) {
            if (bytes1[index] != bytes2[index]) {
                return Byte.compare(bytes1[index], bytes2[index]);
            }
        }
        return bytes1.length - bytes2.length;
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
     *
     * @return <b>true</b> if the bucket is now only privately accessible, <b>false</b> else
     */
    public boolean makePrivate() {
        if (publicMarker.exists()) {
            sirius.kernel.commons.Files.delete(publicMarker);
            publicAccessCache.put(getName(), false);
        }

        return !publicMarker.exists();
    }

    /**
     * Marks the bucket as publicly accessible.
     *
     * @return <b>true</b> if the bucket is now publicly accessible, <b>false</b> else
     */
    public boolean makePublic() {
        if (!publicMarker.exists()) {
            try {
                new FileOutputStream(publicMarker).close();
            } catch (IOException exception) {
                Exceptions.handle(Storage.LOG, exception);
                return false;
            }
        }
        publicAccessCache.put(getName(), true);

        return publicMarker.exists();
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

        return new StoredObject(folder, key);
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
            return stream.filter(path -> filterObjects(path.toFile()))
                         .sorted(Bucket::compareUtf8Binary)
                         .map(Path::toFile)
                         .filter(currentFile -> isMatchingObject(query, currentFile))
                         .filter(limit.asPredicate())
                         .map(StoredObject::new)
                         .toList();
        } catch (IOException exception) {
            throw Exceptions.handle(Storage.LOG, exception);
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
        } catch (IOException exception) {
            throw Exceptions.handle(Storage.LOG, exception);
        }
    }

    private boolean isMatchingObject(@Nullable String query, File currentFile) {
        if (!currentFile.isFile() || currentFile.getName().startsWith("$")) {
            return false;
        }

        return Strings.isEmpty(query) || StoredObject.decodeKey(currentFile.getName()).contains(query);
    }

    protected int getVersion() {
        return version;
    }

    protected void setVersion(int version) {
        this.version = version;
    }

    private int parseVersion() {
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
        } catch (IOException exception) {
            throw Exceptions.handle(Storage.LOG, exception);
        }
    }

    protected void writeVersion() {
        if (version < MOST_RECENT_VERSION) {
            throw Exceptions.handle(Storage.LOG, new IllegalStateException("Bucket is of outdated version."));
        }

        // non-existent buckets always have the most recent version
        if (!exists()) {
            return;
        }

        try {
            // write the version into the version marker file
            Files.write(versionMarker.toPath(), Collections.singletonList(String.valueOf(version)));
        } catch (IOException exception) {
            throw Exceptions.handle(Storage.LOG, exception);
        }
    }

    protected boolean filterObjects(File file) {
        // ignore directories and other strange stuff
        if (!file.isFile()) {
            return false;
        }

        // ignore files not residing in our own folder
        if (!folder.equals(file.getParentFile())) {
            return false;
        }

        // ignore legacy system files
        if (file.getName().startsWith("__ninja_")) {
            return false;
        }

        // ignore marker files
        if (file.equals(publicMarker) || file.equals(versionMarker)) {
            return false;
        }

        // ignore properties files
        return !(file.getName().startsWith("$") && file.getName().endsWith(".properties"));
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
        } catch (Exception ignored) {
            // ignore this, we want the conversion to fail and thus to end up here
        }

        // reaching this point, the name is valid
        return true;
    }
}
