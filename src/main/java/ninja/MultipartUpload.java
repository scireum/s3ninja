/*
 * Made with all the love in the world
 * by scireum in Stuttgart, Germany
 *
 * Copyright by scireum GmbH
 * https://www.scireum.de - info@scireum.de
 */

package ninja;

import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import sirius.kernel.commons.Hasher;
import sirius.kernel.commons.Strings;
import sirius.kernel.health.Exceptions;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Represents a multipart upload which has been initiated, but neither completed nor aborted yet.
 * <p>
 * The whole state of an upload lives in its own folder on disk: a metadata file naming the bucket and key, the
 * properties to store along with the final object, and one file per uploaded part which is named after the part
 * number. Nothing is kept in memory, so that open uploads survive a restart.
 */
public class MultipartUpload {

    /**
     * Contains the lowest part number accepted by S3.
     */
    public static final int MIN_PART_NUMBER = 1;

    /**
     * Contains the highest part number accepted by S3.
     */
    public static final int MAX_PART_NUMBER = 10_000;

    private static final String METADATA_FILENAME = "upload";
    private static final String PROPERTIES_FILENAME = "properties";
    private static final String TEMPORARY_FILE_SUFFIX = ".tmp";

    /**
     * Starts the names of folders of deleted uploads. No upload ID starts like this, so that such a folder is never
     * taken for an upload.
     */
    private static final String DELETED_FOLDER_PREFIX = "deleted-";

    /**
     * Matches the names given to the folders of deleted uploads, which end with a random UUID.
     * <p>
     * Such folders are removed without further ado, so nothing else which happens to start alike may match.
     */
    private static final Pattern DELETED_FOLDER_NAME =
            Pattern.compile(DELETED_FOLDER_PREFIX + "[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}");

    private static final String METADATA_BUCKET = "bucket";
    private static final String METADATA_KEY = "key";
    private static final String METADATA_INITIATED = "initiated";

    /**
     * Matches the names of part files, so that the metadata, the properties and half-written parts are told apart.
     */
    private static final Pattern PART_FILENAME = Pattern.compile("[1-9]\\d{0,4}");

    private final File folder;
    private final String bucket;
    private final String key;
    private final Instant initiated;

    /**
     * Represents a part which has been uploaded completely.
     *
     * @param number the part number, between {@link #MIN_PART_NUMBER} and {@link #MAX_PART_NUMBER}
     * @param file   the file holding the contents of the part
     */
    public record Part(int number, File file) {

        /**
         * Computes the MD5 hash of the contents of this part.
         *
         * @return the raw MD5 hash
         */
        public byte[] md5() {
            return Hasher.md5().hashFile(file).toHash();
        }

        /**
         * Returns the size of this part.
         *
         * @return the size in bytes
         */
        public long size() {
            return file.length();
        }

        /**
         * Returns the point in time when this part has been uploaded.
         *
         * @return the time of the upload
         */
        public Instant lastModified() {
            return Instant.ofEpochMilli(file.lastModified());
        }
    }

    private MultipartUpload(File folder, String bucket, String key, Instant initiated) {
        this.folder = folder;
        this.bucket = bucket;
        this.key = key;
        this.initiated = initiated;
    }

    /**
     * Creates a new upload in the given folder.
     *
     * @param folder     the folder to create and to keep the state of the upload in
     * @param bucket     the name of the bucket the final object is stored in
     * @param key        the key of the final object
     * @param properties the properties to store along with the final object
     * @return the newly created upload
     * @throws IOException if the folder or its files cannot be written
     */
    static MultipartUpload create(File folder, String bucket, String key, Map<String, String> properties)
            throws IOException {
        Files.createDirectories(folder.toPath());
        try {
            Properties objectProperties = new Properties();
            properties.forEach(objectProperties::setProperty);
            storeProperties(objectProperties, new File(folder, PROPERTIES_FILENAME));

            // The metadata file is written last, as its existence is what makes the folder a known upload.
            Instant initiated = Instant.now();
            Properties metadata = new Properties();
            metadata.setProperty(METADATA_BUCKET, bucket);
            metadata.setProperty(METADATA_KEY, key);
            metadata.setProperty(METADATA_INITIATED, String.valueOf(initiated.toEpochMilli()));
            storeProperties(metadata, new File(folder, METADATA_FILENAME));

            return new MultipartUpload(folder, bucket, key, initiated);
        } catch (IOException exception) {
            // A folder without its metadata is no upload, so nothing would ever find and remove it.
            try {
                sirius.kernel.commons.Files.delete(folder.toPath());
            } catch (IOException cleanupException) {
                Exceptions.ignore(cleanupException);
            }
            throw exception;
        }
    }

    /**
     * Loads the upload kept in the given folder.
     *
     * @param folder the folder holding the state of the upload
     * @return the upload, or an empty optional if the folder does not hold an upload
     */
    static Optional<MultipartUpload> load(File folder) {
        File metadataFile = new File(folder, METADATA_FILENAME);
        if (!metadataFile.isFile()) {
            return Optional.empty();
        }

        try {
            Properties metadata = loadProperties(metadataFile);
            String bucket = metadata.getProperty(METADATA_BUCKET);
            String key = metadata.getProperty(METADATA_KEY);
            if (Strings.isEmpty(bucket) || Strings.isEmpty(key)) {
                return Optional.empty();
            }

            // Without its time of initiation, the metadata is incomplete, which a NumberFormatException reports.
            Instant initiated = Instant.ofEpochMilli(Long.parseLong(metadata.getProperty(METADATA_INITIATED, "")));
            return Optional.of(new MultipartUpload(folder, bucket, key, initiated));
        } catch (IOException | NumberFormatException exception) {
            Exceptions.ignore(exception);
            return Optional.empty();
        }
    }

    /**
     * Returns the ID of this upload.
     *
     * @return the upload ID as handed out to the client
     */
    public String getUploadId() {
        return folder.getName();
    }

    /**
     * Returns the name of the bucket the final object is stored in.
     *
     * @return the name of the bucket
     */
    public String getBucket() {
        return bucket;
    }

    /**
     * Returns the key of the final object.
     *
     * @return the key of the object
     */
    public String getKey() {
        return key;
    }

    /**
     * Returns the point in time when this upload has been initiated.
     *
     * @return the time of initiation
     */
    public Instant getInitiated() {
        return initiated;
    }

    /**
     * Returns the properties to store along with the final object.
     *
     * @return the properties given when the upload was initiated
     * @throws IOException if the properties cannot be read
     */
    public Map<String, String> getProperties() throws IOException {
        Map<String, String> result = Maps.newTreeMap();
        loadProperties(new File(folder, PROPERTIES_FILENAME)).forEach((name, value) -> result.put(String.valueOf(name),
                                                                                                  String.valueOf(value)));
        return result;
    }

    /**
     * Receives the given data as part with the given number, without making it a part of this upload yet.
     * <p>
     * The data is written to a file of its own, so that parts which are uploaded in parallel or which are repeated
     * neither see nor disturb each other. The staged part has to be handed to {@link #publishPart(Part)} or
     * {@link #discardPart(Part)}.
     *
     * @param number the part number, between {@link #MIN_PART_NUMBER} and {@link #MAX_PART_NUMBER}
     * @param data   the contents of the part
     * @return the staged part
     * @throws IOException if the part cannot be written, e.g. as the upload has been completed or aborted meanwhile
     */
    public Part stagePart(int number, InputStream data) throws IOException {
        File stagedFile = new File(folder, number + "." + UUID.randomUUID() + TEMPORARY_FILE_SUFFIX);
        try (FileOutputStream out = new FileOutputStream(stagedFile)) {
            ByteStreams.copy(data, out);
        } catch (IOException exception) {
            Files.deleteIfExists(stagedFile.toPath());
            throw exception;
        }

        return new Part(number, stagedFile);
    }

    /**
     * Makes a staged part a part of this upload, replacing a part previously uploaded with that number.
     * <p>
     * The caller has to hold the lock of this upload, so that the parts cannot change while the upload is completed.
     *
     * @param stagedPart the part as returned by {@link #stagePart(int, InputStream)}
     * @return the part which is now visible under its number
     * @throws IOException if the part cannot be moved into place
     */
    public Part publishPart(Part stagedPart) throws IOException {
        File partFile = new File(folder, String.valueOf(stagedPart.number()));
        Files.move(stagedPart.file().toPath(),
                   partFile.toPath(),
                   StandardCopyOption.REPLACE_EXISTING,
                   StandardCopyOption.ATOMIC_MOVE);
        return new Part(stagedPart.number(), partFile);
    }

    /**
     * Drops a staged part which is not to become a part of this upload.
     *
     * @param stagedPart the part as returned by {@link #stagePart(int, InputStream)}
     */
    public void discardPart(Part stagedPart) {
        sirius.kernel.commons.Files.delete(stagedPart.file());
    }

    /**
     * Returns the part with the given number.
     *
     * @param number the number of the part to fetch
     * @return the part, or an empty optional if no part has been uploaded with this number
     */
    public Optional<Part> getPart(int number) {
        if (number < MIN_PART_NUMBER || number > MAX_PART_NUMBER) {
            return Optional.empty();
        }

        File partFile = new File(folder, String.valueOf(number));
        return partFile.isFile() ? Optional.of(new Part(number, partFile)) : Optional.empty();
    }

    /**
     * Lists all parts which have been uploaded so far.
     *
     * @return the parts in ascending order of their numbers
     */
    public List<Part> getParts() {
        List<Part> parts = new ArrayList<>();
        File[] files = folder.listFiles(file -> file.isFile() && PART_FILENAME.matcher(file.getName()).matches());
        if (files != null) {
            for (File file : files) {
                parts.add(new Part(Integer.parseInt(file.getName()), file));
            }
        }
        parts.sort(Comparator.comparingInt(Part::number));
        return parts;
    }

    /**
     * Concatenates the given parts into the given file.
     *
     * @param parts         the parts to concatenate, in the order in which they make up the object
     * @param assembledFile the file to write the concatenated contents to
     * @throws IOException if a part cannot be read or the file cannot be written
     */
    public void assemble(List<Part> parts, File assembledFile) throws IOException {
        try (FileOutputStream outStream = new FileOutputStream(assembledFile);
             FileChannel out = outStream.getChannel()) {
            for (Part part : parts) {
                appendPart(part, out);
            }
        }
    }

    /**
     * Appends the content of the given part to the assembled object.
     * <p>
     * A single transfer may move fewer bytes than asked for, so the part is transferred until all of it is written.
     *
     * @param part the part to append
     * @param out  the channel of the assembled object, positioned at its end
     * @throws IOException if the part cannot be read or the object cannot be written
     */
    private static void appendPart(Part part, FileChannel out) throws IOException {
        try (FileInputStream inStream = new FileInputStream(part.file()); FileChannel in = inStream.getChannel()) {
            long position = 0;
            long size = in.size();
            while (position < size) {
                position += in.transferTo(position, size - position, out);
            }
        }
    }

    /**
     * Determines if the given folder is what is left of an upload which could not be deleted completely.
     *
     * @param folder a folder within the multipart directory
     * @return <tt>true</tt> if the folder belongs to a deleted upload, <tt>false</tt> otherwise
     */
    static boolean isLeftoverOfDeletedUpload(File folder) {
        return folder.isDirectory() && DELETED_FOLDER_NAME.matcher(folder.getName()).matches();
    }

    /**
     * Removes the files of parts which have been received, but neither published nor discarded.
     * <p>
     * Only files which have not been written since the given point in time are removed, so that a part which is on
     * its way right now is never touched.
     *
     * @param abandonedBefore the point in time before which a staged part must have been written last to be removed
     */
    void removeStagedParts(Instant abandonedBefore) {
        File[] stagedFiles = folder.listFiles(file -> file.isFile()
                                                      && file.getName().endsWith(TEMPORARY_FILE_SUFFIX)
                                                      && file.lastModified() < abandonedBefore.toEpochMilli());
        if (stagedFiles != null) {
            for (File stagedFile : stagedFiles) {
                sirius.kernel.commons.Files.delete(stagedFile);
            }
        }
    }

    /**
     * Deletes this upload along with all of its parts.
     * <p>
     * The folder is renamed first, which takes the upload out of sight in one step. A part which is still being
     * uploaded at that moment can no longer be moved into place, and thus cannot keep the folder alive while its
     * files are removed.
     *
     * @throws IOException if the upload cannot be taken out of sight
     */
    public void delete() throws IOException {
        File deletedFolder = new File(folder.getParentFile(), DELETED_FOLDER_PREFIX + UUID.randomUUID());
        Files.move(folder.toPath(), deletedFolder.toPath(), StandardCopyOption.ATOMIC_MOVE);

        try {
            sirius.kernel.commons.Files.delete(deletedFolder.toPath());
        } catch (IOException exception) {
            // The upload is gone for all clients, so being unable to free its disk space must not fail the request.
            Exceptions.handle(Storage.LOG, exception);
        }
    }

    private static void storeProperties(Properties properties, File file) throws IOException {
        try (FileOutputStream out = new FileOutputStream(file)) {
            properties.store(out, "");
        }
    }

    private static Properties loadProperties(File file) throws IOException {
        Properties properties = new Properties();
        if (file.isFile()) {
            try (FileInputStream in = new FileInputStream(file)) {
                properties.load(in);
            }
        }
        return properties;
    }
}
