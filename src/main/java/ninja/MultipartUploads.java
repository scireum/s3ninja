/*
 * Made with all the love in the world
 * by scireum in Stuttgart, Germany
 *
 * Copyright by scireum GmbH
 * https://www.scireum.de - info@scireum.de
 */

package ninja;

import com.google.common.io.BaseEncoding;
import com.google.common.util.concurrent.Striped;
import sirius.kernel.Startable;
import sirius.kernel.commons.Strings;
import sirius.kernel.di.std.ConfigValue;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Exceptions;
import sirius.kernel.timer.EveryHour;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.Lock;
import java.util.regex.Pattern;

/**
 * Keeps track of all multipart uploads which have been initiated, but neither completed nor aborted yet.
 * <p>
 * The uploads are kept as folders within <tt>storage.multipartDir</tt>, see {@link MultipartUpload}. This service
 * holds no state of its own, so that open uploads survive a restart as long as that directory does.
 */
@Register(classes = {MultipartUploads.class, Startable.class, EveryHour.class})
public class MultipartUploads implements Startable, EveryHour {

    /**
     * Ends the names of the files into which objects are assembled within the folder of their bucket.
     * <p>
     * Such a file starts with "$", which no file of an object does, as "$" is encoded in object keys. It is therefore
     * neither listed nor reachable as an object.
     */
    private static final String ASSEMBLING_FILE_SUFFIX = ".assembling";

    /**
     * Contains the number of hex digits at the start of an upload ID which hold the time of initiation.
     * <p>
     * S3 lists the uploads of a key by their time of initiation, but continues a listing after a given upload ID.
     * Leading with the time makes both orders the same, so that paging stays correct even if the upload named as
     * marker has been removed meanwhile.
     */
    private static final int UPLOAD_ID_TIME_DIGITS = 12;

    /**
     * Contains the number of random bytes which follow the time, and keep an upload ID from being guessed.
     */
    private static final int UPLOAD_ID_RANDOM_BYTES = 18;

    /**
     * Matches everything which is accepted as upload ID.
     * <p>
     * Upload IDs are used as folder names, so nothing which could leave the multipart directory may pass. Only the
     * exact form of generated IDs is accepted, as other spellings of the same folder, which file systems ignoring
     * case permit, would not share the lock of the upload.
     */
    private static final Pattern VALID_UPLOAD_ID =
            Pattern.compile("[0-9a-f]{" + (UPLOAD_ID_TIME_DIGITS + 2 * UPLOAD_ID_RANDOM_BYTES) + "}");

    @ConfigValue("storage.multipartDir")
    private String multipartDir;

    @ConfigValue("storage.multipartMinPartSize")
    private long minPartSize;

    @ConfigValue("storage.multipartAbortAfter")
    private Duration abortAfter;

    @Part
    private Storage storage;

    private final SecureRandom random = new SecureRandom();

    /**
     * Provides the locks of the uploads.
     * <p>
     * A lock is held while an object is assembled, which takes its time for a large one. The stripes are therefore
     * plenty, so that two uploads practically never share a lock, and are only created once they are needed.
     */
    private final Striped<Lock> locks = Striped.lazyWeakLock(1 << 16);

    /**
     * Removes what a previous run left behind when it was stopped in the middle of a request.
     * <p>
     * See {@link #removeLeftovers(Instant)} for what is removed. Only files which this process cannot have written
     * are touched, as they are older than the process itself. Requests which are served while this runs can therefore
     * not lose their files, no matter in which order the system starts its components.
     */
    @Override
    public void started() {
        removeLeftovers(Instant.ofEpochMilli(ManagementFactory.getRuntimeMXBean().getStartTime()));
        abortUploadsOfMissingBuckets();
    }

    /**
     * Aborts the uploads of which the bucket no longer exists.
     * <p>
     * Deleting a bucket aborts its uploads, but an upload which is initiated at that very moment, or a system which is
     * stopped in between, can leave one behind. It would show up again once a bucket with the same name is created.
     */
    void abortUploadsOfMissingBuckets() {
        try {
            listAll().stream()
                     .filter(upload -> !Bucket.isValidName(upload.getBucket()) || !storage.getBucket(upload.getBucket())
                                                                                          .exists())
                     .forEach(this::abort);
        } catch (Exception exception) {
            Exceptions.handle(Storage.LOG, exception);
        }
    }

    /**
     * Removes the leftovers of requests which were never finished.
     * <p>
     * Open uploads and their published parts are kept. Removed are parts which were received but never published,
     * uploads which could not be deleted completely or were never initiated completely, and objects which were still
     * being assembled.
     *
     * @param abandonedBefore the point in time before which a file must have been written last to count as leftover.
     *                        Everything written since then may belong to a request which is still being served.
     */
    void removeLeftovers(Instant abandonedBefore) {
        try {
            File[] folders = new File(multipartDir).listFiles(File::isDirectory);
            if (folders != null) {
                for (File folder : folders) {
                    removeLeftoversOfUpload(folder, abandonedBefore);
                }
            }

            for (Bucket bucket : storage.getBuckets()) {
                File[] assemblingFiles = bucket.getFolder()
                                               .listFiles(file -> isAssemblingFile(file)
                                                                  && file.lastModified()
                                                                     < abandonedBefore.toEpochMilli());
                if (assemblingFiles != null) {
                    for (File assemblingFile : assemblingFiles) {
                        sirius.kernel.commons.Files.delete(assemblingFile);
                    }
                }
            }
        } catch (Exception exception) {
            // Leftovers only waste space, so failing to remove them must not keep the system from starting.
            Exceptions.handle(Storage.LOG, exception);
        }
    }

    /**
     * Removes the parts of the given upload which were never published, or the whole folder if it is no upload.
     * <p>
     * A folder named like an upload, but lacking its metadata, stems from an initiation which was interrupted. No
     * request can ever find it, so it is removed once it is old enough not to be an initiation in progress.
     * <p>
     * A failure is only logged, so that one folder which cannot be cleaned up does not keep the others from being so.
     */
    private void removeLeftoversOfUpload(File folder, Instant abandonedBefore) {
        try {
            Optional<MultipartUpload> upload = MultipartUpload.load(folder);
            if (MultipartUpload.isLeftoverOfDeletedUpload(folder)) {
                // Removing such a folder a second time is harmless, so it needs no check of its age.
                sirius.kernel.commons.Files.delete(folder.toPath());
            } else if (upload.isPresent()) {
                upload.get().removeStagedParts(abandonedBefore);
            } else if (VALID_UPLOAD_ID.matcher(folder.getName()).matches()
                       && folder.lastModified() < abandonedBefore.toEpochMilli()) {
                sirius.kernel.commons.Files.delete(folder.toPath());
            }
        } catch (Exception exception) {
            Exceptions.handle(Storage.LOG, exception);
        }
    }

    private static boolean isAssemblingFile(File file) {
        return file.getName().startsWith("$") && file.getName().endsWith(ASSEMBLING_FILE_SUFFIX);
    }

    /**
     * Returns the file into which the object of the given upload is to be assembled.
     * <p>
     * The file resides next to the object, as only a file on the same file system can replace it in one step.
     *
     * @param bucket the bucket the object is stored in
     * @param upload the upload to complete
     * @return the file to assemble the object into
     */
    public File getAssemblingFile(Bucket bucket, MultipartUpload upload) {
        return new File(bucket.getFolder(), "$" + upload.getUploadId() + ASSEMBLING_FILE_SUFFIX);
    }

    /**
     * Initiates a new upload.
     *
     * @param bucket     the name of the bucket the final object is stored in
     * @param key        the key of the final object
     * @param properties the properties to store along with the final object
     * @return the newly created upload, which carries an upload ID that is opaque to clients and cannot be guessed
     * @throws IOException if the upload cannot be stored on disk
     */
    public MultipartUpload create(String bucket, String key, Map<String, String> properties) throws IOException {
        byte[] randomBytes = new byte[UPLOAD_ID_RANDOM_BYTES];
        random.nextBytes(randomBytes);
        String uploadId =
                String.format("%0" + UPLOAD_ID_TIME_DIGITS + "x", System.currentTimeMillis()) + BaseEncoding.base16()
                                                                                                            .lowerCase()
                                                                                                            .encode(randomBytes);

        return MultipartUpload.create(new File(multipartDir, uploadId), bucket, key, properties);
    }

    /**
     * Looks up the upload with the given ID.
     *
     * @param bucket   the name of the bucket the upload has to belong to
     * @param key      the object key the upload has to belong to
     * @param uploadId the ID of the upload
     * @return the upload, or an empty optional if there is no open upload with this ID for the given bucket and key
     */
    public Optional<MultipartUpload> find(String bucket, String key, String uploadId) {
        if (Strings.isEmpty(uploadId) || !VALID_UPLOAD_ID.matcher(uploadId).matches()) {
            return Optional.empty();
        }

        return MultipartUpload.load(new File(multipartDir, uploadId))
                              .filter(upload -> Strings.areEqual(upload.getBucket(), bucket))
                              .filter(upload -> Strings.areEqual(upload.getKey(), key));
    }

    /**
     * Lists all open uploads of the given bucket.
     *
     * @param bucket the name of the bucket to list the uploads of
     * @return the uploads, sorted by key and then by upload ID, which is the order of their initiation
     */
    public List<MultipartUpload> list(String bucket) {
        List<MultipartUpload> uploads = new ArrayList<>(listAll().stream()
                                                                 .filter(upload -> Strings.areEqual(upload.getBucket(),
                                                                                                    bucket))
                                                                 .toList());
        uploads.sort(Comparator.comparing(MultipartUpload::getKey, MultipartUploads::compareKeys)
                               .thenComparing(MultipartUpload::getUploadId));
        return uploads;
    }

    /**
     * Compares two object keys in the order in which S3 lists them, which is the order of their bytes in UTF-8.
     * <p>
     * The order of Java strings differs from that for characters outside of the basic multilingual plane.
     *
     * @param key1 the first key
     * @param key2 the second key
     * @return a negative number, zero or a positive number if the first key is listed before, at the same position
     * as, or after the second one
     */
    static int compareKeys(String key1, String key2) {
        return Arrays.compareUnsigned(key1.getBytes(StandardCharsets.UTF_8), key2.getBytes(StandardCharsets.UTF_8));
    }

    private List<MultipartUpload> listAll() {
        List<MultipartUpload> uploads = new ArrayList<>();
        File[] folders = new File(multipartDir).listFiles(folder -> folder.isDirectory()
                                                                    && VALID_UPLOAD_ID.matcher(folder.getName())
                                                                                      .matches());
        if (folders != null) {
            for (File folder : folders) {
                MultipartUpload.load(folder).ifPresent(uploads::add);
            }
        }
        return uploads;
    }

    /**
     * Aborts all open uploads of the given bucket.
     * <p>
     * This is to be called when a bucket is deleted, as its uploads would otherwise show up again once a bucket with
     * the same name is created.
     *
     * @param bucket the name of the bucket to abort the uploads of
     */
    public void abortAll(String bucket) {
        list(bucket).forEach(this::abort);
    }

    /**
     * Aborts the uploads which have been open for longer than <tt>storage.multipartAbortAfter</tt>.
     * <p>
     * S3 keeps an upload until it is completed or aborted, and so does this system by default. An upload which a
     * client abandoned therefore occupies disk space for good. On a system which runs for a long time, the setting
     * permits to get rid of these, like a lifecycle rule with <tt>AbortIncompleteMultipartUpload</tt> does for S3.
     */
    @Override
    public void runTimer() {
        abortUploadsOfMissingBuckets();
        if (abortAfter != null && !abortAfter.isZero() && !abortAfter.isNegative()) {
            abortUploadsInitiatedBefore(Instant.now().minus(abortAfter));
        }
    }

    /**
     * Aborts all uploads, of all buckets, which have been initiated before the given point in time.
     * <p>
     * As for S3, the time of initiation counts, not the time of the last part.
     *
     * @param initiatedBefore the point in time before which an upload must have been initiated to be aborted
     */
    void abortUploadsInitiatedBefore(Instant initiatedBefore) {
        listAll().stream().filter(upload -> upload.getInitiated().isBefore(initiatedBefore)).forEach(this::abort);
    }

    /**
     * Removes the given upload, unless it has been completed or aborted while waiting for its lock.
     */
    private void abort(MultipartUpload upload) {
        Lock lock = lockFor(upload.getUploadId());
        lock.lock();
        try {
            if (find(upload.getBucket(), upload.getKey(), upload.getUploadId()).isPresent()) {
                upload.delete();
            }
        } catch (IOException exception) {
            Exceptions.handle(Storage.LOG, exception);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns the lock which guards the parts of the upload with the given ID.
     * <p>
     * It has to be held while completing or aborting the upload, as both remove it, and while making a part visible,
     * so that the parts cannot change while they are verified and assembled.
     *
     * @param uploadId the ID of the upload
     * @return the lock to hold while completing or aborting the upload, or publishing one of its parts
     */
    public Lock lockFor(String uploadId) {
        return locks.get(uploadId);
    }

    /**
     * Returns the minimal size of every part of an upload except the last one.
     * <p>
     * This is 5 MiB for S3, and can be lowered via <tt>storage.multipartMinPartSize</tt> to keep tests fast.
     *
     * @return the minimal part size in bytes
     */
    public long getMinPartSize() {
        return minPartSize;
    }
}
