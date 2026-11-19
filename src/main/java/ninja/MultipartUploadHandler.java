/*
 * Made with all the love in the world
 * by scireum in Stuttgart, Germany
 *
 * Copyright by scireum GmbH
 * https://www.scireum.de - info@scireum.de
 */

package ninja;

import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteStreams;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import ninja.errors.S3ErrorCode;
import sirius.kernel.commons.Hasher;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Tuple;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Exceptions;
import sirius.kernel.health.HandledException;
import sirius.kernel.xml.StructuredNode;
import sirius.kernel.xml.XMLReader;
import sirius.kernel.xml.XMLStructuredOutput;
import sirius.web.http.InputStreamHandler;
import sirius.web.http.Response;
import sirius.web.http.WebContext;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.regex.Pattern;

/**
 * Handles the S3 API calls which deal with multipart uploads.
 * <p>
 * The {@link S3Dispatcher} decides which call a request is, and checks the request and its bucket. This class then
 * performs the call on top of the uploads kept by {@link MultipartUploads}.
 */
@Register(classes = MultipartUploadHandler.class)
public class MultipartUploadHandler {

    private static final String ERROR_MULTIPART_UPLOAD_DOES_NOT_EXIST =
            "The specified upload does not exist. The upload ID may be invalid, or the upload may have been aborted or completed.";
    private static final String ERROR_INVALID_PART =
            "One or more of the specified parts could not be found. The part may not have been uploaded, or the specified entity tag may not match the part's entity tag.";
    private static final String ERROR_ENTITY_TOO_SMALL =
            "Your proposed upload is smaller than the minimum allowed size.";
    private static final String ERROR_INVALID_PART_NUMBER =
            "Part number must be an integer between 1 and 10000, inclusive.";
    private static final String ERROR_INVALID_PART_ORDER =
            "The list of parts was not in ascending order. Parts must be ordered by part number.";
    private static final String ERROR_MALFORMED_XML =
            "The XML you provided was not well-formed or did not validate against our published schema.";

    /**
     * Contains the number of entries S3 returns at most, and by default, when listing parts or uploads.
     */
    private static final int DEFAULT_MAX_LISTING_SIZE = 1000;

    /**
     * Limits the size of the body which is accepted when completing an upload, in bytes.
     * <p>
     * A request naming all 10,000 parts along with every checksum S3 knows stays well below this.
     */
    private static final long MAX_COMPLETE_REQUEST_SIZE = 8L * 1024 * 1024;

    /**
     * Matches integers which are short enough to be parsed safely.
     */
    private static final Pattern PART_NUMBER_PATTERN = Pattern.compile("\\d{1,6}");

    @Part
    private Storage storage;

    @Part
    private MultipartUploads multipartUploads;

    @Part
    private S3Responder responder;

    @Part
    private RequestAuthenticator authenticator;

    @Part
    private ObjectHandler objectHandler;

    /**
     * Handles POST /bucket/id?uploads
     *
     * @param webContext the context describing the current request
     * @param bucket     the bucket containing the object to upload
     * @param id         name of the object to upload
     */
    void startMultipartUpload(WebContext webContext, Bucket bucket, String id) {
        Map<String, String> properties = objectHandler.parseUploadProperties(webContext);

        MultipartUpload upload;
        try {
            upload = multipartUploads.create(bucket.getName(), id, properties);
        } catch (IOException exception) {
            responder.signalObjectError(webContext,
                                        bucket.getName(),
                                        id,
                                        S3ErrorCode.InternalError,
                                        Exceptions.handle(Storage.LOG, exception).getMessage());
            return;
        }

        Response response = webContext.respondWith();
        properties.forEach(response::addHeader);
        response.setHeader(S3Dispatcher.HTTP_HEADER_NAME_CONTENT_TYPE, S3Dispatcher.CONTENT_TYPE_XML);

        XMLStructuredOutput out = response.xml();
        out.beginOutput("InitiateMultipartUploadResult");
        out.property(S3Dispatcher.RESPONSE_BUCKET, bucket.getName());
        out.property("Key", id);
        out.property("UploadId", upload.getUploadId());
        out.endOutput();
        responder.signalObjectSuccess(webContext);
    }

    /**
     * Looks up an open multipart upload, and answers the request with <tt>NoSuchUpload</tt> if there is none.
     *
     * @param webContext the context describing the current request
     * @param bucket     the bucket the upload has to belong to
     * @param id         the object key the upload has to belong to
     * @param uploadId   the ID of the upload as given by the client
     * @return the upload, or an empty optional if the request has been answered with an error
     */
    private Optional<MultipartUpload> findMultipartUpload(WebContext webContext,
                                                          Bucket bucket,
                                                          String id,
                                                          String uploadId) {
        Optional<MultipartUpload> upload = multipartUploads.find(bucket.getName(), id, uploadId);
        if (upload.isEmpty()) {
            responder.signalObjectError(webContext,
                                        bucket.getName(),
                                        id,
                                        S3ErrorCode.NoSuchUpload,
                                        ERROR_MULTIPART_UPLOAD_DOES_NOT_EXIST);
        }
        return upload;
    }

    /**
     * Handles PUT /bucket/id?uploadId=X&partNumber=Y
     *
     * @param webContext the context describing the current request
     * @param bucket     the bucket containing the object to upload
     * @param id         name of the object to upload
     * @param uploadId   the multipart upload this part belongs to
     * @param partNumber the number of this part in the complete upload
     * @param part       input stream with the content of this part
     */
    void uploadPart(WebContext webContext,
                    Bucket bucket,
                    String id,
                    String uploadId,
                    String partNumber,
                    InputStreamHandler part) throws IOException {
        Optional<Integer> number = parsePartNumber(partNumber);
        if (number.isEmpty()) {
            ByteStreams.exhaust(part);
            responder.signalObjectError(webContext,
                                        bucket.getName(),
                                        id,
                                        S3ErrorCode.InvalidArgument,
                                        ERROR_INVALID_PART_NUMBER);
            return;
        }

        Optional<MultipartUpload> upload = findMultipartUpload(webContext, bucket, id, uploadId);
        if (upload.isEmpty()) {
            ByteStreams.exhaust(part);
            return;
        }

        MultipartUpload.Part stagedPart = null;
        try {
            stagedPart = upload.get().stagePart(number.get(), part);
            part.close();

            // Checked on the staged file, so that a refused request never touches a part stored by another one.
            byte[] md5 = stagedPart.md5();
            String contentMd5 = webContext.getHeader("Content-MD5");
            if (Strings.isFilled(contentMd5) && !Strings.areEqual(contentMd5,
                                                                  Base64.getEncoder().encodeToString(md5))) {
                upload.get().discardPart(stagedPart);
                responder.signalObjectError(webContext,
                                            bucket.getName(),
                                            id,
                                            S3ErrorCode.BadDigest,
                                            "The Content-MD5 you specified did not match what we received.");
                return;
            }

            publishPart(bucket, id, uploadId, stagedPart);

            String etag = BaseEncoding.base16().lowerCase().encode(md5);
            webContext.respondWith()
                      .setHeader(S3Dispatcher.HTTP_HEADER_NAME_ETAG, etag)
                      .addHeader(HttpHeaderNames.ACCESS_CONTROL_EXPOSE_HEADERS, S3Dispatcher.HTTP_HEADER_NAME_ETAG)
                      .status(HttpResponseStatus.OK);
            responder.signalObjectSuccess(webContext);
        } catch (IOException | HandledException exception) {
            // Hashing reports a file which is gone as HandledException, which is what remains of a part once its
            // upload has been removed. The stream may be closed already, which exhausting it this way tolerates.
            part.exhaust();
            discardStagedPart(stagedPart);
            signalPartNotStored(webContext, bucket, id, uploadId, exception);
        }
    }

    /**
     * Removes what a failed request has staged, so that failures do not pile up files nobody will ever ask for.
     *
     * @param stagedPart the staged part, or <tt>null</tt> if the request failed before staging anything
     */
    private void discardStagedPart(@Nullable MultipartUpload.Part stagedPart) {
        if (stagedPart != null) {
            // Harmless if the part has been published meanwhile, as publishing moves the staged file away.
            sirius.kernel.commons.Files.delete(stagedPart.file());
        }
    }

    /**
     * Makes a staged part a part of its upload.
     * <p>
     * This takes the lock which also guards completing and aborting the upload. A part is therefore never replaced
     * between being verified and being assembled, and is never moved into an upload which is being removed.
     *
     * @param bucket     the bucket the upload belongs to
     * @param id         the object key the upload belongs to
     * @param uploadId   the ID of the upload
     * @param stagedPart the part to publish
     * @return the published part
     * @throws IOException if the upload has been completed or aborted meanwhile, or the part cannot be moved
     */
    private MultipartUpload.Part publishPart(Bucket bucket, String id, String uploadId, MultipartUpload.Part stagedPart)
            throws IOException {
        Lock lock = multipartUploads.lockFor(uploadId);
        lock.lock();
        try {
            Optional<MultipartUpload> upload = multipartUploads.find(bucket.getName(), id, uploadId);
            if (upload.isEmpty()) {
                throw new IOException("The upload has been completed or aborted.");
            }
            return upload.get().publishPart(stagedPart);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Answers a request of which the part could not be stored.
     * <p>
     * This is what happens to a part which is still on its way while its upload is completed or aborted, which is
     * reported as <tt>NoSuchUpload</tt> just as if the part had arrived a moment later.
     */
    private void signalPartNotStored(WebContext webContext,
                                     Bucket bucket,
                                     String id,
                                     String uploadId,
                                     Exception exception) {
        if (multipartUploads.find(bucket.getName(), id, uploadId).isEmpty()) {
            Exceptions.ignore(exception);
            responder.signalObjectError(webContext,
                                        bucket.getName(),
                                        id,
                                        S3ErrorCode.NoSuchUpload,
                                        ERROR_MULTIPART_UPLOAD_DOES_NOT_EXIST);
            return;
        }

        responder.signalObjectError(webContext,
                                    bucket.getName(),
                                    id,
                                    S3ErrorCode.InternalError,
                                    Exceptions.handle(Storage.LOG, exception).getMessage());
    }

    /**
     * Parses the part number given by the client.
     * <p>
     * The value is used as file name, so anything but a number in the range accepted by S3 has to be refused.
     *
     * @param partNumber the part number as found in the query string
     * @return the part number, or an empty optional if the value is not an integer between
     * {@link MultipartUpload#MIN_PART_NUMBER} and {@link MultipartUpload#MAX_PART_NUMBER}
     */
    private Optional<Integer> parsePartNumber(String partNumber) {
        if (Strings.isEmpty(partNumber) || !PART_NUMBER_PATTERN.matcher(partNumber).matches()) {
            return Optional.empty();
        }

        int number = Integer.parseInt(partNumber);
        if (number < MultipartUpload.MIN_PART_NUMBER || number > MultipartUpload.MAX_PART_NUMBER) {
            return Optional.empty();
        }
        return Optional.of(number);
    }

    /**
     * Handles POST /bucket/id?uploadId=X
     * <p>
     * The upload is only removed once the object has been assembled. A request which is rejected leaves the upload
     * untouched, so that the client can correct and repeat it.
     *
     * @param webContext the context describing the current request
     * @param bucket     the bucket containing the object to upload
     * @param id         name of the object to upload
     * @param uploadId   the multipart upload that should be completed
     * @param in         input stream with xml listing uploaded parts
     */
    void completeMultipartUpload(WebContext webContext,
                                 Bucket bucket,
                                 String id,
                                 final String uploadId,
                                 InputStreamHandler in) throws IOException {
        // Checked before reading the body, so that a request for an upload which does not exist costs next to nothing.
        if (multipartUploads.find(bucket.getName(), id, uploadId).isEmpty()) {
            in.exhaust();
            findMultipartUpload(webContext, bucket, id, uploadId);
            return;
        }

        // The body is read before the lock is taken, so that a slow client cannot block other uploads.
        List<Tuple<Integer, String>> requestedParts = new ArrayList<>();
        boolean wellFormed = parseRequestedParts(in, requestedParts);

        Lock lock = multipartUploads.lockFor(uploadId);
        lock.lock();
        try {
            Optional<MultipartUpload> upload = findMultipartUpload(webContext, bucket, id, uploadId);
            if (upload.isEmpty()) {
                return;
            }
            if (!wellFormed || requestedParts.isEmpty()) {
                responder.signalObjectError(webContext,
                                            bucket.getName(),
                                            id,
                                            S3ErrorCode.MalformedXML,
                                            ERROR_MALFORMED_XML);
                return;
            }

            completeMultipartUpload(webContext, bucket, id, upload.get(), requestedParts);
        } finally {
            lock.unlock();
        }
    }

    private void completeMultipartUpload(WebContext webContext,
                                         Bucket bucket,
                                         String id,
                                         MultipartUpload upload,
                                         List<Tuple<Integer, String>> requestedParts) {
        List<MultipartUpload.Part> parts = new ArrayList<>();
        Optional<Tuple<S3ErrorCode, String>> error = verifyRequestedParts(upload, requestedParts, parts);
        if (error.isPresent()) {
            responder.signalObjectError(webContext,
                                        bucket.getName(),
                                        id,
                                        error.get().getFirst(),
                                        error.get().getSecond());
            return;
        }

        StoredObject object = bucket.getObject(id);

        // Assembled next to the object, as only a file on the same file system can replace it in one step.
        File assembledFile = multipartUploads.getAssemblingFile(bucket, upload);
        try {
            upload.assemble(parts, assembledFile);

            String etag = Hasher.md5().hashFile(assembledFile).toHexString();
            Map<String, String> properties = upload.getProperties();
            properties.put(S3Dispatcher.HTTP_HEADER_NAME_ETAG, etag);
            publishObject(object, assembledFile, properties);
            removeCompletedUpload(upload);

            XMLStructuredOutput out = webContext.respondWith().xml();
            out.beginOutput("CompleteMultipartUploadResult");
            out.property("Location", "");
            out.property(S3Dispatcher.RESPONSE_BUCKET, bucket.getName());
            out.property("Key", id);
            out.property(S3Dispatcher.HTTP_HEADER_NAME_ETAG, etag);
            out.endOutput();
            responder.signalObjectSuccess(webContext);
        } catch (IOException exception) {
            responder.signalObjectError(webContext,
                                        bucket.getName(),
                                        id,
                                        S3ErrorCode.InternalError,
                                        Exceptions.handle(Storage.LOG, exception).getMessage());
        } finally {
            // Gone if the object has been published. Whatever else happened, the file must not stay behind.
            sirius.kernel.commons.Files.delete(assembledFile);
        }
    }

    /**
     * Removes an upload of which the object has been published.
     * <p>
     * The object is in place at this point, so the request has succeeded no matter what happens here. Answering with
     * an error would make the client repeat or give up an upload which is done. If the upload cannot be removed, it
     * stays listed as open, and completing it again yields the same object.
     *
     * @param upload the upload to remove
     */
    private void removeCompletedUpload(MultipartUpload upload) {
        try {
            upload.delete();
        } catch (IOException exception) {
            Exceptions.handle(Storage.LOG, exception);
        }
    }

    /**
     * Makes the assembled file and its properties the new state of the given object.
     * <p>
     * The properties are written before the content appears. A reader which finds an object without an entity tag
     * computes and stores one, which, the other way round, could replace the properties of a freshly completed
     * upload by the hash of its content. Content and properties are still two files, so a reader may briefly see
     * the previous content along with the new properties, as for any other way of writing an object.
     *
     * @param object        the object to replace or to create
     * @param assembledFile the file holding the new content, which has to reside in the folder of the bucket
     * @param properties    the properties of the new object
     * @throws IOException if the object cannot be written, in which case its previous state is kept
     */
    private void publishObject(StoredObject object, File assembledFile, Map<String, String> properties)
            throws IOException {
        Map<String, String> previousProperties = object.getPropertiesFile().exists() ? object.getProperties() : null;
        try {
            // Guarded as well, as a write which fails halfway leaves a damaged properties file behind.
            object.setProperties(properties);
            java.nio.file.Files.move(assembledFile.toPath(),
                                     object.getFile().toPath(),
                                     StandardCopyOption.ATOMIC_MOVE,
                                     StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException exception) {
            if (previousProperties != null) {
                object.setProperties(previousProperties);
            } else {
                sirius.kernel.commons.Files.delete(object.getPropertiesFile());
            }
            throw exception;
        }
    }

    /**
     * Reads the list of parts sent along when completing a multipart upload.
     * <p>
     * The body has to be a <tt>CompleteMultipartUpload</tt> element of which every <tt>Part</tt> names an integer as
     * <tt>PartNumber</tt> and an <tt>ETag</tt>. Whether the numbers are in range is not decided here, as that is
     * reported as another error.
     *
     * @param in             input stream with xml listing the parts which make up the object
     * @param requestedParts receives the number and the entity tag of each part, in the order given by the client
     * @return <tt>true</tt> if the body is well-formed and of the expected structure, <tt>false</tt> otherwise
     */
    private boolean parseRequestedParts(InputStreamHandler in, List<Tuple<Integer, String>> requestedParts) {
        AtomicBoolean valid = new AtomicBoolean(false);

        XMLReader reader = new XMLReader();
        reader.addHandler("CompleteMultipartUpload", root -> {
            valid.set(true);
            for (StructuredNode part : root.queryNodeList("Part")) {
                String partNumber = part.queryValue("PartNumber").asString().trim();
                String etag = part.queryValue(S3Dispatcher.HTTP_HEADER_NAME_ETAG).asString().trim();
                if (!PART_NUMBER_PATTERN.matcher(partNumber).matches() || Strings.isEmpty(etag)) {
                    valid.set(false);
                    return;
                }
                requestedParts.add(Tuple.create(Integer.parseInt(partNumber), etag));
            }
        });
        try {
            // The whole body is held in memory while it is parsed. A body beyond the limit is cut off, and what is
            // left of it is refused as malformed.
            reader.parse(ByteStreams.limit(in, MAX_COMPLETE_REQUEST_SIZE));
            return valid.get();
        } catch (IOException exception) {
            Exceptions.ignore(exception);
            return false;
        } finally {
            in.exhaust();
        }
    }

    /**
     * Checks the list of parts sent along when completing a multipart upload against the parts which have actually
     * been uploaded.
     * <p>
     * Nothing is changed here, so that a rejected request leaves the upload as it was.
     *
     * @param upload         the upload to complete
     * @param requestedParts the number and the entity tag of each part, in the order given by the client
     * @param parts          receives the parts to assemble in their order
     * @return the error to answer the request with, or an empty optional if the list of parts is valid
     */
    private Optional<Tuple<S3ErrorCode, String>> verifyRequestedParts(MultipartUpload upload,
                                                                      List<Tuple<Integer, String>> requestedParts,
                                                                      List<MultipartUpload.Part> parts) {
        int previousPartNumber = 0;
        for (Tuple<Integer, String> requestedPart : requestedParts) {
            if (requestedPart.getFirst() < MultipartUpload.MIN_PART_NUMBER
                || requestedPart.getFirst() > MultipartUpload.MAX_PART_NUMBER) {
                return Optional.of(Tuple.create(S3ErrorCode.InvalidArgument, ERROR_INVALID_PART_NUMBER));
            }
            if (requestedPart.getFirst() <= previousPartNumber) {
                return Optional.of(Tuple.create(S3ErrorCode.InvalidPartOrder, ERROR_INVALID_PART_ORDER));
            }
            previousPartNumber = requestedPart.getFirst();
        }

        for (Tuple<Integer, String> requestedPart : requestedParts) {
            Optional<MultipartUpload.Part> part = upload.getPart(requestedPart.getFirst());
            if (part.isEmpty()) {
                return Optional.of(Tuple.create(S3ErrorCode.InvalidPart, ERROR_INVALID_PART));
            }

            String etag = BaseEncoding.base16().lowerCase().encode(part.get().md5());
            if (!etag.equalsIgnoreCase(unquoteEtag(requestedPart.getSecond()))) {
                return Optional.of(Tuple.create(S3ErrorCode.InvalidPart, ERROR_INVALID_PART));
            }
            parts.add(part.get());
        }

        // Only the last part may be smaller than the minimum, which is why this cannot be checked on upload.
        MultipartUpload.Part lastPart = upload.getPart(previousPartNumber).orElse(null);
        for (MultipartUpload.Part part : parts) {
            if (!part.equals(lastPart) && part.size() < multipartUploads.getMinPartSize()) {
                return Optional.of(Tuple.create(S3ErrorCode.EntityTooSmall, ERROR_ENTITY_TOO_SMALL));
            }
        }

        return Optional.empty();
    }

    /**
     * Removes the quotes which surround an entity tag on the wire.
     * <p>
     * Clients send entity tags back as they received them, which may be with or without quotes.
     */
    private String unquoteEtag(String etag) {
        String result = Strings.isEmpty(etag) ? "" : etag.trim();
        if (result.length() >= 2 && result.startsWith("\"") && result.endsWith("\"")) {
            return result.substring(1, result.length() - 1);
        }
        return result;
    }

    /**
     * Handles DELETE /bucket/id?uploadId=X
     *
     * @param webContext the context describing the current request
     * @param bucket     the bucket containing the object to upload
     * @param id         name of the object to upload
     * @param uploadId   the multipart upload that should be cancelled
     */
    void abortMultipartUpload(WebContext webContext, Bucket bucket, String id, String uploadId) {
        Lock lock = multipartUploads.lockFor(uploadId);
        lock.lock();
        try {
            Optional<MultipartUpload> upload = findMultipartUpload(webContext, bucket, id, uploadId);
            if (upload.isEmpty()) {
                return;
            }

            upload.get().delete();

            webContext.respondWith().status(HttpResponseStatus.NO_CONTENT);
            responder.signalObjectSuccess(webContext);
        } catch (IOException exception) {
            responder.signalObjectError(webContext,
                                        bucket.getName(),
                                        id,
                                        S3ErrorCode.InternalError,
                                        Exceptions.handle(Storage.LOG, exception).getMessage());
        } finally {
            lock.unlock();
        }
    }

    /**
     * Handles GET /bucket/id?uploadId=uploadId
     * <p>
     * The parts are listed in ascending order of their numbers. A client resuming an upload relies on that, and on
     * <tt>part-number-marker</tt> and <tt>max-parts</tt> to page through the parts.
     *
     * @param webContext the context describing the current request
     * @param bucket     the bucket containing the object to download
     * @param id         name of the object to use as download
     * @param uploadId   the multipart upload to list the parts of
     */
    void getPartList(WebContext webContext, Bucket bucket, String id, String uploadId) {
        int marker = webContext.get("part-number-marker").asInt(0);
        int maxParts = webContext.get("max-parts").asInt(DEFAULT_MAX_LISTING_SIZE);
        if (maxParts < 0 || maxParts > DEFAULT_MAX_LISTING_SIZE) {
            maxParts = DEFAULT_MAX_LISTING_SIZE;
        }

        List<ListedPart> parts = new ArrayList<>();
        boolean truncated;

        // The parts are read under the lock of the upload, as aborting or completing it removes their files, and a
        // part which is uploaded again replaces its file. The response is written once the lock is released.
        Lock lock = multipartUploads.lockFor(uploadId);
        lock.lock();
        try {
            Optional<MultipartUpload> upload = findMultipartUpload(webContext, bucket, id, uploadId);
            if (upload.isEmpty()) {
                return;
            }

            List<MultipartUpload.Part> remainingParts =
                    upload.get().getParts().stream().filter(part -> part.number() > marker).toList();
            // Nothing can follow a page of no parts, which would otherwise keep a client paging forever.
            truncated = maxParts > 0 && remainingParts.size() > maxParts;
            remainingParts.stream()
                          .limit(maxParts)
                          .map(part -> new ListedPart(part.number(),
                                                      part.lastModified(),
                                                      BaseEncoding.base16().lowerCase().encode(part.md5()),
                                                      part.size()))
                          .forEach(parts::add);
        } finally {
            lock.unlock();
        }

        Response response = webContext.respondWith();

        response.setHeader(S3Dispatcher.HTTP_HEADER_NAME_CONTENT_TYPE, S3Dispatcher.CONTENT_TYPE_XML);

        XMLStructuredOutput out = response.xml();
        out.beginOutput("ListPartsResult");
        out.property(S3Dispatcher.RESPONSE_BUCKET, bucket.getName());
        out.property("Key", id);
        out.property("UploadId", uploadId);

        responder.outputOwnerInfo(out, "Initiator");
        responder.outputOwnerInfo(out, "Owner");

        out.property("StorageClass", "STANDARD");
        out.property("PartNumberMarker", marker);
        out.property("NextPartNumberMarker", parts.isEmpty() ? marker : parts.getLast().number());
        out.property("MaxParts", maxParts);
        out.property("IsTruncated", truncated);

        for (ListedPart part : parts) {
            out.beginObject("Part");
            out.property("PartNumber", part.number());
            out.property("LastModified", S3Dispatcher.ISO8601_INSTANT.format(part.lastModified()));
            out.property(S3Dispatcher.HTTP_HEADER_NAME_ETAG, part.etag());
            out.property("Size", part.size());
            out.endObject();
        }

        out.endOutput();
        responder.signalObjectSuccess(webContext);
    }

    /**
     * Describes a part as it is listed, read while its file cannot change.
     *
     * @param number       the number of the part
     * @param lastModified the point in time when the part has been uploaded
     * @param etag         the entity tag of the part, without quotes
     * @param size         the size of the part in bytes
     */
    private record ListedPart(int number, Instant lastModified, String etag, long size) {
    }
}
