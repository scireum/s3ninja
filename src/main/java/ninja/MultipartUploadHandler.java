/*
 * Made with all the love in the world
 * by scireum in Stuttgart, Germany
 *
 * Copyright by scireum GmbH
 * https://www.scireum.de - info@scireum.de
 */

package ninja;

import com.google.common.collect.Maps;
import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import ninja.errors.S3ErrorCode;
import ninja.errors.S3ErrorSynthesizer;
import sirius.kernel.commons.Hasher;
import sirius.kernel.commons.Strings;
import sirius.kernel.di.std.ConfigValue;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Counter;
import sirius.kernel.health.Exceptions;
import sirius.kernel.xml.XMLReader;
import sirius.kernel.xml.XMLStructuredOutput;
import sirius.web.http.InputStreamHandler;
import sirius.web.http.Response;
import sirius.web.http.WebContext;

import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

/**
 * Handles the S3 API calls which deal with multipart uploads.
 */
@Register(classes = MultipartUploadHandler.class)
public class MultipartUploadHandler {

    private static final String TEMPORARY_PROPERTIES_FILENAME = "properties";

    private static final String PATH_DELIMITER = "/";

    private static final String ERROR_MULTIPART_UPLOAD_DOES_NOT_EXIST = "Multipart Upload does not exist";

    @ConfigValue("storage.multipartDir")
    private String multipartDir;

    private final Set<String> multipartUploads = Collections.synchronizedSet(new TreeSet<>());

    private final Counter uploadIdCounter = new Counter();

    @Part
    private S3ErrorSynthesizer errorSynthesizer;

    @Part
    private S3Responder responder;

    /**
     * Handles POST /bucket/id?uploads
     *
     * @param webContext the context describing the current request
     * @param bucket     the bucket containing the object to upload
     * @param id         name of the object to upload
     */
    void startMultipartUpload(WebContext webContext, Bucket bucket, String id) {
        Response response = webContext.respondWith();

        Map<String, String> properties = Maps.newTreeMap();
        for (String name : webContext.getRequest().headers().names()) {
            String nameLower = name.toLowerCase();
            if (nameLower.startsWith("x-amz-") || "content-md5".equals(nameLower) || "content-type".equals(nameLower)) {
                properties.put(name, webContext.getHeader(name));
                response.addHeader(name, webContext.getHeader(name));
            }
        }
        response.setHeader(S3Dispatcher.HTTP_HEADER_NAME_CONTENT_TYPE, S3Dispatcher.CONTENT_TYPE_XML);

        String uploadId = String.valueOf(uploadIdCounter.inc());
        multipartUploads.add(uploadId);

        getUploadDir(uploadId).mkdirs();

        storePropertiesInUploadDir(properties, uploadId);

        XMLStructuredOutput out = response.xml();
        out.beginOutput("InitiateMultipartUploadResult");
        out.property(S3Dispatcher.RESPONSE_BUCKET, bucket.getName());
        out.property("Key", id);
        out.property("UploadId", uploadId);
        out.endOutput();
    }

    private void storePropertiesInUploadDir(Map<String, String> properties, String uploadId) {
        Properties clonedProperties = new Properties();
        properties.forEach(clonedProperties::setProperty);

        try (FileOutputStream outputStream = new FileOutputStream(new File(getUploadDir(uploadId),
                                                                           TEMPORARY_PROPERTIES_FILENAME))) {
            clonedProperties.store(outputStream, "");
        } catch (IOException exception) {
            Exceptions.handle(exception);
        }
    }

    /**
     * Handles PUT /bucket/id?uploadId=X&partNumber=Y
     *
     * @param webContext the context describing the current request
     * @param uploadId   the multipart upload this part belongs to
     * @param partNumber the number of this part in the complete upload
     * @param part       input stream with the content of this part
     */
    void multiObject(WebContext webContext, String uploadId, String partNumber, InputStreamHandler part) {
        if (!multipartUploads.contains(uploadId)) {
            errorSynthesizer.synthesiseError(webContext,
                                             null,
                                             null,
                                             S3ErrorCode.NoSuchUpload,
                                             ERROR_MULTIPART_UPLOAD_DOES_NOT_EXIST);
            return;
        }

        try {
            File partFile = new File(getUploadDir(uploadId), partNumber);
            partFile.deleteOnExit();
            Files.touch(partFile);

            try (FileOutputStream out = new FileOutputStream(partFile)) {
                ByteStreams.copy(part, out);
            }
            part.close();

            String etag = BaseEncoding.base16().encode(Hasher.md5().hashFile(partFile).toHash()).toLowerCase();
            webContext.respondWith()
                      .setHeader(S3Dispatcher.HTTP_HEADER_NAME_ETAG, etag)
                      .addHeader(HttpHeaderNames.ACCESS_CONTROL_EXPOSE_HEADERS, S3Dispatcher.HTTP_HEADER_NAME_ETAG)
                      .status(HttpResponseStatus.OK);
        } catch (IOException exception) {
            errorSynthesizer.synthesiseError(webContext,
                                             null,
                                             null,
                                             S3ErrorCode.InternalError,
                                             Exceptions.handle(exception).getMessage());
        }
    }

    /**
     * Handles POST /bucket/id?uploadId=X
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
                                 InputStreamHandler in) {
        if (!multipartUploads.remove(uploadId)) {
            errorSynthesizer.synthesiseError(webContext,
                                             null,
                                             null,
                                             S3ErrorCode.NoSuchUpload,
                                             ERROR_MULTIPART_UPLOAD_DOES_NOT_EXIST);
            return;
        }

        final Map<Integer, File> parts = new HashMap<>();

        XMLReader reader = new XMLReader();
        reader.addHandler("Part", part -> {
            int number = part.queryValue("PartNumber").asInt(0);
            parts.put(number, new File(getUploadDir(uploadId), String.valueOf(number)));
        });
        try {
            reader.parse(in);
        } catch (IOException exception) {
            Exceptions.handle(exception);
        }

        File file = combineParts(id,
                                 uploadId,
                                 parts.entrySet()
                                      .stream()
                                      .sorted(Map.Entry.comparingByKey())
                                      .map(Map.Entry::getValue)
                                      .toList());

        file.deleteOnExit();
        if (!file.exists()) {
            errorSynthesizer.synthesiseError(webContext,
                                             null,
                                             null,
                                             S3ErrorCode.NoSuchUpload,
                                             "Multipart File does not exist");
            return;
        }
        try {
            StoredObject object = bucket.getObject(id);
            Files.move(file, object.getFile());

            commitPropertiesFromUploadDir(uploadId, object);

            delete(getUploadDir(uploadId));

            String etag = Hasher.md5().hashFile(object.getFile()).toHexString();

            // update ETAG of the underlying object
            Map<String, String> properties = object.getProperties();
            properties.put(S3Dispatcher.HTTP_HEADER_NAME_ETAG, etag);
            object.setProperties(properties);

            XMLStructuredOutput out = webContext.respondWith().xml();
            out.beginOutput("CompleteMultipartUploadResult");
            out.property("Location", "");
            out.property(S3Dispatcher.RESPONSE_BUCKET, bucket.getName());
            out.property("Key", id);
            out.property(S3Dispatcher.HTTP_HEADER_NAME_ETAG, etag);
            out.endOutput();
        } catch (IOException exception) {
            Exceptions.ignore(exception);
            errorSynthesizer.synthesiseError(webContext,
                                             null,
                                             null,
                                             S3ErrorCode.InternalError,
                                             "Could not build response");
        }
    }

    private void commitPropertiesFromUploadDir(String uploadId, StoredObject object) throws IOException {
        File propertiesFile = new File(getUploadDir(uploadId), TEMPORARY_PROPERTIES_FILENAME);
        if (propertiesFile.exists()) {
            Files.move(propertiesFile, object.getPropertiesFile());
        }
    }

    private File getUploadDir(String uploadId) {
        return new File(multipartDir + PATH_DELIMITER + uploadId);
    }

    private File combineParts(String id, String uploadId, List<File> parts) {
        File file = new File(getUploadDir(uploadId), StoredObject.encodeKey(id));

        try {
            if (!file.createNewFile()) {
                Storage.LOG.WARN("Failed to create multipart result file %s (%s).",

                                 file.getName(), file.getAbsolutePath());
            }
            try (FileOutputStream outStream = new FileOutputStream(file);
                 FileChannel outChannel = outStream.getChannel()) {
                combine(parts, outChannel);
            }
        } catch (IOException exception) {
            throw Exceptions.handle(exception);
        }

        return file;
    }

    private void combine(List<File> parts, FileChannel out) throws IOException {
        for (File part : parts) {
            try (RandomAccessFile randomAccessToPart = new RandomAccessFile(part, "r")) {
                out.write(randomAccessToPart.getChannel()
                                            .map(FileChannel.MapMode.READ_ONLY, 0, randomAccessToPart.length()));
            }
        }
    }

    /**
     * Handles DELETE /bucket/id?uploadId=X
     *
     * @param webContext the context describing the current request
     * @param uploadId   the multipart upload that should be cancelled
     */
    void abortMultipartUpload(WebContext webContext, String uploadId) {
        multipartUploads.remove(uploadId);
        webContext.respondWith().status(HttpResponseStatus.OK);
        delete(getUploadDir(uploadId));
    }

    static void delete(File file) {
        try {
            sirius.kernel.commons.Files.delete(file.toPath());
        } catch (IOException exception) {
            Exceptions.handle(Storage.LOG, exception);
        }
    }

    /**
     * Handles GET /bucket/id?uploadId=uploadId
     *
     * @param webContext the context describing the current request
     * @param bucket     the bucket containing the object to download
     * @param id         name of the object to use as download
     */
    void getPartList(WebContext webContext, Bucket bucket, String id, String uploadId) {
        if (!multipartUploads.contains(uploadId)) {
            errorSynthesizer.synthesiseError(webContext,
                                             null,
                                             null,
                                             S3ErrorCode.NoSuchUpload,
                                             ERROR_MULTIPART_UPLOAD_DOES_NOT_EXIST);
            return;
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

        File uploadDir = getUploadDir(uploadId);
        int marker = webContext.get("part-number-marker").asInt(0);
        int maxParts = webContext.get("max-parts").asInt(0);

        FileFilter filter = file -> !Strings.areEqual(file.getName(), TEMPORARY_PROPERTIES_FILENAME);
        File[] parts = Objects.requireNonNull(uploadDir.listFiles(filter));

        out.property("StorageClass", "STANDARD");
        out.property("PartNumberMarker", marker);
        if ((marker + maxParts) < parts.length) {
            out.property("NextPartNumberMarker", marker + maxParts + 1);
        }

        if (Strings.isFilled(maxParts)) {
            out.property("MaxParts", maxParts);
        }

        boolean truncated = 0 < maxParts && maxParts < parts.length;
        out.property("IsTruncated", truncated);

        for (File part : parts) {
            out.beginObject("Part");
            out.property("PartNumber", part.getName());
            out.property("LastModified",
                         S3Dispatcher.ISO8601_INSTANT.format(Instant.ofEpochMilli(part.lastModified())));
            out.property(S3Dispatcher.HTTP_HEADER_NAME_ETAG, Hasher.md5().hashFile(part).toHexString());
            out.property("Size", part.length());
            out.endObject();
        }

        out.endOutput();
    }
}
