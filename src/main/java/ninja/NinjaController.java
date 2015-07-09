/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package ninja;

import com.google.common.collect.Maps;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import sirius.kernel.commons.PriorityCollector;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Exceptions;
import sirius.kernel.health.HandledException;
import sirius.web.controller.Controller;
import sirius.web.controller.Message;
import sirius.web.controller.Routed;
import sirius.web.http.MimeHelper;
import sirius.web.http.Response;
import sirius.web.http.WebContext;
import sirius.web.security.UserContext;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Takes care of the "management ui".
 * <p>
 * Handles all requests required to render the web-pages.
 */
@Register
public class NinjaController implements Controller {

    @Override
    public void onError(WebContext ctx, HandledException error) {
        if (error != null) {
            UserContext.message(Message.error(error.getMessage()));
        }
        List<Bucket> buckets = Collections.emptyList();
        try {
            buckets = storage.getBuckets();
        } catch (HandledException e) {
            UserContext.message(Message.error(e.getMessage()));
        }
        ctx.respondWith()
           .template("view/index.html",
                     buckets,
                     storage.getBasePath(),
                     storage.getAwsAccessKey(),
                     storage.getAwsSecretKey());
    }

    @Part
    private Storage storage;

    @Part
    private APILog log;

    /**
     * Handles requests to /
     *
     * @param ctx the context describing the current request
     */
    @Routed("/")
    public void index(WebContext ctx) {
        if (ctx.isPOST() && ctx.get("bucketName").isFilled()) {
            storage.getBucket(ctx.get("bucketName").asString()).create();
            UserContext.message(Message.info("Bucket successfully created."));
        }
        onError(ctx, null);
    }

    /**
     * Handles requests to /ui/license
     *
     * @param ctx the context describing the current request
     */
    @Routed(value = "/ui/license", priority = PriorityCollector.DEFAULT_PRIORITY - 1)
    public void license(WebContext ctx) {
        ctx.respondWith().template("view/license.html");
    }

    /**
     * Handles requests to /ui/api
     *
     * @param ctx the context describing the current request
     */
    @Routed(value = "/ui/api", priority = PriorityCollector.DEFAULT_PRIORITY - 1)
    public void api(WebContext ctx) {
        ctx.respondWith().template("view/api.html");
    }

    /**
     * Handles requests to /ui/log
     *
     * @param ctx the context describing the current request
     */
    @Routed(value = "/ui/log", priority = PriorityCollector.DEFAULT_PRIORITY - 1)
    public void log(WebContext ctx) {
        int start = ctx.get("start").asInt(1) - 1;
        int pageSize = 50;

        List<APILog.Entry> entries = log.getEntries(start, pageSize + 1);
        boolean canPagePrev = start > 0;
        boolean canPageNext = entries.size() > pageSize;
        if (canPageNext) {
            entries.remove(entries.size() - 1);
        }
        ctx.respondWith()
           .template("view/log.html",
                     entries,
                     canPagePrev,
                     canPageNext,
                     (start + 1) + " - " + (start + entries.size()),
                     Math.max(1, start - pageSize + 1),
                     start + pageSize + 1);
    }

    /**
     * Handles requests to /ui/[bucketName]
     * <p>
     * This will list the contents of the bucket.
     * </p>
     *
     * @param ctx        the context describing the current request
     * @param bucketName name of the bucket to show
     */
    @Routed("/ui/:1")
    public void bucket(WebContext ctx, String bucketName) {
        Bucket bucket = storage.getBucket(bucketName);
        ctx.respondWith().template("view/bucket.html", bucket);
    }

    /**
     * Handles requests to /ui/[bucketName]/[object]
     * <p>
     * This will start a download of the requested object. No access checks will be performed.
     * </p>
     *
     * @param ctx        the context describing the current request
     * @param bucketName name of the bucket to show
     * @param id         the name of the object to fetch
     */
    @Routed("/ui/:1/:2")
    public void object(WebContext ctx, String bucketName, String id) {
        try {
            Bucket bucket = storage.getBucket(bucketName);
            if (!bucket.exists()) {
                ctx.respondWith().error(HttpResponseStatus.NOT_FOUND, "Bucket does not exist");
                return;
            }
            StoredObject object = bucket.getObject(id);
            if (!object.exists()) {
                ctx.respondWith().error(HttpResponseStatus.NOT_FOUND, "Object does not exist");
                return;
            }
            Response response = ctx.respondWith();
            for (Map.Entry<Object, Object> entry : object.getProperties()) {
                response.addHeader(entry.getKey().toString(), entry.getValue().toString());
            }
            response.file(object.getFile());
        } catch (Exception e) {
            ctx.respondWith().error(HttpResponseStatus.BAD_REQUEST, Exceptions.handle(UserContext.LOG, e));
        }
    }

    /**
     * Handles manual object uploads
     *
     * @param ctx    the context describing the current request
     * @param bucket the name of the target bucket
     */
    @Routed(priority = PriorityCollector.DEFAULT_PRIORITY - 1, value = "/ui/:1/upload")
    public void uploadFile(WebContext ctx, String bucket) {
        try {
            String name = ctx.get("filename").asString(ctx.get("qqfile").asString());
            Bucket storageBucket = storage.getBucket(bucket);
            StoredObject object = storageBucket.getObject(name);
            try (InputStream inputStream = ctx.getContent()) {
                try (FileOutputStream out = new FileOutputStream(object.getFile())) {
                    ByteStreams.copy(inputStream, out);
                }
            }

            Map<String, String> properties = Maps.newTreeMap();
            properties.put(HttpHeaders.Names.CONTENT_TYPE,
                           ctx.getHeaderValue(HttpHeaders.Names.CONTENT_TYPE).asString(MimeHelper.guessMimeType(name)));
            HashCode hash = Files.hash(object.getFile(), Hashing.md5());
            String md5 = BaseEncoding.base64().encode(hash.asBytes());
            properties.put("Content-MD5", md5);
            object.storeProperties(properties);

            ctx.respondWith().direct(HttpResponseStatus.OK, "{ success: true }");
        } catch (IOException e) {
            UserContext.handle(e);
            ctx.respondWith().direct(HttpResponseStatus.OK, "{ success: false }");
        }
    }

    /**
     * Handles requests to /ui/[bucketName]/delete
     * <p>
     * This will delete the given bucket and list the remaining.
     * </p>
     *
     * @param ctx    the context describing the current request
     * @param bucket name of the bucket to delete
     */
    @Routed(priority = PriorityCollector.DEFAULT_PRIORITY - 1, value = "/ui/:1/delete")
    public void deleteBucket(WebContext ctx, String bucket) {
        storage.getBucket(bucket).delete();
        UserContext.message(Message.info("Bucket successfully deleted."));
        onError(ctx, null);
    }

    /**
     * Handles requests to /ui/[bucketName]/[object]/delete
     * <p>
     * This will delete the given object and list the remaining.
     * </p>
     *
     * @param ctx        the context describing the current request
     * @param bucketName name of the bucket which contains the object to delete
     * @param id         name of the object to delete
     */
    @Routed("/ui/:1/:2/delete")
    public void deleteObject(WebContext ctx, String bucketName, String id) {
        Bucket bucket = storage.getBucket(bucketName);
        if (bucket.exists()) {
            StoredObject object = bucket.getObject(id);
            if (object.exists()) {
                object.delete();
                UserContext.message(Message.info("Object successfully deleted."));
            }
        }
        bucket(ctx, bucketName);
    }

    /**
     * Handles requests to /ui/[bucketName]/makePublic
     * <p>
     * This will make the given bucket public and list its contents.
     * </p>
     *
     * @param ctx    the context describing the current request
     * @param bucket name of the bucket to make public
     */
    @Routed(priority = PriorityCollector.DEFAULT_PRIORITY - 1, value = "/ui/:1/makePublic")
    public void makePublic(WebContext ctx, String bucket) {
        Bucket storageBucket = storage.getBucket(bucket);
        storageBucket.makePublic();
        UserContext.message(Message.info("ACLs successfully changed"));
        ctx.respondWith().template("view/bucket.html", storageBucket);
    }

    /**
     * Handles requests to /ui/[bucketName]/makePrivate
     * <p>
     * This will make the given bucket private and list its contents.
     * </p>
     *
     * @param ctx    the context describing the current request
     * @param bucket name of the bucket to make private
     */
    @Routed(priority = PriorityCollector.DEFAULT_PRIORITY - 1, value = "/ui/:1/makePrivate")
    public void makePrivate(WebContext ctx, String bucket) {
        Bucket storageBucket = storage.getBucket(bucket);
        storageBucket.makePrivate();
        UserContext.message(Message.info("ACLs successfully changed"));
        ctx.respondWith().template("view/bucket.html", storageBucket);
    }
}
