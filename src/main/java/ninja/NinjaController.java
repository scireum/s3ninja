/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package ninja;

import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import sirius.kernel.commons.Hasher;
import sirius.kernel.commons.PriorityCollector;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Exceptions;
import sirius.kernel.health.HandledException;
import sirius.kernel.nls.NLS;
import sirius.web.controller.BasicController;
import sirius.web.controller.DefaultRoute;
import sirius.web.controller.Message;
import sirius.web.controller.Page;
import sirius.web.controller.Routed;
import sirius.web.http.InputStreamHandler;
import sirius.web.http.MimeHelper;
import sirius.web.http.Response;
import sirius.web.http.WebContext;
import sirius.web.security.UserContext;
import sirius.web.services.JSONStructuredOutput;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Takes care of the "management ui".
 * <p>
 * Handles all requests required to render the web-pages.
 */
@Register
public class NinjaController extends BasicController {

    @Part
    private Storage storage;

    @Part
    private APILog log;

    /**
     * Handles requests to /ui, possibly branching away for special sites.
     *
     * @param webContext the context describing the current request
     */
    @DefaultRoute
    @Routed("/ui")
    public void index(WebContext webContext) {
        if (webContext.hasParameter("license")) {
            webContext.respondWith().template("/templates/license.html.pasta");
            return;
        }
        if (webContext.hasParameter("api")) {
            webContext.respondWith().template("/templates/api.html.pasta");
            return;
        }
        if (webContext.hasParameter("log")) {
            log(webContext);
            return;
        }
        buckets(webContext);
    }

    /**
     * Handles requests to /ui?log
     *
     * @param webContext the context describing the current request
     */
    private void log(WebContext webContext) {
        int start = webContext.get("start").asInt(1) - 1;
        int pageSize = 50;

        List<APILog.Entry> entries = log.getEntries(start, pageSize + 1);
        boolean canPagePrev = start > 0;
        boolean canPageNext = entries.size() > pageSize;
        if (canPageNext) {
            entries.remove(entries.size() - 1);
        }
        webContext.respondWith()
                  .template("/templates/log.html.pasta",
                            entries,
                            canPagePrev,
                            canPageNext,
                            (start + 1) + " - " + (start + entries.size()),
                            Math.max(1, start - pageSize + 1),
                            start + pageSize + 1);
    }

    private void buckets(WebContext webContext) {
        List<Bucket> buckets = Collections.emptyList();
        try {
            buckets = storage.getBuckets();
        } catch (HandledException e) {
            UserContext.message(Message.error(e.getMessage()));
        }
        webContext.respondWith()
                  .template("/templates/index.html.pasta",
                            buckets,
                            storage.getBasePath(),
                            storage.getAwsAccessKey(),
                            storage.getAwsSecretKey());
    }

    /**
     * Handles requests to /ui/[bucketName]
     * <p>
     * This will list the contents of the bucket.
     * </p>
     *
     * @param webContext the context describing the current request
     * @param bucketName name of the bucket to show
     */
    @Routed("/ui/:1")
    public void bucket(WebContext webContext, String bucketName) {
        try {
            Bucket bucket = storage.getBucket(bucketName);
            if (!bucket.exists()) {
                if (webContext.hasParameter("create")) {
                    bucket.create();
                    UserContext.message(Message.info("Bucket successfully created."));
                } else {
                    webContext.respondWith().error(HttpResponseStatus.NOT_FOUND, "Bucket does not exist");
                    return;
                }
            }

            Page<StoredObject> page = new Page<StoredObject>().bindToRequest(webContext);
            page.withLimitedItemsSupplier(limit -> bucket.getObjects(page.getQuery(), limit));
            page.withTotalItems(bucket.countObjects(page.getQuery()));

            webContext.respondWith().template("/templates/bucket.html.pasta", bucket, page);
        } catch (Exception e) {
            webContext.respondWith().error(HttpResponseStatus.BAD_REQUEST, Exceptions.handle(UserContext.LOG, e));
        }
    }

    /**
     * Handles requests to /ui/[bucketName]/[object]
     * <p>
     * This will start a download of the requested object. No access checks will be performed.
     * </p>
     *
     * @param webContext the context describing the current request
     * @param bucketName name of the bucket to show
     * @param id         the name of the object to fetch
     */
    @Routed("/ui/:1/:2")
    public void object(WebContext webContext, String bucketName, String id) {
        try {
            Bucket bucket = storage.getBucket(bucketName);
            if (!bucket.exists()) {
                webContext.respondWith().error(HttpResponseStatus.NOT_FOUND, "Bucket does not exist");
                return;
            }
            StoredObject object = bucket.getObject(id);
            if (!object.exists()) {
                webContext.respondWith().error(HttpResponseStatus.NOT_FOUND, "Object does not exist");
                return;
            }
            Response response = webContext.respondWith();
            for (Map.Entry<Object, Object> entry : object.getProperties().entrySet()) {
                response.addHeader(entry.getKey().toString(), entry.getValue().toString());
            }
            response.file(object.getFile());
        } catch (Exception e) {
            webContext.respondWith().error(HttpResponseStatus.BAD_REQUEST, Exceptions.handle(UserContext.LOG, e));
        }
    }

    /**
     * Handles manual object uploads
     *
     * @param webContext the context describing the current request
     * @param out        the output to write to
     * @param bucket     the name of the target bucket
     * @param input      the data stream to read from
     */
    @Routed(priority = PriorityCollector.DEFAULT_PRIORITY - 1,
            value = "/ui/:1/upload",
            jsonCall = true,
            preDispatchable = true)
    public void uploadFile(WebContext webContext, JSONStructuredOutput out, String bucket, InputStreamHandler input) {
        try {
            String name = webContext.get("filename").asString(webContext.get("qqfile").asString());
            Bucket storageBucket = storage.getBucket(bucket);
            StoredObject object = storageBucket.getObject(name);
            try (FileOutputStream fos = new FileOutputStream(object.getFile())) {
                ByteStreams.copy(input, fos);
            }

            Map<String, String> properties = Maps.newTreeMap();
            properties.put(HttpHeaderNames.CONTENT_TYPE.toString(),
                           webContext.getHeaderValue(HttpHeaderNames.CONTENT_TYPE)
                                     .asString(MimeHelper.guessMimeType(name)));
            String md5 = Hasher.md5().hashFile(object.getFile()).toBase64String();
            properties.put("Content-MD5", md5);
            object.storeProperties(properties);

            out.property("message", "File successfully uploaded.");
            out.property("action", "/ui/" + bucket);
            out.property("actionLabel", NLS.get("NLS.refresh"));
            out.property("refresh", "true");
        } catch (IOException e) {
            UserContext.handle(e);
            webContext.respondWith().direct(HttpResponseStatus.OK, "{ success: false }");
        }
    }

    /**
     * Handles requests to /ui/[bucketName]/delete
     * <p>
     * This will delete the given bucket and list the remaining.
     * </p>
     *
     * @param webContext the context describing the current request
     * @param bucket     name of the bucket to delete
     */
    @Routed(priority = PriorityCollector.DEFAULT_PRIORITY - 1, value = "/ui/:1/delete")
    public void deleteBucket(WebContext webContext, String bucket) {
        storage.getBucket(bucket).delete();
        UserContext.message(Message.info("Bucket successfully deleted."));
        buckets(webContext);
    }

    /**
     * Handles requests to /ui/[bucketName]/[object]/delete
     * <p>
     * This will delete the given object and list the remaining.
     * </p>
     *
     * @param webContext the context describing the current request
     * @param bucketName name of the bucket which contains the object to delete
     * @param id         name of the object to delete
     */
    @Routed("/ui/:1/:2/delete")
    public void deleteObject(WebContext webContext, String bucketName, String id) {
        Bucket bucket = storage.getBucket(bucketName);
        if (bucket.exists()) {
            StoredObject object = bucket.getObject(id);
            if (object.exists()) {
                object.delete();
                UserContext.message(Message.info("Object successfully deleted."));
            }
        }
        bucket(webContext, bucketName);
    }

    /**
     * Handles requests to /ui/[bucketName]/makePublic
     * <p>
     * This will make the given bucket public and list its contents.
     * </p>
     *
     * @param webContext the context describing the current request
     * @param bucket     name of the bucket to make public
     */
    @Routed(priority = PriorityCollector.DEFAULT_PRIORITY - 1, value = "/ui/:1/makePublic")
    public void makePublic(WebContext webContext, String bucket) {
        storage.getBucket(bucket).makePublic();
        UserContext.message(Message.info("ACLs successfully changed"));
        bucket(webContext, bucket);
    }

    /**
     * Handles requests to /ui/[bucketName]/makePrivate
     * <p>
     * This will make the given bucket private and list its contents.
     * </p>
     *
     * @param webContext the context describing the current request
     * @param bucket     name of the bucket to make private
     */
    @Routed(priority = PriorityCollector.DEFAULT_PRIORITY - 1, value = "/ui/:1/makePrivate")
    public void makePrivate(WebContext webContext, String bucket) {
        storage.getBucket(bucket).makePrivate();
        UserContext.message(Message.info("ACLs successfully changed"));
        bucket(webContext, bucket);
    }
}
