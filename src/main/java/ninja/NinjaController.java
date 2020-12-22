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
import sirius.web.http.MimeHelper;
import sirius.web.http.Response;
import sirius.web.http.WebContext;
import sirius.web.security.UserContext;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Takes care of the "management UI".
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
     * Handles requests to <tt>/ui</tt>.
     * <p>
     * By default, this lists all buckets available on the system.
     * <p>
     * Optional query parameters include:
     * <ul>
     *     <li><tt>/ui?license</tt>: Shows the license terms.</li>
     *     <li><tt>/ui?api</tt>: Shows the implemented S3 API.</li>
     *     <li><tt>/ui?log</tt>: Shows the log.</li>
     * </ul>
     *
     * @param webContext the context describing the current request
     */
    @DefaultRoute
    @Routed("/ui")
    public void index(WebContext webContext) {
        // handle /ui?license
        if (webContext.hasParameter("license")) {
            webContext.respondWith().template("/templates/license.html.pasta");
            return;
        }

        // handle /ui?api
        if (webContext.hasParameter("api")) {
            webContext.respondWith().template("/templates/api.html.pasta");
            return;
        }

        // handle /ui?log
        if (webContext.hasParameter("log")) {
            log(webContext);
            return;
        }

        // handle /ui
        buckets(webContext);
    }

    /**
     * Handles requests to <tt>/ui?log</tt>.
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
     * Handles requests to <tt>/ui/[bucketName]</tt>.
     * <p>
     * By default, this lists the contents of the bucket.
     * <p>
     * Optional query parameters include:
     * <ul>
     *     <li><tt>/ui/[bucketName]?create</tt>: Creates the bucket.</li>
     *     <li><tt>/ui/[bucketName]?delete</tt>: Deletes the bucket.</li>
     *     <li><tt>/ui/[bucketName]?make-public</tt>: Makes the bucket public.</li>
     *     <li><tt>/ui/[bucketName]?make-private</tt>: Makes the bucket private.</li>
     * </ul>
     *
     * @param webContext the context describing the current request
     * @param bucketName the name of the bucket to process
     */
    @Routed("/ui/:1")
    public void bucket(WebContext webContext, String bucketName) {
        try {
            Bucket bucket = storage.getBucket(bucketName);

            // handle /ui/[bucket]?create
            if (webContext.hasParameter("create")) {
                if (bucket.exists()) {
                    // todo: forward to /ui/[bucket] and show error
                    webContext.respondWith().error(HttpResponseStatus.BAD_REQUEST, "Bucket does already exist.");
                    return;
                }

                bucket.create();

                // todo: forward to /ui/[bucket] and show message
                UserContext.message(Message.info("Bucket successfully created."));
                objects(webContext, bucket);
                return;
            }

            // from this point on, make sure that the bucket exists
            if (!bucket.exists()) {
                // todo: forward to /ui and show error
                webContext.respondWith().error(HttpResponseStatus.NOT_FOUND, "Bucket does not exist.");
                return;
            }

            // handle /ui/[bucket]?make-public
            if (webContext.hasParameter("make-public")) {
                bucket.makePublic();

                // todo: forward to /ui/[bucket] and show message
                UserContext.message(Message.info("ACLs successfully changed"));
                objects(webContext, bucket);
                return;
            }

            // handle /ui/[bucket]?make-private
            if (webContext.hasParameter("make-private")) {
                bucket.makePrivate();

                // todo: forward to /ui/[bucket] and show message
                UserContext.message(Message.info("ACLs successfully changed"));
                objects(webContext, bucket);
                return;
            }

            // handle /ui/[bucket]?delete
            if (webContext.hasParameter("delete")) {
                bucket.delete();

                // todo: forward to /ui and show message
                UserContext.message(Message.info("Bucket successfully deleted."));
                buckets(webContext);
                return;
            }

            // handle /ui/[bucket]?upload
            if (webContext.hasParameter("upload")) {
                uploadFile(webContext, bucket);
                return;
            }

            objects(webContext, bucket);
        } catch (Exception e) {
            webContext.respondWith().error(HttpResponseStatus.BAD_REQUEST, Exceptions.handle(UserContext.LOG, e));
        }
    }

    /**
     * Handles manual object via <tt>/ui/[bucketName]?upload</tt>.
     *
     * @param webContext the context describing the current request
     * @param bucket     the target bucket
     */
    private void uploadFile(WebContext webContext, Bucket bucket) {
        try {
            String name = webContext.get("filename").asString(webContext.get("qqfile").asString());
            StoredObject object = bucket.getObject(name);
            try (FileOutputStream fos = new FileOutputStream(object.getFile())) {
                ByteStreams.copy(webContext.getContent(), fos);
            }

            Map<String, String> properties = Maps.newTreeMap();
            properties.put(HttpHeaderNames.CONTENT_TYPE.toString(),
                           webContext.getHeaderValue(HttpHeaderNames.CONTENT_TYPE)
                                     .asString(MimeHelper.guessMimeType(name)));
            String md5 = Hasher.md5().hashFile(object.getFile()).toBase64String();
            properties.put("Content-MD5", md5);
            object.storeProperties(properties);

            webContext.respondWith()
                      .json()
                      .beginResult()
                      .property("success", true)
                      .property("error", false)
                      .property("message", "File successfully uploaded.")
                      .property("action", "/ui/" + bucket)
                      .property("actionLabel", NLS.get("NLS.refresh"))
                      .property("refresh", "true")
                      .endResult();
        } catch (IOException e) {
            webContext.respondWith()
                      .json(HttpResponseStatus.OK)
                      .beginResult()
                      .property("success", false)
                      .property("error", true)
                      .property("message", Exceptions.handle(e).getMessage())
                      .endResult();
        }
    }

    private void objects(WebContext webContext, Bucket bucket) {
        Page<StoredObject> page = new Page<StoredObject>().bindToRequest(webContext);
        page.withLimitedItemsSupplier(limit -> bucket.getObjects(page.getQuery(), limit));
        page.withTotalItems(bucket.countObjects(page.getQuery()));

        webContext.respondWith().template("/templates/bucket.html.pasta", bucket, page);
    }

    /**
     * Handles requests to /ui/[bucketName]/[object]
     * <p>
     * This will start a download of the requested object. No access checks will be performed.
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
     * Handles requests to /ui/[bucketName]/[object]/delete
     * <p>
     * This will delete the given object and list the remaining.
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
}
