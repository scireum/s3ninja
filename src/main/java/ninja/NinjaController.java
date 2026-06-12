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
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import sirius.kernel.commons.Hasher;
import sirius.kernel.commons.PriorityCollector;
import sirius.kernel.commons.Strings;
import sirius.kernel.di.GlobalContext;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Exceptions;
import sirius.kernel.health.HandledException;
import sirius.kernel.nls.NLS;
import sirius.web.controller.BasicController;
import sirius.web.controller.Controller;
import sirius.web.controller.DefaultRoute;
import sirius.web.controller.HttpMethod;
import sirius.web.controller.Message;
import sirius.web.controller.Page;
import sirius.web.controller.Routed;
import sirius.web.http.MimeHelper;
import sirius.web.http.Response;
import sirius.web.http.WebContext;
import sirius.web.security.UserContext;
import sirius.web.services.InternalService;
import sirius.web.services.JSONStructuredOutput;

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

    @Part
    private S3Dispatcher s3Dispatcher;

    @Part
    private GlobalContext globalContext;

    @Part
    private PresignedPostService presignedPostService;

    /**
     * Handles requests to <tt>/ui/presigned-post</tt>.
     * <p>
     * Show the UI to edit / launch and copy a Presign Post.
     * <p>
     *
     * @param webContext the context describing the current request
     */
    @Routed(value = "/ui/presigned-post", priority = PriorityCollector.DEFAULT_PRIORITY - 1)
    public void presignedPostUI(WebContext webContext) {
        webContext.respondWith()
                  .template("/templates/presigned-post.html.pasta",
                            storage.getAwsAccessKey(),
                            storage.getAwsSecretKey());
    }

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
            entries.removeLast();
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
            UserContext.message(Message.error().withTextMessage(e.getMessage()));
        }
        webContext.respondWith()
                  .template("/templates/index.html.pasta",
                            buckets,
                            storage.getBasePath(),
                            storage.getAwsAccessKey(),
                            storage.getAwsSecretKey());
    }

    /**
     * Handles requests to <tt>/ui/[bucket]</tt>.
     * <p>
     * By default, this lists the contents of the bucket.
     * <p>
     * Optional query parameters include:
     * <ul>
     *     <li><tt>/ui/[bucket]?create</tt>: Creates the bucket.</li>
     *     <li><tt>/ui/[bucket]?delete</tt>: Deletes the bucket.</li>
     *     <li><tt>/ui/[bucket]?make-public</tt>: Makes the bucket public.</li>
     *     <li><tt>/ui/[bucket]?make-private</tt>: Makes the bucket private.</li>
     * </ul>
     *
     * @param webContext the context describing the current request
     * @param bucketName the name of the bucket to process
     */
    @Routed("/ui/:1")
    public void bucket(WebContext webContext, String bucketName) {
        Bucket bucket = storage.getBucket(bucketName);

        // handle /ui/[bucket]?create
        if (webContext.hasParameter("create")) {
            assertPostRequest(webContext);

            if (bucket.exists()) {
                UserContext.message(Message.error().withTextMessage("Bucket does already exist."));
                webContext.respondWith().redirectTemporarily("/ui/" + bucket.getEncodedName());
                return;
            }

            if (!bucket.create()) {
                throw Exceptions.createHandled()
                                .to(Storage.LOG)
                                .withDirectMessage("Failed creating bucket. Missing file system permission?")
                                .handle();
            }

            UserContext.message(Message.info().withTextMessage("Bucket successfully created."));
            webContext.respondWith().redirectTemporarily("/ui/" + bucket.getEncodedName());
            return;
        }

        // from this point on, make sure that the bucket exists
        if (!bucket.exists()) {
            UserContext.message(Message.error().withTextMessage("Bucket does not exist."));
            webContext.respondWith().redirectTemporarily("/ui");
            return;
        }

        // we only accept known paths below /ui as return address
        String address = webContext.getParameter("return");
        if (Strings.areEqual(address, "/ui") || Strings.areEqual(address, "/ui/") || Strings.areEqual(address, "ui")) {
            address = "/ui";
        } else {
            address = "/ui/" + bucket.getEncodedName();
        }

        // handle /ui/[bucket]?make-public
        if (webContext.hasParameter("make-public")) {
            assertPostRequest(webContext);

            if (!bucket.makePublic()) {
                throw Exceptions.createHandled()
                                .to(Storage.LOG)
                                .withDirectMessage("Failed making bucket public. Missing file system permission?")
                                .handle();
            }

            UserContext.message(Message.info().withTextMessage("ACLs successfully changed"));
            webContext.respondWith().redirectTemporarily(address);
            return;
        }

        // handle /ui/[bucket]?make-private
        if (webContext.hasParameter("make-private")) {
            assertPostRequest(webContext);

            if (!bucket.makePrivate()) {
                throw Exceptions.createHandled()
                                .to(Storage.LOG)
                                .withDirectMessage("Failed making bucket private. Missing file system permission?")
                                .handle();
            }

            UserContext.message(Message.info().withTextMessage("ACLs successfully changed"));
            webContext.respondWith().redirectTemporarily(address);
            return;
        }

        // handle /ui/[bucket]?delete
        if (webContext.hasParameter("delete")) {
            assertPostRequest(webContext);

            if (!bucket.delete()) {
                throw Exceptions.createHandled()
                                .to(Storage.LOG)
                                .withDirectMessage("Failed deleting bucket. Missing file system permission?")
                                .handle();
            }

            UserContext.message(Message.info().withTextMessage("Bucket successfully deleted."));
            webContext.respondWith().redirectTemporarily("/ui");
            return;
        }

        // handle /ui/[bucket]?upload
        if (webContext.hasParameter("upload")) {
            assertPostRequest(webContext);

            uploadFile(webContext, bucket);
            return;
        }

        objects(webContext, bucket);
    }

    /**
     * Handles manual object via <tt>/ui/[bucket]?upload</tt>.
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
            properties.put("Content-MD5", Hasher.md5().hashFile(object.getFile()).toBase64String());
            object.setProperties(properties);

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
     * Handles requests to /ui/[bucket]/[object]
     * <p>
     * By default, this starts a download of the requested object. No access checks will be performed.
     * <p>
     * Optional query parameters include:
     * <ul>
     *     <li><tt>/ui/[bucket]/[object]?delete</tt>: Deletes the object.</li>
     * </ul>
     *
     * @param webContext the context describing the current request
     * @param bucketName the name of the bucket to consider
     * @param idParts    the name of the object to fetch
     */
    @Routed(value = "/ui/:1/**", priority = PriorityCollector.DEFAULT_PRIORITY + 1)
    public void object(WebContext webContext, String bucketName, List<String> idParts) {
        Bucket bucket = storage.getBucket(bucketName);
        if (!bucket.exists()) {
            UserContext.message(Message.error().withTextMessage("Bucket does not exist."));
            webContext.respondWith().redirectTemporarily("/ui");
            return;
        }

        String id = String.join("/", idParts);
        StoredObject object = bucket.getObject(id);
        if (!object.exists()) {
            UserContext.message(Message.error().withTextMessage("Object does not exist."));
            webContext.respondWith().redirectTemporarily("/ui/" + bucket.getEncodedName());
            return;
        }

        // handle /ui/[bucket]/[object]?delete
        if (webContext.hasParameter("delete")) {
            assertPostRequest(webContext);

            object.delete();

            UserContext.message(Message.info().withTextMessage("Object successfully deleted."));
            webContext.respondWith().redirectTemporarily("/ui/" + bucket.getEncodedName());
            return;
        }

        Response response = webContext.respondWith().named(object.getKey());
        for (Map.Entry<String, String> entry : object.getProperties().entrySet()) {
            response.addHeader(entry.getKey(), entry.getValue());
        }
        response.file(object.getFile());
    }

    /**
     * Handles requests to <tt>/.api/generate-session-token</tt>.
     * <p>
     * Generates a session token that can be used to authenticate requests.
     * The token is valid for 24 hours.
     * <p>
     * Use this token in requests by adding either:
     * <ul>
     *     <li>Header: <tt>X-Session-Token: [token]</tt></li>
     *     <li>Query parameter: <tt>?sessionToken=[token]</tt></li>
     *     <li>Form field (for POST): <tt>X-Amz-Security-Token: [token]</tt></li>
     * </ul>
     *
     * @param webContext the context describing the current request
     * @param output     the JSON output to write the response to
     */
    @InternalService
    @Routed(value = "/.api/generate-session-token", methods = HttpMethod.POST)
    public void generateSessionToken(WebContext webContext, JSONStructuredOutput output) {
        String token = s3Dispatcher.generateSessionToken();

        output.property("success", true)
              .property("token", token)
              .property("expiresIn", "24 hours")
              .property("usage",
                        "Add 'X-Session-Token: "
                        + token
                        + "' header or 'X-Amz-Security-Token="
                        + token
                        + "' form field");
    }

    /**
     * Handles requests to <tt>/.api/generate-presigned-post</tt>.
     * <p>
     * Generates a presigned POST following the AWS guidelines. The response contains several fields that match an
     * AWS-accepted policy:
     * <ul>
     *     <li>PolicyJson</li>
     *     <li>Policy</li>
     *     <li>Signature</li>
     * </ul>
     *
     * @param webContext the context describing the current request
     */
    @Routed(value = "/.api/generate-presigned-post", methods = HttpMethod.POST)
    public void generatePresignedPost(WebContext webContext) {
        try {
            PresignedPostRequest request = PresignedPostRequest.from(webContext);
            PresignedPostResponse response = presignedPostService.generate(request);

            var jsonBuilder = webContext.respondWith()
                                        .json()
                                        .beginResult()
                                        .property("success", true)
                                        .property("url", response.url());

            // Properly serialize the fields map as a JSON object
            jsonBuilder.beginObject("fields");
            for (var entry : response.fields().entrySet()) {
                jsonBuilder.property(entry.getKey(), entry.getValue());
            }
            jsonBuilder.endObject();

            jsonBuilder.property("policyJson", response.policyJson())
                       .property("policy", response.policyBase64())
                       .property("signature", response.signature())
                       .endResult();
        } catch (HandledException handled) {
            webContext.respondWith()
                      .json()
                      .beginResult()
                      .property("success", false)
                      .property("message", handled.getMessage())
                      .endResult();
        }
    }

    /**
     * Ensures that mutation branches in mixed GET/POST routes are only processed for POST requests.
     * <p>
     * CSRF validation itself is handled by the framework dispatcher before this controller is invoked.
     *
     * @param webContext the context describing the current request
     */
    private void assertPostRequest(WebContext webContext) {
        if (webContext.isPostRequest()) {
            return;
        }

        throw Exceptions.createHandled()
                        .withDirectMessage("HTTP method not allowed, expecting " + HttpMethod.POST.name())
                        .hint(Controller.HTTP_STATUS, HttpResponseStatus.METHOD_NOT_ALLOWED.code())
                        .hint(Controller.HTTP_HEADER_ALLOW, HttpMethod.POST.name())
                        .handle();
    }
}
