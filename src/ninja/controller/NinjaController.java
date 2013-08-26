package ninja.controller;

import com.google.common.collect.Maps;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import ninja.APILog;
import ninja.Bucket;
import ninja.Storage;
import ninja.StoredObject;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.multipart.Attribute;
import sirius.kernel.commons.PriorityCollector;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Exceptions;
import sirius.kernel.health.HandledException;
import sirius.web.controller.Controller;
import sirius.web.controller.Message;
import sirius.web.controller.Routed;
import sirius.web.controller.UserContext;
import sirius.web.http.MimeHelper;
import sirius.web.http.Response;
import sirius.web.http.WebContext;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Andreas Haufler (aha@scireum.de)
 * @since 2013/08
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
        ctx.respondWith().template("view/index.html", buckets, storage.getBasePath(), storage.getAwsAccessKey(), storage.getAwsSecretKey());
    }

    @Part
    private Storage storage;

    @Part
    private APILog log;

    @Routed("/")
    public void index(WebContext ctx) {
        if (ctx.isPOST() && ctx.get("bucketName").isFilled()) {
            storage.getBucket(ctx.get("bucketName").asString()).create();
            UserContext.message(Message.info("Bucket successfully created."));
        }
        onError(ctx, null);
    }

    @Routed(value = "/ui/license", priority = PriorityCollector.DEFAULT_PRIORITY - 1)
    public void license(WebContext ctx) {
        ctx.respondWith().template("view/license.html");
    }

    @Routed(value = "/ui/api", priority = PriorityCollector.DEFAULT_PRIORITY - 1)
    public void api(WebContext ctx) {
        ctx.respondWith().template("view/api.html");
    }

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

    @Routed("/ui/:1")
    public void bucket(WebContext ctx, String bucketName) {
        Bucket bucket = storage.getBucket(bucketName);
        ctx.respondWith().template("view/bucket.html", bucket);
    }

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

    @Routed(priority = PriorityCollector.DEFAULT_PRIORITY - 1, value = "/ui/:1/upload")
    public void uploadFile(WebContext ctx, String bucket) {
        try {
            String name = ctx.get("filename").asString(ctx.get("qqfile").asString());
            Bucket storageBucket = storage.getBucket(bucket);
            StoredObject object = storageBucket.getObject(name);
            Attribute attr = ctx.getContent();
            ChannelBufferInputStream inputStream = new ChannelBufferInputStream(attr.getChannelBuffer());
            try {
                FileOutputStream out = new FileOutputStream(object.getFile());
                try {
                    ByteStreams.copy(inputStream, out);
                } finally {
                    out.close();
                }
            } finally {
                inputStream.close();
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

    @Routed(priority = PriorityCollector.DEFAULT_PRIORITY - 1, value = "/ui/:1/delete")
    public void deleteBucket(WebContext ctx, String bucket) {
        storage.getBucket(bucket).delete();
        UserContext.message(Message.info("Bucket successfully deleted."));
        onError(ctx, null);
    }

    @Routed("/ui/:1/:2/delete")
    public void deleteObject(WebContext ctx, String bucketName, String id) {
        Bucket bucket = storage.getBucket(bucketName);
        if (bucket.exists()) {
            StoredObject object = bucket.getObject(id);
            if (object.exists()) {
                object.delete();
                UserContext.message(Message.info("Bucket successfully deleted."));
            }
        }
        bucket(ctx, bucketName);
    }

    @Routed(priority = PriorityCollector.DEFAULT_PRIORITY - 1, value = "/ui/:1/makePublic")
    public void makePublic(WebContext ctx, String bucket) {
        Bucket storageBucket = storage.getBucket(bucket);
        storageBucket.makePublic();
        UserContext.message(Message.info("ACLs successfully changed"));
        ctx.respondWith().template("view/bucket.html", storageBucket);
    }

    @Routed(priority = PriorityCollector.DEFAULT_PRIORITY - 1, value = "/ui/:1/makePrivate")
    public void makePrivate(WebContext ctx, String bucket) {
        Bucket storageBucket = storage.getBucket(bucket);
        storageBucket.makePrivate();
        UserContext.message(Message.info("ACLs successfully changed"));
        ctx.respondWith().template("view/bucket.html", storageBucket);
    }

}
