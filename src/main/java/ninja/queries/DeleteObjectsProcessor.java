/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package ninja.queries;

import ninja.Bucket;
import ninja.StoredObject;
import ninja.errors.S3ErrorCode;
import sirius.kernel.di.std.Register;
import sirius.kernel.xml.StructuredNode;
import sirius.kernel.xml.XMLStructuredInput;
import sirius.kernel.xml.XMLStructuredOutput;
import sirius.web.http.WebContext;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;

/**
 * Processes <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html">delete object</a> requests.
 */
@Register(name = "delete")
public class DeleteObjectsProcessor implements S3QueryProcessor {

    @Override
    public void processQuery(@Nonnull WebContext webContext,
                             @Nullable Bucket bucket,
                             @Nullable String key,
                             @Nonnull String query) {
        XMLStructuredOutput xml = webContext.respondWith().xml();
        xml.beginOutput("DeleteResult");

        String hostId = webContext.getRequest().headers().get("x-amz-id-2");
        String requestId = webContext.getRequest().headers().get("x-amz-request-id");

        try {
            XMLStructuredInput request = new XMLStructuredInput(webContext.getContent(), null);
            boolean quiet = request.root().queryValue("Quiet").asBoolean();
            request.root()
                   .queryNodeList("Object")
                   .forEach(objectNode -> processDeletionRequest(bucket, objectNode, xml, quiet));
        } catch (IOException exception) {
            xml.beginObject("Error");
            xml.property("Code", "MalformedXML");
            xml.property("Message", "Failed to parse XML from request: " + exception.getMessage());
            xml.propertyIfFilled("RequestId", requestId);
            xml.propertyIfFilled("HostId", hostId);
            xml.endObject();
        }

        xml.endOutput();
    }

    private void processDeletionRequest(Bucket bucket,
                                        StructuredNode objectNode,
                                        XMLStructuredOutput output,
                                        boolean quiet) {
        String key = objectNode.queryValue("Key").asString();

        StoredObject object = bucket.getObject(key);
        if (!object.exists()) {
            output.beginObject("Error");
            output.property("Key", key);
            output.property("Code", S3ErrorCode.NoSuchKey);
            output.property("Message", "No Such Key");
            output.endObject();
            return;
        }

        object.delete();

        if (!quiet) {
            output.beginObject("Deleted");
            output.property("Key", key);
            output.endObject();
        }
    }
}
