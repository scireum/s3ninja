/*
 * Made with all the love in the world
 * by scireum in Stuttgart, Germany
 *
 * Copyright by scireum GmbH
 * https://www.scireum.de - info@scireum.de
 */

package ninja.queries;

import ninja.Bucket;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import sirius.kernel.di.std.Register;
import sirius.kernel.xml.Attribute;
import sirius.kernel.xml.XMLStructuredOutput;
import sirius.web.http.WebContext;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Synthesizes <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketAcl">bucket acl</a> responses.
 */
@Register(name = "acl")
public class BucketAclSynthesizer implements S3QueryProcessor {

    @Override
    public void processQuery(@Nonnull WebContext webContext,
                             @Nonnull Bucket bucket,
                             @Nullable String key,
                             @Nonnull String query) {
        if (!HttpMethod.GET.equals(webContext.getRequest().method())) {
            // ACLs are not kept, so changing them is accepted and has no effect.
            webContext.respondWith().status(HttpResponseStatus.OK);
            return;
        }

        // As ACLs are not kept, every bucket and object is reported as fully controlled by its owner.
        XMLStructuredOutput xml = webContext.respondWith().xml();
        xml.beginOutput("AccessControlPolicy", Attribute.set("xmlns", "http://s3.amazonaws.com/doc/2006-03-01/"));
        outputOwner(xml, "Owner");
        xml.beginObject("AccessControlList");
        xml.beginObject("Grant");
        outputOwner(xml,
                    "Grantee",
                    Attribute.set("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance"),
                    Attribute.set("xsi:type", "CanonicalUser"));
        xml.property("Permission", "FULL_CONTROL");
        xml.endObject();
        xml.endObject();
        xml.endOutput();
    }

    private void outputOwner(XMLStructuredOutput xml, String name, Attribute... attributes) {
        xml.beginObject(name, attributes);
        xml.property("ID", "initiatorId");
        xml.property("DisplayName", "initiatorName");
        xml.endObject();
    }
}
