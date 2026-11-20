/*
 * Made with all the love in the world
 * by scireum in Stuttgart, Germany
 *
 * Copyright by scireum GmbH
 * https://www.scireum.de - info@scireum.de
 */

package ninja.queries;

import ninja.Bucket;
import sirius.kernel.di.std.Register;
import sirius.kernel.xml.Attribute;
import sirius.kernel.xml.XMLStructuredOutput;
import sirius.web.http.WebContext;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Synthesizes <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLocation">bucket location</a>
 * responses.
 */
@Register(name = "location")
public class BucketLocationSynthesizer implements S3QueryProcessor {

    @Override
    public void processQuery(@Nonnull WebContext webContext,
                             @Nonnull Bucket bucket,
                             @Nullable String key,
                             @Nonnull String query) {
        // The location is the content of the root element itself, which is what clients parse.
        XMLStructuredOutput xml = webContext.respondWith().xml();
        xml.beginOutput("LocationConstraint", Attribute.set("xmlns", "http://s3.amazonaws.com/doc/2006-03-01/"));
        xml.text("EU");
        xml.endOutput();
    }
}
