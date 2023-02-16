/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package ninja.queries;

import ninja.Bucket;
import sirius.kernel.di.std.Register;
import sirius.kernel.xml.XMLStructuredOutput;
import sirius.web.http.WebContext;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Synthesizes <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketRequestPayment">bucket request
 * payment</a> responses.
 */
@Register(name = "requestPayment")
public class BucketRequestPaymentSynthesizer implements S3QueryProcessor {

    @Override
    public void processQuery(@Nonnull WebContext webContext,
                             @Nullable Bucket bucket,
                             @Nullable String key,
                             @Nonnull String query) {
        XMLStructuredOutput xml = webContext.respondWith().xml();
        xml.beginOutput("GetBucketRequestPaymentOutput");
        xml.property("Payer", "BucketOwner");
        xml.endOutput();
    }
}
