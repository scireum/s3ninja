/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package ninja.queries;

import sirius.kernel.di.std.Register;
import sirius.kernel.xml.XMLStructuredOutput;
import sirius.web.http.WebContext;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Synthesises <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketRequestPayment">bucket request
 * payment</a> responses.
 */
@Register(name = "requestPayment")
public class BucketRequestPaymentSynthesizer implements S3QuerySynthesizer {

    @Override
    public void processQuery(@Nonnull WebContext ctx,
                             @Nullable String bucket,
                             @Nullable String key,
                             @Nonnull String query) {
        XMLStructuredOutput xml = ctx.respondWith().xml();
        xml.beginOutput("GetBucketRequestPaymentOutput");
        xml.property("Payer", "BucketOwner");
        xml.endOutput();
    }
}
