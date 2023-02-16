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
 * Synthesizes <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketCors">bucket cors</a> responses.
 */
@Register(name = "cors")
public class BucketCorsSynthesizer implements S3QueryProcessor {

    @Override
    public void processQuery(@Nonnull WebContext webContext,
                             @Nullable Bucket bucket,
                             @Nullable String key,
                             @Nonnull String query) {
        XMLStructuredOutput xml = webContext.respondWith().xml();
        xml.beginOutput("GetBucketCorsOutput");
        xml.endOutput();
    }
}
