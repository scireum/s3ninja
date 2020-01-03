/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package ninja.queries;

import io.netty.handler.codec.http.HttpResponseStatus;
import sirius.kernel.di.std.Register;
import sirius.kernel.xml.XMLStructuredOutput;
import sirius.web.http.WebContext;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Synthesises <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLifecycle">bucket lifecycle</a>
 * responses.
 */
@Register(name = "lifecycle")
public class BucketLifecycleSynthesizer implements S3QuerySynthesizer {
    @Override
    public void processQuery(@Nonnull WebContext ctx,
                             @Nullable String bucket,
                             @Nullable String key,
                             @Nonnull String query) {
        // todo: replace with error handler, see #123
        XMLStructuredOutput xml = new XMLStructuredOutput(ctx.respondWith().outputStream(HttpResponseStatus.NOT_FOUND, "text/xml"));
        xml.beginOutput("Error");
        xml.property("Code", "NoSuchLifecycleConfiguration");
        xml.endOutput();
    }
}
