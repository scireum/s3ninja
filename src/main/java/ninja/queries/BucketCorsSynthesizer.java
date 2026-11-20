/*
 * Made with all the love in the world
 * by scireum in Stuttgart, Germany
 *
 * Copyright by scireum GmbH
 * https://www.scireum.de - info@scireum.de
 */

package ninja.queries;

import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import ninja.Bucket;
import ninja.errors.S3ErrorCode;
import ninja.errors.S3ErrorSynthesizer;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.web.http.WebContext;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Synthesizes <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketCors">bucket cors</a> responses.
 */
@Register(name = "cors")
public class BucketCorsSynthesizer implements S3QueryProcessor {

    @Part
    private S3ErrorSynthesizer errorSynthesizer;

    @Override
    public void processQuery(@Nonnull WebContext webContext,
                             @Nonnull Bucket bucket,
                             @Nullable String key,
                             @Nonnull String query) {
        // CORS configurations are not kept, so a bucket never has one. Storing or deleting one is accepted without
        // any effect, like for ACLs, as scripts which set up a bucket for S3 would otherwise fail here.
        HttpMethod method = webContext.getRequest().method();
        if (HttpMethod.GET.equals(method)) {
            errorSynthesizer.synthesiseError(webContext,
                                             bucket.getName(),
                                             key,
                                             S3ErrorCode.NoSuchCORSConfiguration,
                                             "The CORS configuration does not exist");
        } else if (HttpMethod.DELETE.equals(method)) {
            webContext.respondWith().status(HttpResponseStatus.NO_CONTENT);
        } else if (HttpMethod.PUT.equals(method)) {
            webContext.respondWith().status(HttpResponseStatus.OK);
        } else {
            errorSynthesizer.synthesiseError(webContext,
                                             bucket.getName(),
                                             key,
                                             S3ErrorCode.MethodNotAllowed,
                                             "The specified method is not allowed against this resource.");
        }
    }
}
