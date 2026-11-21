/*
 * Made with all the love in the world
 * by scireum in Stuttgart, Germany
 *
 * Copyright by scireum GmbH
 * https://www.scireum.de - info@scireum.de
 */

package ninja;

import com.google.common.io.ByteStreams;
import io.netty.handler.codec.http.HttpHeaderNames;
import ninja.errors.S3ErrorCode;
import ninja.errors.S3ErrorSynthesizer;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.web.http.InputStreamHandler;
import sirius.web.http.WebContext;

import java.io.IOException;
import java.util.Optional;

/**
 * Handles browser-based uploads, which are POST requests carrying a signed policy that states what may be
 * uploaded.
 */
@Register(classes = PostObjectHandler.class)
public class PostObjectHandler {

    @Part
    private S3ErrorSynthesizer errorSynthesizer;

    @Part
    private ObjectHandler objectHandler;

    @Part
    private PostPolicyVerifier postPolicyVerifier;

    @Part
    private BrowserUploadResponder browserUploadResponder;

    /**
     * Handles POST /bucket/id
     * <p>
     * The request is only served if it carries a policy and a signature which {@link PostPolicyVerifier} accepts.
     *
     * @param webContext the context describing the current request
     * @param bucket     the bucket to upload into
     * @param key        the key of the object to upload
     * @param data       the content of the object
     */
    void handlePostObject(WebContext webContext, Bucket bucket, String key, InputStreamHandler data)
            throws IOException {
        if (!postPolicyVerifier.hasPolicy(webContext)) {
            // S3 only accepts a POST to an object as browser-based upload, which always carries a policy.
            ByteStreams.exhaust(data);
            errorSynthesizer.synthesiseError(webContext,
                                             bucket.getName(),
                                             key,
                                             S3ErrorCode.MethodNotAllowed,
                                             "The specified method is not allowed against this resource.");
            return;
        }

        PostPolicyVerifier.Result verification = postPolicyVerifier.verify(webContext, bucket, key);
        if (!verification.isValid()) {
            ByteStreams.exhaust(data);
            errorSynthesizer.synthesiseError(webContext,
                                             bucket.getName(),
                                             key,
                                             verification.errorCode(),
                                             verification.message());
            return;
        }

        // The content is the body of the request, which is streamed and therefore not known to the request itself.
        S3Policy s3Policy = verification.policy();
        long contentSize = webContext.getHeaderValue(HttpHeaderNames.CONTENT_LENGTH).asLong(0);
        S3ErrorCode sizeError = postPolicyVerifier.verifySize(s3Policy, contentSize);
        if (sizeError != null) {
            ByteStreams.exhaust(data);
            errorSynthesizer.synthesiseError(webContext,
                                             bucket.getName(),
                                             key,
                                             sizeError,
                                             PostPolicyVerifier.ERROR_SIZE_NOT_PERMITTED);
            return;
        }

        // Stored without answering, as a browser-based upload is answered as its form asks for.
        Optional<String> etag = objectHandler.storeObject(webContext, bucket, key, data);
        if (etag.isPresent()) {
            browserUploadResponder.respond(webContext, bucket, key, etag.get());
        }
    }
}
