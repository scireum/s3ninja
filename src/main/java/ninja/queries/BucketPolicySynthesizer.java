/*
 * Made with all the love in the world
 * by scireum in Stuttgart, Germany
 *
 * Copyright by scireum GmbH
 * https://www.scireum.de - info@scireum.de
 */

package ninja.queries;

import ninja.Bucket;
import ninja.errors.S3ErrorCode;
import ninja.errors.S3ErrorSynthesizer;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.web.http.WebContext;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Synthesizes <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketPolicy">bucket policy</a>
 * responses.
 */
@Register(name = "policy")
public class BucketPolicySynthesizer implements S3QueryProcessor {

    @Part
    private S3ErrorSynthesizer errorSynthesizer;

    @Override
    public void processQuery(@Nonnull WebContext webContext,
                             @Nonnull Bucket bucket,
                             @Nullable String key,
                             @Nonnull String query) {
        errorSynthesizer.synthesiseError(webContext,
                                         bucket.getName(),
                                         key,
                                         S3ErrorCode.NoSuchBucketPolicy,
                                         "The bucket does not have a policy.");
    }
}
