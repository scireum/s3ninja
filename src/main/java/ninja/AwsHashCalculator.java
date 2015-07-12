/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package ninja;

import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Exceptions;
import sirius.web.http.WebContext;
import sirius.web.security.UserContext;

import java.util.regex.Pattern;

/**
 * Class in charge of generating the appropriate hash for the given request and path prefix by
 * delegating the computation to either {@link Aws4HashCalculator} or {@link
 * AwsLegacyHashCalculator} depending of whether or not  Aws4HashCalculator supports the request
 * or not
 */
@Register(classes = AwsHashCalculator.class)
public class AwsHashCalculator {

    protected static final Pattern AWS_AUTH_PATTERN = Pattern.compile("AWS ([^:]+):(.*)");

    @Part
    private Aws4HashCalculator aws4HashCalculator;

    @Part
    private AwsLegacyHashCalculator legacyHashCalculator;

    /**
     * Computes the authentication hash as specified by the AWS SDK for verification purposes.
     *
     * @param ctx        the current request to fetch parameters from
     * @param pathPrefix the path prefix to append to the current uri
     * @return the computes hash value
     */
    public String computeHash(WebContext ctx, String pathPrefix) {
        try {
            return doComputeHash(ctx, pathPrefix);
        } catch (Throwable e) {
            throw Exceptions.handle(UserContext.LOG, e);
        }
    }

    private String doComputeHash(final WebContext ctx, final String pathPrefix) throws Exception {
        if (aws4HashCalculator.supports(ctx)) {
            return aws4HashCalculator.computeHash(ctx);
        } else {
            return legacyHashCalculator.computeHash(ctx, pathPrefix);
        }
    }
}
