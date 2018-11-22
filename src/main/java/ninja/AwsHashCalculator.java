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

import java.util.Collection;
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
     * Computes possible authentication hashes as specified by the AWS SDK for verification purposes.
     *
     * @param ctx the current request to fetch parameters from
     * @return the computes hash values
     */
    public Collection<String> computeHash(WebContext ctx) {
        try {
            if (aws4HashCalculator.supports(ctx)) {
                return aws4HashCalculator.computeHash(ctx);
            } else {
                return legacyHashCalculator.computeHash(ctx);
            }
        } catch (Exception e) {
            throw Exceptions.handle(UserContext.LOG, e);
        }
    }
}
