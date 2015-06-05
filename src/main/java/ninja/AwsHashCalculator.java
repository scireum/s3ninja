package ninja;

import java.util.regex.Pattern;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Exceptions;
import sirius.web.http.WebContext;
import sirius.web.security.UserContext;

/**
 * @author Milos Milivojevic | mmilivojevic@deployinc.com
 */
@Register(classes = AwsHashCalculator.class)
public class AwsHashCalculator {

    public static final Pattern AWS_AUTH_PATTERN = Pattern.compile("AWS ([^:]+):(.*)");

    @Part
    private Aws4HashCalculator aws4HashCalculator;

    @Part
    private AwsLegacyHashCalculator legacyHashCalculator;
    
    public String computeHash(WebContext ctx, String pathPrefix) {
        try {
            if (aws4HashCalculator.supports(ctx)) {
                return aws4HashCalculator.computeHash(ctx);
            } else {
                return legacyHashCalculator.computeHash(ctx, pathPrefix);
            }
        } catch (Throwable e) {
            throw Exceptions.handle(UserContext.LOG, e);
        }
    }
}
