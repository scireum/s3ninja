package ninja;

import com.google.common.base.Charsets;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import sirius.kernel.commons.Strings;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.web.http.WebContext;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.hash.Hashing.sha256;
import static com.google.common.io.BaseEncoding.base16;

/**
 * @author Milos Milivojevic | mmilivojevic@deployinc.com
 */
@Register(classes = Aws4HashCalculator.class)
public class Aws4HashCalculator {
    public static final Pattern AWS_AUTH4_PATTERN = Pattern.compile(
        "AWS4-HMAC-SHA256 Credential=([^/]+)/([^/]+)/([^/]+)/s3/aws4_request, SignedHeaders=([^,]+), Signature=(.+)");


    @Part
    private Storage storage;

    public boolean supports(final WebContext ctx) {
        final Matcher aws4Header = buildMatcher(ctx);
        return aws4Header.matches();
    }

    private Matcher initializedMatcher(final WebContext ctx) {
        Matcher matcher = buildMatcher(ctx);
        return matcher.matches() ? matcher : null;
    }

    private Matcher buildMatcher(final WebContext ctx) {
        return AWS_AUTH4_PATTERN.matcher(ctx.getHeaderValue("Authorization").asString(""));
    }

    public String computeHash(WebContext ctx) throws Exception {
        final MatchResult aws4Header = initializedMatcher(ctx);
        
        byte[] dateKey = hmacSHA256(("AWS4" + storage.getAwsSecretKey()).getBytes(UTF_8), aws4Header.group(2));
        byte[] dateRegionKey = hmacSHA256(dateKey, aws4Header.group(3));
        byte[] dateRegionServiceKey = hmacSHA256(dateRegionKey, "s3");
        byte[] signingKey = hmacSHA256(dateRegionServiceKey, "aws4_request");

        byte[] signedData = hmacSHA256(signingKey, buildStringToSign(ctx));
        return base16().lowerCase().encode(signedData);
    }

    private String buildStringToSign(final WebContext ctx) {
        final StringBuilder canonicalRequest = buildCanonicalRequest(ctx);
        final MatchResult aws4Header = initializedMatcher(ctx);
        return "AWS4-HMAC-SHA256\n" + ctx.getHeader("x-amz-date") + "\n" + ctx
            .getHeader("x-amz-date").substring(0, 8) + "/" + aws4Header.group(3)
            + "/s3/aws4_request\n" + hashedCanonicalRequest(canonicalRequest);
    }

    private StringBuilder buildCanonicalRequest(final WebContext ctx) {
        final MatchResult aws4Header = initializedMatcher(ctx);
        StringBuilder canonicalRequest = new StringBuilder(ctx.getRequest().getMethod().name());
        canonicalRequest.append("\n");
        canonicalRequest.append(ctx.getRequestedURI());
        canonicalRequest.append("\n");
        canonicalRequest.append(ctx.getQueryString());
        canonicalRequest.append("\n");

        for (String name : aws4Header.group(4).split(";")) {
            canonicalRequest.append(name.trim());
            canonicalRequest.append(":");
            canonicalRequest.append(
                Strings.join(ctx.getRequest().headers().getAll(name), ",").trim());
            canonicalRequest.append("\n");
        }
        canonicalRequest.append("\n");
        canonicalRequest.append(aws4Header.group(4));
        canonicalRequest.append("\n");
        canonicalRequest.append(ctx.getHeader("x-amz-content-sha256"));
        return canonicalRequest;
    }

    private String hashedCanonicalRequest(final StringBuilder canonicalRequest) {
        return sha256().hashString(canonicalRequest, UTF_8).toString();
    }

    private byte[] hmacSHA256(byte[] key, String value) throws Exception {
        SecretKeySpec keySpec = new SecretKeySpec(key, "HmacSHA256");
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(keySpec);
        return mac.doFinal(value.getBytes(Charsets.UTF_8));
    }
}
