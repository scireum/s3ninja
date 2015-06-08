package ninja;

import com.google.common.base.Charsets;
import com.google.common.io.BaseEncoding;
import io.netty.handler.codec.http.HttpHeaders;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.web.http.WebContext;

import static sirius.kernel.commons.Strings.join;

/**
 * Hash calculator for legacy AWS signature calculation
 */
@Register(classes = AwsLegacyHashCalculator.class)
public class AwsLegacyHashCalculator {

    @Part
    private Storage storage;

    public String computeHash(WebContext ctx, String pathPrefix) throws Exception {
        StringBuilder stringToSign = new StringBuilder(ctx.getRequest().getMethod().name());
        stringToSign.append("\n");
        stringToSign.append(ctx.getHeaderValue("Content-MD5").asString(""));
        stringToSign.append("\n");
        stringToSign.append(ctx.getHeaderValue("Content-Type").asString(""));
        stringToSign.append("\n");
        stringToSign.append(ctx.get("Expires").asString(
            ctx.getHeaderValue("x-amz-date").asString(ctx.getHeaderValue("Date").asString(""))));
        stringToSign.append("\n");

        HttpHeaders requestHeaders = ctx.getRequest().headers();
        List<String> headers = requestHeaders.names().stream()
            .filter(this::relevantAmazonHeader)
            .map(name -> toHeaderStringRepresentation(name, requestHeaders))
            .collect(Collectors.toList());
        
        Collections.sort(headers);
        for (String header : headers) {
            stringToSign.append(header);
            stringToSign.append("\n");
        }

        stringToSign.append(pathPrefix).append(ctx.getRequestedURI().substring(3));

        SecretKeySpec keySpec = new SecretKeySpec(storage.getAwsSecretKey().getBytes(), "HmacSHA1");
        Mac mac = Mac.getInstance("HmacSHA1");
        mac.init(keySpec);
        byte[] result = mac.doFinal(stringToSign.toString().getBytes(Charsets.UTF_8.name()));
        return BaseEncoding.base64().encode(result);
    }

    private boolean relevantAmazonHeader(final String name) {
        return name.toLowerCase().startsWith("x-amz-") && !"x-amz-date".equals(name.toLowerCase());
    }

    private String toHeaderStringRepresentation(
        final String headerName, final HttpHeaders requestHeaders) {
        return headerName.toLowerCase().trim() + ":" + 
            join(requestHeaders.getAll(headerName), ",").trim();
    }
}
