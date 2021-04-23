/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package ninja.errors;

import sirius.kernel.commons.Strings;
import sirius.kernel.di.std.Register;
import sirius.kernel.xml.XMLStructuredOutput;
import sirius.web.http.WebContext;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Synthesizes <em>S3</em> <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html">error
 * responses</a>.
 */
@Register(classes = S3ErrorSynthesizer.class)
public class S3ErrorSynthesizer {

    /**
     * Synthesizes an error response.
     *
     * @param webContext the request to process
     * @param bucket     the requested bucket, potentially <b>null</b>
     * @param key        the requested object's key, potentially <b>null</b>
     * @param code       the error code to send
     * @param message    a human-readable description of the error
     */
    public void synthesiseError(@Nonnull WebContext webContext,
                                @Nullable String bucket,
                                @Nullable String key,
                                @Nonnull S3ErrorCode code,
                                @Nullable String message) {
        XMLStructuredOutput xml =
                new XMLStructuredOutput(webContext.respondWith().outputStream(code.getHttpStatusCode(), "text/xml"));

        String resource = null;
        if (Strings.isFilled(bucket)) {
            resource = "/" + bucket;
            if (Strings.isFilled(key)) {
                resource += "/" + key;
            }
        }

        xml.beginOutput("Error");
        xml.property("Code", code.toString());
        xml.propertyIfFilled("Message", message);
        xml.propertyIfFilled("Resource", resource);
        xml.endOutput();
    }
}
