/*
 * Made with all the love in the world
 * by scireum in Stuttgart, Germany
 *
 * Copyright by scireum GmbH
 * https://www.scireum.de - info@scireum.de
 */

package ninja;

import io.netty.handler.codec.http.HttpMethod;
import ninja.errors.S3ErrorCode;
import ninja.errors.S3ErrorSynthesizer;
import sirius.kernel.async.CallContext;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.kernel.xml.XMLStructuredOutput;
import sirius.web.http.WebContext;

/**
 * Sends what all handlers of S3 calls have in common: errors, entries in the API log and recurring parts of
 * responses.
 */
@Register(classes = S3Responder.class)
public class S3Responder {

    private static final String RESPONSE_DISPLAY_NAME = "DisplayName";

    @Part
    private APILog log;

    @Part
    private S3ErrorSynthesizer errorSynthesizer;

    /**
     * Writes an API error to the log
     */
    void signalObjectError(WebContext webContext, String bucket, String key, S3ErrorCode errorCode, String message) {
        if (HttpMethod.HEAD.equals(webContext.getRequest().method())) {
            webContext.respondWith().status(errorCode.getHttpStatusCode());
        } else {
            errorSynthesizer.synthesiseError(webContext, bucket, key, errorCode, message);
        }
        log.log(webContext.getRequest().method().name(),
                message + " - " + webContext.getRequestedURI(),
                APILog.Result.ERROR,
                CallContext.getCurrent().getWatch());
    }

    /**
     * Writes an API success entry to the log
     */
    void signalObjectSuccess(WebContext webContext) {
        log.log(webContext.getRequest().method().name(),
                webContext.getRequestedURI(),
                APILog.Result.OK,
                CallContext.getCurrent().getWatch());
    }

    void outputOwnerInfo(XMLStructuredOutput out, String name) {
        out.beginObject(name);
        out.property("ID", "initiatorId");
        out.property(RESPONSE_DISPLAY_NAME, "initiatorName");
        out.endObject();
    }
}
