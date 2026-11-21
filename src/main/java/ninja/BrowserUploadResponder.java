/*
 * Made with all the love in the world
 * by scireum in Stuttgart, Germany
 *
 * Copyright by scireum GmbH
 * https://www.scireum.de - info@scireum.de
 */

package ninja;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Urls;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.kernel.xml.XMLStructuredOutput;
import sirius.web.http.Response;
import sirius.web.http.WebContext;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;

/**
 * Answers a browser-based upload which has been stored, the way its form asks for.
 * <p>
 * As for S3, a form may name a URL in <tt>success_action_redirect</tt> (or the deprecated <tt>redirect</tt>), which
 * the client is redirected to with <tt>303 See Other</tt>, the bucket, the key and the entity tag being appended as
 * parameters. A URL which cannot be interpreted is ignored. Otherwise the form may ask for a status in
 * <tt>success_action_status</tt>: 200 and 204 come without content, 201 with a <tt>PostResponse</tt> document
 * describing the object. A missing or invalid status is answered with 204. Every answer carries the entity tag and
 * the location of the object.
 */
@Register(classes = BrowserUploadResponder.class)
public class BrowserUploadResponder {

    private static final String FIELD_SUCCESS_ACTION_REDIRECT = "success_action_redirect";
    private static final String FIELD_REDIRECT = "redirect";
    private static final String FIELD_SUCCESS_ACTION_STATUS = "success_action_status";

    @Part
    private S3Responder responder;

    /**
     * Answers a browser-based upload which has been stored successfully.
     *
     * @param webContext the request carrying the form
     * @param bucket     the bucket the object has been stored in
     * @param key        the key of the stored object
     * @param etag       the entity tag of the stored object, without quotes
     */
    public void respond(WebContext webContext, Bucket bucket, String key, String etag) {
        String quotedEtag = ObjectHandler.quoteEtag(etag);
        Optional<String> redirect = determineRedirect(webContext);
        if (redirect.isPresent()) {
            String parameters =
                    "bucket=" + Urls.encode(bucket.getName()) + "&key=" + Urls.encode(key) + "&etag=" + Urls.encode(
                            quotedEtag);
            webContext.respondWith()
                      .setHeader(S3Dispatcher.HTTP_HEADER_NAME_ETAG, quotedEtag)
                      .setHeader(HttpHeaderNames.ACCESS_CONTROL_EXPOSE_HEADERS, S3Dispatcher.HTTP_HEADER_NAME_ETAG)
                      .setHeader(HttpHeaderNames.LOCATION, appendParameters(redirect.get(), parameters))
                      .status(HttpResponseStatus.SEE_OTHER);
            responder.signalObjectSuccess(webContext);
            return;
        }

        String location = webContext.getBaseURL() + "/" + bucket.getName() + "/" + StoredObject.encodeKey(key);
        Response response = webContext.respondWith()
                                      .setHeader(S3Dispatcher.HTTP_HEADER_NAME_ETAG, quotedEtag)
                                      .setHeader(HttpHeaderNames.ACCESS_CONTROL_EXPOSE_HEADERS,
                                                 S3Dispatcher.HTTP_HEADER_NAME_ETAG)
                                      .setHeader(HttpHeaderNames.LOCATION, location);
        switch (webContext.get(FIELD_SUCCESS_ACTION_STATUS).asString("")) {
            case "200" -> response.status(HttpResponseStatus.OK);
            case "201" -> {
                XMLStructuredOutput out = response.xml(HttpResponseStatus.CREATED);
                out.beginOutput("PostResponse");
                out.property("Location", location);
                out.property(S3Dispatcher.RESPONSE_BUCKET, bucket.getName());
                out.property("Key", key);
                out.property(S3Dispatcher.HTTP_HEADER_NAME_ETAG, quotedEtag);
                out.endOutput();
            }
            default -> response.status(HttpResponseStatus.NO_CONTENT);
        }
        responder.signalObjectSuccess(webContext);
    }

    /**
     * Appends the given parameters to the query of the given URL, in front of its fragment, if any.
     */
    private String appendParameters(String url, String parameters) {
        int fragmentStart = url.indexOf('#');
        String base = fragmentStart < 0 ? url : url.substring(0, fragmentStart);
        String fragment = fragmentStart < 0 ? "" : url.substring(fragmentStart);
        return base + (base.contains("?") ? "&" : "?") + parameters + fragment;
    }

    /**
     * Determines the URL to redirect to, if the form names one which can be interpreted.
     *
     * @param webContext the request carrying the form
     * @return the URL, or an empty optional if the form names none, or none which is an absolute HTTP(S) URL
     */
    private Optional<String> determineRedirect(WebContext webContext) {
        String redirect =
                webContext.get(FIELD_SUCCESS_ACTION_REDIRECT).asString(webContext.get(FIELD_REDIRECT).asString());
        if (Strings.isEmpty(redirect)) {
            return Optional.empty();
        }
        try {
            URI uri = new URI(redirect);
            boolean httpUrl = "http".equalsIgnoreCase(uri.getScheme()) || "https".equalsIgnoreCase(uri.getScheme());
            return httpUrl && Strings.isFilled(uri.getHost()) ? Optional.of(redirect) : Optional.empty();
        } catch (URISyntaxException _) {
            // As for S3, a URL which cannot be interpreted is ignored.
            return Optional.empty();
        }
    }
}
