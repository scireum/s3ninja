/*
 * Made with all the love in the world
 * by scireum in Stuttgart, Germany
 *
 * Copyright by scireum GmbH
 * https://www.scireum.de - info@scireum.de
 */

package ninja;

import sirius.kernel.commons.Strings;
import sirius.kernel.health.Exceptions;
import sirius.web.http.WebContext;

/**
 * DTO describing the presigned POST generation request.
 */
public record PresignedPostRequest(String bucket, String key, String region, int expirationHours, String sessionToken,
                                   String accessKey) {

    /**
     * Reads a presigned POST generation request from the current web request.
     *
     * @param context the web request containing the generation parameters
     * @return the parsed presigned POST generation request
     */
    public static PresignedPostRequest from(WebContext context) {
        String bucket = context.require("bucket").asString();
        String key = context.require("key").asString();
        String region = context.require("region").asString();
        int expiration = context.require("expirationHours").asInt(24);
        String sessionToken = context.get("sessionToken").asString();
        if (Strings.isEmpty(sessionToken)) {
            sessionToken = null;
        }
        String accessKey = context.require("accessKey").asString();

        return new PresignedPostRequest(bucket, key, region, expiration, sessionToken, accessKey);
    }
}

