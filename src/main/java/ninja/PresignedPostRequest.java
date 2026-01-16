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

    public static PresignedPostRequest from(WebContext ctx) {
        String bucket = ctx.get("bucket").asString();
        String key = ctx.get("key").asString();
        String region = ctx.get("region").asString();
        int expiration = ctx.get("expirationHours").asInt(24);
        String sessionToken = ctx.get("sessionToken").asString();
        if (Strings.isEmpty(sessionToken)) {
            sessionToken = null;
        }
        String accessKey = ctx.get("accessKey").asString();

        if (Strings.isEmpty(bucket) || Strings.isEmpty(key) || Strings.isEmpty(region) || Strings.isEmpty(accessKey)) {
            throw Exceptions.createHandled().withDirectMessage("Missing required fields").handle();
        }

        return new PresignedPostRequest(bucket, key, region, expiration, sessionToken, accessKey);
    }
}

