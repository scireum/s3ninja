/*
 * Made with all the love in the world
 * by scireum in Stuttgart, Germany
 *
 * Copyright by scireum GmbH
 * https://www.scireum.de - info@scireum.de
 */

package ninja;

import java.util.Map;

/**
 * DTO returned to the UI for presigned POST generation.
 */
public record PresignedPostResponse(String url,
                                    Map<String, String> fields,
                                    String policyJson,
                                    String policyBase64,
                                    String signature) {
}

