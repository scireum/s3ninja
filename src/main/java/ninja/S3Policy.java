/*
 * Made with all the love in the world
 * by scireum in Stuttgart, Germany
 *
 * Copyright by scireum GmbH
 * https://www.scireum.de - info@scireum.de
 */

package ninja;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import sirius.kernel.commons.Value;
import sirius.web.http.WebContext;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Handles AWS S3 POST policies validation.
 */
public class S3Policy {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final JsonNode policyData;
    private final Instant expiration;
    private final List<Map<String, Object>> conditions;
    public S3Policy(String base64Policy) {
        try {
            String jsonPolicy = new String(Base64.getDecoder().decode(base64Policy), StandardCharsets.UTF_8);
            this.policyData = MAPPER.readTree(jsonPolicy);
            this.expiration = Instant.parse(policyData.get("expiration").asText());
            this.conditions = parseConditions(policyData.get("conditions"));
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid policy document", e);
        }
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> parseConditions(JsonNode conditionsNode) {
        List<Map<String, Object>> result = new ArrayList<>();
        if (conditionsNode instanceof ArrayNode arrayNode) {
            arrayNode.forEach(condition -> {
                if (condition.isArray()) {
                    Map<String, Object> conditionMap = new HashMap<>();
                    conditionMap.put("operator", condition.get(0).asText());
                    String field = condition.get(1).asText();
                    if (field.startsWith("$") && field.length() > 1) {
                        field = field.substring(1); // Remove '$' prefix
                    }
                    conditionMap.put("field", field);
                    conditionMap.put("value", condition.get(2).asText());
                    result.add(conditionMap);
                } else if (condition.isObject()) {
                    Map<String, Object> conditionMap = new HashMap<>();
                    condition.fields().forEachRemaining(entry ->
                        conditionMap.put(entry.getKey(), entry.getValue().asText()));
                    result.add(conditionMap);
                }
            });
        }
        return result;
    }

    /**
     * Validates if the policy allows the current request
     *
     * @param webContext the request context
     * @param bucket the target bucket
     * @param key the object key
     * @return true if the request is allowed, false otherwise
     */
    public boolean isRequestValid(WebContext webContext, Bucket bucket, String key) {
        if (Instant.now().isAfter(expiration)) {
            return false;
        }

        // Validate each condition
        return conditions.stream().allMatch(condition -> validateCondition(condition, webContext, bucket, key));
    }

    private boolean validateCondition(Map<String, Object> condition, WebContext webContext, Bucket bucket, String key) {
        if (condition.containsKey("operator")) {
            // Format: ["eq", "$key", "user/eric/"]
            String operator = (String) condition.get("operator");
            String field = (String) condition.get("field");
            String value = (String) condition.get("value");

            String actualValue = getFieldValue(field, webContext, bucket, key);
            return switch (operator) {
                case "eq" -> value.equals(actualValue);
                case "starts-with" -> actualValue != null && actualValue.startsWith(value);
                default -> false;
            };
        } else {
            // Format: {"success_action_redirect": "https://example.com/success"}
            Map.Entry<String, Object> entry = condition.entrySet().iterator().next();
            String field = entry.getKey();
            String expectedValue = (String) entry.getValue();

            String actualValue = getFieldValue(field, webContext, bucket, key);
            return expectedValue.equals(actualValue);
        }
    }

    private String getFieldValue(String field, WebContext webContext, Bucket bucket, String key) {
        return switch (field) {
            case "bucket" -> bucket.getName();
            case "key" -> key;
            case "content-length-range" -> webContext.getHeaderValue(HttpHeaderNames.CONTENT_LENGTH).asString();
            case "Content-Type" -> webContext.getHeader("Content-Type");
            case "success_action_redirect" -> webContext.get("success_action_redirect").asString();
            case "success_action_status" -> webContext.get("success_action_status").asString();
            default -> webContext.get(field).asString();
        };
    }

    /**
     * Gets the maximum allowed file size from the policy
     *
     * @return the maximum allowed file size in bytes, or -1 if not specified
     */
    public long getMaxFileSize() {
        return conditions.stream()
                .filter(condition -> condition.containsKey("operator") &&
                        "content-length-range".equals(condition.get("field")))
                .findFirst()
                .map(condition -> Value.of(condition.get("value")).asLong(-1))
                .orElse(-1L);
    }

    /**
     * Gets the success response configuration
     *
     * @return the HTTP status code to use for successful uploads
     */
    public HttpResponseStatus getSuccessResponse() {
        String status = conditions.stream()
                .filter(condition -> condition.containsKey("success_action_status"))
                .findFirst()
                .map(condition -> (String) condition.get("success_action_status"))
                .orElse("204");

        return switch (status) {
            case "200", "201", "204" -> HttpResponseStatus.valueOf(Integer.parseInt(status));
            default -> HttpResponseStatus.NO_CONTENT;
        };
    }

    /**
     * Gets the success redirect URL if specified in the policy
     *
     * @return the URL to redirect to on successful upload, or null if not specified
     */
    public String getSuccessRedirect() {
        return conditions.stream()
                .filter(condition -> condition.containsKey("success_action_redirect"))
                .findFirst()
                .map(condition -> (String) condition.get("success_action_redirect"))
                .orElse(null);
    }
}
