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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
 * Handles AWS S3 POST policies validation and parsing.
 * <p>
 * This class implements the AWS S3 POST policy validation mechanism as specified in the
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-HTTPPOSTConstructPolicy.html">AWS S3 Documentation</a>.
 * <p>
 * An S3 POST policy is a JSON document (base64-encoded) that defines the conditions under which
 * a browser-based upload can be performed. The policy includes:
 * <ul>
 *     <li><b>expiration</b>: An ISO 8601 timestamp after which the policy is no longer valid</li>
 *     <li><b>conditions</b>: An array of constraints that the upload must satisfy</li>
 * </ul>
 * <p>
 * <b>Supported condition formats:</b>
 * <ul>
 *     <li><b>Exact match</b>: {@code {"bucket": "mybucket"}} - Field must exactly match the value</li>
 *     <li><b>Equality operator</b>: {@code ["eq", "$key", "user/data.txt"]} - Field equals value</li>
 *     <li><b>Starts-with operator</b>: {@code ["starts-with", "$key", "user/"]} - Field starts with prefix</li>
 *     <li><b>Content-length range</b>: {@code ["content-length-range", 0, 10485760]} - File size constraints</li>
 * </ul>
 * <p>
 * <b>Special fields:</b>
 * <ul>
 *     <li>{@code success_action_status}: HTTP status code for successful uploads (200, 201, or 204)</li>
 *     <li>{@code success_action_redirect}: URL to redirect to after successful upload</li>
 *     <li>{@code x-amz-security-token}: AWS STS session token for temporary credentials</li>
 *     <li>{@code x-amz-algorithm}: Signature algorithm (typically AWS4-HMAC-SHA256)</li>
 *     <li>{@code x-amz-credential}: AWS credential scope</li>
 *     <li>{@code x-amz-date}: Request timestamp in ISO 8601 format</li>
 * </ul>
 * <p>
 * <b>Example policy JSON:</b>
 * <pre>{@code
 * {
 *   "expiration": "2026-01-10T12:00:00.000Z",
 *   "conditions": [
 *     {"bucket": "my-bucket"},
 *     ["eq", "$key", "uploads/file.txt"],
 *     ["starts-with", "$Content-Type", "image/"],
 *     ["content-length-range", 0, 10485760],
 *     {"x-amz-algorithm": "AWS4-HMAC-SHA256"},
 *     {"success_action_status": "201"}
 *   ]
 * }
 * }</pre>
 * <p>
 * <b>Thread-safety:</b> This class is immutable and thread-safe after construction.
 * <p>
 * <b>Usage:</b>
 * <pre>{@code
 * String base64Policy = "eyJleHBpcmF0aW9uIjoi..."; // Base64-encoded policy
 * S3Policy policy = new S3Policy(base64Policy);
 *
 * if (policy.isRequestValid(webContext, bucket, key)) {
 *     HttpResponseStatus status = policy.getSuccessResponse();
 *     String redirectUrl = policy.getSuccessRedirect();
 *     // Process upload...
 * }
 * }</pre>
 *
 * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-HTTPPOSTConstructPolicy.html">AWS S3 POST Policy</a>
 * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-post-example.html">AWS S3 POST Example</a>
 */
public class S3Policy {
    private static final Logger LOG = LoggerFactory.getLogger(S3Policy.class);

    /**
     * Jackson ObjectMapper for parsing JSON policy documents.
     */
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final JsonNode policyData;

    /**
     * The expiration timestamp for this policy.
     * Requests received after this instant will be rejected.
     */
    private final Instant expiration;

    /**
     * The list of conditions that must be satisfied for the upload to succeed.
     * Each condition is represented as a map containing operator, field, and value keys.
     */
    private final List<Map<String, Object>> conditions;

    /**
     * Constructs an S3Policy from a base64-encoded policy document.
     * <p>
     * The policy document must be a valid JSON string containing:
     * <ul>
     *     <li>{@code expiration}: ISO 8601 timestamp (e.g., "2026-01-10T12:00:00.000Z")</li>
     *     <li>{@code conditions}: Array of condition objects or arrays</li>
     * </ul>
     * <p>
     * The constructor performs the following steps:
     * <ol>
     *     <li>Decodes the base64-encoded policy string</li>
     *     <li>Parses the JSON document using Jackson</li>
     *     <li>Extracts and validates the expiration timestamp</li>
     *     <li>Parses all conditions into an internal representation</li>
     * </ol>
     *
     * @param base64Policy the base64-encoded JSON policy document
     * @throws IllegalArgumentException if the policy document is invalid, malformed, cannot be parsed,
     *                                  or does not contain required fields (expiration, conditions)
     */
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

    /**
     * Parses the conditions array from the policy JSON.
     * <p>
     * Handles two condition formats:
     * <ul>
     *     <li><b>Array format</b>: {@code ["operator", "$field", "value"]} - e.g., ["eq", "$key", "file.txt"]</li>
     *     <li><b>Object format</b>: {@code {"field": "value"}} - e.g., {"bucket": "mybucket"}</li>
     * </ul>
     * <p>
     * For array format conditions, the method:
     * <ul>
     *     <li>Extracts the operator from position 0 (e.g., "eq", "starts-with")</li>
     *     <li>Extracts the field name from position 1, removing the '$' prefix if present</li>
     *     <li>Extracts the value from position 2</li>
     * </ul>
     * <p>
     * For object format conditions, all key-value pairs are directly copied to the result map.
     *
     * @param conditionsNode the JSON node containing the conditions array from the policy
     * @return a list of maps, each representing a parsed condition with keys:
     *         <ul>
     *             <li>{@code operator}, {@code field}, {@code value} for array-format conditions</li>
     *             <li>Direct field-value pairs for object-format conditions</li>
     *         </ul>
     */
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
                    condition.properties().forEach(entry ->
                                                           conditionMap.put(entry.getKey(), entry.getValue().asText()));
                    result.add(conditionMap);
                }
            });
        }
        return result;
    }

    /**
     * Validates if the policy allows the current request.
     * <p>
     * This method performs two levels of validation:
     * <ol>
     *     <li><b>Expiration check</b>: Verifies that the current timestamp is before the policy's expiration</li>
     *     <li><b>Condition validation</b>: Ensures all conditions in the policy are satisfied by the request</li>
     * </ol>
     * <p>
     * The request is considered valid only if:
     * <ul>
     *     <li>The policy has not expired</li>
     *     <li>ALL conditions are satisfied (logical AND operation)</li>
     * </ul>
     * <p>
     * <b>Common validation scenarios:</b>
     * <ul>
     *     <li>Bucket name must match the policy</li>
     *     <li>Object key must match or start with the specified prefix</li>
     *     <li>Content-Type must match if specified</li>
     *     <li>AWS signature fields (x-amz-*) must be present and correct</li>
     * </ul>
     *
     * @param webContext the HTTP request context containing headers, form fields, and parameters
     * @param bucket the target bucket for the upload
     * @param key the object key (file path) for the upload
     * @return true if the request satisfies all policy conditions and has not expired, false otherwise
     */
    public boolean isRequestValid(WebContext webContext, Bucket bucket, String key) {
        if (Instant.now().isAfter(expiration)) {
            return false;
        }

        // Validate each condition
        return conditions.stream().allMatch(condition -> validateCondition(condition, webContext, bucket, key));
    }

    /**
     * Validates a single condition against the current request.
     * <p>
     * Supports two validation modes:
     * <ul>
     *     <li><b>Operator-based validation</b>: For array-format conditions like ["eq", "$key", "value"]
     *         <ul>
     *             <li>{@code eq}: Exact match - the field value must equal the condition value</li>
     *             <li>{@code starts-with}: Prefix match - the field value must start with the condition value</li>
     *         </ul>
     *     </li>
     *     <li><b>Direct match validation</b>: For object-format conditions like {"bucket": "mybucket"}
     *         <ul>
     *             <li>The actual field value must exactly equal the expected value</li>
     *         </ul>
     *     </li>
     * </ul>
     * <p>
     * <b>Examples:</b>
     * <pre>{@code
     * // Operator-based: ["eq", "$key", "uploads/file.txt"]
     * // Validates that the object key is exactly "uploads/file.txt"
     *
     * // Operator-based: ["starts-with", "$key", "uploads/"]
     * // Validates that the object key starts with "uploads/"
     *
     * // Direct match: {"bucket": "my-bucket"}
     * // Validates that the bucket name is exactly "my-bucket"
     * }</pre>
     *
     * @param condition a map representing the condition (parsed by {@link #parseConditions(JsonNode)})
     * @param webContext the HTTP request context
     * @param bucket the target bucket
     * @param key the object key
     * @return true if the condition is satisfied, false otherwise
     */
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

    /**
     * Extracts the actual value of a field from the request context.
     * <p>
     * Handles special fields with specific extraction logic:
     * <ul>
     *     <li>{@code bucket}: Returns the bucket name from the bucket object</li>
     *     <li>{@code key}: Returns the object key parameter</li>
     *     <li>{@code content-length-range}: Returns the Content-Length header value</li>
     *     <li>{@code Content-Type}: Returns the Content-Type header value</li>
     *     <li>{@code success_action_redirect}: Returns the redirect URL from form field</li>
     *     <li>{@code success_action_status}: Returns the success status from form field</li>
     * </ul>
     * <p>
     * For all other fields (including AWS signature fields like x-amz-algorithm, x-amz-credential, etc.),
     * the method attempts to extract the value from form parameters or request parameters.
     *
     * @param field the field name to extract (without '$' prefix, which was already removed by {@link #parseConditions(JsonNode)})
     * @param webContext the HTTP request context
     * @param bucket the target bucket
     * @param key the object key
     * @return the field value as a string, or null if not present
     */
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
     * Gets the maximum allowed file size from the policy.
     * <p>
     * This method searches for a {@code content-length-range} condition in the policy
     * and extracts the maximum value (the second number in the range).
     * <p>
     * <b>Example policy condition:</b>
     * <pre>{@code
     * ["content-length-range", 0, 10485760]  // Min: 0 bytes, Max: 10 MB
     * }</pre>
     * <p>
     * The method returns the maximum value (10485760 in the example above).
     *
     * @return the maximum allowed file size in bytes, or -1 if no content-length-range condition is specified
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
     * Gets the success response HTTP status code configuration from the policy.
     * <p>
     * This method extracts the {@code success_action_status} field from the policy conditions.
     * AWS S3 supports only three values:
     * <ul>
     *     <li>{@code 200}: Returns HTTP 200 OK with an empty body</li>
     *     <li>{@code 201}: Returns HTTP 201 CREATED with XML document containing the object location</li>
     *     <li>{@code 204}: Returns HTTP 204 NO_CONTENT with no response body (default)</li>
     * </ul>
     * <p>
     * If an invalid status code is specified (not 200, 201, or 204), the method:
     * <ul>
     *     <li>Logs a warning message</li>
     *     <li>Returns 204 (NO_CONTENT) as the fallback default</li>
     * </ul>
     * <p>
     * <b>Example policy condition:</b>
     * <pre>{@code
     * {"success_action_status": "201"}
     * }</pre>
     *
     * @return the HTTP status code to use for successful uploads (200, 201, or 204)
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-post-example.html">AWS S3 success_action_status</a>
     */
    public HttpResponseStatus getSuccessResponse() {
        String status = conditions.stream()
                                  .filter(condition -> condition.containsKey("success_action_status"))
                                  .findFirst()
                                  .map(condition -> (String) condition.get("success_action_status"))
                                  .orElse("204");

        return switch (status) {
            case "200", "201", "204" -> HttpResponseStatus.valueOf(Integer.parseInt(status));
            default -> {
                LOG.warn("Success values\\_action\\_status unsupported: `{}`. 204\\(NO\\_CONTENT\\). " +
                         "Allow values: 200\\, 201\\, 204\\.", status);
                yield HttpResponseStatus.NO_CONTENT;
            }
        };
    }

    /**
     * Gets the success redirect URL from the policy if specified.
     * <p>
     * This method extracts the {@code success_action_redirect} field from the policy conditions.
     * When present, AWS S3 will redirect the client to this URL after a successful upload instead of
     * returning a standard HTTP response.
     * <p>
     * The redirect URL will typically include query parameters with information about the uploaded object:
     * <ul>
     *     <li>{@code bucket}: The name of the bucket</li>
     *     <li>{@code key}: The object key (filename)</li>
     *     <li>{@code etag}: The ETag of the uploaded object</li>
     * </ul>
     * <p>
     * <b>Example policy condition:</b>
     * <pre>{@code
     * {"success_action_redirect": "https://example.com/upload-success"}
     * }</pre>
     * <p>
     * After a successful upload, the client would be redirected to:
     * <pre>
     * <a href="https://example.com/upload-success?bucket=mybucket&key=file.txt&etag=abc123">...</a>...
     * </pre>
     * <p>
     * <b>Note:</b> If both {@code success_action_redirect} and {@code success_action_status}
     * are specified, the redirect takes precedence.
     *
     * @return the URL to redirect to on successful upload, or null if not specified in the policy
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-post-example.html">AWS S3 success_action_redirect</a>
     */
    public String getSuccessRedirect() {
        return conditions.stream()
                .filter(condition -> condition.containsKey("success_action_redirect"))
                .findFirst()
                .map(condition -> (String) condition.get("success_action_redirect"))
                .orElse(null);
    }
}
