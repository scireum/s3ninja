/*
 * Made with all the love in the world
 * by scireum in Stuttgart, Germany
 *
 * Copyright by scireum GmbH
 * https://www.scireum.de - info@scireum.de
 */

package ninja;

import io.netty.handler.codec.http.HttpHeaderNames;
import sirius.kernel.commons.Value;
import sirius.web.http.WebContext;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ArrayNode;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
 *     // Process upload...
 * }
 * }</pre>
 *
 * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-HTTPPOSTConstructPolicy.html">AWS S3 POST Policy</a>
 * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-post-example.html">AWS S3 POST Example</a>
 */
public class S3Policy {

    /**
     * Holds the operator of a condition given as array, e.g. <tt>eq</tt> or <tt>starts-with</tt>.
     */
    private static final String CONDITION_OPERATOR = "operator";

    /**
     * Holds the name of the form field a condition given as array is about.
     */
    private static final String CONDITION_FIELD = "field";

    /**
     * Holds the value a condition given as array compares with, or the maximum of a size range.
     */
    private static final String CONDITION_VALUE = "value";

    /**
     * Names the form field which states the type of the uploaded object.
     */
    private static final String FIELD_CONTENT_TYPE = "Content-Type";

    /**
     * Names the form field which states where to redirect to after a successful upload.
     */
    private static final String FIELD_SUCCESS_ACTION_REDIRECT = "success_action_redirect";

    /**
     * Names the form field which states the status to answer a successful upload with.
     */
    private static final String FIELD_SUCCESS_ACTION_STATUS = "success_action_status";

    private static final String CONTENT_LENGTH_RANGE = "content-length-range";

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
            this.expiration = Instant.parse(policyData.get("expiration").asString(""));
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
     * <ul>
     *     <li>{@code operator}, {@code field}, {@code value} for array-format conditions</li>
     *     <li>Direct field-value pairs for object-format conditions</li>
     * </ul>
     */
    private List<Map<String, Object>> parseConditions(JsonNode conditionsNode) {
        List<Map<String, Object>> result = new ArrayList<>();
        if (conditionsNode instanceof ArrayNode arrayNode) {
            arrayNode.forEach(condition -> parseCondition(condition).ifPresent(result::add));
        }
        return result;
    }

    /**
     * Parses a single condition of a policy.
     *
     * @param condition the condition, given as array or as object
     * @return the condition as map, or an empty optional if it is of neither form
     */
    private Optional<Map<String, Object>> parseCondition(JsonNode condition) {
        if (condition.isArray() && CONTENT_LENGTH_RANGE.equals(condition.get(0).asString(""))) {
            // Format: ["content-length-range", 0, 1048576], which names no field but the permitted sizes.
            Map<String, Object> conditionMap = new HashMap<>();
            conditionMap.put(CONDITION_OPERATOR, CONTENT_LENGTH_RANGE);
            conditionMap.put(CONDITION_FIELD, CONTENT_LENGTH_RANGE);
            conditionMap.put("minimum", condition.get(1).asString(""));
            conditionMap.put(CONDITION_VALUE, condition.get(2).asString(""));
            return Optional.of(conditionMap);
        }

        if (condition.isArray()) {
            Map<String, Object> conditionMap = new HashMap<>();
            conditionMap.put(CONDITION_OPERATOR, condition.get(0).asString(""));
            String field = condition.get(1).asString("");
            if (field.startsWith("$") && field.length() > 1) {
                field = field.substring(1); // Remove '$' prefix
            }
            conditionMap.put(CONDITION_FIELD, field);
            conditionMap.put(CONDITION_VALUE, condition.get(2).asString(""));
            return Optional.of(conditionMap);
        }

        if (condition.isObject()) {
            if (condition.isEmpty()) {
                throw new IllegalArgumentException("A condition must name a field.");
            }
            Map<String, Object> conditionMap = new HashMap<>();
            condition.properties().forEach(entry -> conditionMap.put(entry.getKey(), entry.getValue().asString("")));
            return Optional.of(conditionMap);
        }

        return Optional.empty();
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
     * @param bucket     the target bucket for the upload
     * @param key        the object key (file path) for the upload
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
     * @param condition  a map representing the condition (parsed by {@link #parseConditions(JsonNode)})
     * @param webContext the HTTP request context
     * @param bucket     the target bucket
     * @param key        the object key
     * @return true if the condition is satisfied, false otherwise
     */
    private boolean validateCondition(Map<String, Object> condition, WebContext webContext, Bucket bucket, String key) {
        if (condition.containsKey(CONDITION_OPERATOR)) {
            // Format: ["eq", "$key", "user/eric/"]
            String operator = (String) condition.get(CONDITION_OPERATOR);
            String field = (String) condition.get(CONDITION_FIELD);
            String value = (String) condition.get(CONDITION_VALUE);

            String actualValue = getFieldValue(field, webContext, bucket, key);
            return switch (operator) {
                case "eq" -> value.equals(actualValue);
                case "starts-with" -> actualValue != null && actualValue.startsWith(value);
                // The size of the upload is only known once it has been received, see isPermittedSize(long).
                case CONTENT_LENGTH_RANGE -> true;
                default -> false;
            };
        } else {
            // A condition given as object names a field and the exact value it has to have.
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
     * @param field      the field name to extract (without '$' prefix, which was already removed by {@link #parseConditions(JsonNode)})
     * @param webContext the HTTP request context
     * @param bucket     the target bucket
     * @param key        the object key
     * @return the field value as a string, or null if not present
     */
    private String getFieldValue(String field, WebContext webContext, Bucket bucket, String key) {
        return switch (field) {
            case "bucket" -> bucket.getName();
            case "key" -> key;
            case CONTENT_LENGTH_RANGE -> webContext.getHeaderValue(HttpHeaderNames.CONTENT_LENGTH).asString();
            // The type of the object is a field of the form. The header describes the form itself.
            case FIELD_CONTENT_TYPE -> PostPolicyVerifier.readFormField(webContext, FIELD_CONTENT_TYPE)
                                                         .asString(webContext.getHeader(HttpHeaderNames.CONTENT_TYPE));
            case FIELD_SUCCESS_ACTION_REDIRECT, FIELD_SUCCESS_ACTION_STATUS ->
                    PostPolicyVerifier.readFormField(webContext, field).asString();
            default -> PostPolicyVerifier.readFormField(webContext, field).asString("");
        };
    }

    /**
     * Determines if a condition of this policy names the given form field.
     * <p>
     * Names are compared without regard to case.
     *
     * @param fieldName the name of a field of the form
     * @return <tt>true</tt> if a condition is about the field, <tt>false</tt> otherwise
     */
    public boolean coversField(String fieldName) {
        return conditions.stream().anyMatch(condition -> namesField(condition, fieldName));
    }

    private boolean namesField(Map<String, Object> condition, String fieldName) {
        if (CONTENT_LENGTH_RANGE.equals(condition.get(CONDITION_OPERATOR))) {
            // Limits the size of the file, and names no field of the form.
            return false;
        }
        if (condition.containsKey(CONDITION_OPERATOR)) {
            return fieldName.equalsIgnoreCase((String) condition.get(CONDITION_FIELD));
        }
        return condition.keySet().stream().anyMatch(fieldName::equalsIgnoreCase);
    }

    /**
     * Determines if an upload of the given size meets the <tt>content-length-range</tt> condition of this policy.
     *
     * @param size the size of the uploaded content in bytes
     * @return <tt>true</tt> if the policy states no such condition or the size lies within its range,
     * <tt>false</tt> otherwise
     */
    public boolean isPermittedSize(long size) {
        return !isBelowMinimumSize(size) && conditions.stream()
                                                      .filter(condition -> CONTENT_LENGTH_RANGE.equals(condition.get(
                                                              CONDITION_OPERATOR)))
                                                      .allMatch(condition -> size <= Value.of(condition.get(
                                                              CONDITION_VALUE)).asLong(Long.MAX_VALUE));
    }

    /**
     * Determines if an upload of the given size is smaller than the <tt>content-length-range</tt> condition of this
     * policy permits.
     *
     * @param size the size of the uploaded content in bytes
     * @return <tt>true</tt> if the policy states a minimum which the size does not reach, <tt>false</tt> otherwise
     */
    public boolean isBelowMinimumSize(long size) {
        return conditions.stream()
                         .filter(condition -> CONTENT_LENGTH_RANGE.equals(condition.get(CONDITION_OPERATOR)))
                         .anyMatch(condition -> size < Value.of(condition.get("minimum")).asLong(0));
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
                         .filter(condition -> condition.containsKey(CONDITION_OPERATOR) && CONTENT_LENGTH_RANGE.equals(
                                 condition.get(CONDITION_FIELD)))
                         .findFirst()
                         .map(condition -> Value.of(condition.get(CONDITION_VALUE)).asLong(-1))
                         .orElse(-1L);
    }
}
