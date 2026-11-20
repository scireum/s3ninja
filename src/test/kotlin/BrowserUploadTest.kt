/*
 * Made with all the love in the world
 * by scireum in Stuttgart, Germany
 *
 * Copyright by scireum GmbH
 * https://www.scireum.de - info@scireum.de
 */

import com.google.common.io.BaseEncoding
import ninja.PresignedPostRequest
import ninja.PresignedPostService
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import sirius.kernel.SiriusExtension
import sirius.kernel.di.Injector
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.S3Configuration
import software.amazon.awssdk.services.s3.model.S3Exception
import java.net.HttpURLConnection
import java.net.URI
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

/**
 * Verifies that browser-based uploads, which are POST requests carrying a form with a signed policy, are only served
 * if policy and signature are in order.
 * <p>
 * Forms are signed here the way S3 and its SDKs do: the signature covers the policy in its Base64 encoding.
 */
@ExtendWith(SiriusExtension::class)
class BrowserUploadTest {

    @Test
    fun `Form signed with signature version 4 is accepted for a private bucket`() {
        assertEquals(204, postForm("/$BUCKET", signedFields(KEY)))
        assertEquals(CONTENT, readObject(KEY))
    }

    @Test
    fun `Form signed with signature version 2 is accepted for a private bucket`() {
        val policy = encodePolicy(Instant.now().plusSeconds(3600), KEY, emptyList())
        val fields = linkedMapOf(
            "key" to KEY,
            "AWSAccessKeyId" to BaseSdkSupportTest.ACCESS_KEY,
            "policy" to policy,
            "signature" to BaseEncoding.base64().encode(hmac("HmacSHA1", secretKey(""), policy))
        )

        assertEquals(204, postForm("/$BUCKET", fields))
        assertEquals(CONTENT, readObject(KEY))
    }

    @Test
    fun `Form signed with signature version 2 has to name the access key`() {
        val policy = encodePolicy(Instant.now().plusSeconds(3600), KEY, emptyList())
        val signature = BaseEncoding.base64().encode(hmac("HmacSHA1", secretKey(""), policy))
        val withoutAccessKey = linkedMapOf("key" to KEY, "policy" to policy, "signature" to signature)
        val withUnknownAccessKey = LinkedHashMap(withoutAccessKey).also { it["AWSAccessKeyId"] = "AKIAUNKNOWN" }

        assertEquals(403, postForm("/$BUCKET", withoutAccessKey))
        assertEquals(403, postForm("/$BUCKET", withUnknownAccessKey))
        assertFailsWith<S3Exception> { readObject(KEY) }
    }

    @Test
    fun `Form with a wrong or missing signature is refused and stores nothing`() {
        val wrongSignature = signedFields(KEY).also { it["x-amz-signature"] = "0".repeat(64) }
        val signedForOtherPolicy = signedFields(KEY).also {
            it["policy"] = encodePolicy(Instant.now().plusSeconds(7200), KEY, emptyList())
        }
        val noSignature = signedFields(KEY).also { it.remove("x-amz-signature") }

        assertEquals(403, postForm("/$BUCKET", wrongSignature))
        assertEquals(403, postForm("/$BUCKET", signedForOtherPolicy))
        assertEquals(403, postForm("/$BUCKET", noSignature))
        assertFailsWith<S3Exception> { readObject(KEY) }
    }

    @Test
    fun `Form of which the policy was signed before encoding it is refused`() {
        val fields = signedFields(KEY)
        val policyJson = String(BaseEncoding.base64().decode(fields.getValue("policy")), StandardCharsets.UTF_8)
        fields["x-amz-signature"] = signVersion4(policyJson, fields.getValue("x-amz-credential"))

        assertEquals(403, postForm("/$BUCKET", fields))
        assertFailsWith<S3Exception> { readObject(KEY) }
    }

    @Test
    fun `Form with an expired policy is refused`() {
        assertEquals(403, postForm("/$BUCKET", signedFields(KEY, expiration = Instant.now().minusSeconds(60))))
        assertFailsWith<S3Exception> { readObject(KEY) }
    }

    @Test
    fun `Form which does not meet the conditions of its policy is refused`() {
        val fields = signedFields(KEY).also { it["key"] = "another-key" }

        assertEquals(403, postForm("/$BUCKET", fields))
        assertFailsWith<S3Exception> { readObject("another-key") }
    }

    @Test
    fun `Form is only accepted if the upload has a size its policy permits`() {
        val largeEnough = signedFields(KEY, furtherConditions = listOf("""["content-length-range", 1, 1024]"""))
        val tooLarge = signedFields("large.txt", furtherConditions = listOf("""["content-length-range", 0, 5]"""))
        val tooSmall = signedFields("small.txt", furtherConditions = listOf("""["content-length-range", 100, 1024]"""))

        assertEquals(204, postForm("/$BUCKET", largeEnough))
        assertEquals(CONTENT, readObject(KEY))
        assertEquals(400, postForm("/$BUCKET", tooLarge))
        assertEquals(400, postForm("/$BUCKET", tooSmall))
        assertFailsWith<S3Exception> { readObject("large.txt") }
        assertFailsWith<S3Exception> { readObject("small.txt") }
    }

    @Test
    fun `Form is answered with the status it asks for, and with 204 if that is none S3 knows`() {
        assertEquals(201, postForm("/$BUCKET", signedFields(KEY).also { it["success_action_status"] = "201" }))
        assertEquals(200, postForm("/$BUCKET", signedFields(KEY).also { it["success_action_status"] = "200" }))
        assertEquals(204, postForm("/$BUCKET", signedFields("other.txt").also { it["success_action_status"] = "999" }))
        assertEquals(CONTENT, readObject("other.txt"))
    }

    @Test
    fun `Upload sent to the URL of an object is verified like one sent to the bucket`() {
        val asQuery = { fields: Map<String, String> ->
            fields.entries.joinToString("&") { (name, value) ->
                "$name=${URLEncoder.encode(value, StandardCharsets.UTF_8)}"
            }
        }
        val wrongSignature = signedFields(KEY).also { it["x-amz-signature"] = "0".repeat(64) }
        val expired = signedFields(KEY, expiration = Instant.now().minusSeconds(60))
        val tooLarge = signedFields(KEY, furtherConditions = listOf("""["content-length-range", 0, 5]"""))

        assertEquals(403, postContent("/$BUCKET/$KEY?${asQuery(wrongSignature)}", CONTENT))
        assertEquals(403, postContent("/$BUCKET/$KEY?${asQuery(expired)}", CONTENT))
        assertEquals(403, postContent("/$BUCKET/other.txt?${asQuery(signedFields(KEY))}", CONTENT))
        assertEquals(400, postContent("/$BUCKET/$KEY?${asQuery(tooLarge)}", CONTENT))
        assertFailsWith<S3Exception> { readObject(KEY) }
        assertFailsWith<S3Exception> { readObject("other.txt") }

        assertEquals(200, postContent("/$BUCKET/$KEY?${asQuery(signedFields(KEY))}", CONTENT))
        assertEquals(CONTENT, readObject(KEY))
    }

    @Test
    fun `Form created by the generator of the UI is accepted, as form and as query string`() {
        val service = Injector.context().getPart(PresignedPostService::class.java)!!
        val generated = service.generate(
            PresignedPostRequest(BUCKET, KEY, "eu-central-1", 1, null, BaseSdkSupportTest.ACCESS_KEY)
        )

        assertEquals(204, postForm("/$BUCKET", LinkedHashMap(generated.fields())))
        assertEquals(CONTENT, readObject(KEY))

        val query = generated.fields().entries.joinToString("&") { (name, value) ->
            "$name=${URLEncoder.encode(value, StandardCharsets.UTF_8)}"
        }
        assertEquals(200, postContent("${generated.url()}?$query", "Sent as body"))
        assertEquals("Sent as body", readObject(KEY))
    }

    @BeforeEach
    fun createBucket() {
        createClient().use { client -> client.createBucket { it.bucket(BUCKET) } }
    }

    @AfterEach
    fun removeBucket() {
        createClient().use { client -> BaseSdkSupportTest.cleanupBuckets(client, BUCKET) }
    }

    /**
     * Builds the fields of a form which permits to upload the given key, signed with signature version 4.
     */
    private fun signedFields(
        key: String,
        expiration: Instant = Instant.now().plusSeconds(3600),
        furtherConditions: List<String> = emptyList()
    ): MutableMap<String, String> {
        val now = Instant.now().atZone(ZoneOffset.UTC)
        val date = DateTimeFormatter.ofPattern("yyyyMMdd").format(now)
        val timestamp = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").format(now)
        val credential = "${BaseSdkSupportTest.ACCESS_KEY}/$date/eu-central-1/s3/aws4_request"
        val policy = encodePolicy(
            expiration,
            key,
            listOf(
                """{"x-amz-algorithm": "AWS4-HMAC-SHA256"}""",
                """{"x-amz-credential": "$credential"}""",
                """{"x-amz-date": "$timestamp"}"""
            ) + furtherConditions
        )

        return linkedMapOf(
            "key" to key,
            "x-amz-algorithm" to "AWS4-HMAC-SHA256",
            "x-amz-credential" to credential,
            "x-amz-date" to timestamp,
            "policy" to policy,
            "x-amz-signature" to signVersion4(policy, credential)
        )
    }

    private fun encodePolicy(expiration: Instant, key: String, furtherConditions: List<String>): String {
        val conditions = listOf("""{"bucket": "$BUCKET"}""", """["eq", "${'$'}key", "$key"]""") + furtherConditions
        val policy = """{"expiration": "$expiration", "conditions": [${conditions.joinToString(", ")}]}"""
        return BaseEncoding.base64().encode(policy.toByteArray(StandardCharsets.UTF_8))
    }

    private fun signVersion4(stringToSign: String, credential: String): String {
        val (_, date, region, service, terminator) = credential.split("/")
        val dateKey = hmac("HmacSHA256", secretKey("AWS4"), date)
        val regionKey = hmac("HmacSHA256", dateKey, region)
        val serviceKey = hmac("HmacSHA256", regionKey, service)
        val signingKey = hmac("HmacSHA256", serviceKey, terminator)
        return BaseEncoding.base16().lowerCase().encode(hmac("HmacSHA256", signingKey, stringToSign))
    }

    private fun secretKey(prefix: String): ByteArray {
        return (prefix + BaseSdkSupportTest.SECRET_ACCESS_KEY).toByteArray(StandardCharsets.UTF_8)
    }

    private fun hmac(algorithm: String, key: ByteArray, data: String): ByteArray {
        val mac = Mac.getInstance(algorithm)
        mac.init(SecretKeySpec(key, algorithm))
        return mac.doFinal(data.toByteArray(StandardCharsets.UTF_8))
    }

    /**
     * Sends the given fields, followed by the file to upload, as a form without any signature of its own, and
     * returns the status code of the response.
     */
    private fun postForm(path: String, fields: Map<String, String>): Int {
        val body = buildString {
            fields.forEach { (name, value) ->
                append("--$BOUNDARY\r\nContent-Disposition: form-data; name=\"$name\"\r\n\r\n$value\r\n")
            }
            append("--$BOUNDARY\r\nContent-Disposition: form-data; name=\"file\"; filename=\"test.txt\"\r\n")
            append("Content-Type: text/plain\r\n\r\n$CONTENT\r\n--$BOUNDARY--\r\n")
        }
        return post(path, "multipart/form-data; boundary=$BOUNDARY", body)
    }

    /**
     * Sends the given content as body of a request without any signature of its own, and returns the status code
     * of the response.
     */
    private fun postContent(pathAndQuery: String, content: String): Int {
        return post(pathAndQuery, "text/plain", content)
    }

    private fun post(pathAndQuery: String, contentType: String, body: String): Int {
        val connection = URI(BaseSdkSupportTest.ENDPOINT + pathAndQuery).toURL().openConnection() as HttpURLConnection
        connection.requestMethod = "POST"
        connection.doOutput = true
        connection.setRequestProperty("Content-Type", contentType)
        connection.outputStream.use { it.write(body.toByteArray(StandardCharsets.UTF_8)) }
        return connection.responseCode.also { connection.disconnect() }
    }

    private fun readObject(key: String): String {
        return createClient().use { client ->
            client.getObject { it.bucket(BUCKET).key(key) }.use { String(it.readAllBytes(), StandardCharsets.UTF_8) }
        }
    }

    private fun createClient(): S3Client {
        return S3Client.builder()
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(BaseSdkSupportTest.ACCESS_KEY, BaseSdkSupportTest.SECRET_ACCESS_KEY)
                )
            )
            .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
            .endpointOverride(URI.create(BaseSdkSupportTest.ENDPOINT))
            .region(Region.EU_CENTRAL_1)
            .build()
    }

    companion object {
        const val BUCKET = "browser-upload"
        const val KEY = "uploaded.txt"
        const val CONTENT = "Uploaded by a form"
        const val BOUNDARY = "----FormBoundary7MA4YWxkTrZu0gW"
    }
}
