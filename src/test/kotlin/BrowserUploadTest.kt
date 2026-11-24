/*
 * Made with all the love in the world
 * by scireum in Stuttgart, Germany
 *
 * Copyright by scireum GmbH
 * https://www.scireum.de - info@scireum.de
 */

import BaseSdkSupportTest.Companion.md5Hex
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
import kotlin.test.assertTrue

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
        val policy = encodePolicy(Instant.now().plusSeconds(3600), keyEquals(KEY), emptyList())
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
        val policy = encodePolicy(Instant.now().plusSeconds(3600), keyEquals(KEY), emptyList())
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
            it["policy"] = encodePolicy(Instant.now().plusSeconds(7200), keyEquals(KEY), emptyList())
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
        assertEquals(201, postForm("/$BUCKET", withStatus(KEY, "201")))
        assertEquals(200, postForm("/$BUCKET", withStatus(KEY, "200")))
        assertEquals(204, postForm("/$BUCKET", withStatus("other.txt", "999")))
        assertEquals(CONTENT, readObject("other.txt"))
    }

    @Test
    fun `Form asking for 201 is answered with a document describing the object`() {
        val answer = postFormForAnswer("/$BUCKET", withStatus(KEY, "201"))

        assertEquals(201, answer.status)
        assertEquals(ETAG, answer.etag)
        assertTrue(answer.location.orEmpty().endsWith("/$BUCKET/$KEY"), answer.location)
        assertTrue(answer.body.contains("<PostResponse>"), answer.body)
        assertTrue(answer.body.contains("<Bucket>$BUCKET</Bucket>"), answer.body)
        assertTrue(answer.body.contains("<Key>$KEY</Key>"), answer.body)
        assertTrue(answer.body.contains(md5Hex(CONTENT.toByteArray())), answer.body)
    }

    @Test
    fun `Form without a status is answered with 204, its entity tag and its location`() {
        val answer = postFormForAnswer("/$BUCKET", signedFields(KEY))

        assertEquals(204, answer.status)
        assertEquals(ETAG, answer.etag)
        assertTrue(answer.location.orEmpty().endsWith("/$BUCKET/$KEY"), answer.location)
    }

    @Test
    fun `Form naming a redirect is redirected there with bucket, key and entity tag`() {
        val redirect = "https://example.com/done?upload=1"
        val fields = signedFields(KEY, furtherConditions = listOf("""{"success_action_redirect": "$redirect"}"""))
        fields["success_action_redirect"] = redirect

        val answer = postFormForAnswer("/$BUCKET", fields)

        assertEquals(303, answer.status)
        val etag = URLEncoder.encode(ETAG, StandardCharsets.UTF_8)
        assertEquals("$redirect&bucket=$BUCKET&key=$KEY&etag=$etag", answer.location)
        assertEquals(CONTENT, readObject(KEY))
    }

    @Test
    fun `Parameters are added to a redirect in front of its fragment`() {
        val redirect = "https://example.com/done#top"
        val fields = signedFields(KEY, furtherConditions = listOf("""{"success_action_redirect": "$redirect"}"""))
        fields["success_action_redirect"] = redirect

        val answer = postFormForAnswer("/$BUCKET", fields)

        val etag = URLEncoder.encode(ETAG, StandardCharsets.UTF_8)
        assertEquals("https://example.com/done?bucket=$BUCKET&key=$KEY&etag=$etag#top", answer.location)
    }

    @Test
    fun `Location of an uploaded object encodes its key as a path`() {
        val answer = postFormForAnswer("/$BUCKET", withStatus("my file.txt", "201"))

        assertEquals(201, answer.status)
        assertTrue(answer.location.orEmpty().endsWith("/$BUCKET/my%20file.txt"), answer.location)
        assertEquals(CONTENT, readObject("my file.txt"))
    }

    @Test
    fun `Form naming a redirect which cannot be interpreted is answered as without one`() {
        val fields = signedFields(KEY, furtherConditions = listOf("""{"success_action_redirect": "not a url"}"""))
        fields["success_action_redirect"] = "not a url"

        assertEquals(204, postForm("/$BUCKET", fields))
        assertEquals(CONTENT, readObject(KEY))
    }

    @Test
    fun `Form is checked against a condition on the type of its object`() {
        val imagesOnly = listOf("""["starts-with", "${'$'}Content-Type", "image/"]""")
        val image = signedFields(KEY, furtherConditions = imagesOnly).also { it["Content-Type"] = "image/png" }
        val text = signedFields("other.txt", furtherConditions = imagesOnly).also { it["Content-Type"] = "text/plain" }

        assertEquals(204, postForm("/$BUCKET", image))
        val contentType =
            createClient().use { client -> client.headObject { it.bucket(BUCKET).key(KEY) }.contentType() }
        assertEquals("image/png", contentType)
        assertEquals(403, postForm("/$BUCKET", text))
        assertFailsWith<S3Exception> { readObject("other.txt") }
    }

    @Test
    fun `Upload sent to the URL of an object is answered like a form`() {
        val answer = postContentForAnswer("/$BUCKET/$KEY?${asQuery(withStatus(KEY, "201"))}", CONTENT)

        assertEquals(201, answer.status)
        assertEquals(ETAG, answer.etag)
        assertTrue(answer.body.contains("<Key>$KEY</Key>"), answer.body)
    }

    @Test
    fun `Upload sent to the URL of an object is verified like one sent to the bucket`() {
        val wrongSignature = signedFields(KEY).also { it["x-amz-signature"] = "0".repeat(64) }
        val expired = signedFields(KEY, expiration = Instant.now().minusSeconds(60))
        val tooLarge = signedFields(KEY, furtherConditions = listOf("""["content-length-range", 0, 5]"""))

        assertEquals(403, postContent("/$BUCKET/$KEY?${asQuery(wrongSignature)}", CONTENT))
        assertEquals(403, postContent("/$BUCKET/$KEY?${asQuery(expired)}", CONTENT))
        assertEquals(403, postContent("/$BUCKET/other.txt?${asQuery(signedFields(KEY))}", CONTENT))
        assertEquals(400, postContent("/$BUCKET/$KEY?${asQuery(tooLarge)}", CONTENT))
        assertFailsWith<S3Exception> { readObject(KEY) }
        assertFailsWith<S3Exception> { readObject("other.txt") }

        // Answered once, with the status S3 answers a form with that asks for none.
        assertEquals(204, postContent("/$BUCKET/$KEY?${asQuery(signedFields(KEY))}", CONTENT))
        assertEquals(CONTENT, readObject(KEY))
    }

    @Test
    fun `Upload sent to the URL of an object which cannot be stored is answered with its error`() {
        val otherDigest =
            BaseEncoding.base64().encode(BaseEncoding.base16().lowerCase().decode(md5Hex("other".toByteArray())))
        val answer = postContentForAnswer(
            "/$BUCKET/$KEY?${asQuery(withStatus(KEY, "201"))}",
            CONTENT,
            mapOf("Content-MD5" to otherDigest)
        )

        assertEquals(400, answer.status)
        assertTrue(answer.body.contains("<Code>BadDigest</Code>"), answer.body)
        assertEquals(null, answer.etag)
        assertFailsWith<S3Exception> { readObject(KEY) }
    }

    @Test
    fun `Name of the uploaded file is filled in where the key of the form contains a variable for it`() {
        val formKey = "uploads/\${filename}"
        val prefixCondition = """["starts-with", "${'$'}key", "uploads/"]"""

        assertEquals(204, postForm("/$BUCKET", signedFields(formKey, keyCondition = prefixCondition)))
        assertEquals(CONTENT, readObject("uploads/test.txt"))
        assertFailsWith<S3Exception> { readObject(formKey) }
    }

    @Test
    fun `Policy is evaluated for the key with the name of the file filled in`() {
        // As for S3, which expands the variable before it validates the policy.
        val formKey = "uploads/\${filename}"
        val literalCondition = """["eq", "${'$'}key", "uploads/${'$'}{filename}"]"""

        assertEquals(403, postForm("/$BUCKET", signedFields(formKey, keyCondition = literalCondition)))
        assertFailsWith<S3Exception> { readObject("uploads/test.txt") }
    }

    @Test
    fun `Variable for the name of the file is removed if the upload names no file`() {
        // As for S3, which fills in an empty string, e.g. for a form of a browser without a chosen file name.
        val formKey = "upload-\${filename}.txt"
        val prefixCondition = """["starts-with", "${'$'}key", "upload-"]"""
        val fields = signedFields(formKey, keyCondition = prefixCondition)

        assertEquals(204, postFormForAnswer("/$BUCKET", fields, filename = "").status)
        assertEquals(CONTENT, readObject("upload-.txt"))
    }

    @Test
    fun `Form with the field names of the AWS SDK for JavaScript is accepted`() {
        // The SDK capitalises the names of the fields and of its conditions, and sends the bucket as a field as well.
        val now = Instant.now().atZone(ZoneOffset.UTC)
        val timestamp = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").format(now)
        val credential = "${BaseSdkSupportTest.ACCESS_KEY}/${DateTimeFormatter.ofPattern("yyyyMMdd").format(now)}" +
                "/eu-central-1/s3/aws4_request"
        val policy = encodePolicy(
            Instant.now().plusSeconds(3600),
            keyEquals(KEY),
            listOf(
                """{"X-Amz-Algorithm": "AWS4-HMAC-SHA256"}""",
                """{"X-Amz-Credential": "$credential"}""",
                """{"X-Amz-Date": "$timestamp"}"""
            )
        )
        val fields = linkedMapOf(
            "bucket" to BUCKET,
            "key" to KEY,
            "X-Amz-Algorithm" to "AWS4-HMAC-SHA256",
            "X-Amz-Credential" to credential,
            "X-Amz-Date" to timestamp,
            "Policy" to policy,
            "X-Amz-Signature" to signVersion4(policy, credential)
        )

        assertEquals(204, postForm("/$BUCKET", fields))
        assertEquals(CONTENT, readObject(KEY))
    }

    @Test
    fun `Form with a field which its policy does not cover is refused`() {
        val uncovered = signedFields(KEY).also { it["x-amz-meta-color"] = "blue" }
        val answer = postFormForAnswer("/$BUCKET", uncovered)

        assertEquals(403, answer.status)
        assertTrue(answer.body.contains("Extra input fields: x-amz-meta-color"), answer.body)
        assertFailsWith<S3Exception> { readObject(KEY) }
    }

    @Test
    fun `Condition on the size of the file covers no field of that name`() {
        val fields = signedFields(KEY, furtherConditions = listOf("""["content-length-range", 0, 1024]"""))
        fields["content-length-range"] = "1024"
        val answer = postFormForAnswer("/$BUCKET", fields)

        assertEquals(403, answer.status)
        assertTrue(answer.body.contains("Extra input fields: content-length-range"), answer.body)
    }

    @Test
    fun `Fields after the file are ignored, as by S3`() {
        val late = mapOf("x-amz-meta-late" to "ignored", "success_action_status" to "201")
        val answer = postFormForAnswer("/$BUCKET", signedFields(KEY), late)

        assertEquals(204, answer.status)
        val metadata = createClient().use { client -> client.headObject { it.bucket(BUCKET).key(KEY) }.metadata() }
        assertEquals(null, metadata["late"])
    }

    @Test
    fun `Fields after the file are ignored whatever the case of their names`() {
        val late = mapOf("content-type" to "text/html", "Success_Action_Status" to "201")
        val answer = postFormForAnswer("/$BUCKET", signedFields(KEY), late)

        assertEquals(204, answer.status)
        val contentType =
            createClient().use { client -> client.headObject { it.bucket(BUCKET).key(KEY) }.contentType() }
        assertTrue(contentType != "text/html", contentType)
    }

    @Test
    fun `Field repeated after the file keeps the value it has before the file`() {
        val fields = signedFields(KEY, furtherConditions = listOf("""{"x-amz-meta-color": "blue"}"""))
        fields["x-amz-meta-color"] = "blue"
        val late = mapOf("key" to "other.txt", "x-amz-meta-color" to "red")

        assertEquals(204, postFormForAnswer("/$BUCKET", fields, late).status)
        assertEquals(CONTENT, readObject(KEY))
        val metadata = createClient().use { client -> client.headObject { it.bucket(BUCKET).key(KEY) }.metadata() }
        assertEquals("blue", metadata["color"])
        assertFailsWith<S3Exception> { readObject("other.txt") }
    }

    @Test
    fun `Only fields which describe the object are stored, so that none replaces its entity tag`() {
        val permitted = listOf("""{"ETag": "forged"}""", """{"Cache-Control": "no-cache"}""")
        val fields = signedFields(KEY, furtherConditions = permitted)
        fields["ETag"] = "forged"
        fields["Cache-Control"] = "no-cache"

        val answer = postFormForAnswer("/$BUCKET", fields)

        assertEquals(204, answer.status)
        assertEquals(ETAG, answer.etag)
        val head = createClient().use { client -> client.headObject { it.bucket(BUCKET).key(KEY) } }
        assertEquals(ETAG, head.eTag())
        assertEquals("no-cache", head.cacheControl())
    }

    @Test
    fun `Conditions on bucket and key apply to the request, whatever the case of their names`() {
        // A form field of that name must not stand in for the bucket the form is sent to.
        val otherBucket = signedFields(KEY, bucketCondition = """{"Bucket": "other-bucket"}""")
        otherBucket["bucket"] = "other-bucket"
        // The key of the form differs from the key it is stored under, which the condition has to be evaluated for.
        val ownBucket = signedFields(
            "uploads/\${filename}",
            bucketCondition = """["eq", "${'$'}Bucket", "$BUCKET"]""",
            keyCondition = """["eq", "${'$'}Key", "uploads/test.txt"]"""
        )

        assertEquals(403, postForm("/$BUCKET", otherBucket))
        assertFailsWith<S3Exception> { readObject(KEY) }
        assertEquals(204, postForm("/$BUCKET", ownBucket))
        assertEquals(CONTENT, readObject("uploads/test.txt"))
    }

    @Test
    fun `Form with fields its policy covers or which are to be ignored is accepted`() {
        val covered = signedFields(KEY, furtherConditions = listOf("""{"x-amz-meta-color": "blue"}"""))
        covered["x-amz-meta-color"] = "blue"
        covered["x-ignore-tracking"] = "anything"

        assertEquals(204, postForm("/$BUCKET", covered))
        val metadata = createClient().use { client -> client.headObject { it.bucket(BUCKET).key(KEY) }.metadata() }
        assertEquals("blue", metadata["color"])
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
        assertEquals(204, postContent("${generated.url()}?$query", "Sent as body"))
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
        furtherConditions: List<String> = emptyList(),
        keyCondition: String = keyEquals(key),
        bucketCondition: String = """{"bucket": "$BUCKET"}"""
    ): MutableMap<String, String> {
        val now = Instant.now().atZone(ZoneOffset.UTC)
        val date = DateTimeFormatter.ofPattern("yyyyMMdd").format(now)
        val timestamp = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").format(now)
        val credential = "${BaseSdkSupportTest.ACCESS_KEY}/$date/eu-central-1/s3/aws4_request"
        val policy = encodePolicy(
            expiration,
            keyCondition,
            listOf(
                """{"x-amz-algorithm": "AWS4-HMAC-SHA256"}""",
                """{"x-amz-credential": "$credential"}""",
                """{"x-amz-date": "$timestamp"}"""
            ) + furtherConditions,
            bucketCondition
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

    /**
     * Builds the fields of a form which asks to be answered with the given status, which its policy permits.
     */
    private fun withStatus(key: String, status: String): MutableMap<String, String> {
        val fields =
            signedFields(key, furtherConditions = listOf("""["starts-with", "${'$'}success_action_status", ""]"""))
        fields["success_action_status"] = status
        return fields
    }

    private fun asQuery(fields: Map<String, String>): String {
        return fields.entries.joinToString("&") { (name, value) ->
            "$name=${URLEncoder.encode(value, StandardCharsets.UTF_8)}"
        }
    }

    private fun keyEquals(key: String): String {
        return """["eq", "${'$'}key", "$key"]"""
    }

    private fun encodePolicy(
        expiration: Instant,
        keyCondition: String,
        furtherConditions: List<String>,
        bucketCondition: String = """{"bucket": "$BUCKET"}"""
    ): String {
        val conditions = listOf(bucketCondition, keyCondition) + furtherConditions
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
        return postFormForAnswer(path, fields).status
    }

    /**
     * Sends the given fields, followed by the file to upload, as a form without any signature of its own, and
     * returns the answer without following a redirect.
     */
    private fun postFormForAnswer(
        path: String,
        fields: Map<String, String>,
        fieldsAfterFile: Map<String, String> = emptyMap(),
        filename: String = "test.txt"
    ): Answer {
        val body = buildString {
            fields.forEach { (name, value) ->
                append("--$BOUNDARY\r\nContent-Disposition: form-data; name=\"$name\"\r\n\r\n$value\r\n")
            }
            append("--$BOUNDARY\r\nContent-Disposition: form-data; name=\"file\"; filename=\"$filename\"\r\n")
            append("Content-Type: text/plain\r\n\r\n$CONTENT\r\n")
            fieldsAfterFile.forEach { (name, value) ->
                append("--$BOUNDARY\r\nContent-Disposition: form-data; name=\"$name\"\r\n\r\n$value\r\n")
            }
            append("--$BOUNDARY--\r\n")
        }
        return post(path, "multipart/form-data; boundary=$BOUNDARY", body)
    }

    private fun postContentForAnswer(
        pathAndQuery: String,
        content: String,
        headers: Map<String, String> = emptyMap()
    ): Answer {
        return post(pathAndQuery, "text/plain", content, headers)
    }

    /**
     * Sends the given content as body of a request without any signature of its own, and returns the status code
     * of the response.
     */
    private fun postContent(pathAndQuery: String, content: String): Int {
        return post(pathAndQuery, "text/plain", content).status
    }

    private fun post(
        pathAndQuery: String,
        contentType: String,
        body: String,
        headers: Map<String, String> = emptyMap()
    ): Answer {
        val connection = URI(BaseSdkSupportTest.ENDPOINT + pathAndQuery).toURL().openConnection() as HttpURLConnection
        connection.requestMethod = "POST"
        connection.instanceFollowRedirects = false
        connection.doOutput = true
        connection.setRequestProperty("Content-Type", contentType)
        headers.forEach { (name, value) -> connection.setRequestProperty(name, value) }
        connection.outputStream.use { it.write(body.toByteArray(StandardCharsets.UTF_8)) }
        val status = connection.responseCode
        val stream = if (status >= 400) connection.errorStream else connection.inputStream
        val content = stream?.use { String(it.readAllBytes(), StandardCharsets.UTF_8) }.orEmpty()
        return Answer(status, connection.getHeaderField("Location"), connection.getHeaderField("ETag"), content)
            .also { connection.disconnect() }
    }

    /**
     * Describes the answer to an upload: its status, the headers which matter here, and its content.
     */
    private data class Answer(val status: Int, val location: String?, val etag: String?, val body: String)

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
        val ETAG = "\"${md5Hex(CONTENT.toByteArray())}\""
    }
}
