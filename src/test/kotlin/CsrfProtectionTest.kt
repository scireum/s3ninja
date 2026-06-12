import io.netty.handler.codec.http.HttpResponseStatus
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import sirius.kernel.SiriusExtension
import sirius.web.http.TestRequest
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Verifies the CSRF-by-default protection for the routed S3 Ninja UI controller.
 */
@ExtendWith(SiriusExtension::class)
class CsrfProtectionTest {

    @Test
    fun `internal POST helpers reject missing CSRF token`() {
        val sessionTokenResult = TestRequest.POST("/.api/generate-session-token").executeAndBlock()
        assertEquals(HttpResponseStatus.FORBIDDEN, sessionTokenResult.status)

        val presignedPostResult = TestRequest.POST("/.api/generate-presigned-post")
            .withParameter("bucket", "csrf-test")
            .withParameter("key", "test.txt")
            .withParameter("region", "eu-west-3")
            .withParameter("expirationHours", "24")
            .withParameter("sessionToken", "")
            .withParameter("accessKey", ACCESS_KEY)
            .executeAndBlock()
        assertEquals(HttpResponseStatus.FORBIDDEN, presignedPostResult.status)
    }

    @Test
    fun `internal POST helpers accept valid CSRF token`() {
        val sessionTokenResult = TestRequest.SAFEPOST("/.api/generate-session-token").executeAndBlock()
        assertEquals(HttpResponseStatus.OK, sessionTokenResult.status)
        assertTrue(sessionTokenResult.contentAsJson["success"].asBoolean())
        assertTrue(sessionTokenResult.contentAsJson["token"].asText().isNotBlank())

        val presignedPostResult = TestRequest.SAFEPOST("/.api/generate-presigned-post")
            .withParameter("bucket", "csrf-test")
            .withParameter("key", "test.txt")
            .withParameter("region", "eu-west-3")
            .withParameter("expirationHours", "24")
            .withParameter("sessionToken", "")
            .withParameter("accessKey", ACCESS_KEY)
            .executeAndBlock()
        assertEquals(HttpResponseStatus.OK, presignedPostResult.status)
        assertTrue(presignedPostResult.contentAsJson["success"].asBoolean())
        assertTrue(presignedPostResult.contentAsString.contains("\"x-amz-signature\""))
    }

    @Test
    fun `GET mutation requests are rejected`() {
        assertEquals(
            HttpResponseStatus.METHOD_NOT_ALLOWED,
            TestRequest.GET("/.api/generate-session-token").executeAndBlock().status
        )
        assertEquals(
            HttpResponseStatus.METHOD_NOT_ALLOWED,
            TestRequest.GET("/ui/csrf-migration-test?create").executeAndBlock().status
        )
    }

    @Test
    fun `bucket create and delete accept valid CSRF token`() {
        val bucket = "csrf-migration-test"

        val createResult = TestRequest.SAFEPOST("/ui/$bucket?create").executeAndBlock()
        assertTrue(createResult.status.code() in 300..399)

        val deleteResult = TestRequest.SAFEPOST("/ui/$bucket?delete").executeAndBlock()
        assertTrue(deleteResult.status.code() in 300..399)
    }

    private companion object {
        private const val ACCESS_KEY = "AKIAIOSFODNN7EXAMPLE"
    }
}
