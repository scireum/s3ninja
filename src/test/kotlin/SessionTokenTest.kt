import ninja.S3Dispatcher
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import sirius.kernel.SiriusExtension
import sirius.kernel.di.std.Part
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * Tests for session token generation and validation in S3Dispatcher.
 */
@ExtendWith(SiriusExtension::class)
class SessionTokenTest {

    @Part
    var s3Dispatcher: S3Dispatcher = S3Dispatcher()

    @Test
    fun `generateSessionToken returns a valid token`() {
        val token = s3Dispatcher.generateSessionToken()

        assertNotNull(token)
        assertTrue(token.isNotEmpty())
        assertTrue(token.length >= 32, "Token should have at least 32 characters")
    }

    @Test
    fun `generated token is valid`() {
        val token = s3Dispatcher.generateSessionToken()

        assertTrue(s3Dispatcher.validateSessionToken(token))
    }

    @Test
    fun `validateSessionToken rejects empty token`() {
        assertFalse(s3Dispatcher.validateSessionToken(""))
    }

    @Test
    fun `validateSessionToken rejects null token`() {
        assertFalse(s3Dispatcher.validateSessionToken(null))
    }

    @Test
    fun `validateSessionToken rejects invalid token`() {
        assertFalse(s3Dispatcher.validateSessionToken("invalid-token-12345"))
    }

    @Test
    fun `session token can be validated multiple times`() {
        val token = s3Dispatcher.generateSessionToken()

        assertTrue(s3Dispatcher.validateSessionToken(token))
        assertTrue(s3Dispatcher.validateSessionToken(token))
        assertTrue(s3Dispatcher.validateSessionToken(token))
    }

    @Test
    fun `multiple tokens can be generated and validated independently`() {
        val token1 = s3Dispatcher.generateSessionToken()
        val token2 = s3Dispatcher.generateSessionToken()

        assertTrue(s3Dispatcher.validateSessionToken(token1))
        assertTrue(s3Dispatcher.validateSessionToken(token2))
        assertTrue(token1 != token2, "Each token should be unique")
    }
}

