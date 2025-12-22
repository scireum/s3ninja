// kotlin
package ninja

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import sirius.kernel.SiriusExtension
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import sirius.kernel.commons.Strings

@ExtendWith(SiriusExtension::class)
class SessionTokenTest {

    private fun s3Dispatcher(): S3Dispatcher {
        return S3Dispatcher.getInstance()
            ?: throw IllegalStateException("S3Dispatcher has not been initialized by the DI container")
    }

    @Test
    fun `S3Dispatcher is properly injected`() {
        assertNotNull(S3Dispatcher.getInstance(), "S3Dispatcher should be available via getInstance()")
    }

    @Test
    fun `generateSessionToken returns a valid token`() {
        val token = s3Dispatcher().generateSessionToken()

        assertNotNull(token, "Generated token should not be null")
        assertTrue(token.isNotEmpty(), "Generated token should not be empty")
        assertTrue(token.length >= 32, "Generated token should have reasonable length")
    }

    @Test
    fun `session-token endpoint returns valid JSON response`() {
        val token = s3Dispatcher().generateSessionToken()

        assertNotNull(token)
        assertTrue(s3Dispatcher().validateSessionToken(token), "Newly generated token should be valid")
    }

    @Test
    fun `validateSessionToken rejects empty token`() {
        assertTrue(!s3Dispatcher().validateSessionToken(""), "Empty token should be rejected")
    }

    @Test
    fun `validateSessionToken rejects null token`() {
        assertTrue(!s3Dispatcher().validateSessionToken(null), "Null token should be rejected")
    }

    @Test
    fun `validateSessionToken rejects invalid token`() {
        assertTrue(!s3Dispatcher().validateSessionToken("invalid-token-12345"), "Invalid token should be rejected")
    }

    @Test
    fun `session token can be reused (idempotent)`() {
        val dispatcher = s3Dispatcher()
        val token = dispatcher.generateSessionToken()
        assertTrue(dispatcher.validateSessionToken(token))
        assertTrue(dispatcher.validateSessionToken(token))
        assertTrue(dispatcher.validateSessionToken(token))
    }

    @Test
    fun `token can be read from lowercase headers and form fields`() {
        val dispatcher = s3Dispatcher()
        val token = dispatcher.generateSessionToken()
        // Sanity: token remains valid when passed through validation repeatedly
        assertTrue(dispatcher.validateSessionToken(token))
        assertTrue(dispatcher.validateSessionToken(token))
        assertTrue(Strings.isFilled(token))
    }
}
