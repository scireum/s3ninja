import ninja.RequestAuthenticator
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import sirius.kernel.SiriusExtension
import sirius.kernel.di.std.Part
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * Tests for session token generation and validation in RequestAuthenticator.
 */
@ExtendWith(SiriusExtension::class)
class SessionTokenTest {

    @Part
    var authenticator: RequestAuthenticator = RequestAuthenticator()

    @Test
    fun `generateSessionToken returns a valid token`() {
        val token = authenticator.generateSessionToken()

        assertNotNull(token)
        assertTrue(token.isNotEmpty())
        assertTrue(token.length >= 32, "Token should have at least 32 characters")
    }

    @Test
    fun `generated token is valid`() {
        val token = authenticator.generateSessionToken()

        assertTrue(authenticator.validateSessionToken(token))
    }

    @Test
    fun `validateSessionToken rejects empty token`() {
        assertFalse(authenticator.validateSessionToken(""))
    }

    @Test
    fun `validateSessionToken rejects null token`() {
        assertFalse(authenticator.validateSessionToken(null))
    }

    @Test
    fun `validateSessionToken rejects invalid token`() {
        assertFalse(authenticator.validateSessionToken("invalid-token-12345"))
    }

    @Test
    fun `session token can be validated multiple times`() {
        val token = authenticator.generateSessionToken()

        assertTrue(authenticator.validateSessionToken(token))
        assertTrue(authenticator.validateSessionToken(token))
        assertTrue(authenticator.validateSessionToken(token))
    }

    @Test
    fun `multiple tokens can be generated and validated independently`() {
        val token1 = authenticator.generateSessionToken()
        val token2 = authenticator.generateSessionToken()

        assertTrue(authenticator.validateSessionToken(token1))
        assertTrue(authenticator.validateSessionToken(token2))
        assertTrue(token1 != token2, "Each token should be unique")
    }
}

