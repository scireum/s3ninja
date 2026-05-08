package ninja

import io.mockk.every
import io.mockk.mockk
import io.netty.buffer.Unpooled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import sirius.web.http.WebContext
import java.nio.charset.StandardCharsets

class SignedChunkHandlerTest {

    @Test
    fun `split trailer signature after final chunk is accepted`() {
        val webContext = mockk<WebContext>()
        every { webContext.getHeader("x-amz-content-sha256") } returns "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"

        val handler = SignedChunkHandler(webContext)
        val firstContent = listOf(
            "5;chunk-signature=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "hello",
            "0;chunk-signature=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        ).joinToString(separator = "\r\n", postfix = "\r\n")
        val trailerContent = listOf(
            "x-amz-trailer-signature:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
            ""
        ).joinToString(separator = "\r\n", postfix = "\r\n")

        assertDoesNotThrow {
            handler.handle(Unpooled.copiedBuffer(firstContent, StandardCharsets.UTF_8), false)
            handler.handle(Unpooled.copiedBuffer(trailerContent, StandardCharsets.UTF_8), true)
        }
    }
}
