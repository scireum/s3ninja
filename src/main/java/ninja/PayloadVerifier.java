/*
 * Made with all the love in the world
 * by scireum in Stuttgart, Germany
 *
 * Copyright by scireum GmbH
 * https://www.scireum.de - info@scireum.de
 */

package ninja;

import com.google.common.io.BaseEncoding;
import ninja.errors.S3ErrorCode;
import sirius.kernel.commons.Explain;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Tuple;
import sirius.kernel.di.std.Register;
import sirius.web.http.WebContext;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.zip.CRC32;
import java.util.zip.CRC32C;
import java.util.zip.Checksum;

/**
 * Verifies the content received for an object or for a part against the hashes and checksums its request states.
 * <p>
 * S3 refuses content which does not match, so that data which was damaged on its way, or a client which hashes
 * something else than it sends, is noticed. Three kinds of statements are checked:
 * <ul>
 *     <li><tt>Content-MD5</tt></li>
 *     <li><tt>x-amz-content-sha256</tt>, if it carries a hash and not one of the markers for unsigned or streamed
 *     content</li>
 *     <li><tt>x-amz-checksum-crc32</tt>, <tt>-crc32c</tt>, <tt>-sha1</tt> and <tt>-sha256</tt> if sent as headers.
 *     Checksums sent as trailers of a streamed upload, and <tt>-crc64nvme</tt>, are not verified.</li>
 * </ul>
 */
@Register(classes = PayloadVerifier.class)
public class PayloadVerifier {

    private static final String HEADER_CONTENT_MD5 = "Content-MD5";
    private static final String HEADER_CONTENT_SHA256 = "x-amz-content-sha256";
    private static final String HEADER_CHECKSUM_PREFIX = "x-amz-checksum-";

    private static final String ALGORITHM_MD5 = "MD5";
    private static final String ALGORITHM_SHA1 = "SHA-1";
    private static final String ALGORITHM_SHA256 = "SHA-256";
    private static final String CHECKSUM_CRC32 = "crc32";
    private static final String CHECKSUM_CRC32C = "crc32c";
    private static final String CHECKSUM_SHA1 = "sha1";
    private static final String CHECKSUM_SHA256 = "sha256";

    private static final int MD5_LENGTH = 16;
    private static final int BUFFER_SIZE = 64 * 1024;

    /**
     * Matches a SHA-256 hash in hex, as opposed to markers like <tt>UNSIGNED-PAYLOAD</tt>.
     */
    private static final Pattern SHA256_HEX_PATTERN = Pattern.compile("[0-9a-fA-F]{64}");

    /**
     * Lists the values of <tt>x-amz-content-sha256</tt> which state that there is no hash of the whole content, as
     * the content is either not signed at all, or signed chunk by chunk.
     */
    private static final Set<String> PAYLOAD_MARKERS = Set.of("UNSIGNED-PAYLOAD",
                                                              "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
                                                              "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
                                                              "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER",
                                                              "STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD",
                                                              "STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD-TRAILER");

    /**
     * Verifies the given content against everything the given request states about it.
     *
     * @param webContext the request which delivered the content
     * @param content    the file holding the content as received
     * @return the error to answer the request with, or an empty optional if the content is what the client announced
     * @throws IOException if the content cannot be read
     */
    public Optional<Tuple<S3ErrorCode, String>> verify(WebContext webContext, File content) throws IOException {
        String contentMd5 = webContext.getHeader(HEADER_CONTENT_MD5);
        byte[] expectedMd5 = decodeBase64(contentMd5);
        if (Strings.isFilled(contentMd5) && (expectedMd5 == null || expectedMd5.length != MD5_LENGTH)) {
            return Optional.of(Tuple.create(S3ErrorCode.InvalidDigest, "The Content-MD5 you specified was invalid."));
        }

        String contentSha256 = webContext.getHeader(HEADER_CONTENT_SHA256);
        boolean verifySha256 = Strings.isFilled(contentSha256) && SHA256_HEX_PATTERN.matcher(contentSha256).matches();
        if (Strings.isFilled(contentSha256) && !verifySha256 && !PAYLOAD_MARKERS.contains(contentSha256)) {
            return Optional.of(Tuple.create(S3ErrorCode.InvalidArgument,
                                            "x-amz-content-sha256 must be UNSIGNED-PAYLOAD, one of the STREAMING markers, or a valid sha256 value."));
        }

        Map<String, String> statedChecksums = new LinkedHashMap<>();
        for (String algorithm : new String[]{CHECKSUM_CRC32, CHECKSUM_CRC32C, CHECKSUM_SHA1, CHECKSUM_SHA256}) {
            String value = webContext.getHeader(HEADER_CHECKSUM_PREFIX + algorithm);
            if (Strings.isFilled(value)) {
                statedChecksums.put(algorithm, value.trim());
            }
        }

        if (expectedMd5 == null && !verifySha256 && statedChecksums.isEmpty()) {
            return Optional.empty();
        }

        Digests digests = Digests.of(content);

        if (expectedMd5 != null && !MessageDigest.isEqual(expectedMd5, digests.md5.digest())) {
            return Optional.of(Tuple.create(S3ErrorCode.BadDigest,
                                            "The Content-MD5 you specified did not match what we received."));
        }
        if (verifySha256 && !contentSha256.equalsIgnoreCase(BaseEncoding.base16()
                                                                        .lowerCase()
                                                                        .encode(digests.sha256.digest()))) {
            return Optional.of(Tuple.create(S3ErrorCode.XAmzContentSHA256Mismatch,
                                            "The provided 'x-amz-content-sha256' header does not match what was computed."));
        }
        for (Map.Entry<String, String> statedChecksum : statedChecksums.entrySet()) {
            if (!Strings.areEqual(statedChecksum.getValue(), digests.checksum(statedChecksum.getKey()))) {
                return Optional.of(Tuple.create(S3ErrorCode.BadDigest,
                                                Strings.apply(
                                                        "The %s you specified did not match the calculated checksum.",
                                                        statedChecksum.getKey().toUpperCase())));
            }
        }

        return Optional.empty();
    }

    private byte[] decodeBase64(String value) {
        if (Strings.isEmpty(value)) {
            return null;
        }

        try {
            return Base64.getDecoder().decode(value.trim());
        } catch (IllegalArgumentException _) {
            return new byte[0];
        }
    }

    /**
     * Holds all hashes and checksums of a file, computed in a single pass over its content.
     */
    @SuppressWarnings("java:S4790")
    @Explain("MD5 and SHA-1 are what the S3 protocol asks for here; they detect damaged content and protect nothing.")
    private static class Digests {

        private final MessageDigest md5 = createDigest(ALGORITHM_MD5);
        private final MessageDigest sha1 = createDigest(ALGORITHM_SHA1);
        private final MessageDigest sha256 = createDigest(ALGORITHM_SHA256);
        private final MessageDigest checksumSha256 = createDigest(ALGORITHM_SHA256);
        private final Checksum crc32 = new CRC32();
        private final Checksum crc32c = new CRC32C();

        private static Digests of(File file) throws IOException {
            Digests digests = new Digests();
            byte[] buffer = new byte[BUFFER_SIZE];
            try (InputStream in = new FileInputStream(file)) {
                int length;
                while ((length = in.read(buffer)) >= 0) {
                    digests.md5.update(buffer, 0, length);
                    digests.sha1.update(buffer, 0, length);
                    digests.sha256.update(buffer, 0, length);
                    digests.checksumSha256.update(buffer, 0, length);
                    digests.crc32.update(buffer, 0, length);
                    digests.crc32c.update(buffer, 0, length);
                }
            }
            return digests;
        }

        /**
         * Returns the checksum of the given algorithm the way S3 expects it in a header, which is Base64.
         */
        private String checksum(String algorithm) {
            byte[] checksum = switch (algorithm) {
                case CHECKSUM_CRC32 -> ByteBuffer.allocate(Integer.BYTES).putInt((int) crc32.getValue()).array();
                case CHECKSUM_CRC32C -> ByteBuffer.allocate(Integer.BYTES).putInt((int) crc32c.getValue()).array();
                case CHECKSUM_SHA1 -> sha1.digest();
                default -> checksumSha256.digest();
            };
            return Base64.getEncoder().encodeToString(checksum);
        }

        private static MessageDigest createDigest(String algorithm) {
            try {
                return MessageDigest.getInstance(algorithm);
            } catch (NoSuchAlgorithmException exception) {
                throw new IllegalStateException(exception);
            }
        }
    }
}
