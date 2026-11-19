/*
 * Made with all the love in the world
 * by scireum in Stuttgart, Germany
 *
 * Copyright by scireum GmbH
 * https://www.scireum.de - info@scireum.de
 */

package ninja

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import sirius.kernel.Sirius
import sirius.kernel.SiriusExtension
import sirius.kernel.di.Injector
import java.io.File
import java.time.Duration
import java.time.Instant
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * Verifies that leftovers of unfinished requests are removed without touching what a running request may still need.
 */
@ExtendWith(SiriusExtension::class)
class MultipartLeftoversTest {

    @Test
    fun `Leftovers are removed while recent files and published parts are kept`() {
        val uploads = Injector.context().getPart(MultipartUploads::class.java)!!
        val bucket = Injector.context().getPart(Storage::class.java)!!.getBucket(BUCKET).also { it.create() }
        val now = Instant.now()
        val longAgo = now.minus(Duration.ofHours(1))

        val upload = uploads.create(BUCKET, "key", emptyMap())
        val uploadFolder = File(Sirius.getSettings().getString("storage.multipartDir"), upload.uploadId)
        val publishedPart = upload.publishPart(upload.stagePart(1, "Test".byteInputStream())).file()
        publishedPart.setLastModified(longAgo.toEpochMilli())
        val abandonedPart = createFile(uploadFolder, "2.abandoned.tmp", longAgo)
        val partOnItsWay = createFile(uploadFolder, "3.arriving.tmp", now)
        val abandonedObject = createFile(bucket.folder, "\$abandoned.assembling", longAgo)
        val objectBeingAssembled = createFile(bucket.folder, "\$current.assembling", now)
        val deletedUpload = File(uploadFolder.parentFile, "deleted-${UUID.randomUUID()}").also { it.mkdirs() }
        val foreignFolder = File(uploadFolder.parentFile, "deleted-scenes").also { it.mkdirs() }
        val interruptedInitiation = File(uploadFolder.parentFile, "0".repeat(48)).also { it.mkdirs() }
        createFile(interruptedInitiation, "properties", longAgo)
        interruptedInitiation.setLastModified(longAgo.toEpochMilli())
        val initiationInProgress = File(uploadFolder.parentFile, "1".repeat(48)).also { it.mkdirs() }

        uploads.removeLeftovers(now.minus(Duration.ofMinutes(1)))

        assertFalse(abandonedPart.exists())
        assertFalse(abandonedObject.exists())
        assertFalse(deletedUpload.exists())
        assertTrue(foreignFolder.exists())
        assertFalse(interruptedInitiation.exists())
        assertTrue(initiationInProgress.exists())
        assertTrue(partOnItsWay.exists())
        assertTrue(objectBeingAssembled.exists())
        assertTrue(publishedPart.exists())
        assertEquals(listOf(1), uploads.find(BUCKET, "key", upload.uploadId).get().parts.map { it.number() })

        upload.delete()
        initiationInProgress.delete()
        foreignFolder.delete()
        bucket.delete()
    }

    private fun createFile(folder: File, name: String, lastModified: Instant): File {
        return File(folder, name).apply {
            writeText("Test")
            setLastModified(lastModified.toEpochMilli())
        }
    }

    companion object {
        const val BUCKET = "leftovers"
    }
}
