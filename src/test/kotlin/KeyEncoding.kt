import ninja.StoredObject
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import sirius.kernel.SiriusExtension

@ExtendWith(SiriusExtension::class)
class KeyEncoding {

    @Test
    fun `key encoding and decoding works`() {
        val keys = listOf(
            "simple_key",
            "this/is/one/heck/of/a/complicated/key😛",
            "\$\$\$ to make!!!",
            "🧐🧝‍♂️🧑🏿‍🚀",
            "\"Was geht?\" fragte der Fuchs, Pfeffer und Salz 'erbei'olend"
        )
        val encodedKeys = listOf(
            "simple_key",
            "this%2Fis%2Fone%2Fheck%2Fof%2Fa%2Fcomplicated%2Fkey%F0%9F%98%9B",
            "%24%24%24%20to%20make%21%21%21",
            "%F0%9F%A7%90%F0%9F%A7%9D%E2%80%8D%E2%99%82%EF%B8%8F%F0%9F%A7%91%F0%9F%8F%BF%E2%80%8D%F0%9F%9A%80",
            "%22Was%20geht%3F%22%20fragte%20der%20Fuchs%2C%20Pfeffer%20und%20Salz%20%27erbei%27olend"
        )
        for (i in 0 until keys.size) {
            val key = keys[i]
            val encodedKey = encodedKeys[i]
            val encoded = StoredObject.encodeKey(key)
            val decoded = StoredObject.decodeKey(encodedKey)
            Assertions.assertEquals(encoded, encodedKey)
            Assertions.assertEquals(decoded, key)
        }
    }
}
