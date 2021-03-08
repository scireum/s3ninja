/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

import ninja.StoredObject
import sirius.kernel.BaseSpecification

class KeyEncodingSpec extends BaseSpecification {

    def "key encoding and decoding works"() {
        when:
        String encoded = StoredObject.encodeKey(key)
        and:
        String decoded = StoredObject.decodeKey(encoded)
        then:
        encoded == encodedKey
        decoded == key
        where:
        key                                                             | encodedKey
        "simple_key"                                                    | "simple_key"
        "this/is/one/heck/of/a/complicated/key😛"                       | "this%2Fis%2Fone%2Fheck%2Fof%2Fa%2Fcomplicated%2Fkey%F0%9F%98%9B"
        "\$\$\$ to make!!!"                                             | "%24%24%24%20to%20make%21%21%21"
        "🧐🧝‍♂️🧑🏿‍🚀"                                                       | "%F0%9F%A7%90%F0%9F%A7%9D%E2%80%8D%E2%99%82%EF%B8%8F%F0%9F%A7%91%F0%9F%8F%BF%E2%80%8D%F0%9F%9A%80"
        "\"Was geht?\" fragte der Fuchs, Pfeffer und Salz 'erbei'olend" | "%22Was%20geht%3F%22%20fragte%20der%20Fuchs%2C%20Pfeffer%20und%20Salz%20%27erbei%27olend"
    }

}
