package io.github.fdb.io

import kotlin.test.Test
import kotlin.test.assertEquals

class InlineCommandParserTest {

    @Test
    fun `can create set command correctly from input`() {

        val parser = InlineCommandParser()

        val input = "SET foo \"This is my string.\""
        val result = parser.parse(input) as SetCommand

        assertEquals("foo", result.key)
        assertEquals("\"This is my string.\"", result.value)
    }
}