package io.github.fdb.io

import io.github.fdb.io.RespParser.Companion.CRLF
import java.io.ByteArrayInputStream
import kotlin.test.Test
import kotlin.test.assertEquals

class RespParserTest {


    @Test
    fun doSomething() {

        val command = "*3${CRLF}$3${CRLF}SET${CRLF}$3${CRLF}foo${CRLF}$5${CRLF}hello".encodeToByteArray()
        val iStream = ByteArrayInputStream(command)
        val parser = RespParser(iStream)

        val result = parser.parse() as SetCommand

        assertEquals("foo", result.key)
        assertEquals("hello", result.value)
    }
}