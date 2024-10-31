package io.github.fdb.core

import io.github.fdb.io.InlineCommandParser
import kotlin.test.Test
import kotlin.test.assertEquals

class CommandProcessorServiceIT {


    @Test
    fun `can process SET and GET command`() {
        /*
        val index = Index()
        val chunkService = ChunkService(index)
        val mappedChunkService = MappedChunkService(index)
        val inlineParser = InlineCommandParser()
        val commandService = CommandProcessorService(chunkService, mappedChunkService, inlineParser)

        // test STRING
        commandService.process("SET foo \"bar\"")
        val item = commandService.process("GET foo")
        assertEquals("bar", item)

        // test INTEGER
        commandService.process("SET foo 3")
        val integerResult = commandService.process("GET foo")
        assertEquals("3", integerResult)

        // test DOUBLE
        commandService.process("SET foo 1.351646")
        val doubleResult = commandService.process("GET foo")
        assertEquals("1.351646", doubleResult)

        // test BOOLEAN
        commandService.process("SET foo t")
        val booleanResult = commandService.process("GET foo")
        assertEquals("t", booleanResult)

         */
    }
}