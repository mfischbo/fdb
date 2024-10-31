package io.github.fdb.core

import io.github.fdb.core.TypeHint.STRING
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class ChunkServiceTest {

    @Test
    fun `can retrieve stored item`() {

        val index = Index()
        val service = ChunkService(index)

        service.store("1", "Hello".encodeToByteArray(), STRING)
        service.store("2", "World".encodeToByteArray(), STRING)
        service.store("3", "Cool".encodeToByteArray(), STRING)

        val one = service.get("1")
        val two = service.get("2")
        val three = service.get("3")

        assertEquals(STRING, one!!.first)
        assertEquals(STRING, two!!.first)
        assertEquals(STRING, three!!.first)

        assertEquals("Hello", String(one.second))
        assertEquals("World", String(two.second))
        assertEquals("Cool", String(three.second))
    }

    @Test
    fun `return null if item not present`() {
        val index = Index()
        val service = ChunkService(index)
        assertNull(service.get("1"))
    }

    @Test
    fun `can add item if no data exists`() {

        val index = Index()
        val service = ChunkService(index)

        service.store("foo", "Hello World".encodeToByteArray(), STRING)

        assertTrue(index.chunks["foo"] != null)
        val chunk = index.chunks["foo"]!!
        assertEquals("foo", chunk.header.first().key)
        assertEquals(0, chunk.header.first().offset)
        assertEquals("Hello World".encodeToByteArray().size, chunk.header.first().size)
        assertEquals("Hello World", String(chunk.data))
    }


    @Test
    fun `can add new data to existing chunk`() {

        val index = Index()
        val service = ChunkService(index)

        service.store("foo", "bar".encodeToByteArray(), STRING)
        service.store("test", "hello".encodeToByteArray(), STRING)

        assertNotNull(index.chunks["test"])
        assertEquals(index.chunks["test"], index.chunks["foo"])
    }


    @Test
    fun `can replace data from the beginning of the chunk`() {

        val index = Index()
        val service = ChunkService(index)

        service.store("1", "[1]".encodeToByteArray(), STRING)
        service.store("2", "[2]".encodeToByteArray(), STRING)
        service.store("3", "[3]".encodeToByteArray(), STRING)
        service.store("1", "[replaced]".encodeToByteArray(), STRING)

        // 3 keys all pointing to the same chunk
        assertEquals(3, index.chunks.size)
        assertEquals(index.chunks["1"], index.chunks["2"])
        assertEquals(index.chunks["2"], index.chunks["3"])

        // assert header data in chunk
        val chunk = index.chunks["1"]!!
        assertEquals(0, chunk.header[0].offset)
        assertEquals(3, chunk.header[0].size)
        assertEquals(3, chunk.header[1].offset)
        assertEquals(3, chunk.header[1].size)
        assertEquals(6, chunk.header[2].offset)
        assertEquals(10, chunk.header[2].size)

        // assert content
        assertEquals("[2][3][replaced]", String(chunk.data))
    }

    @Test
    fun `can replace data from the middle of the chunk`() {

        val index = Index()
        val service = ChunkService(index)

        service.store("1", "[1]".encodeToByteArray(), STRING)
        service.store("2", "[2]".encodeToByteArray(), STRING)
        service.store("3", "[3]".encodeToByteArray(), STRING)
        service.store("2", "[replaced]".encodeToByteArray(), STRING)


        // 3 keys all pointing to the same chunk
        assertEquals(3, index.chunks.size)
        assertEquals(index.chunks["1"], index.chunks["2"])
        assertEquals(index.chunks["2"], index.chunks["3"])

        // assert header data in chunk
        val chunk = index.chunks["1"]!!
        assertEquals(0, chunk.header[0].offset)
        assertEquals(3, chunk.header[0].size)
        assertEquals(3, chunk.header[1].offset)
        assertEquals(3, chunk.header[1].size)
        assertEquals(6, chunk.header[2].offset)
        assertEquals(10, chunk.header[2].size)

        // assert content
        assertEquals("[1][3][replaced]", String(chunk.data))
    }

    @Test
    fun `can replace data from the end of the chunk`() {

        val index = Index()
        val service = ChunkService(index)

        service.store("1", "[1]".encodeToByteArray(), STRING)
        service.store("2", "[2]".encodeToByteArray(), STRING)
        service.store("3", "[3]".encodeToByteArray(), STRING)
        service.store("3", "[replaced]".encodeToByteArray(), STRING)


        // 3 keys all pointing to the same chunk
        assertEquals(3, index.chunks.size)
        assertEquals(index.chunks["1"], index.chunks["2"])
        assertEquals(index.chunks["2"], index.chunks["3"])

        // assert header data in chunk
        val chunk = index.chunks["1"]!!
        assertEquals(0, chunk.header[0].offset)
        assertEquals(3, chunk.header[0].size)
        assertEquals(3, chunk.header[1].offset)
        assertEquals(3, chunk.header[1].size)
        assertEquals(6, chunk.header[2].offset)
        assertEquals(10, chunk.header[2].size)

        // assert content
        assertEquals("[1][2][replaced]", String(chunk.data))
    }

    @Test
    fun `can delete single item`() {

        val index = Index()
        val service = ChunkService(index)

        service.store("1", "[1]".encodeToByteArray(), STRING)
        service.delete("1")

        assertEquals(0, index.chunks.size)
    }

    @Test
    fun `can delete item from start of chunk`() {

        val index = Index()
        val service = ChunkService(index)

        service.store("1", "[1]".encodeToByteArray(), STRING)
        service.store("2", "[2]".encodeToByteArray(), STRING)
        service.store("3", "[3]".encodeToByteArray(), STRING)
        service.delete("1")

        assertEquals(2, index.chunks.size)
        assertEquals(index.chunks["2"], index.chunks["3"])

        val chunk = index.chunks["2"]!!
        assertEquals(0, chunk.header[0].offset)
        assertEquals(3, chunk.header[0].size)
        assertEquals(3, chunk.header[1].offset)
        assertEquals(3, chunk.header[1].size)
        assertEquals("[2][3]", String(chunk.data))
    }

    @Test
    fun `can delete item from middle of chunk`() {

        val index = Index()
        val service = ChunkService(index)

        service.store("1", "[1]".encodeToByteArray(), STRING)
        service.store("2", "[2]".encodeToByteArray(), STRING)
        service.store("3", "[3]".encodeToByteArray(), STRING)
        service.delete("2")

        assertEquals(2, index.chunks.size)
        assertEquals(index.chunks["1"], index.chunks["3"])

        val chunk = index.chunks["1"]!!
        assertEquals(0, chunk.header[0].offset)
        assertEquals(3, chunk.header[0].size)
        assertEquals(3, chunk.header[1].offset)
        assertEquals(3, chunk.header[1].size)
        assertEquals("[1][3]", String(chunk.data))
    }

    @Test
    fun `can delete item from end of chunk`() {

        val index = Index()
        val service = ChunkService(index)

        service.store("1", "[1]".encodeToByteArray(), STRING)
        service.store("2", "[2]".encodeToByteArray(), STRING)
        service.store("3", "[3]".encodeToByteArray(), STRING)
        service.delete("3")

        assertEquals(2, index.chunks.size)
        assertEquals(index.chunks["1"], index.chunks["2"])

        val chunk = index.chunks["1"]!!
        assertEquals(0, chunk.header[0].offset)
        assertEquals(3, chunk.header[0].size)
        assertEquals(3, chunk.header[1].offset)
        assertEquals(3, chunk.header[1].size)
        assertEquals("[1][2]", String(chunk.data))
    }
}