package io.github.fdb.core

import io.github.fdb.core.TypeHint.STRING
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class MappedChunkServiceTest {


    @Test
    fun `can add item to mapped chunk`() {

        val index = Index()
        val service = MappedChunkService(index)

        service.storeItem("myMap", "foo", "[1]".encodeToByteArray(), STRING)
        service.storeItem("myMap", "bar", "[2]".encodeToByteArray(), STRING)

        assertEquals(1, index.chunks.size)
        assertEquals("myMap", index.chunks.keys.first())

        // assert structure of chunk
        val chunk = index.chunks.values.first()
        assertTrue(chunk.header.map { it.key }.containsAll(listOf("bar", "foo")))
        assertEquals("[1][2]", String(chunk.data))
    }

    @Test
    fun `can replace item in mapped chunk`() {

        val index = Index()
        val service = MappedChunkService(index)

        service.storeItem("myMap", "foo", "[1]".encodeToByteArray(), STRING)
        service.storeItem("myMap", "bar", "[2]".encodeToByteArray(), STRING)
        service.storeItem("myMap", "buz", "[3]".encodeToByteArray(), STRING)

        service.storeItem("myMap", "bar", "[replaced]".encodeToByteArray(), STRING)

        val chunk = index.chunks.values.first()
        assertEquals("[1][3][replaced]", String(chunk.data))
    }

    @Test
    fun `returns null if item not in mapped chunk`() {

        val index = Index()
        val service = MappedChunkService(index)

        service.storeItem("myMap", "foo", "[1]".encodeToByteArray(), STRING)
        val item = service.getItem("myMap", "bar")
        assertNull(item)
    }

    @Test
    fun `returns item from map`() {
        val index = Index()
        val service = MappedChunkService(index)

        service.storeItem("myMap", "foo", "[1]".encodeToByteArray(), STRING)
        service.storeItem("myMap", "bar", "[2]".encodeToByteArray(), STRING)
        service.storeItem("myMap", "buz", "[3]".encodeToByteArray(), STRING)

        val item = service.getItem("myMap", "bar")
        assertEquals("[2]", String(item!!))
    }

    @Test
    fun `can delete item from mapped chunk`() {

        val index = Index()
        val service = MappedChunkService(index)

        service.storeItem("myMap", "foo", "[1]".encodeToByteArray(), STRING)
        service.storeItem("myMap", "bar", "[2]".encodeToByteArray(), STRING)
        service.storeItem("myMap", "buz", "[3]".encodeToByteArray(), STRING)

        service.deleteItem("myMap", "foo")
        val chunk = index.chunks.values.first()
        assertEquals("[2][3]", String(chunk.data))
    }
}