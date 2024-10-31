package io.github.fdb.core

import io.github.fdb.core.TypeHint.STRING
import org.apache.commons.lang3.RandomStringUtils
import org.junit.jupiter.api.Disabled
import java.util.UUID
import kotlin.test.Test

@Disabled
class ChunkServicePerformanceTest {

    @Test
    fun `performance test`() {

        val index = Index()
        val service = ChunkService(index)

        val startInsert = System.currentTimeMillis()
        // generate data
        for (i in 0..100_000) {
            if (i % 10_000 == 0) {
                println("Inserted $i items")
            }
            service.store(UUID.randomUUID().toString(), RandomStringUtils.insecure().next(146).encodeToByteArray(), STRING)
        }
        val endInsert = System.currentTimeMillis()

        var warmUpIds = mutableListOf<String>()
        (0..10000).forEach { i ->
            warmUpIds.add(index.chunks.keys.random())
        }

        var ids = mutableListOf<String>()
        (0..10000).forEach { i ->
            ids.add(index.chunks.keys.random())
        }

        // prewarm JIT
        println("Warming up JIT...")
        warmUpIds.forEach{ service.get(it) }
        var id = warmUpIds[5000]

        // actual measurement
        println("Actual measurement...")
        var t = System.nanoTime()
        ids.forEach {
            service.get(it)
        }
        var x = System.nanoTime()
        service.get(id)
        var xTime = System.nanoTime()

        println("Insertion took: ${endInsert - startInsert}ms")
        println("Lookup took average: ${(x - t) / ids.size}ns")
        println("Last lookup took: ${xTime - x}ns")
    }
}