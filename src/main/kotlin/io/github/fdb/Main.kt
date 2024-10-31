package io.github.fdb

import io.github.fdb.core.ChunkService
import io.github.fdb.core.CommandProcessorService
import io.github.fdb.core.CommandQueue
import io.github.fdb.core.Index
import io.github.fdb.core.MappedChunkService
import io.github.fdb.core.TypeHint
import io.github.fdb.io.InlineCommandParser
import io.github.fdb.io.TcpSocketServer
import org.apache.commons.lang3.RandomStringUtils
import java.util.UUID

fun main() {

    val index = Index()
    val inlineCommandParser = InlineCommandParser()
    val chunkService = ChunkService(index)
    val mappedChunkService = MappedChunkService(index)
    val commandProcessorService = CommandProcessorService(chunkService, mappedChunkService)
    val commandQueue = CommandQueue(commandProcessorService)
    val server = TcpSocketServer(inlineCommandParser, commandQueue)
    server.execute()

}

private fun executeCorePerformanceBenchmark() {

    val index = Index()
    val service = ChunkService(index)

    val startInsert = System.currentTimeMillis()

    // generate data
    for (i in 0..100_000) {
        if (i % 10_000 == 0) {
            println("Inserted $i items")
        }
        service.store(UUID.randomUUID().toString(),
            RandomStringUtils.insecure().next(146).encodeToByteArray(), TypeHint.STRING)
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