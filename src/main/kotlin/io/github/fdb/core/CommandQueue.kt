package io.github.fdb.core

import io.github.fdb.io.Command
import io.github.fdb.io.Reply
import io.github.fdb.io.SimpleError
import io.github.oshai.kotlinlogging.KotlinLogging
import java.util.concurrent.CompletableFuture
import java.util.concurrent.LinkedBlockingQueue

class CommandQueue(
    val commandProcessor: CommandProcessorService
) {

    private val queue = LinkedBlockingQueue<Pair<Command, CompletableFuture<Reply>>>()
    private val logger = KotlinLogging.logger {}

    fun submit(command: Command, completableFuture: CompletableFuture<Reply>) {
        val result = queue.add(Pair(command, completableFuture))
        if (result) {
            logger.debug { "Accepted operation $command" }
        }
    }

    fun startProcessing() {
        Thread
            .ofVirtual()
            .name("command-queue")
            .start { internalProcess() }
    }

    fun internalProcess() {
        while (true) {

            // check item in the queue
            val item = queue.take()

            // process command
            try {
                val result = commandProcessor.process(item.first)

                // complete future
                val future = item.second
                future.complete(result)
                Thread.yield()
            } catch (e: Exception) {
                // TODO: Error handling. Create error result
                item.second.complete(SimpleError("ERR ${e.message}"))
            }
        }
    }
}