package io.github.fdb.io

import io.github.fdb.core.CommandQueue
import io.github.oshai.kotlinlogging.KotlinLogging
import java.net.ServerSocket
import java.util.concurrent.CompletableFuture

class TcpSocketServer(
    private val inlineCommandParser: InlineCommandParser,
    private val commandQueue: CommandQueue,
) {

    private val logger = KotlinLogging.logger {}

    fun execute() {
        // TODO: Externalize config
        val server = ServerSocket(6335)
        commandQueue.startProcessing()

        var clientConnections = 0
        while (true) {

            val client = server.accept()
            clientConnections++

            Thread.ofVirtual().name("client-worker-${clientConnections}").start {
                var running = true
                val parser = RespParser(client.inputStream)

                while (running) {
                    try {
                        val command = parser.parse()
                        val future = CompletableFuture<Reply>()
                        commandQueue.submit(command, future)

                        val result = future.get()
                        if (result != null) {
                            logger.debug { String(result.serialize()) }
                            client.outputStream.write(result.serialize())
                            client.outputStream.flush()
                        }
                    } catch (ex: Exception) {
                        logger.error(ex) { "Failed to write response. Cause: ${ex.message}" }
                        running = false
                    }
                }
            }
        }
    }

    companion object {
        val CRLF = "\r\n".toByteArray()
    }
}