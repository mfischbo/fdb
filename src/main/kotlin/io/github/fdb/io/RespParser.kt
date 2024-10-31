package io.github.fdb.io

import io.github.oshai.kotlinlogging.KotlinLogging
import java.io.InputStream

/**
 * For commands only requires us to be able to read:
 * - array's
 * - bulk strings
 */
class RespParser(
    private val inputStream: InputStream,
) {

    private val logger = KotlinLogging.logger {}

    companion object {
        const val CRLF = "\r\n"
        const val ARRAY_BYTE = '*'.code
        const val BULK_STRING_BYTE = '$'.code
    }


    fun parse(): Command {

        val id = inputStream.read()
        if (id == -1) {
            throw Exception("Connection closed")
        }

        if (id == ARRAY_BYTE) {
            val argsSize = nextLine(inputStream).toInt()
            val args = mutableListOf<BulkString>()
            (0..argsSize -1).forEach { i -> args.add(readBulkString(inputStream)) }

            // create command
            return createCommand(args)
        }

        return NoOpCommand()
    }

    private fun createCommand(bulks: List<BulkString>): Command {

        bulks.forEach { bulk -> logger.debug { String(bulk.data) }}

        val keyword = String(bulks.first().data).uppercase()

        if (keyword == "COPY") {
            return CopyCommand(String(bulks[1].data), String(bulks[2].data))
        }

        if (keyword == "SET") {
            return SetCommand(String(bulks[1].data), String(bulks[2].data)) // TODO: Should retain the raw binary data
        }

        if (keyword == "GET") {
            return GetCommand(String(bulks[1].data))
        }

        if (keyword == "DEL") {
            return DeleteCommand(String(bulks[1].data))
        }

        if (keyword == "MGET") {
            val args = bulks.subList(1, bulks.size).map { String(it.data) }
            return MGetCommand(args)
        }

        if (keyword == "COMMAND" && String(bulks[1].data) == "DOCS") {
            return CommandDocsCommand(emptyList())
        }

        if (keyword == "PING") {
            if (bulks.size == 1) return PingCommand("") else return PingCommand(String(bulks[1].data))
        }

        return NoOpCommand()
    }

    private fun readBulkString(input: InputStream): BulkString {

        val id = input.read()
        if (id != BULK_STRING_BYTE) {
            throw Exception("No bulk string")
        }
        val size = nextLine(input).toInt()
        val bytes = input.readNBytes(size)

        // read CLRF and discard
        input.readNBytes(2)
        return BulkString(size, bytes)
    }

    private fun nextLine(input: InputStream): String {

        val sb = mutableListOf<Byte>()
        while (true) {
            val b = input.read()
            if (b == -1) {
                return String(sb.toByteArray()).substring(0, sb.size - CRLF.length)
            }
            if (b == 10 && sb.last().toInt() == 13) {
                val str = String(sb.toByteArray())
                return str.substring(0, sb.size - 1)
            }
            sb.add(b.toByte())
        }
    }
}