package io.github.fdb.io

import io.github.fdb.io.RespParser.Companion.CRLF

abstract class Reply {
    abstract fun serialize(): ByteArray
}

data class SimpleString(val message: String): Reply() {

    override fun serialize(): ByteArray {
        return "+$message${CRLF}".toByteArray()
    }
}

data class SimpleError(val message: String): Reply() {

    override fun serialize(): ByteArray {
        return "-${message}${CRLF}".toByteArray()
    }
}

data class Integer(val value: Int): Reply() {

    override fun serialize(): ByteArray {
        return ":$value${CRLF}".toByteArray()
    }
}

data class BulkString(val size: Int, val data: ByteArray): Reply() {
    override fun serialize(): ByteArray {
        val prefix = "$${size}${CRLF}".toByteArray()
        val crlf = CRLF.toByteArray()
        return prefix.plus(data).plus(crlf)
    }
}

data class ArrayReply(val items: List<ByteArray?>): Reply() {

    override fun serialize(): ByteArray {
        val prefix = "*${items.size}${CRLF}".toByteArray()

        val data = items.map {
            if (it == null) {
                "_${CRLF}".toByteArray()
            } else {
                BulkString(it.size, it).serialize()
            }
        }
        var buffer = prefix
        data.forEach { buffer = buffer.plus(it) }
        return buffer
    }
}


data class NullReply(val message: String = "_"): Reply() {
    override fun serialize(): ByteArray {
        return "_${CRLF}".toByteArray()
    }
}

class ParsingException(
    message: String
): Exception(message)