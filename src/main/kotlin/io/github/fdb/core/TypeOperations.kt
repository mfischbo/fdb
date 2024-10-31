package io.github.fdb.core

import java.nio.ByteBuffer

class TypeOperations {

    companion object {

        fun parseAndConvert(data: String, typeHint: TypeHint): ByteArray {

            return when (typeHint) {
                TypeHint.RAW -> return data.toByteArray()
                TypeHint.STRING -> return data.encodeToByteArray()
                TypeHint.BOOLEAN -> if (data.lowercase() == "t") convert(1) else convert(0)
                TypeHint.INTEGER -> convert(data.toInt())
                TypeHint.DOUBLE -> convert(data.toDouble())
            }
        }

        fun convert(int: Int): ByteArray {
            return ByteBuffer.allocate(Int.SIZE_BYTES).putInt(int).array()
        }

        fun convert(double: Double): ByteArray {
            return ByteBuffer.allocate(Double.SIZE_BYTES).putDouble(double).array()
        }
    }
}