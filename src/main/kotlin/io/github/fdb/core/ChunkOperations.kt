package io.github.fdb.core

class ChunkOperations {

    companion object {

        @JvmStatic
        fun append(chunk: Chunk, value: ByteArray) {
            val tmp = ByteArray(chunk.data.size + value.size)
            System.arraycopy(chunk.data, 0, tmp, 0, chunk.data.size)
            System.arraycopy(value, 0, tmp, chunk.data.size, value.size)
            chunk.data = tmp
        }

        @JvmStatic
        fun removeAndCompact(chunk: Chunk, key: String) {

            val header = chunk.header.first { it.key == key }
            val nSize = chunk.data.size - header.size
            val tmp = ByteArray(nSize)

            chunk.header.filter { it.key != key }.forEach {

                if (it.offset < header.offset) {
                    System.arraycopy(chunk.data, it.offset, tmp, it.offset, it.size)
                } else {
                    it.offset = it.offset - header.size
                    System.arraycopy(chunk.data, it.offset + header.size, tmp, it.offset, it.size)
                }
            }
            chunk.data = tmp
        }
    }
}