package io.github.fdb.core

class Chunk {
    val header: MutableList<ChunkHeader> = mutableListOf<ChunkHeader>()
    var data: ByteArray = ByteArray(0)
}

data class ChunkHeader(
    val key: String,
    val type: TypeHint = TypeHint.RAW,
    var offset: Int,
    val size: Int
)

enum class TypeHint {
    INTEGER, DOUBLE, STRING, BOOLEAN, RAW
}