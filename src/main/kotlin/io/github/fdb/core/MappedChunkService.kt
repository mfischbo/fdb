package io.github.fdb.core

class MappedChunkService(
    private val index: Index
) {

    fun storeItem(indexKey: String, mapKey: String, item: ByteArray, hint: TypeHint) {

        val chunk = allocateChunk(indexKey)

        chunk.header.firstOrNull { it.key == mapKey }?.let {
            ChunkOperations.removeAndCompact(chunk, mapKey)
            chunk.header.remove(it)
        }

        val offset = chunk.data.size
        ChunkOperations.append(chunk, item)
        chunk.header.add(ChunkHeader(key = mapKey, type = hint, offset = offset, size = item.size))
        index.chunks[indexKey] = chunk
    }


    fun getItem(indexKey: String, mapKey: String): ByteArray? {
        if (!index.chunks.containsKey(indexKey)) return null
        val chunk = index.chunks[indexKey]!!
        chunk.header.firstOrNull { it.key == mapKey }?.let {
            return chunk.data.copyOfRange(it.offset, it.offset + it.size)
        }
        return null
    }

    fun deleteItem(indexKey: String, mapKey: String) {
        if (!index.chunks.containsKey(indexKey)) return

        val chunk = index.chunks[indexKey]!!
        chunk.header.firstOrNull { it.key == mapKey }?.let {
            ChunkOperations.removeAndCompact(chunk, mapKey)
            chunk.header.remove(it)

            if (chunk.header.isEmpty()) {
                index.chunks.remove(indexKey)
            }
        }
    }

    fun deleteMap(indexKey: String) {
        // TODO: Disk sync
        index.chunks.remove(indexKey)
    }

    private fun allocateChunk(indexKey: String): Chunk {
        if (index.chunks.containsKey(indexKey)) {
            return index.chunks[indexKey]!!
        }
        return Chunk()
    }
}