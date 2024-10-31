package io.github.fdb.core

open class ChunkService(
    private val index: Index
) {

    companion object {
        // TODO: Externalize config
        const val PREFERRED_CHUNK_SIZE = 128 * 1024
    }

    fun get(key: String): Pair<TypeHint, ByteArray>? {
        val chunk = index.chunks[key] ?: return null
        val idx = chunk.header.first { it.key == key }
        return Pair(idx.type, chunk.data.copyOfRange(idx.offset, idx.offset + idx.size))
    }

    fun delete(key: String) {

        val chunk = index.chunks[key] ?: return
        if (chunk.header.size == 1 && chunk.header.first().key == key) {
            index.chunks.remove(key)
            // TODO: If chunks are persisted to disk, we need to remove the file
            return
        }

        chunk.header.firstOrNull { it.key == key }?.let {
            ChunkOperations.removeAndCompact(chunk, key)
            chunk.header.remove(it)
            index.chunks.remove(key)
        }
    }

    fun store(key: String, value: ByteArray, hint: TypeHint) {

        val chunk = allocateChunk(key, value.size)

        // Case 1: Data already present in chunk and needs to be replaced
        chunk.header.firstOrNull { it.key == key }?.let {

            // remove data portion and header
            ChunkOperations.removeAndCompact(chunk, key)
            chunk.header.remove(it)

            // append data and add header
            ChunkOperations.append(chunk, value)
            index.chunks[key] = chunk
            chunk.header.add(
                ChunkHeader(key = key, offset = chunk.data.size - value.size, size = value.size, type = hint))
            return
        }

        // Case 2: Chunk is empty. Add data and set headers
        if (chunk.header.isEmpty()) {
            chunk.data = value
            chunk.header.add(ChunkHeader(key = key, offset = 0, size = value.size, type = hint))
            index.chunks[key] = chunk
            return
        }

        // Case 3: Data not present, but chunk is not empty
        // Append data and recalculate headers
        val originalSize = chunk.data.size
        ChunkOperations.append(chunk, value)
        index.chunks[key] = chunk
        chunk.header.add(ChunkHeader(key = key, offset = originalSize, size = value.size, type = hint))
    }

    /**
     * Traverses all chunks to find a suitable one for the given storage requirements.
     * First a lookup of the index will be performed to check whether the key is already known.
     * In that case the chunk will be returned.
     * In case the key does not exist a suitable chunk will be picked.
     * If no suitable chunk exists a new one will be allocated.
     * None of the operations will modify the index!
     */
    protected fun allocateChunk(key: String, size: Int): Chunk {

        // return for existing key
        if (index.chunks.contains(key)) {
            return index.chunks[key]!!
        }

        // return for enough storage space
        val presentChunk = index.chunks.values.filter {
            it.data.size + size <= PREFERRED_CHUNK_SIZE
        }.firstOrNull()
        if (presentChunk != null) {
            return presentChunk
        }

        // return a new one
        return Chunk()
    }
}