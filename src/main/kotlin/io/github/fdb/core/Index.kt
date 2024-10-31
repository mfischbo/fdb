package io.github.fdb.core

import java.util.Collections

data class Index(
    val chunks: MutableMap<String, Chunk> =
        Collections.synchronizedMap<String, Chunk>(HashMap<String, Chunk>(50_000))// HashMap(50_000)
)
