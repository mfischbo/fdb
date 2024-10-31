package io.github.fdb.core

import io.github.fdb.io.ArrayReply
import io.github.fdb.io.BulkString
import io.github.fdb.io.Command
import io.github.fdb.io.CommandDocsCommand
import io.github.fdb.io.CopyCommand
import io.github.fdb.io.DeleteCommand
import io.github.fdb.io.GetCommand
import io.github.fdb.io.Integer
import io.github.fdb.io.MGetCommand
import io.github.fdb.io.NullReply
import io.github.fdb.io.PingCommand
import io.github.fdb.io.Reply
import io.github.fdb.io.SetCommand
import io.github.fdb.io.SimpleError
import io.github.fdb.io.SimpleString

class CommandProcessorService(
    private val chunkService: ChunkService,
    private val mappedChunkService: MappedChunkService,
) {

    fun process(cmd: Command): Reply {

        return when (cmd) {
            is CommandDocsCommand -> execute(cmd)
            is PingCommand -> execute(cmd)

            is CopyCommand -> execute(cmd)
            is DeleteCommand -> execute(cmd)
            is GetCommand -> execute(cmd)
            is MGetCommand -> execute(cmd)
            is SetCommand -> execute(cmd)
            else -> SimpleError("ERR Unknown command '${cmd.javaClass.simpleName}'")
        }
    }

    private fun execute(cmd: CommandDocsCommand): Reply {
        return SimpleString("OK");
    }
    private fun execute(cmd: PingCommand): Reply {
        return SimpleString(cmd.message)
    }


    private fun execute(setCommand: SetCommand): Reply {
        val hint = TypeHint.RAW
        chunkService.store(setCommand.key, setCommand.value.toByteArray(), hint)
        return SimpleString("OK")
    }

    private fun execute(cmd: CopyCommand): Reply {
        val value = chunkService.get(cmd.source)
        if (value == null) {
            return Integer(0)
        }
        chunkService.store(cmd.destination, value.second, TypeHint.RAW)
        return Integer(1)
    }

    private fun execute(command: DeleteCommand): SimpleString {
        chunkService.delete(command.key)
        return SimpleString("OK")
    }

    private fun execute(command: GetCommand): Reply {
        val result = chunkService.get(command.key)
        if (result != null) {
            return BulkString(result.second.size, result.second)
        }
        return NullReply()
    }

    private fun execute(cmd: MGetCommand): Reply {
        val results = cmd.keys.map {
            chunkService.get(it)?.second
        }
        return ArrayReply(results)
    }

    private fun getTypeHint(input: String): TypeHint {
        if (input.isEmpty()) {
            return TypeHint.RAW
        }

        val tmp = input.toCharArray()
        if (tmp[0] == '"' && tmp[tmp.size -1] == '"') {
            return TypeHint.STRING
        }

        if (tmp.size == 1 && (tmp[0].lowercaseChar() == 't' || tmp[0].lowercaseChar() == 'f')) {
            return TypeHint.BOOLEAN
        }

        if (tmp.contains('.')) {
            return TypeHint.DOUBLE
        }

        return TypeHint.INTEGER
    }
}