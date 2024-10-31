package io.github.fdb.io

class InlineCommandParser {

    fun parse(input: String): Command {

        if (input.startsWith("SET ")) {
            val tmp = input.substringAfter("SET ").split(" ", limit = 2)
            return SetCommand(tmp[0], tmp[1])
        }

        if (input.startsWith("GET ")) {
            val tmp = input.substringAfter("GET ").split(" ")
            return GetCommand(tmp[0])
        }

        if (input.startsWith("DEL ")) {
            val tmp = input.substringAfter("DEL ").split(" ")
            return DeleteCommand(tmp[0])
        }

        return NoOpCommand()
    }
}