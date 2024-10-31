package io.github.fdb.io

open class Command { }

class NoOpCommand(): Command()

class CopyCommand(val source: String, val destination: String): Command()
class DeleteCommand(val key: String) : Command()
class GetCommand(val key: String): Command()
class MGetCommand(val keys: List<String>): Command()
class SetCommand(val key: String, val value: String) : Command()


class CommandDocsCommand(val args: List<String>): Command()
class PingCommand(val message: String): Command()