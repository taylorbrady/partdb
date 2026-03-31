package io.partdb.app;

sealed interface AppCommand
    permits DeleteCommand, ErrorCommand, GetCommand, HelpCommand, MemberCommand, PutCommand, StartCommand, StatusCommand, VersionCommand {

    int execute(CliRuntime runtime);
}
