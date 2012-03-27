package com.github.cwilper.fcrepo.store.util.commands;

/**
 * An executable command.
 */
public interface Command {
    void execute();
    void close();
}
