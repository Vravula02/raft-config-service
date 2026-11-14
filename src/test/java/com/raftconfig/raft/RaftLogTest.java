package com.raftconfig.raft;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RaftLogTest {

    @TempDir
    Path dir;

    @Test
    void discardsPartiallyWrittenLastEntry() throws IOException {
        Path file = dir.resolve("raft.log");
        // Simulate a crash in the middle of writing entry 3 (no trailing newline)
        Files.writeString(file, "1 1 SET a 1\n2 1 SET b 2\n3 1 SET c", StandardCharsets.UTF_8);

        RaftLog log = new RaftLog(dir);
        try {
            assertEquals(2, log.lastIndex());
            assertEquals("1 1 SET a 1\n2 1 SET b 2\n", Files.readString(file, StandardCharsets.UTF_8),
                    "the torn line is removed from the file too");
        } finally {
            log.close();
        }
    }

    @Test
    void commandsWithNewlinesAndBackslashesSurviveReopen() {
        String command = "SET note line1\nline2 \\ end";
        RaftLog log = new RaftLog(dir);
        log.append(1, command);
        log.close();

        RaftLog reopened = new RaftLog(dir);
        try {
            assertEquals(command, reopened.get(1).command());
        } finally {
            reopened.close();
        }
    }

    @Test
    void truncationIsDurableAndAppendsContinueAfterIt() {
        RaftLog log = new RaftLog(dir);
        log.append(1, "SET a 1");
        log.append(1, "SET b 2");
        log.append(1, "SET c 3");
        log.truncateFrom(2);
        log.append(2, "SET b 20");
        log.close();

        RaftLog reopened = new RaftLog(dir);
        try {
            assertEquals(2, reopened.lastIndex());
            assertEquals("SET b 20", reopened.get(2).command());
            assertEquals(2, reopened.get(2).term());
        } finally {
            reopened.close();
        }
    }
}