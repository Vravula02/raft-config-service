package com.raftconfig.raft;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 * The replicated log, 1-indexed (index 0 is a virtual empty entry with term 0).
 * Kept in memory and in an append-only file, one entry per line:
 *   <index> <term> <command>
 * with \ , newline and carriage return escaped in the command.
 *
 * Every write is fsynced before the method returns.
 * Not thread-safe by itself: RaftState's lock guards every access.
 */
public class RaftLog {

    private static final Logger log = LoggerFactory.getLogger(RaftLog.class);

    private final List<LogEntry> entries = new ArrayList<>();
    private final Path file;
    private final Path tmp;
    private FileChannel channel;

    public RaftLog(Path dir) {
        this.file = dir.resolve("raft.log");
        this.tmp = dir.resolve("raft.log.tmp");
        try {
            Files.createDirectories(dir);
            load();
            if (channel == null) {
                channel = FileChannel.open(file, CREATE, WRITE, APPEND);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot open log " + file, e);
        }
    }

    // ---------- Reading ----------

    public long lastIndex() {
        return entries.size();
    }

    public long lastTerm() {
        return entries.isEmpty() ? 0 : entries.getLast().term();
    }

    /** Term at index; 0 for index 0; -1 if there's no entry there. */
    public long termAt(long index) {
        if (index == 0) return 0;
        if (index < 0 || index > lastIndex()) return -1;
        return entries.get((int) index - 1).term();
    }

    public LogEntry get(long index) {
        if (index < 1 || index > lastIndex()) {
            throw new IndexOutOfBoundsException("No log entry at index " + index);
        }
        return entries.get((int) index - 1);
    }

    public List<LogEntry> entriesFrom(long index) {
        if (index > lastIndex()) return List.of();
        return List.copyOf(entries.subList((int) Math.max(index, 1) - 1, entries.size()));
    }

    public List<LogEntry> all() {
        return List.copyOf(entries);
    }

    // ---------- Writing (all durable before returning) ----------

    /** Leader: create a new entry at the end of the log. */
    public LogEntry append(long term, String command) {
        LogEntry entry = new LogEntry(lastIndex() + 1, term, command);
        writeAndSync(List.of(entry));
        entries.add(entry);
        return entry;
    }

    /** Follower: add entries from the leader. Must start at lastIndex + 1 and be contiguous. */
    public void appendAll(List<LogEntry> batch) {
        if (batch.isEmpty()) return;
        long expected = lastIndex() + 1;
        for (LogEntry e : batch) {
            if (e.index() != expected) {
                throw new IllegalStateException("Expected index " + expected + " but got " + e.index());
            }
            expected++;
        }
        writeAndSync(batch);   // one fsync for the whole batch
        entries.addAll(batch);
    }

    /** Delete the entry at index and everything after it, then rewrite the file. */
    public void truncateFrom(long index) {
        if (index < 1 || index > lastIndex()) return;
        entries.subList((int) index - 1, entries.size()).clear();
        try {
            rewrite();
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot rewrite log " + file, e);
        }
    }

    // ---------- File handling ----------

    private void writeAndSync(List<LogEntry> batch) {
        StringBuilder sb = new StringBuilder();
        for (LogEntry e : batch) {
            sb.append(encode(e));
        }
        ByteBuffer buf = ByteBuffer.wrap(sb.toString().getBytes(StandardCharsets.UTF_8));
        try {
            while (buf.hasRemaining()) {
                channel.write(buf);
            }
            channel.force(true);   // fsync
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot append to log " + file, e);
        }
    }

    /** Write the whole in-memory log to a temp file, fsync, atomically replace the real file. */
    private void rewrite() throws IOException {
        if (channel != null) {
            channel.close();
            channel = null;
        }
        StringBuilder sb = new StringBuilder();
        for (LogEntry e : entries) {
            sb.append(encode(e));
        }
        ByteBuffer buf = ByteBuffer.wrap(sb.toString().getBytes(StandardCharsets.UTF_8));
        try (FileChannel ch = FileChannel.open(tmp, CREATE, WRITE, TRUNCATE_EXISTING)) {
            while (buf.hasRemaining()) {
                ch.write(buf);
            }
            ch.force(true);
        }
        Files.move(tmp, file, REPLACE_EXISTING, ATOMIC_MOVE);
        channel = FileChannel.open(file, CREATE, WRITE, APPEND);
    }

    private void load() throws IOException {
        if (!Files.exists(file)) {
            log.info("No log file yet at {}", file);
            return;
        }
        String content = Files.readString(file, StandardCharsets.UTF_8);
        // A crash mid-write leaves a last line without '\n'. It was never acknowledged: drop it.
        boolean torn = !content.isEmpty() && !content.endsWith("\n");
        String[] lines = content.split("\n");
        int usable = torn ? lines.length - 1 : lines.length;

        for (int i = 0; i < usable; i++) {
            if (lines[i].isEmpty()) continue;
            LogEntry e = decode(lines[i]);
            if (e.index() != entries.size() + 1) {
                throw new IllegalStateException("Corrupt log " + file + ": expected index "
                        + (entries.size() + 1) + " on line " + (i + 1) + " but found " + e.index());
            }
            entries.add(e);
        }
        if (torn) {
            log.warn("Discarding partially written last entry in {}", file);
            rewrite();
        }
        log.info("Loaded {} log entries from {}", entries.size(), file);
    }

    private static String encode(LogEntry e) {
        return e.index() + " " + e.term() + " " + escape(e.command()) + "\n";
    }

    private static LogEntry decode(String line) {
        String[] parts = line.split(" ", 3);
        long index = Long.parseLong(parts[0]);
        long term = Long.parseLong(parts[1]);
        String command = parts.length == 3 ? unescape(parts[2]) : "";
        return new LogEntry(index, term, command);
    }

    private static String escape(String s) {
        return s.replace("\\", "\\\\").replace("\n", "\\n").replace("\r", "\\r");
    }

    private static String unescape(String s) {
        StringBuilder out = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\' && i + 1 < s.length()) {
                char next = s.charAt(++i);
                switch (next) {
                    case 'n' -> out.append('\n');
                    case 'r' -> out.append('\r');
                    default -> out.append(next);   // covers "\\"
                }
            } else {
                out.append(c);
            }
        }
        return out.toString();
    }
}