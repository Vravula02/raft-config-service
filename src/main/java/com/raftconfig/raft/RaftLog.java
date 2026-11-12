package com.raftconfig.raft;

import java.util.ArrayList;
import java.util.List;

/**
 * The replicated log. 1-indexed: index 0 is a virtual empty entry with term 0.
 *
 * Not thread-safe by itself: RaftState's lock guards every access.
 */
public class RaftLog {

    private final List<LogEntry> entries = new ArrayList<>();

    /** Index of the last entry, or 0 if the log is empty. */
    public long lastIndex() {
        return entries.size();
    }

    /** Term of the last entry, or 0 if the log is empty. */
    public long lastTerm() {
        return entries.isEmpty() ? 0 : entries.getLast().term();
    }

    /** Term of the entry at index; 0 for index 0; -1 if no entry exists there. */
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

    /** All entries from index (inclusive) to the end. Used by the leader to replicate. */
    public List<LogEntry> entriesFrom(long index) {
        if (index > lastIndex()) return List.of();
        return List.copyOf(entries.subList((int) Math.max(index, 1) - 1, entries.size()));
    }

    /** Leader: create a new entry at the end of the log. */
    public LogEntry append(long term, String command) {
        LogEntry entry = new LogEntry(lastIndex() + 1, term, command);
        entries.add(entry);
        return entry;
    }

    /** Follower: add an entry received from the leader (must be the next index). */
    public void appendReplicated(LogEntry entry) {
        if (entry.index() != lastIndex() + 1) {
            throw new IllegalStateException("Expected index " + (lastIndex() + 1) + " but got " + entry.index());
        }
        entries.add(entry);
    }

    /** Delete the entry at index and everything after it (used to fix conflicts). */
    public void truncateFrom(long index) {
        if (index < 1 || index > lastIndex()) return;
        entries.subList((int) index - 1, entries.size()).clear();
    }

    public List<LogEntry> all() {
        return List.copyOf(entries);
    }
}