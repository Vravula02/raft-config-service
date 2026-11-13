package com.raftconfig.raft;

import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 * Durable storage for currentTerm and votedFor.
 * Write temp file -> fsync -> atomic rename, so a crash never leaves a half-written file.
 */
public class MetadataStore {

    public record Metadata(long currentTerm, String votedFor) {}

    private final Path file;
    private final Path tmp;

    public MetadataStore(Path dir) {
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot create data dir " + dir, e);
        }
        this.file = dir.resolve("metadata.properties");
        this.tmp = dir.resolve("metadata.properties.tmp");
    }

    public Metadata load() {
        if (!Files.exists(file)) {
            return new Metadata(0, null);
        }
        Properties p = new Properties();
        try (Reader r = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            p.load(r);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read " + file, e);
        }
        long term = Long.parseLong(p.getProperty("currentTerm", "0").trim());
        String voted = p.getProperty("votedFor", "").trim();
        return new Metadata(term, voted.isEmpty() ? null : voted);
    }

    public void save(long currentTerm, String votedFor) {
        String content = "currentTerm=" + currentTerm + "\n"
                + "votedFor=" + (votedFor == null ? "" : votedFor) + "\n";
        ByteBuffer buf = ByteBuffer.wrap(content.getBytes(StandardCharsets.UTF_8));
        try (FileChannel ch = FileChannel.open(tmp, CREATE, WRITE, TRUNCATE_EXISTING)) {
            while (buf.hasRemaining()) {
                ch.write(buf);
            }
            ch.force(true);   // fsync: bytes are on disk, not just in the OS cache
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot write " + tmp, e);
        }
        try {
            Files.move(tmp, file, REPLACE_EXISTING, ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot replace " + file, e);
        }
    }
}