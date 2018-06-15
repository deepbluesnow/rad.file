package com.ucex.rad.file.simple;

import com.ucex.rad.file.DataFile;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Consumer;

public class SimplePattern {

    /**
     * Produce constantly into a data file into sequential record buckets, rotating back to the beginning of the file when at the end. One way to use this is to read from a queue (possibly zero sized too aka SynchronousQueue) and then write.
     */
    public static void produceContinuously(DataFile f, Consumer<ByteBuffer> c, BackoffPolicy backoffPolicy) throws InterruptedException {
        for (int i = 0; true; i++) {
            checkInterrupted();
            if (i == f.maxRecords) i = 0;
            int retries = 0;
            while (!f.write(i, c)) {
                checkInterrupted();
                backoffPolicy.backoff(retries++);
            }
        }
    }

    /**
     * Consume constantly from a data file from sequential record buckets, rotating back to the beginning of the file when at the end.
     */
    public static void consumeContinuously(DataFile f, Consumer<ByteBuffer> c, BackoffPolicy backoffPolicy) throws InterruptedException {
        for (int i = 0; true; i++) {
            checkInterrupted();
            if (i == f.maxRecords) i = 0;
            int retries = 0;
            while (!f.read(i, c)) {
                checkInterrupted();
                backoffPolicy.backoff(retries++);
            }
        }
    }

    /**
     * Generally used when detecting unprocessed files upon app startup. Will consume all records then move it to backup dir.
     */
    public static void consumeAllAndThenBackup(Path p, Path backupDir, Consumer<ByteBuffer> c) throws IOException, InterruptedException {
        DataFile f = DataFile.openFile(p);
        for (int i = 0; i < f.maxRecords; i++) {
            checkInterrupted();
            f.read(i, c);
        }
        Files.move(p, backupDir.resolve(p.getFileName()));
    }

    public interface BackoffPolicy {
        // TODO implement push notifications from DataFile
        default void backoff(int numRetries) throws InterruptedException {
            Thread.sleep(10L);
        }
    }

    public static void checkInterrupted() throws InterruptedException {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException();
        }
    }
}
