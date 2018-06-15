package com.ucex.rad.file.simple.example;

import com.ucex.rad.file.DataFile;
import com.ucex.rad.file.simple.SimplePattern;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Example {
    static AtomicInteger i = new AtomicInteger();

    static Path dataDir = Paths.get("exampleOut/in");
    static Path backupDir = Paths.get("exampleOut/backup");

    public static void main(String[] args) throws Throwable {

        Files.createDirectories(dataDir);
        Files.createDirectories(backupDir);

        // we simulate starting an application, and then stopping it (eg for updating new version),
        // and then relaunching it again.

        // during each launch, a new data file is created where we write and consume from continuously.
        // also, the data dir is scanned for any files that were left over from previous runs, and if there were, we would
        // consume all records and then move them to backup

        while (true) {
            ExecutorService executorService = launchApplication();
            Thread.sleep(5000L);
            stopApplication(executorService);
        }

    }

    private static void stopApplication(ExecutorService executorService) {
        System.out.println("Stopping application");
        executorService.shutdownNow();
        try {
            executorService.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static ExecutorService launchApplication() throws Throwable {
        System.out.println("Launching application");
        ExecutorService executorService = Executors.newCachedThreadPool();

        // handle pending records
        Files.list(dataDir).forEach(
                f-> executorService.submit(()-> {
                    try {
                        SimplePattern.consumeAllAndThenBackup(f, backupDir, b->System.out.println("READ INT FROM OLDDATAFILE " + b.getInt()));
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                })
        );

        // create our data file
        Path p = dataDir.resolve(UUID.randomUUID().toString());
        DataFile f = DataFile.newFile(100, 50000, p);

        // continuous production
        executorService.submit(()-> {
            try {
                SimplePattern.produceContinuously(f, b -> b.putInt(i.getAndIncrement()), new SimplePattern.BackoffPolicy() {
                });
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        // continuous consumption
        executorService.submit(()-> {
            try {
                SimplePattern.consumeContinuously(f, b -> System.out.println("READ INT FROM CURRENT DATAFILE " + b.getInt()), new SimplePattern.BackoffPolicy() {
                });
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        return executorService;
    }


}
