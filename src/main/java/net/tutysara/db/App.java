package net.tutysara.db;

import picocli.CommandLine;
import picocli.CommandLine.Option;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Init file
 */
public class App implements Callable<Integer> {

    @Option(names = {"-f", "--datafile"}, description = "Data File Name")
    private String dataFileName = "test.db";

    @Option(names = {"-d"}, description = "Delete data file")
    private boolean deleteFile = false;

    @Option(names = {"-n", "--numthreads"}, description = "Number of threads")
    private Integer numThreads = 10;

    @Option(names = {"-o", "--opsperthread"}, description = "Ops per thread")
    private Integer operationsPerThread = 100;

    @Option(names = {"-l", "--logfreq"}, description = "Log Frequency")
    private Integer logFreq = 1000;

    public static void main(String[] args) throws Exception {
        int exitCode = new CommandLine(new App()).execute(args);
        System.exit(exitCode);
    }

    public void loadTest(Store store) throws Exception {

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch latch = new CountDownLatch(numThreads);

        long startTime = System.currentTimeMillis();

        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                System.out.printf("Starting on thread %s\n", Thread.currentThread().threadId());
                for (int i = 0; i < operationsPerThread; i++) {
                    String key = "key-" + threadId + "-" + i;
                    String value = "val-" + i;
                    if (i % logFreq == 0) {
                        System.out.printf("Progress on thread %s\n", Thread.currentThread().threadId());
                    }
                    store.set(key, value);
                    store.get(key);  // simulate read
                }
                System.out.printf("Completed on thread %s\n", Thread.currentThread().threadId());
                latch.countDown();
            });
        }

        latch.await();
        executor.shutdown();

        long duration = System.currentTimeMillis() - startTime;
        System.out.printf("Completed %,d operations in %d ms%n", numThreads * operationsPerThread * 2, duration);
    }

    @Override
    public Integer call() throws Exception {
        Store store = null;
        try {
            store = new DiskStore(dataFileName);
            if (deleteFile) {
                Files.deleteIfExists(Path.of(dataFileName));
                loadTest(store);
            } else {
                store.set("ferrari", "charles leclrec");
                store.set("redbull", "max verstappen");
                store.set("mercedes", "lewis hamilton");
                store.set("mclaren", "lando norris");

                var rbDriver = store.get("redbull");
                System.out.println(String.format("%s drives for redbull racing!\n", rbDriver));

                System.out.println("Current keys:");
                for (var key : store.listKeys()) {
                    System.out.println(String.format("key: %s\n", key));
                }
                System.out.printf("Total number of keys= %d\n", store.listKeys().size());
            }
        } finally {
            if (store != null) {
                store.close();
            }
        }
        return null;
    }
}
