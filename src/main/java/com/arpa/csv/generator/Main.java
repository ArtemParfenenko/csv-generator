package com.arpa.csv.generator;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
    private static final String FILE_PREFIX = "csv";
    private static final String FILE_POSTFIX = ".csv";
    private static final String SEPARATOR = ",";
    private static final String END_LINE = "\r\n";
    private static final Random RANDOM = new Random();

    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.println("Incorrect argument number. Should be 3 (path, number of files, number of records in each file) but found " + args.length);
            return;
        }

        String path = args[0];
        long fileNumber = Long.parseLong(args[1]);
        long recordsNumber = Long.parseLong(args[2]);

        ExecutorService executorService = Executors.newFixedThreadPool(5);

        for (long fileIndex = 0; fileIndex < fileNumber; fileIndex++) {
            final long stableFileIndex = fileIndex;
            executorService.submit(() -> createCsvFile(path, stableFileIndex, recordsNumber));
        }

        executorService.shutdown();
    }

    private static void createCsvFile(String path, long fileIndex, long recordsNumber) {
        final String fullPath = path + FILE_PREFIX + fileIndex + FILE_POSTFIX;

        try (FileOutputStream fileOutputStream = new FileOutputStream(fullPath)) {
            for (long recordIndex = 0; recordIndex < recordsNumber; recordIndex++) {
                fileOutputStream.write(getContent().getBytes());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static String getContent() {
        return getRand100() + SEPARATOR + getRand100() + END_LINE;
    }

    private static int getRand100() {
        return RANDOM.nextInt(100) + 1;
    }
}
