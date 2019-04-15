package com.arpa.csv.generator;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
    //    /home/netcrk/minio/data/csv/ 9000 1000 3000000
    private static final String FILE_PREFIX = "csv";
    private static final String FILE_POSTFIX = ".csv";
    private static final String SEPARATOR = ",";
    private static final String END_LINE = "\r\n";
    private static final Random RANDOM = new Random();

    public static void main(String[] args) {
        if (args.length != 4) {
            System.out.println("Incorrect argument number. Should be 3 (path, number of files, number of records in each file) but found " + args.length);
            return;
        }

        String path = args[0];
        long fileNumber = Long.parseLong(args[1]);
        long recordsNumber = Long.parseLong(args[2]);
        int ids = Integer.parseInt(args[3]);

        ExecutorService executorService = Executors.newFixedThreadPool(5);

        for (long fileIndex = 0; fileIndex < fileNumber; fileIndex++) {
            final long stableFileIndex = fileIndex;
            executorService.submit(() -> createCsvFile(path, stableFileIndex, recordsNumber, ids));
        }
        executorService.shutdown();
    }

    private static void createCsvFile(String path, long fileIndex, long recordsNumber, int ids) {
        final String fullPath = path + FILE_PREFIX + fileIndex + FILE_POSTFIX;

        try (FileOutputStream fileOutputStream = new FileOutputStream(fullPath)) {
            for (long recordIndex = 0; recordIndex < recordsNumber; recordIndex++) {
                fileOutputStream.write(getContent(ids).getBytes());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String getContent(int ids) {
        return getRand(ids) + SEPARATOR + 1 + SEPARATOR + 1 + END_LINE;
    }

    private static int getRand(int ids) {
        return RANDOM.nextInt(ids) + 1;
    }
}