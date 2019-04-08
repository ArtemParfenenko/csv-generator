package com.arpa.csv.generator;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class Main {
    //example args : D:\TEST_CSV 30 300 1002  //

    private static final String FILE_PREFIX = "csv";
    private static final String FILE_POSTFIX = ".csv";
    private static final String DOMAIN_PREFIX = "domain-";
    private static final String SEPARATOR = ",";
    private static final String END_LINE = "\r\n";
    private static final Random RANDOM = new Random();
    private static final int MAGIC_COUNT = 3;

    private static AtomicLong idAccount = new AtomicLong(0);

    public static void main(String[] args) {
        if (args.length != 4) {
            System.err.println("Incorrect argument number. Should be 3 (path, number of files, number of records in each file) but found " + args.length);
            return;
        }

        String path = args[0];
        long countDomainGroup = Long.parseLong(args[1]);
        long fileNumber = Long.parseLong(args[2]);
        long recordsNumber = Long.parseLong(args[3])/MAGIC_COUNT;

        ExecutorService executorService = Executors.newFixedThreadPool(5);

        for (int i = 1; i <= countDomainGroup; i++) {
            for (long fileIndex = 0; fileIndex < fileNumber; fileIndex++) {
                final long stableFileIndex = fileIndex;
                int currentDomainGroup = i;
                executorService.submit(() -> createCsvFile(path, stableFileIndex, recordsNumber, currentDomainGroup));
            }
        }
        executorService.shutdown();
    }

    private static void createCsvFile(String path, long fileIndex, long recordsNumber, int domainGroup) {
        final String fullPath = path + DOMAIN_PREFIX + domainGroup + "-" + FILE_PREFIX + fileIndex + FILE_POSTFIX;

        try (FileOutputStream fileOutputStream = new FileOutputStream(fullPath)) {
            for (long recordIndex = 0; recordIndex < recordsNumber; recordIndex++) {
                List<String> contents = getContents(MAGIC_COUNT, domainGroup);
                for (String content : contents) {
                    fileOutputStream.write(content.getBytes());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static List<String> getContents(int count, int domainGroup) {
        long idAccount = getIdAccount();
        List<String> contents = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            String content = idAccount + SEPARATOR + domainGroup + SEPARATOR + getRandValue() + END_LINE;
            contents.add(content);
        }
        return contents;
    }

    private static int getRandValue() {
        return RANDOM.nextInt(1000) + 1;
    }

    private static long getIdAccount() {
        long idAccountLong = idAccount.incrementAndGet();
        idAccount = new AtomicLong(idAccountLong);
        return idAccountLong;
    }
}