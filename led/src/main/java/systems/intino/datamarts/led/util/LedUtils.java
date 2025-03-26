package systems.intino.datamarts.led.util;

import io.intino.alexandria.logger.Logger;
import systems.intino.datamarts.led.util.sorting.LedExternalMergeSort;

import java.io.File;
import java.io.IOException;

public final class LedUtils {

    public static void sort(File srcFile, File destFile) {
        createIfNotExists(destFile);
        new LedExternalMergeSort(srcFile, destFile).sort();
    }

    public static void sort(File tempDir, File srcFile, File destFile, int numTransactionsInMemory) {
        createIfNotExists(destFile);
        new LedExternalMergeSort(srcFile, destFile).chunksDirectory(tempDir).numTransactionsInMemory(numTransactionsInMemory)
                .sort();
    }

    public static void sort(File srcFile, File destFile, int numTransactionsInMemory) {
        createIfNotExists(destFile);
        new LedExternalMergeSort(srcFile, destFile).numTransactionsInMemory(numTransactionsInMemory).sort();
    }

    private static void createIfNotExists(File destFile) {
        if(!destFile.exists()) {
            try {
                destFile.createNewFile();
            } catch (IOException e) {
                Logger.error(e);
                throw new RuntimeException(e);
            }
        }
    }

    private LedUtils() {}
}
