package systems.intino.datamarts.led.util;

import io.intino.alexandria.logger.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class LedSessionSorter {
	public static void sort(File[] sessions, Function<File, File> fileMapper, File tempFolder) {
		try {
			AtomicInteger processed = new AtomicInteger(0);
			AtomicInteger processedPerc = new AtomicInteger(0);
			ExecutorService pool = Executors.newFixedThreadPool(Math.max(4, Runtime.getRuntime().availableProcessors() / 2));
			if (sessions.length == 0) return;
			Logger.info("Sorting led sessions...");
			Arrays.stream(sessions).<Runnable>map(f -> () -> {
				sealSession(f, fileMapper, tempFolder);
				notifyProcess(processed, processedPerc, sessions.length);
			}).forEach(pool::execute);
			pool.shutdown();
			pool.awaitTermination(1, TimeUnit.DAYS);
			Logger.info("Leds sorted!");
			deleteDirectory(tempFolder);
		} catch (InterruptedException e) {
			Logger.error(e);
		}
	}

	private static void notifyProcess(AtomicInteger processed, AtomicInteger currentPerc, int total) {
		int processedPerc = Math.round(((float) processed.incrementAndGet() / total) * 100);
		if (processedPerc / 10 > processed.get() / 10) Logger.info("Sorted " + processedPerc + "% of leds");
		currentPerc.set(processedPerc);
	}

	private static void sealSession(File session, Function<File, File> fileMapper, File tempFolder) {
		try {
			File sorted = sort(session, tempFolder);
			File destination = fileMapper.apply(session);
			destination.getParentFile().mkdirs();
			Files.move(sorted.toPath(), destination.toPath(), StandardCopyOption.REPLACE_EXISTING);
			session.renameTo(new File(session.getAbsolutePath() + ".treated"));
		} catch (IOException e) {
			Logger.error(e);
		}
	}

	private static File sort(File led, File tempFolder) {
		File file = new File(led.getParentFile(), led.getName() + ".sort");
		LedUtils.sort(new File(tempFolder, "Chunks_" + led.getName() + "_" + Thread.currentThread().getName()), led, file, 1_000_000);
		return file;
	}

	private static void deleteDirectory(File directoryToBeDeleted) {
		File[] allContents = directoryToBeDeleted.listFiles();
		if (allContents != null) for (File file : allContents) deleteDirectory(file);
		directoryToBeDeleted.delete();
	}

}
