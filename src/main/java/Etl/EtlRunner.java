package Etl;

import Utils.Logging.LoggingUtil;

import java.time.Duration;
import java.time.Instant;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class EtlRunner {
    private static final Logger LOGGER = LoggingUtil.getLogger(EtlRunner.class);

    private EtlRunner() { /* no instances */ }

    public static void start() {
        runWithTiming(
                EtlRunner::performEtl
        );
    }

    private static void performEtl() {
        var data = extractAllData();
        logExtractionCounts(data);
        loadAllData(data);
    }

    private static DataExtractor.CsvData extractAllData() {
        return DataExtractor.readAll();
    }

    private static void logExtractionCounts(DataExtractor.CsvData data) {
        LOGGER.info(() -> String.format(
                "Extracted %d stations, %d pollutants, %d measures",
                data.stations().size(),
                data.pollutants().size(),
                data.measures().size()
        ));
    }

    private static void loadAllData(DataExtractor.CsvData data) {
        try {
            DataLoader.loadAll(data);
            LOGGER.info("Loading completed successfully");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE,
                    "Error while loading data into database: " + e.getMessage(), e);
            System.exit(1);
        }
    }

    private static void runWithTiming(Runnable action) {
        printBanner("Starting QualiteAir ETL");
        Instant start = Instant.now();

        action.run();

        Instant end = Instant.now();
        logElapsed(start, end);
        printBanner("Finishing QualiteAir ETL");
    }

    private static void logElapsed(Instant start, Instant end) {
        Duration elapsed = Duration.between(start, end);
        long ms     = elapsed.toMillis();
        long mins   = elapsed.toMinutes();
        long secs   = elapsed.getSeconds() % 60;
        LOGGER.info(() -> String.format(
                "=== ETL completed in %d ms (%d min %d sec) ===",
                ms, mins, secs
        ));
    }

    private static void printBanner(String message) {
        LOGGER.info("=== " + message + " ===");
    }
}
