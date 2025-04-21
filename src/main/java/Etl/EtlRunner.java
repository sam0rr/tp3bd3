package Etl;

import Utils.LoggingUtil;

import java.util.logging.Level;
import java.util.logging.Logger;

public final class EtlRunner {
    private static final Logger LOGGER = LoggingUtil.getLogger(EtlRunner.class);

    public static void start() {
        printBanner("Starting QualiteAir ETL");

        CsvReader.CsvData data = extractAllData();
        LOGGER.info(() -> String.format("Extracted %d stations, %d pollutants, %d measures",
                data.stations().size(), data.pollutants().size(), data.measures().size()));

        loadAllData(data);

        printBanner("Finishing QualiteAir ETL");
    }

    private static void printBanner(String message) {
        LOGGER.info("=== " + message + " ===");
    }

    private static CsvReader.CsvData extractAllData() {
        return CsvReader.readAll();
    }

    private static void loadAllData(CsvReader.CsvData data) {
        try {
            DataLoader.loadAll(data);
            LOGGER.info("Loading completed successfully");
        } catch (Exception e) {
            handleLoadError(e);
        }
    }

    private static void handleLoadError(Exception e) {
        LOGGER.log(Level.SEVERE, "Error while loading data into database: " + e.getMessage(), e);
        System.exit(1);
    }
}
