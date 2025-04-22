package Etl.Extractors;

import Utils.Logging.LoggingUtil;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import lombok.Getter;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class BaseExtractor<T> {

    @Getter
    private static final Logger LOGGER = LoggingUtil.getLogger(BaseExtractor.class);

    protected abstract String getFilePath();

    protected abstract Class<T> getTargetClass();

    protected List<T> extractData() {
        String filePath = getFilePath();
        logStartExtraction(filePath);

        try {
            List<T> result = readAndParseCsvFile(filePath);
            logExtractionCompletion(filePath, result.size());
            return result;
        } catch (IOException e) {
            handleExtractionError(filePath, e);
            throw new RuntimeException("Failed to extract data from CSV: " + filePath, e);
        }
    }

    private void logStartExtraction(String filePath) {
        LOGGER.info("Reading CSV file: " + filePath);
    }

    private List<T> readAndParseCsvFile(String filePath) throws IOException {
        File csvFile = new File(filePath);
        CsvMapper mapper = new CsvMapper();
        CsvSchema schema = CsvSchema.emptySchema().withHeader();

        MappingIterator<T> iterator = createMappingIterator(mapper, schema, csvFile);
        return collectResults(iterator);
    }

    private MappingIterator<T> createMappingIterator(CsvMapper mapper, CsvSchema schema, File csvFile) throws IOException {
        return mapper
                .readerFor(getTargetClass())
                .with(schema)
                .readValues(csvFile);
    }

    private List<T> collectResults(MappingIterator<T> iterator) {
        List<T> result = new ArrayList<>();
        while (iterator.hasNext()) {
            result.add(iterator.next());
        }
        return result;
    }

    private void logExtractionCompletion(String filePath, int recordCount) {
        LOGGER.info(() -> "Extracted " + recordCount + " records from " + filePath);
    }

    private void handleExtractionError(String filePath, Exception e) {
        LOGGER.log(Level.SEVERE, "Error reading CSV file: " + filePath, e);
    }

    protected void logProcessingError(T record, Exception ex) {
        LOGGER.log(Level.WARNING,
                "Error processing record: " + record, ex);
    }
}