package Etl;

import Models.Mesure;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public class CsvReader {
    private static final String CSV_FILE_PATH = "data/rsqa-indice-qualite-air-station.csv";

    public static List<Mesure> readMesures() {
        CsvParser parser = createCsvParser();
        List<String[]> rows = parseCsvFile(parser);
        return mapRowsToMesures(rows);
    }

    private static CsvParser createCsvParser() {
        CsvParserSettings settings = new CsvParserSettings();
        settings.setHeaderExtractionEnabled(true);
        return new CsvParser(settings);
    }

    private static List<String[]> parseCsvFile(CsvParser parser) {
        try (FileReader reader = new FileReader(CsvReader.CSV_FILE_PATH)) {
            return parser.parseAll(reader);
        } catch (IOException e) {
            throw new RuntimeException("Error reading csv file : " + e.getMessage(), e);
        }
    }

    private static List<Mesure> mapRowsToMesures(List<String[]> rows) {
        List<Mesure> mesures = new ArrayList<>();
        for (String[] row : rows) {
            try {
                Mesure mesure = Mesure.builder()
                        .stationId(Integer.parseInt(row[0]))
                        .date(LocalDate.parse(row[1]))
                        .heure(Integer.parseInt(row[2]))
                        .codePolluant(row[3])
                        .valeur(Integer.parseInt(row[4]))
                        .build();
                mesures.add(mesure);
            } catch (Exception e) {
                System.err.println("Error mapping csv line : " + e.getMessage());
            }
        }
        return mesures;
    }
}
