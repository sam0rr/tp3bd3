package Etl;

import Models.Mesure;
import Models.Polluant;
import Models.PolluantType;
import Models.Station;
import Utils.Logging.LoggingUtil;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDate;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DataExtractor {

    private static final Logger LOGGER = LoggingUtil.getLogger(DataExtractor.class);
    private static final String CSV_FILE_PATH = "data/rsqa-indice-qualite-air-station.csv";

    public record CsvData(List<Station> stations,
                          List<Polluant> pollutants,
                          List<Mesure> measures) {}

    public static CsvData readAll() {
        LOGGER.info("Reading CSV file: " + CSV_FILE_PATH);
        CsvParser parser = createParser();
        List<String[]> rows = parseAllRows(parser);
        LOGGER.info(() -> "Parsed " + rows.size() + " data rows");
        return mapRows(rows);
    }

    private static CsvParser createParser() {
        CsvParserSettings settings = new CsvParserSettings();
        settings.setHeaderExtractionEnabled(true);
        return new CsvParser(settings);
    }

    private static List<String[]> parseAllRows(CsvParser parser) {
        try (FileReader fr = new FileReader(CSV_FILE_PATH)) {
            return parser.parseAll(fr);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE,
                    "Unable to read CSV file: " + CSV_FILE_PATH, e);
            throw new RuntimeException("Unable to read CSV file", e);
        }
    }

    private static CsvData mapRows(List<String[]> rows) {
        Map<Integer, Station> stationMap = new LinkedHashMap<>();
        Map<String, Polluant> pollutantMap = new LinkedHashMap<>();
        List<Mesure> measures = new ArrayList<>();

        for (String[] row : rows) {
            try {
                processRow(row, stationMap, pollutantMap, measures);
            } catch (Exception ex) {
                LOGGER.log(Level.WARNING,
                        "Error mapping CSV row: " + Arrays.toString(row), ex);
            }
        }

        LOGGER.info(() -> String.format(
                "Deduplicated to %d stations and %d pollutants",
                stationMap.size(), pollutantMap.size()
        ));

        return new CsvData(
                new ArrayList<>(stationMap.values()),
                new ArrayList<>(pollutantMap.values()),
                measures
        );
    }

    private static void processRow(String[] row,
                                   Map<Integer, Station> stationMap,
                                   Map<String, Polluant> pollutantMap,
                                   List<Mesure> measures) {
        // 1 - Station
        int stationId = Integer.parseInt(row[0]);
        stationMap.computeIfAbsent(stationId,
                id -> parseStation(id, row)
        );

        // 2 - Pollutant
        String code = row[6];
        pollutantMap.computeIfAbsent(code,
                DataExtractor::parsePolluant
        );

        // 3 - Measure
        measures.add(parseMesure(row, stationId, code));
    }

    private static Station parseStation(int id, String[] row) {
        return Station.builder()
                .stationId(id)
                .adresse(row[1])
                .latitude(Double.parseDouble(row[2]))
                .longitude(Double.parseDouble(row[3]))
                .xCoord(Double.parseDouble(row[4]))
                .yCoord(Double.parseDouble(row[5]))
                .build();
    }

    private static Polluant parsePolluant(String code) {
        PolluantType type = PolluantType.fromCode(code);
        return Polluant.builder()
                .codePolluant(type.name())
                .description(type.getDescription())
                .build();
    }

    private static Mesure parseMesure(String[] row, int stationId, String code) {
        return Mesure.builder()
                .stationId(stationId)
                .date(LocalDate.parse(row[8]))
                .heure(Integer.parseInt(row[9]))
                .codePolluant(code)
                .valeur(Integer.parseInt(row[7]))
                .build();
    }
}