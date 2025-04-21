package Etl;

import Models.Mesure;
import Models.Polluant;
import Models.Station;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDate;
import java.util.*;

public class CsvReader {

    private static final String CSV_FILE_PATH = "data/rsqa-indice-qualite-air-station.csv";

    public record CsvData(List<Station> stations, List<Polluant> pollutants, List<Mesure> measures) {}

    public static CsvData readAll() {
        CsvParser parser = createParser();
        List<String[]> rows = parseAllRows(parser);
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
            throw new RuntimeException("Unable to read CSV file: " + e.getMessage(), e);
        }
    }

    private static CsvData mapRows(List<String[]> rows) {
        Map<Integer, Station> stationMap    = new LinkedHashMap<>();
        Map<String,  Polluant> pollutantMap = new LinkedHashMap<>();
        List<Mesure>           measures      = new ArrayList<>();

        for (String[] row : rows) {
            try {
                processRow(row, stationMap, pollutantMap, measures);
            } catch (Exception ex) {
                System.err.println("Error mapping CSV row " +
                        Arrays.toString(row) + " → " + ex.getMessage());
            }
        }

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
        // 1- Station
        int stationId = Integer.parseInt(row[0]);
        stationMap.computeIfAbsent(stationId, id -> parseStation(id, row));

        // 2- Polluant
        String code = row[3];
        pollutantMap.computeIfAbsent(code, CsvReader::parsePolluant);

        // 3- Mesure
        measures.add(parseMesure(row, stationId, code));
    }

    private static Station parseStation(int id, String[] row) {
        return Station.builder()
                .stationId(id)
                .adresse(row[5])
                .latitude(Double.parseDouble(row[6]))
                .longitude(Double.parseDouble(row[7]))
                .xCoord(0)
                .yCoord(0)
                .build();
    }

    private static Polluant parsePolluant(String code) {
        return Polluant.builder()
                .codePolluant(code)
                .description(getPolluantDescription(code))
                .build();
    }

    private static Mesure parseMesure(String[] row, int stationId, String code) {
        return Mesure.builder()
                .stationId(stationId)
                .date(LocalDate.parse(row[1]))
                .heure(Integer.parseInt(row[2]))
                .codePolluant(code)
                .valeur(Integer.parseInt(row[4]))
                .build();
    }

    private static String getPolluantDescription(String code) {
        return switch (code.toUpperCase()) {
            case "CO"  -> "Monoxyde de carbone";
            case "NO2" -> "Dioxyde d'azote";
            case "O3"  -> "Ozone troposphérique";
            case "PM"  -> "Particules fines (PM2.5)";
            case "SO2" -> "Dioxyde de soufre";
            default    -> "Inconnu";
        };
    }
}
