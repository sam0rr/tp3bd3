package Etl.Extractors;

import Models.Etl.Extractors.Csv.StationCsvModel;
import Models.Etl.Extractors.Dto.StationData;
import Models.Station;
import Utils.Logging.LoggingUtil;
import lombok.Getter;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static Utils.Parsing.ParsingUtil.parseDateOrNull;

public class StationExtractor extends BaseExtractor<StationCsvModel> {

    @Getter
    private static final Logger LOGGER = LoggingUtil.getLogger(StationExtractor.class);

    @Getter
    private static final String CSV_FILE_PATH = "data/rsqaq_station_1975-2024.csv";

    @Override
    protected String getFilePath() {
        return CSV_FILE_PATH;
    }

    @Override
    protected Class<StationCsvModel> getTargetClass() {
        return StationCsvModel.class;
    }

    public StationData extract() {
        List<StationCsvModel> csvModels = extractData();
        return processStationData(csvModels);
    }

    private StationData processStationData(List<StationCsvModel> csvModels) {
        Map<Integer, Station> stationMap = new LinkedHashMap<>();
        Map<String, Integer> municipalityMap = new LinkedHashMap<>();
        Map<String, Integer> typeMap = new LinkedHashMap<>();
        Map<Integer, String> stationMunicipalites = new HashMap<>();
        Map<Integer, String> stationTypeMilieux = new HashMap<>();

        AtomicInteger nextMunicipalityId = new AtomicInteger(1);
        AtomicInteger nextTypeId = new AtomicInteger(1);

        for (StationCsvModel model : csvModels) {
            try {
                String municipalityName = model.getMunicipalite();
                String typeName = model.getTypeMilieu();
                int stationId = model.getStationId();

                municipalityMap.computeIfAbsent(municipalityName, key -> nextMunicipalityId.getAndIncrement());
                typeMap.computeIfAbsent(typeName, key -> nextTypeId.getAndIncrement());

                stationMunicipalites.put(stationId, municipalityName);
                stationTypeMilieux.put(stationId, typeName);

                Station station = buildStationFromModel(model, municipalityMap, typeMap);
                stationMap.put(stationId, station);
            } catch (Exception ex) {
                logProcessingError(model, ex);
            }
        }

        logProcessedDataSummary(stationMap.size(), municipalityMap.size(), typeMap.size());

        return StationData.builder()
                .stations(new ArrayList<>(stationMap.values()))
                .stationMunicipalites(stationMunicipalites)
                .stationTypeMilieux(stationTypeMilieux)
                .municipalityIdMap(municipalityMap)
                .typeIdMap(typeMap)
                .build();
    }

    private Station buildStationFromModel(StationCsvModel model, Map<String, Integer> municipalityMap, Map<String, Integer> typeMap) {
        return Station.builder()
                .stationId(model.getStationId())
                .adresse(model.getAdresse())
                .latitude(model.getLatitude())
                .longitude(model.getLongitude())
                .dateOuverture(parseDateOrNull(model.getDateOuverture()))
                .dateFermeture(parseDateOrNull(model.getDateFermeture()))
                .municipaliteId(municipalityMap.get(model.getMunicipalite()))
                .typeMilieuId(typeMap.get(model.getTypeMilieu()))
                .build();
    }

    private void logProcessedDataSummary(int stationCount, int municipalityCount, int typeCount) {
        LOGGER.info(() -> String.format(
                "Processed %d unique stations, %d municipalities, and %d environment types",
                stationCount, municipalityCount, typeCount
        ));
    }
}
