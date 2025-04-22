package Etl.Extractors;


import Models.Etl.Extractors.Dto.StationData;
import Models.Station;
import Models.Etl.Extractors.Csv.StationCsvModel;
import Utils.Logging.LoggingUtil;
import lombok.Getter;

import java.util.*;
import java.util.logging.Logger;

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

        int nextMunicipalityId = 1;
        int nextTypeId = 1;

        for (StationCsvModel model : csvModels) {
            try {
                int stationId = model.getStationId();
                String municipalityName = model.getMunicipalite();
                String typeName = model.getTypeMilieu();

                if (!municipalityMap.containsKey(municipalityName)) {
                    municipalityMap.put(municipalityName, nextMunicipalityId++);
                }

                if (!typeMap.containsKey(typeName)) {
                    typeMap.put(typeName, nextTypeId++);
                }

                stationMunicipalites.put(stationId, municipalityName);
                stationTypeMilieux.put(stationId, typeName);

                Station station = Station.builder()
                        .stationId(stationId)
                        .adresse(model.getAdresse())
                        .latitude(model.getLatitude())
                        .longitude(model.getLongitude())
                        .xCoord(0.0)
                        .yCoord(0.0)
                        .municipaliteId(municipalityMap.get(municipalityName))
                        .typeMilieuId(typeMap.get(typeName))
                        .build();

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

    private void logProcessedDataSummary(int stationCount, int municipalityCount, int typeCount) {
        LOGGER.info(() -> String.format(
                "Processed %d unique stations, %d municipalities, and %d environment types",
                stationCount, municipalityCount, typeCount
        ));
    }
}