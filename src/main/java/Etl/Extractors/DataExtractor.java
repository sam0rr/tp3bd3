package Etl.Extractors;

import Models.Etl.Extractors.Dto.CsvData;
import Models.Etl.Extractors.Dto.MesureData;
import Models.Etl.Extractors.Dto.StationData;
import Models.Municipalite;
import Models.Station;
import Models.TypeMilieu;
import Utils.Logging.LoggingUtil;
import lombok.Getter;

import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class DataExtractor {

    @Getter
    private static final Logger LOGGER = LoggingUtil.getLogger(DataExtractor.class);
    private static final String DEFAULT_MUNICIPALITY = "Montréal";
    private static final String DEFAULT_ENVIRONMENT_TYPE = "Urbain";

    public static CsvData readAll() {
        LOGGER.info("Starting data extraction process");

        StationExtractor stationExtractor = new StationExtractor();
        StationData stationData = stationExtractor.extract();

        MesureExtractor mesureExtractor = new MesureExtractor();
        MesureData mesureData = mesureExtractor.extract();

        CsvData combinedData = combineData(stationData, mesureData);

        logExtractedDataSummary(combinedData);

        return combinedData;
    }

    private static CsvData combineData(
            StationData stationData,
            MesureData mesureData) {

        List<Municipalite> municipalites = createMunicipaliteEntities(stationData.getMunicipalityIdMap());
        List<TypeMilieu> typeMilieux = createTypeMilieuEntities(stationData.getTypeIdMap());

        List<Station> mergedStations = mergeStations(
                stationData.getStations(),
                mesureData.getStations(),
                stationData.getStationMunicipalites(),
                stationData.getStationTypeMilieux(),
                stationData.getMunicipalityIdMap(),
                stationData.getTypeIdMap()
        );

        return CsvData.builder()
                .stations(mergedStations)
                .pollutants(mesureData.getPollutants())
                .measures(mesureData.getMeasures())
                .municipalites(municipalites)
                .typeMilieux(typeMilieux)
                .build();
    }

    private static List<Municipalite> createMunicipaliteEntities(Map<String, Integer> municipalityMap) {
        return municipalityMap.entrySet().stream()
                .map(entry -> new Municipalite(entry.getValue(), entry.getKey()))
                .collect(Collectors.toList());
    }

    private static List<TypeMilieu> createTypeMilieuEntities(Map<String, Integer> typeMap) {
        return typeMap.entrySet().stream()
                .map(entry -> new TypeMilieu(entry.getValue(), entry.getKey()))
                .collect(Collectors.toList());
    }

    private static List<Station> mergeStations(
            List<Station> stationStations,
            List<Station> mesureStations,
            Map<Integer, String> stationMunicipalites,
            Map<Integer, String> stationTypeMilieux,
            Map<String, Integer> municipalityIdMap,
            Map<String, Integer> typeIdMap) {

        Map<Integer, Station> stationMap = convertStationsToMap(stationStations);
        Map<Integer, Station> mesureStationMap = convertStationsToMap(mesureStations);

        ensureDefaultValuesExist(municipalityIdMap, typeIdMap);

        Map<Integer, Station> mergedStations = new LinkedHashMap<>(stationMap);

        mesureStationMap.values().forEach(mesureStation ->
                processMesureStation(
                        mesureStation,
                        mergedStations,
                        stationMunicipalites,
                        stationTypeMilieux,
                        municipalityIdMap,
                        typeIdMap
                )
        );

        return new ArrayList<>(mergedStations.values());
    }

    private static Map<Integer, Station> convertStationsToMap(List<Station> stations) {
        return stations.stream()
                .collect(Collectors.toMap(Station::getStationId, station -> station));
    }

    private static void ensureDefaultValuesExist(
            Map<String, Integer> municipalityIdMap,
            Map<String, Integer> typeIdMap) {

        int defaultMunicipalityId = getDefaultEntityId(municipalityIdMap, DEFAULT_MUNICIPALITY);
        int defaultTypeId = getDefaultEntityId(typeIdMap, DEFAULT_ENVIRONMENT_TYPE);

        if (!municipalityIdMap.containsKey(DEFAULT_MUNICIPALITY)) {
            municipalityIdMap.put(DEFAULT_MUNICIPALITY, defaultMunicipalityId);
        }
        if (!typeIdMap.containsKey(DEFAULT_ENVIRONMENT_TYPE)) {
            typeIdMap.put(DEFAULT_ENVIRONMENT_TYPE, defaultTypeId);
        }
    }

    private static int getDefaultEntityId(Map<String, Integer> entityMap, String entityName) {
        return entityMap.getOrDefault(entityName, 1);
    }

    private static void processMesureStation(
            Station mesureStation,
            Map<Integer, Station> mergedStations,
            Map<Integer, String> stationMunicipalites,
            Map<Integer, String> stationTypeMilieux,
            Map<String, Integer> municipalityIdMap,
            Map<String, Integer> typeIdMap) {

        int stationId = mesureStation.getStationId();

        if (mergedStations.containsKey(stationId)) {
            updateExistingStation(mergedStations, mesureStation);
        } else {
            addNewStation(
                    mergedStations,
                    mesureStation,
                    stationMunicipalites,
                    stationTypeMilieux,
                    municipalityIdMap,
                    typeIdMap
            );
        }
    }

    private static void updateExistingStation(
            Map<Integer, Station> mergedStations,
            Station mesureStation) {

        int stationId = mesureStation.getStationId();
        mergedStations.computeIfPresent(stationId, (k, existing) -> updateStationWithCoordinates(existing, mesureStation));
    }

    private static void addNewStation(
            Map<Integer, Station> mergedStations,
            Station mesureStation,
            Map<Integer, String> stationMunicipalites,
            Map<Integer, String> stationTypeMilieux,
            Map<String, Integer> municipalityIdMap,
            Map<String, Integer> typeIdMap) {

        int stationId = mesureStation.getStationId();

        String municipality = getStationMunicipality(stationId, stationMunicipalites);
        String type = getStationEnvironmentType(stationId, stationTypeMilieux);

        int municipalityId = municipalityIdMap.getOrDefault(
                municipality,
                municipalityIdMap.get(DEFAULT_MUNICIPALITY)
        );
        int typeId = typeIdMap.getOrDefault(
                type,
                typeIdMap.get(DEFAULT_ENVIRONMENT_TYPE)
        );

        Station newStation = createStationWithMetadata(
                mesureStation,
                municipalityId,
                typeId
        );
        mergedStations.put(stationId, newStation);
    }

    private static String getStationMunicipality(
            int stationId,
            Map<Integer, String> stationMunicipalites) {

        return stationMunicipalites.getOrDefault(stationId, DEFAULT_MUNICIPALITY);
    }

    private static String getStationEnvironmentType(
            int stationId,
            Map<Integer, String> stationTypeMilieux) {

        return stationTypeMilieux.getOrDefault(stationId, DEFAULT_ENVIRONMENT_TYPE);
    }

    private static Station createStationWithMetadata(
            Station mesureStation,
            int municipalityId,
            int typeId) {

        return Station.builder()
                .stationId(mesureStation.getStationId())
                .adresse(mesureStation.getAdresse())
                .latitude(mesureStation.getLatitude())
                .longitude(mesureStation.getLongitude())
                .xCoord(mesureStation.getXCoord())
                .yCoord(mesureStation.getYCoord())
                .municipaliteId(municipalityId)
                .typeMilieuId(typeId)
                .build();
    }

    private static Station updateStationWithCoordinates(Station station, Station coordinateSource) {
        return Station.builder()
                .stationId(station.getStationId())
                .adresse(station.getAdresse())
                .latitude(coordinateSource.getLatitude())
                .longitude(coordinateSource.getLongitude())
                .xCoord(coordinateSource.getXCoord())
                .yCoord(coordinateSource.getYCoord())
                .municipaliteId(station.getMunicipaliteId())
                .typeMilieuId(station.getTypeMilieuId())
                .build();
    }

    private static void logExtractedDataSummary(CsvData data) {
        LOGGER.info(() -> String.format(
                "Data extraction complete: %d stations, %d pollutants, %d measurements, %d municipalities, %d environment types",
                data.getStations().size(),
                data.getPollutants().size(),
                data.getMeasures().size(),
                data.getMunicipalites().size(),
                data.getTypeMilieux().size()
        ));
    }
}