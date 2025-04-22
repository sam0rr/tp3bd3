package Etl;

import Etl.Extractors.MesureExtractor;
import Etl.Extractors.StationExtractor;
import Models.Etl.Extractors.Dto.CsvData;
import Models.Etl.Extractors.Dto.MesureData;
import Models.Etl.Extractors.Dto.StationData;
import Models.Municipalite;
import Models.Station;
import Models.TypeMilieu;
import Utils.Logging.LoggingUtil;
import lombok.Getter;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static Utils.Env.EnvUtil.*;

public class DataExtractor {

    @Getter
    private static final Logger LOGGER = LoggingUtil.getLogger(DataExtractor.class);

    private static final String DEFAULT_MUNICIPALITY = getString("DEFAULT_MUNICIPALITY", "Montréal");
    private static final String DEFAULT_ENVIRONMENT_TYPE = getString("DEFAULT_ENVIRONMENT_TYPE", "Urbain");
    private static final int DEFAULT_ENTITY_ID = getInt("DEFAULT_ENTITY_ID", 1);

    public static CsvData readAll() {
        LOGGER.info("Starting data extraction process");

        StationExtractor stationExtractor = new StationExtractor();
        StationData stationData = stationExtractor.extract();
        LOGGER.info("Station extraction complete: " + stationData.getStations().size() + " stations");

        MesureExtractor mesureExtractor = new MesureExtractor();
        MesureData mesureData = mesureExtractor.extract();
        LOGGER.info("Measurement extraction complete: " + mesureData.getMeasures().size() + " measurements");

        CsvData combinedData = combineData(stationData, mesureData);

        logExtractedDataSummary(combinedData);

        return combinedData;
    }

    private static CsvData combineData(
            StationData stationData,
            MesureData mesureData) {

        Map<String, Integer> municipalityIdMap = ensureDefaultsInMunicipalityMap(
                new HashMap<>(stationData.getMunicipalityIdMap())
        );

        Map<String, Integer> typeIdMap = ensureDefaultsInTypeMap(
                new HashMap<>(stationData.getTypeIdMap())
        );

        List<Municipalite> municipalites = createMunicipaliteEntities(municipalityIdMap);
        List<TypeMilieu> typeMilieux = createTypeMilieuEntities(typeIdMap);

        List<Station> mergedStations = mergeStations(
                stationData.getStations(),
                mesureData.getStations(),
                stationData.getStationMunicipalites(),
                stationData.getStationTypeMilieux(),
                municipalityIdMap,
                typeIdMap
        );

        return CsvData.builder()
                .stations(mergedStations)
                .pollutants(mesureData.getPollutants())
                .measures(mesureData.getMeasures())
                .municipalites(municipalites)
                .typeMilieux(typeMilieux)
                .build();
    }

    private static Map<String, Integer> ensureDefaultsInMunicipalityMap(Map<String, Integer> municipalityMap) {
        if (!municipalityMap.containsKey(DEFAULT_MUNICIPALITY)) {
            municipalityMap.put(DEFAULT_MUNICIPALITY, DEFAULT_ENTITY_ID);
            LOGGER.info("Added default municipality '" + DEFAULT_MUNICIPALITY + "' with ID " + DEFAULT_ENTITY_ID);
        }
        return municipalityMap;
    }

    private static Map<String, Integer> ensureDefaultsInTypeMap(Map<String, Integer> typeMap) {
        if (!typeMap.containsKey(DEFAULT_ENVIRONMENT_TYPE)) {
            typeMap.put(DEFAULT_ENVIRONMENT_TYPE, DEFAULT_ENTITY_ID);
            LOGGER.info("Added default environment type '" + DEFAULT_ENVIRONMENT_TYPE + "' with ID " + DEFAULT_ENTITY_ID);
        }
        return typeMap;
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

        logMissingStations(stationMap, mesureStationMap);

        Map<Integer, Station> mergedStations = new LinkedHashMap<>(stationMap);

        for (Station mesureStation : mesureStationMap.values()) {
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

        return new ArrayList<>(mergedStations.values());
    }

    private static void logMissingStations(
            Map<Integer, Station> stationMap,
            Map<Integer, Station> mesureStationMap) {

        Set<Integer> missingStationIds = new HashSet<>();

        for (Integer stationId : mesureStationMap.keySet()) {
            if (!stationMap.containsKey(stationId)) {
                missingStationIds.add(stationId);
            }
        }

        if (!missingStationIds.isEmpty()) {
            LOGGER.log(Level.WARNING, String.format(
                    "Found %d stations in measurements that are not in station data: %s",
                    missingStationIds.size(), missingStationIds));
        }
    }

    private static Map<Integer, Station> convertStationsToMap(List<Station> stations) {
        return stations.stream()
                .collect(Collectors.toMap(
                        Station::getStationId,
                        station -> station,
                        (existing, replacement) -> existing,
                        LinkedHashMap::new
                ));
    }

    private static void updateExistingStation(
            Map<Integer, Station> mergedStations,
            Station mesureStation) {

        int stationId = mesureStation.getStationId();
        Station existing = mergedStations.get(stationId);

        Station updated = updateStationWithCoordinates(existing, mesureStation);
        mergedStations.put(stationId, updated);
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

        if (!municipalityIdMap.containsKey(municipality)) {
            LOGGER.warning("Municipality '" + municipality + "' not found in ID map, using default");
            municipality = DEFAULT_MUNICIPALITY;
        }

        if (!typeIdMap.containsKey(type)) {
            LOGGER.warning("Environment type '" + type + "' not found in ID map, using default");
            type = DEFAULT_ENVIRONMENT_TYPE;
        }

        int municipalityId = municipalityIdMap.get(municipality);
        int typeId = typeIdMap.get(type);

        Station newStation = createStationWithMetadata(
                mesureStation,
                municipalityId,
                typeId
        );

        mergedStations.put(stationId, newStation);

        LOGGER.info(String.format(
                "Created new station with ID: %d, municipality: '%s' (ID: %d), type: '%s' (ID: %d)",
                stationId, municipality, municipalityId, type, typeId));
    }

    private static String getStationMunicipality(
            int stationId,
            Map<Integer, String> stationMunicipalites) {

        String municipality = stationMunicipalites.getOrDefault(stationId, DEFAULT_MUNICIPALITY);

        if (!stationMunicipalites.containsKey(stationId)) {
            LOGGER.fine("No municipality found for station ID: " + stationId + ", using default: " + DEFAULT_MUNICIPALITY);
        }

        return municipality;
    }

    private static String getStationEnvironmentType(
            int stationId,
            Map<Integer, String> stationTypeMilieux) {

        String type = stationTypeMilieux.getOrDefault(stationId, DEFAULT_ENVIRONMENT_TYPE);

        if (!stationTypeMilieux.containsKey(stationId)) {
            LOGGER.fine("No environment type found for station ID: " + stationId + ", using default: " + DEFAULT_ENVIRONMENT_TYPE);
        }

        return type;
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
                .dateOuverture(mesureStation.getDateOuverture())
                .dateFermeture(mesureStation.getDateFermeture())
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
                .dateOuverture(station.getDateOuverture())
                .dateFermeture(station.getDateFermeture())
                .municipaliteId(station.getMunicipaliteId())
                .typeMilieuId(station.getTypeMilieuId())
                .build();
    }

    private static void logExtractedDataSummary(CsvData data) {
        LOGGER.info(String.format(
                "Data extraction complete: %d stations, %d pollutants, %d measurements, %d municipalities, %d environment types",
                data.getStations().size(),
                data.getPollutants().size(),
                data.getMeasures().size(),
                data.getMunicipalites().size(),
                data.getTypeMilieux().size()
        ));
    }
}