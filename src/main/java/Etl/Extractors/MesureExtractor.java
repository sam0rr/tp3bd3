package Etl.Extractors;

import Models.Etl.Extractors.Csv.MesureCsvModel;
import Models.Etl.Extractors.Dto.MesureData;
import Models.Etl.Utils.PolluantType;
import Models.Mesure;
import Models.Polluant;
import Models.Station;

import Utils.Logging.LoggingUtil;
import lombok.Getter;

import java.time.LocalDate;
import java.util.*;
import java.util.logging.Logger;

public class MesureExtractor extends BaseExtractor<MesureCsvModel> {

    @Getter
    private static final Logger LOGGER = LoggingUtil.getLogger(MesureExtractor.class);

    @Getter
    private static final String CSV_FILE_PATH = "data/rsqa-indice-qualite-air-station.csv";

    @Override
    protected String getFilePath() {
        return CSV_FILE_PATH;
    }

    @Override
    protected Class<MesureCsvModel> getTargetClass() {
        return MesureCsvModel.class;
    }

    public MesureData extract() {
        List<MesureCsvModel> csvModels = extractData();
        return processMesureData(csvModels);
    }

    private MesureData processMesureData(List<MesureCsvModel> csvModels) {
        Map<Integer, Station> stationMap = new LinkedHashMap<>();
        Map<String, Polluant> pollutantMap = new LinkedHashMap<>();
        List<Mesure> measures = new ArrayList<>();

        for (MesureCsvModel model : csvModels) {
            try {
                int stationId = model.getStationId();
                String pollutantCode = model.getCodePolluant();

                if (!stationMap.containsKey(stationId)) {
                    Station station = Station.builder()
                            .stationId(stationId)
                            .adresse(model.getAdresse())
                            .latitude(model.getLatitude())
                            .longitude(model.getLongitude())
                            .xCoord(model.getXCoord())
                            .yCoord(model.getYCoord())
                            .municipaliteId(1)
                            .typeMilieuId(1)
                            .build();

                    stationMap.put(stationId, station);
                }

                if (!pollutantMap.containsKey(pollutantCode)) {
                    PolluantType type = PolluantType.fromCode(pollutantCode);
                    Polluant polluant = Polluant.builder()
                            .codePolluant(type.name())
                            .description(type.getDescription())
                            .build();

                    pollutantMap.put(pollutantCode, polluant);
                }

                Mesure mesure = Mesure.builder()
                        .stationId(stationId)
                        .date(LocalDate.parse(model.getDate()))
                        .heure(model.getHeure())
                        .codePolluant(pollutantCode)
                        .valeur(model.getValeur())
                        .build();

                measures.add(mesure);

            } catch (Exception ex) {
                logProcessingError(model, ex);
            }
        }

        logProcessedDataSummary(stationMap.size(), pollutantMap.size(), measures.size());

        return MesureData.builder()
                .stations(new ArrayList<>(stationMap.values()))
                .pollutants(new ArrayList<>(pollutantMap.values()))
                .measures(measures)
                .build();
    }

    private void logProcessedDataSummary(int stationCount, int pollutantCount, int measureCount) {
        LOGGER.info(() -> String.format(
                "Processed %d stations, %d pollutants, and %d measurements",
                stationCount, pollutantCount, measureCount
        ));
    }
}