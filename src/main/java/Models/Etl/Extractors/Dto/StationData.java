package Models.Etl.Extractors.Dto;

import Models.Station;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class StationData {

    @Builder.Default
    private List<Station> stations = new ArrayList<>();

    @Builder.Default
    private Map<Integer, String> stationMunicipalites = new HashMap<>();

    @Builder.Default
    private Map<Integer, String> stationTypeMilieux = new HashMap<>();

    @Builder.Default
    private Map<String, Integer> municipalityIdMap = new HashMap<>();

    @Builder.Default
    private Map<String, Integer> typeIdMap = new HashMap<>();
}