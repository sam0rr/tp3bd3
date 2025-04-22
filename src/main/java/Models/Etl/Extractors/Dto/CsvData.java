package Models.Etl.Extractors.Dto;

import Models.Mesure;
import Models.Municipalite;
import Models.Polluant;
import Models.Station;
import Models.TypeMilieu;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CsvData {

    @Builder.Default
    private List<Station> stations = new ArrayList<>();

    @Builder.Default
    private List<Polluant> pollutants = new ArrayList<>();

    @Builder.Default
    private List<Mesure> measures = new ArrayList<>();

    @Builder.Default
    private List<Municipalite> municipalites = new ArrayList<>();

    @Builder.Default
    private List<TypeMilieu> typeMilieux = new ArrayList<>();
}