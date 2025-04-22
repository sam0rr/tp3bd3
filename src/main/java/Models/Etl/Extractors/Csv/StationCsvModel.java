package Models.Etl.Extractors.Csv;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class StationCsvModel {

    @JsonProperty("ID_STATION")
    private int stationId;

    @JsonProperty("NOM_STATION")
    private String nomStation;

    @JsonProperty("ADRESSE")
    private String adresse;

    @JsonProperty("MUNICIPALITE")
    private String municipalite;

    @JsonProperty("TYPE_MILIEU")
    private String typeMilieu;

    @JsonProperty("DATE_OUVERTURE")
    private String dateOuverture;

    @JsonProperty("DATE_FERMETURE")
    private String dateFermeture;

    @JsonProperty("LATITUDE")
    private double latitude;

    @JsonProperty("LONGITUDE")
    private double longitude;
}