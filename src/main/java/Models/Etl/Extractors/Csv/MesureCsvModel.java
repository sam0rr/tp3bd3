package Models.Etl.Extractors.Csv;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class MesureCsvModel {

    @JsonProperty("stationId")
    private int stationId;

    @JsonProperty("adresse")
    private String adresse;

    @JsonProperty("latitude")
    private double latitude;

    @JsonProperty("longitude")
    private double longitude;

    @JsonProperty("X")
    private double xCoord;

    @JsonProperty("Y")
    private double yCoord;

    @JsonProperty("polluant")
    private String codePolluant;

    @JsonProperty("valeur")
    private int valeur;

    @JsonProperty("date")
    private String date;

    @JsonProperty("heure")
    private int heure;
}