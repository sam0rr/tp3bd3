package Models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Station {
    private int stationId;
    private String adresse;
    private double latitude;
    private double longitude;
    private double xCoord;
    private double yCoord;
}
