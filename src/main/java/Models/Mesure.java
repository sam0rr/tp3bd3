package Models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDate;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Mesure {
    private int stationId;
    private LocalDate date;
    private int heure;
    private String codePolluant;
    private int valeur;
}
