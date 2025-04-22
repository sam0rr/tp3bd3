package Models.Etl.Utils;

import lombok.Getter;

@Getter
public enum PolluantType {
    CO("Monoxyde de carbone"),
    NO2("Dioxyde d'azote"),
    O3("Ozone troposphérique"),
    PM("Particules fines (PM2.5)"),
    SO2("Dioxyde de soufre"),
    UNKNOWN("Inconnu"),
    PM25("Particules fines (PM2.5)"),
    PM10("Particules grossières (PM10)");

    private final String description;

    PolluantType(String description) {
        this.description = description;
    }

    public static PolluantType fromCode(String code) {
        try {
            return valueOf(code.toUpperCase());
        } catch (IllegalArgumentException | NullPointerException e) {
            return UNKNOWN;
        }
    }
}
