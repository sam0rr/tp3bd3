DROP SCHEMA IF EXISTS qualite_air CASCADE;
CREATE SCHEMA qualite_air;
ALTER DATABASE qualite_air SET search_path = qualite_air, public;

SET search_path = qualite_air, public;

CREATE TABLE IF NOT EXISTS station (
    station_id INTEGER PRIMARY KEY,
    adresse     VARCHAR(255),
    latitude    DOUBLE PRECISION,
    longitude   DOUBLE PRECISION,
    x_coord     DOUBLE PRECISION,
    y_coord     DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS polluant (
    code_polluant VARCHAR(20) PRIMARY KEY,
    description    VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS mesure (
    station_id    INTEGER,
    date          DATE,
    heure         SMALLINT,
    code_polluant VARCHAR(20),
    valeur        INTEGER,
    PRIMARY KEY (station_id, date, heure),
    FOREIGN KEY (station_id)    REFERENCES station(station_id)    ON DELETE CASCADE,
    FOREIGN KEY (code_polluant) REFERENCES polluant(code_polluant) ON DELETE CASCADE
);
