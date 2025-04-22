DROP SCHEMA IF EXISTS qualite_air CASCADE;
CREATE SCHEMA qualite_air;
ALTER DATABASE qualite_air SET search_path = qualite_air, public;

SET search_path = qualite_air, public;

CREATE TABLE IF NOT EXISTS type_milieu (
    type_milieu_id SERIAL PRIMARY KEY,
    nom      VARCHAR(50) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS municipalite (
    municipalite_id SERIAL PRIMARY KEY,
    nom             VARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS station (
    station_id       INTEGER PRIMARY KEY,
    adresse          VARCHAR(255),
    latitude         DOUBLE PRECISION,
    longitude        DOUBLE PRECISION,
    x_coord          DOUBLE PRECISION,
    y_coord          DOUBLE PRECISION,
    municipalite_id  INTEGER NOT NULL,
    type_milieu_id   INTEGER NOT NULL,
    FOREIGN KEY (municipalite_id)
       REFERENCES municipalite(municipalite_id)
       ON DELETE SET NULL,
    FOREIGN KEY (type_milieu_id)
       REFERENCES type_milieu(type_milieu_id)
       ON DELETE SET NULL
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
    FOREIGN KEY (station_id)
      REFERENCES station(station_id)
      ON DELETE CASCADE,
    FOREIGN KEY (code_polluant)
      REFERENCES polluant(code_polluant)
      ON DELETE CASCADE
);
