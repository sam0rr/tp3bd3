package Etl;

import Models.Mesure;
import Models.Polluant;
import Models.Station;
import Utils.DatabaseUtil;
import Utils.LoggingUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.logging.Logger;

public final class DataLoader {
    private static final Logger LOGGER = LoggingUtil.getLogger(DataLoader.class);

    private DataLoader() { /* no instantiation */ }

    public static void loadAll(CsvReader.CsvData data) {
        DatabaseUtil.runTransaction(connection -> {
            LOGGER.info(() -> "Inserting " + data.stations().size() + " stations");
            insertStations(connection, data.stations());

            LOGGER.info(() -> "Inserting " + data.pollutants().size() + " pollutants");
            insertPolluants(connection, data.pollutants());

            LOGGER.info(() -> "Inserting " + data.measures().size() + " measures");
            insertMesures(connection, data.measures());
        });
    }

    private static void insertStations(Connection conn, List<Station> stations) throws SQLException {
        var sql = """
            INSERT INTO station
              (station_id, adresse, latitude, longitude, x_coord, y_coord)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (station_id) DO NOTHING
            """;

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (Station s : stations) {
                ps.setInt(1, s.getStationId());
                ps.setString(2, s.getAdresse());
                ps.setDouble(3, s.getLatitude());
                ps.setDouble(4, s.getLongitude());
                ps.setDouble(5, s.getXCoord());
                ps.setDouble(6, s.getYCoord());
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    private static void insertPolluants(Connection conn, List<Polluant> pollutants) throws SQLException {
        var sql = """
            INSERT INTO polluant
              (code_polluant, description)
            VALUES (?, ?)
            ON CONFLICT (code_polluant) DO NOTHING
            """;

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (Polluant p : pollutants) {
                ps.setString(1, p.getCodePolluant());
                ps.setString(2, p.getDescription());
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    private static void insertMesures(Connection conn, List<Mesure> measures) throws SQLException {
        var sql = """
            INSERT INTO mesure
              (station_id, date, heure, code_polluant, valeur)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (station_id, date, heure) DO NOTHING
            """;

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (Mesure m : measures) {
                ps.setInt(1, m.getStationId());
                ps.setDate(2, java.sql.Date.valueOf(m.getDate()));
                ps.setInt(3, m.getHeure());
                ps.setString(4, m.getCodePolluant());
                ps.setInt(5, m.getValeur());
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }
}
