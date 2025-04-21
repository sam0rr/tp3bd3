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
import java.util.logging.Level;
import java.util.logging.Logger;

public final class DataLoader {
    private static final Logger LOGGER = LoggingUtil.getLogger(DataLoader.class);

    private DataLoader() { /* no instantiation */ }

    public static void loadAll(CsvReader.CsvData data) {
        LOGGER.info("Beginning database load transaction");
        try (Connection conn = DatabaseUtil.getConnection()) {
            conn.setAutoCommit(false);

            insertStations(conn, data.stations());
            insertPolluants(conn, data.pollutants());
            insertMesures(conn, data.measures());

            conn.commit();
            LOGGER.info("Database load transaction committed successfully");
        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, "Error inserting data into DB, rolling back", e);
            throw new RuntimeException("Error inserting data into DB: " + e.getMessage(), e);
        }
    }

    private static void insertStations(Connection conn, List<Station> stations) throws SQLException {
        LOGGER.info(() -> "Inserting " + stations.size() + " stations");
        String sql = """
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
            LOGGER.info("Stations insert batch executed");
        }
    }

    private static void insertPolluants(Connection conn, List<Polluant> pollutants) throws SQLException {
        LOGGER.info(() -> "Inserting " + pollutants.size() + " pollutants");
        String sql = """
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
            LOGGER.info("Pollutants insert batch executed");
        }
    }

    private static void insertMesures(Connection conn, List<Mesure> measures) throws SQLException {
        LOGGER.info(() -> "Inserting " + measures.size() + " measures");
        String sql = """
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
            LOGGER.info("Measures insert batch executed");
        }
    }
}
