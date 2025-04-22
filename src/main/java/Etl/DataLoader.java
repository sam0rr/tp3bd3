package Etl;

import Models.Etl.Extractors.Dto.CsvData;
import Models.Mesure;
import Models.Municipalite;
import Models.Polluant;
import Models.Station;
import Models.TypeMilieu;
import Utils.Database.DatabaseUtil;
import Utils.Logging.LoggingUtil;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import static Utils.Database.StatementUtil.setOrNull;

public final class DataLoader {
    private static final Logger LOGGER = LoggingUtil.getLogger(DataLoader.class);

    private DataLoader() { /* no instantiation */ }

    public static void loadAll(CsvData data) {
        LOGGER.info("Starting database load process");

        DatabaseUtil.runTransaction(connection -> {
            insertTypeMilieux(connection, data.getTypeMilieux());
            insertMunicipalites(connection, data.getMunicipalites());
            insertStations(connection, data.getStations());
            insertPolluants(connection, data.getPollutants());
            insertMesures(connection, data.getMeasures());

            LOGGER.info("Database load completed successfully");
        });
    }

    private static void insertTypeMilieux(Connection conn, List<TypeMilieu> typeMilieux) throws SQLException {
        LOGGER.info(() -> "Inserting " + typeMilieux.size() + " environment types");

        var sql = """
            INSERT INTO type_milieu
              (type_milieu_id, nom)
            VALUES (?, ?)
            ON CONFLICT (type_milieu_id) DO UPDATE
            SET nom = EXCLUDED.nom
            """;

        executeBatch(conn, sql, typeMilieux.size(), ps -> {
            for (TypeMilieu t : typeMilieux) {
                setOrNull(ps, 1, t.getTypeMilieuId(), Types.INTEGER);
                setOrNull(ps, 2, t.getNom(), Types.VARCHAR);
                ps.addBatch();
            }
        });
    }

    private static void insertMunicipalites(Connection conn, List<Municipalite> municipalites) throws SQLException {
        LOGGER.info(() -> "Inserting " + municipalites.size() + " municipalities");

        var sql = """
            INSERT INTO municipalite
              (municipalite_id, nom)
            VALUES (?, ?)
            ON CONFLICT (municipalite_id) DO UPDATE
            SET nom = EXCLUDED.nom
            """;

        executeBatch(conn, sql, municipalites.size(), ps -> {
            for (Municipalite m : municipalites) {
                setOrNull(ps, 1, m.getMunicipaliteId(), Types.INTEGER);
                setOrNull(ps, 2, m.getNom(), Types.VARCHAR);
                ps.addBatch();
            }
        });
    }

    private static void insertStations(Connection conn, List<Station> stations) throws SQLException {
        LOGGER.info(() -> "Inserting " + stations.size() + " stations");

        var sql = """
            INSERT INTO station
              (station_id, adresse, latitude, longitude, x_coord, y_coord, date_ouverture, date_fermeture, municipalite_id, type_milieu_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (station_id) DO UPDATE
            SET adresse = EXCLUDED.adresse,
                latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude,
                x_coord = EXCLUDED.x_coord,
                y_coord = EXCLUDED.y_coord,
                date_ouverture = EXCLUDED.date_ouverture,
                date_fermeture = EXCLUDED.date_fermeture,
                municipalite_id = EXCLUDED.municipalite_id,
                type_milieu_id = EXCLUDED.type_milieu_id
            """;

        executeBatch(conn, sql, stations.size(), ps -> {
            for (Station s : stations) {
                setOrNull(ps, 1, s.getStationId(), Types.INTEGER);
                setOrNull(ps, 2, s.getAdresse(), Types.VARCHAR);
                setOrNull(ps, 3, s.getLatitude(), Types.DOUBLE);
                setOrNull(ps, 4, s.getLongitude(), Types.DOUBLE);
                setOrNull(ps, 5, s.getXCoord(), Types.DOUBLE);
                setOrNull(ps, 6, s.getYCoord(), Types.DOUBLE);
                setOrNull(ps, 7, s.getDateOuverture(), Types.DATE);
                setOrNull(ps, 8, s.getDateFermeture(), Types.DATE);
                setOrNull(ps, 9, s.getMunicipaliteId(), Types.INTEGER);
                setOrNull(ps, 10, s.getTypeMilieuId(), Types.INTEGER);
                ps.addBatch();
            }
        });
    }

    private static void insertPolluants(Connection conn, List<Polluant> pollutants) throws SQLException {
        LOGGER.info(() -> "Inserting " + pollutants.size() + " pollutants");

        var sql = """
            INSERT INTO polluant
              (code_polluant, description)
            VALUES (?, ?)
            ON CONFLICT (code_polluant) DO UPDATE
            SET description = EXCLUDED.description
            """;

        executeBatch(conn, sql, pollutants.size(), ps -> {
            for (Polluant p : pollutants) {
                setOrNull(ps, 1, p.getCodePolluant(), Types.VARCHAR);
                setOrNull(ps, 2, p.getDescription(), Types.VARCHAR);
                ps.addBatch();
            }
        });
    }

    private static void insertMesures(Connection conn, List<Mesure> measures) throws SQLException {
        LOGGER.info(() -> "Inserting " + measures.size() + " measures");

        var sql = """
            INSERT INTO mesure
              (station_id, date, heure, code_polluant, valeur)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (station_id, date, heure) DO UPDATE
            SET code_polluant = EXCLUDED.code_polluant,
                valeur = EXCLUDED.valeur
            """;

        executeBatch(conn, sql, measures.size(), ps -> {
            for (Mesure m : measures) {
                setOrNull(ps, 1, m.getStationId(), Types.INTEGER);
                setOrNull(ps, 2, m.getDate(), Types.DATE);
                setOrNull(ps, 3, m.getHeure(), Types.SMALLINT);
                setOrNull(ps, 4, m.getCodePolluant(), Types.VARCHAR);
                setOrNull(ps, 5, m.getValeur(), Types.INTEGER);
                ps.addBatch();
            }
        });
    }

    @FunctionalInterface
    private interface BatchOperation {
        void execute(PreparedStatement ps) throws SQLException;
    }

    private static void executeBatch(Connection conn, String sql, int batchSize, BatchOperation operation)
            throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            operation.execute(ps);
            int[] results = ps.executeBatch();
            logBatchResults(batchSize, results);
        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, "Error executing batch operation", e);
            throw e;
        }
    }

    private static void logBatchResults(int expectedSize, int[] results) {
        int successCount = (int) Arrays.stream(results).filter(result -> result >= 0 || result == Statement.SUCCESS_NO_INFO).count();

        LOGGER.info(() -> String.format("Batch execution completed: %d/%d successful operations",
                successCount, expectedSize));
    }
}
