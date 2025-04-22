package Utils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class DatabaseUtil {
    private static final Logger LOGGER = LoggingUtil.getLogger(DatabaseUtil.class);

    private DatabaseUtil() { /* no instances */ }

    @FunctionalInterface
    public interface TransactionalOperation {
        void execute(Connection connection) throws SQLException;
    }

    public static void runTransaction(TransactionalOperation operation) {
        Connection connection = null;
        try {
            connection = DataSourceWrapper.getConnection();
            connection.setAutoCommit(false);

            operation.execute(connection);

            commitTransaction(connection);
            LOGGER.fine("Transaction committed");
        } catch (Throwable t) {
            rollbackTransaction(connection, t);
            throw new RuntimeException("Transaction failed: " + t.getMessage(), t);
        } finally {
            releaseConnection(connection);
        }
    }

    private static void commitTransaction(Connection connection) throws SQLException {
        if (connection != null) {
            connection.commit();
        }
    }

    private static void rollbackTransaction(Connection connection, Throwable error) {
        if (connection != null) {
            try {
                connection.rollback();
                LOGGER.info("Transaction rolled back");
            } catch (SQLException e) {
                LOGGER.log(Level.SEVERE, "Failed to roll back after error: " + error.getMessage(), e);
            }
        }
    }

    private static void releaseConnection(Connection connection) {
        if (connection != null) {
            try {
                DataSourceWrapper.releaseConnection(connection);
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Error releasing connection", e);
            }
        }
    }
}
