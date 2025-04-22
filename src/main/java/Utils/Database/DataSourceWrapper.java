package Utils.Database;

import Utils.Logging.LoggingUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.logging.Logger;

import static Utils.Env.EnvUtil.*;

public final class DataSourceWrapper {
    private static final Logger LOGGER = LoggingUtil.getLogger(DataSourceWrapper.class);

    private static final Deque<Connection> available = new ArrayDeque<>();
    private static final Deque<Connection> used      = new ArrayDeque<>();

    private static final String URL       = getRequired("DB_URL");
    private static final String USER      = getRequired("POSTGRES_USER");
    private static final String PASSWORD  = getRequired("POSTGRES_PASSWORD");
    private static final String SCHEMA    = getRequired("POSTGRES_DB");
    private static final int    POOL_SIZE = getInt("DB_POOL_SIZE", 10);

    static {
        initializePool();
    }

    private DataSourceWrapper() { /* no instances */ }

    private static void initializePool() {
        for (int i = 0; i < POOL_SIZE; i++) {
            available.add(createConnection());
        }
        LOGGER.info("Initialized pool with " + POOL_SIZE + " connections (schema=" + SCHEMA + ")");
    }

    private static Connection createConnection() {
        try {
            return DriverManager.getConnection(URL, USER, PASSWORD);
        } catch (SQLException e) {
            throw new RuntimeException("Error creating a new connection", e);
        }
    }

    public static synchronized Connection getConnection() throws SQLException {
        if (available.isEmpty()) {
            throw new SQLException("Connection pool exhausted (size=" + POOL_SIZE + ")");
        }
        Connection c = available.removeFirst();
        used.add(c);
        return c;
    }

    public static synchronized void releaseConnection(Connection c) {
        if (c == null) return;
        used.remove(c);
        available.addLast(c);
    }

    public static synchronized void shutdown() {
        used.forEach(DataSourceWrapper::closeQuietly);
        available.forEach(DataSourceWrapper::closeQuietly);
        used.clear();
        available.clear();
    }

    private static void closeQuietly(Connection c) {
        try {
            if (!c.isClosed()) c.close();
        } catch (SQLException e) {
            LOGGER.log(java.util.logging.Level.WARNING, "Error closing connection", e);
        }
    }
}
