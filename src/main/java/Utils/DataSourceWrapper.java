package Utils;

import io.github.cdimascio.dotenv.Dotenv;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.logging.Logger;

public final class DataSourceWrapper {
    private static final Logger LOGGER = LoggingUtil.getLogger(DataSourceWrapper.class);

    private static final Dotenv dotenv = Dotenv.configure()
            .ignoreIfMissing()
            .load();

    private static final Deque<Connection> available = new ArrayDeque<>();
    private static final Deque<Connection> used      = new ArrayDeque<>();

    private static final String URL        = requireEnv("DB_URL");
    private static final String USER       = requireEnv("DB_USER");
    private static final String PASSWORD   = requireEnv("DB_PASSWORD");
    private static final String SCHEMA     = requireEnv("POSTGRES_DB");
    private static final int    POOL_SIZE  = Integer.parseInt(requireEnv("DB_POOL_SIZE"));

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
            Connection c = DriverManager.getConnection(URL, USER, PASSWORD);
            if (!SCHEMA.isBlank()) {
                c.createStatement()
                        .execute("SET search_path TO " + SCHEMA);
            }
            return c;
        } catch (SQLException e) {
            throw new RuntimeException("Error creating a new connection", e);
        }
    }

    private static String requireEnv(String key) {
        String v = dotenv.get(key);
        if (v == null || v.isBlank()) {
            String msg = "Environment variable '" + key + "' must be defined in .env file";
            LOGGER.severe(msg);
            throw new IllegalStateException(msg);
        }
        return v;
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
