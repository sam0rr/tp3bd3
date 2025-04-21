package Utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class DatabaseUtil {
    private static final Logger LOGGER = LoggingUtil.getLogger(DatabaseUtil.class);

    private static final String url;
    private static final String user;
    private static final String password;

    private DatabaseUtil() { /* prevent instantiation */ }

    static {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            LOGGER.log(Level.SEVERE, "PostgreSQL JDBC Driver not found", e);
            throw new ExceptionInInitializerError(e);
        }

        Map<String, String> env = System.getenv();
        url      = getEnv(env, "DB_URL");
        user     = getEnv(env, "DB_USER");
        password = getEnv(env, "DB_PASSWORD");
    }

    private static String getEnv(Map<String, String> env, String key) {
        String val = env.get(key);
        if (val == null || val.isBlank()) {
            LOGGER.severe("Required environment variable " + key + " is not set");
            throw new IllegalStateException(key + " must be defined");
        }
        return val;
    }

    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }
}
