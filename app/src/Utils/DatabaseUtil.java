package Utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public final class DatabaseUtil {
    private static final String ENV_DB_URL       = "DB_URL";
    private static final String ENV_DB_USER      = "DB_USER";
    private static final String ENV_DB_PASSWORD  = "DB_PASSWORD";

    private static final String url;
    private static final String user;
    private static final String password;

    private DatabaseUtil() {/* no instantiation */}

    static {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            throw new ExceptionInInitializerError("PostgreSQL JDBC Driver manquant : " + e.getMessage());
        }

        url      = System.getenv(ENV_DB_URL);
        user     = System.getenv(ENV_DB_USER);
        password = System.getenv(ENV_DB_PASSWORD);

        if (url == null || url.isBlank()) {
            throw new IllegalStateException("La variable d'environnement DB_URL n'est pas définie");
        }
        if (user == null || user.isBlank()) {
            throw new IllegalStateException("La variable d'environnement DB_USER n'est pas définie");
        }
        if (password == null || password.isBlank()) {
            throw new IllegalStateException("La variable d'environnement DB_PASSWORD n'est pas définie");
        }
    }

    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }
}
