package Utils.Env;

import io.github.cdimascio.dotenv.Dotenv;
import java.util.logging.Logger;

public final class EnvUtil {
    private static final Logger LOGGER = Logger.getLogger(EnvUtil.class.getName());
    private static final Dotenv DOTENV = Dotenv.configure()
            .ignoreIfMissing()
            .load();

    private EnvUtil() { /* no instances */ }

    public static String getRequired(String key) {
        String value = DOTENV.get(key);
        if (value == null || value.isBlank()) {
            String msg = "Environment variable '" + key + "' must be defined in the .env file";
            LOGGER.severe(msg);
            throw new IllegalStateException(msg);
        }
        return value;
    }

    public static String getString(String key, String defaultValue) {
        String value = DOTENV.get(key);
        return (value != null && !value.isBlank()) ? value : defaultValue;
    }

    public static int getInt(String key, int defaultValue) {
        String value = DOTENV.get(key);
        if (value != null) {
            try {
                return Integer.parseInt(value.trim());
            } catch (NumberFormatException e) {
                LOGGER.warning("Invalid integer for key '" + key + "', using default " + defaultValue);
            }
        }
        return defaultValue;
    }

    public static boolean getBoolean(String key, boolean defaultValue) {
        String value = DOTENV.get(key);
        if (value != null) {
            return value.equalsIgnoreCase("true") || value.equalsIgnoreCase("1");
        }
        return defaultValue;
    }
}
