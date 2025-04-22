package Utils.Logging;

import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public final class LoggingUtil {
    private static final Logger ROOT_LOGGER = Logger.getLogger("QualiteAirETL");

    private LoggingUtil() {/* no instantiation */}

    static {
        ROOT_LOGGER.setUseParentHandlers(false);
        ConsoleHandler handler = new ConsoleHandler();
        handler.setLevel(Level.ALL);
        handler.setFormatter(new SimpleFormatter());
        ROOT_LOGGER.addHandler(handler);
        ROOT_LOGGER.setLevel(Level.INFO);
    }

    public static Logger getLogger(Class<?> clazz) {
        return Logger.getLogger("QualiteAirETL." + clazz.getSimpleName());
    }
}
