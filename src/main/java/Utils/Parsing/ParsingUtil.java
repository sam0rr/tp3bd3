package Utils.Parsing;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.logging.Logger;

public final class ParsingUtil {

    private static final Logger LOGGER = Logger.getLogger(ParsingUtil.class.getName());
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private ParsingUtil() { /* no instances */ }

    public static LocalDate parseDateOrNull(String dateStr) {
        if (dateStr == null || dateStr.isBlank()) {
            return null;
        }
        try {
            return LocalDate.parse(dateStr, DATE_FORMATTER);
        } catch (DateTimeParseException e) {
            LOGGER.warning("Failed to parse date: " + dateStr + ", returning null");
            return null;
        }
    }
}
