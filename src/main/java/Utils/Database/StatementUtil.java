package Utils.Database;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;

public final class StatementUtil {

    private StatementUtil() { /* no instances */ }

    public static void setOrNull(PreparedStatement ps, int paramIndex, Object value, int sqlType) throws SQLException {
        switch (value) {
            case null -> ps.setNull(paramIndex, sqlType);
            case String str -> ps.setString(paramIndex, str);
            case Integer i -> ps.setInt(paramIndex, i);
            case Double d -> ps.setDouble(paramIndex, d);
            case Float f -> ps.setFloat(paramIndex, f);
            case Long l -> ps.setLong(paramIndex, l);
            case Boolean b -> ps.setBoolean(paramIndex, b);
            case LocalDate localDate -> ps.setDate(paramIndex, Date.valueOf(localDate));
            case java.util.Date utilDate -> ps.setDate(paramIndex, new Date(utilDate.getTime()));
            default -> throw new IllegalArgumentException("Unsupported parameter type: " + value.getClass());
        }
    }
}
