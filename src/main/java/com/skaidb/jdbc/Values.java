package com.skaidb.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

/**
 * Conversions between JDBC's view of values and the driver's (Long, Double,
 * BigDecimal, String, byte[], UUID, Instant, List, Map).
 *
 * <p>skaidb timestamps are absolute instants. The legacy {@code java.sql}
 * Date / Time classes use the Calendar's zone, else the JVM default (the JDBC
 * convention); the {@code java.time} local types use UTC.
 */
final class Values {
    private Values() {}

    // ---- binding -----------------------------------------------------------------

    /** A JDBC parameter value as a value the driver binds. */
    static Object toBind(Object x) throws SQLException {
        if (x == null || x instanceof Boolean || x instanceof Long || x instanceof Double
                || x instanceof String || x instanceof byte[] || x instanceof UUID || x instanceof Instant
                || x instanceof BigDecimal) return x;
        if (x instanceof Integer || x instanceof Short || x instanceof Byte) return ((Number) x).longValue();
        if (x instanceof Float) return (double) (Float) x;
        if (x instanceof BigInteger) return new BigDecimal((BigInteger) x);
        if (x instanceof CharSequence || x instanceof Character) return x.toString();
        if (x instanceof Timestamp) return ((Timestamp) x).toInstant();
        if (x instanceof java.sql.Date) return startOfDay(((java.sql.Date) x).toLocalDate(), null);
        if (x instanceof Time) throw new SQLDataException("skaidb has no time-of-day type; bind a String", "22023");
        if (x instanceof java.util.Date) return ((java.util.Date) x).toInstant();
        if (x instanceof OffsetDateTime) return ((OffsetDateTime) x).toInstant();
        if (x instanceof ZonedDateTime) return ((ZonedDateTime) x).toInstant();
        if (x instanceof LocalDateTime) return ((LocalDateTime) x).toInstant(ZoneOffset.UTC);
        if (x instanceof LocalDate) return ((LocalDate) x).atStartOfDay(ZoneOffset.UTC).toInstant();
        if (x instanceof Calendar) return ((Calendar) x).toInstant();
        if (x instanceof Array) return toBind(((Array) x).getArray());
        if (x instanceof Collection) {
            List<Object> out = new ArrayList<>();
            for (Object o : (Collection<?>) x) out.add(toBind(o));
            return out;
        }
        if (x.getClass().isArray()) {
            int n = java.lang.reflect.Array.getLength(x);
            List<Object> out = new ArrayList<>(n);
            for (int i = 0; i < n; i++) out.add(toBind(java.lang.reflect.Array.get(x, i)));
            return out;
        }
        if (x instanceof Map) {
            Map<String, Object> out = new LinkedHashMap<>();
            for (Map.Entry<?, ?> e : ((Map<?, ?>) x).entrySet()) {
                if (!(e.getKey() instanceof CharSequence)) throw new SQLDataException("document keys must be strings", "22023");
                out.put(e.getKey().toString(), toBind(e.getValue()));
            }
            return out;
        }
        throw new SQLDataException("cannot bind a " + x.getClass().getName(), "22023");
    }

    /** setObject(i, x, targetSqlType): convert x to the requested JDBC type first. */
    static Object toBind(Object x, int sqlType) throws SQLException {
        if (x == null) return null;
        switch (sqlType) {
            case Types.CHAR: case Types.VARCHAR: case Types.LONGVARCHAR:
            case Types.NCHAR: case Types.NVARCHAR: case Types.LONGNVARCHAR: case Types.CLOB:
                return x instanceof byte[] ? new String((byte[]) x, StandardCharsets.UTF_8) : asString(x);
            case Types.TINYINT: case Types.SMALLINT: case Types.INTEGER: case Types.BIGINT:
                return asLong(x, Long.MIN_VALUE, Long.MAX_VALUE, "BIGINT");
            case Types.REAL: case Types.FLOAT: case Types.DOUBLE:
                return asDouble(x);
            case Types.DECIMAL: case Types.NUMERIC:
                return asBigDecimal(x);
            case Types.BIT: case Types.BOOLEAN:
                return asBoolean(x);
            case Types.BINARY: case Types.VARBINARY: case Types.LONGVARBINARY: case Types.BLOB:
                return asBytes(x);
            case Types.TIMESTAMP: case Types.TIMESTAMP_WITH_TIMEZONE: case Types.DATE:
                return asInstant(x);
            default:
                return toBind(x);
        }
    }

    static Instant startOfDay(LocalDate d, Calendar cal) {
        return d.atStartOfDay(zone(cal)).toInstant();
    }

    static ZoneId zone(Calendar cal) {
        return cal == null ? ZoneId.systemDefault() : cal.getTimeZone().toZoneId();
    }

    static String readAll(Reader r, long limit) throws SQLException {
        StringBuilder b = new StringBuilder();
        char[] buf = new char[8192];
        try {
            int n;
            while ((n = r.read(buf)) > 0) {
                b.append(buf, 0, n);
                if (limit >= 0 && b.length() >= limit) { b.setLength((int) limit); break; }
            }
        } catch (IOException e) {
            throw new SQLException("reading the parameter stream failed: " + e.getMessage(), "HY000", e);
        }
        return b.toString();
    }

    static byte[] readAll(InputStream in, long limit) throws SQLException {
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        byte[] buf = new byte[8192];
        try {
            int n;
            long total = 0;
            while ((n = in.read(buf)) > 0) {
                if (limit >= 0 && total + n > limit) n = (int) (limit - total);
                out.write(buf, 0, n);
                total += n;
                if (limit >= 0 && total >= limit) break;
            }
        } catch (IOException e) {
            throw new SQLException("reading the parameter stream failed: " + e.getMessage(), "HY000", e);
        }
        return out.toByteArray();
    }

    // ---- reading -----------------------------------------------------------------

    /** What ResultSet.getObject(int) returns: JDBC's standard classes where one exists. */
    static Object jdbcObject(Object v) {
        if (v instanceof Instant) return Timestamp.from((Instant) v);
        if (v instanceof List) return new SkaidbArray((List<?>) v);
        return v;
    }

    static String asString(Object v) {
        if (v == null) return null;
        if (v instanceof String) return (String) v;
        if (v instanceof BigDecimal) return ((BigDecimal) v).toPlainString();
        if (v instanceof byte[] || v instanceof List || v instanceof Map) return json(v);
        return v.toString();
    }

    static long asLong(Object v, long min, long max, String target) throws SQLException {
        if (v == null) return 0;
        long n;
        if (v instanceof Long || v instanceof Integer || v instanceof Short || v instanceof Byte) {
            n = ((Number) v).longValue();
        } else if (v instanceof Double || v instanceof Float) {
            double d = ((Number) v).doubleValue();
            if (Double.isNaN(d) || d < -9.223372036854775808E18 || d >= 9.223372036854775808E18) throw range(v, target);
            n = (long) d;
        } else if (v instanceof BigDecimal) {
            BigInteger bi = ((BigDecimal) v).toBigInteger();
            if (bi.bitLength() > 63) throw range(v, target);
            n = bi.longValue();
        } else if (v instanceof Boolean) {
            n = (Boolean) v ? 1 : 0;
        } else if (v instanceof String) {
            String s = ((String) v).trim();
            try {
                n = Long.parseLong(s);
            } catch (NumberFormatException e) {
                try {
                    return asLong(new BigDecimal(s), min, max, target);
                } catch (NumberFormatException e2) {
                    throw Errors.conversion(v, target);
                }
            }
        } else {
            throw Errors.conversion(v, target);
        }
        if (n < min || n > max) throw range(v, target);
        return n;
    }

    private static SQLDataException range(Object v, String target) {
        return new SQLDataException("value " + v + " is out of range for " + target, "22003");
    }

    static double asDouble(Object v) throws SQLException {
        if (v == null) return 0;
        if (v instanceof Number) return ((Number) v).doubleValue();
        if (v instanceof Boolean) return (Boolean) v ? 1 : 0;
        if (v instanceof String) {
            try {
                return Double.parseDouble(((String) v).trim());
            } catch (NumberFormatException e) {
                throw Errors.conversion(v, "DOUBLE");
            }
        }
        throw Errors.conversion(v, "DOUBLE");
    }

    static BigDecimal asBigDecimal(Object v) throws SQLException {
        if (v == null) return null;
        if (v instanceof BigDecimal) return (BigDecimal) v;
        if (v instanceof Long || v instanceof Integer || v instanceof Short || v instanceof Byte)
            return BigDecimal.valueOf(((Number) v).longValue());
        if (v instanceof BigInteger) return new BigDecimal((BigInteger) v);
        if (v instanceof Double || v instanceof Float) {
            double d = ((Number) v).doubleValue();
            if (Double.isNaN(d) || Double.isInfinite(d)) throw Errors.conversion(v, "DECIMAL");
            return BigDecimal.valueOf(d);
        }
        if (v instanceof Boolean) return (Boolean) v ? BigDecimal.ONE : BigDecimal.ZERO;
        if (v instanceof String) {
            try {
                return new BigDecimal(((String) v).trim());
            } catch (NumberFormatException e) {
                throw Errors.conversion(v, "DECIMAL");
            }
        }
        throw Errors.conversion(v, "DECIMAL");
    }

    static boolean asBoolean(Object v) throws SQLException {
        if (v == null) return false;
        if (v instanceof Boolean) return (Boolean) v;
        if (v instanceof BigDecimal) return ((BigDecimal) v).signum() != 0;
        if (v instanceof Number) return ((Number) v).doubleValue() != 0;
        if (v instanceof String) {
            switch (((String) v).trim().toLowerCase(Locale.ROOT)) {
                case "true": case "t": case "1": case "yes": case "y": case "on": return true;
                case "false": case "f": case "0": case "no": case "n": case "off": return false;
                default: break;
            }
        }
        throw Errors.conversion(v, "BOOLEAN");
    }

    static byte[] asBytes(Object v) throws SQLException {
        if (v == null) return null;
        if (v instanceof byte[]) return (byte[]) v;
        if (v instanceof String) return ((String) v).getBytes(StandardCharsets.UTF_8);
        if (v instanceof UUID) {
            UUID u = (UUID) v;
            return ByteBuffer.allocate(16).putLong(u.getMostSignificantBits()).putLong(u.getLeastSignificantBits()).array();
        }
        throw Errors.conversion(v, "BINARY");
    }

    static Instant asInstant(Object v) throws SQLException {
        if (v == null) return null;
        if (v instanceof Instant) return (Instant) v;
        if (v instanceof Long || v instanceof Integer) return Instant.ofEpochMilli(((Number) v).longValue());
        if (v instanceof Timestamp) return ((Timestamp) v).toInstant();
        if (v instanceof java.util.Date || v instanceof OffsetDateTime || v instanceof ZonedDateTime
                || v instanceof LocalDateTime || v instanceof LocalDate) return (Instant) toBind(v);
        if (v instanceof String) {
            String s = ((String) v).trim();
            try {
                return Instant.parse(s);
            } catch (DateTimeParseException e) {
                try {
                    return OffsetDateTime.parse(s).toInstant();
                } catch (DateTimeParseException e2) {
                    try {
                        return LocalDateTime.parse(s.replace(' ', 'T')).toInstant(ZoneOffset.UTC);
                    } catch (DateTimeParseException e3) {
                        throw Errors.conversion(v, "TIMESTAMP");
                    }
                }
            }
        }
        throw Errors.conversion(v, "TIMESTAMP");
    }

    static java.sql.Date asDate(Object v, Calendar cal) throws SQLException {
        Instant i = asInstant(v);
        return i == null ? null : java.sql.Date.valueOf(i.atZone(zone(cal)).toLocalDate());
    }

    static Time asTime(Object v, Calendar cal) throws SQLException {
        Instant i = asInstant(v);
        if (i == null) return null;
        LocalTime t = i.atZone(zone(cal)).toLocalTime();
        return Time.valueOf(t);
    }

    static UUID asUuid(Object v) throws SQLException {
        if (v == null) return null;
        if (v instanceof UUID) return (UUID) v;
        if (v instanceof String) {
            try {
                return UUID.fromString(((String) v).trim());
            } catch (IllegalArgumentException e) {
                throw Errors.conversion(v, "UUID");
            }
        }
        if (v instanceof byte[] && ((byte[]) v).length == 16) {
            ByteBuffer b = ByteBuffer.wrap((byte[]) v);
            return new UUID(b.getLong(), b.getLong());
        }
        throw Errors.conversion(v, "UUID");
    }

    /** ResultSet.getObject(i, type). */
    static <T> T as(Object v, Class<T> type) throws SQLException {
        if (type == null) throw new SQLException("type is null", "HY009");
        if (v == null) {
            if (type.isPrimitive()) throw new SQLDataException("NULL cannot be read as " + type, "22002");
            return null;
        }
        Object out;
        if (type == Object.class) out = jdbcObject(v);
        else if (type == String.class) out = asString(v);
        else if (type == Long.class || type == long.class) out = asLong(v, Long.MIN_VALUE, Long.MAX_VALUE, "BIGINT");
        else if (type == Integer.class || type == int.class) out = (int) asLong(v, Integer.MIN_VALUE, Integer.MAX_VALUE, "INTEGER");
        else if (type == Short.class || type == short.class) out = (short) asLong(v, Short.MIN_VALUE, Short.MAX_VALUE, "SMALLINT");
        else if (type == Byte.class || type == byte.class) out = (byte) asLong(v, Byte.MIN_VALUE, Byte.MAX_VALUE, "TINYINT");
        else if (type == Double.class || type == double.class) out = asDouble(v);
        else if (type == Float.class || type == float.class) out = (float) asDouble(v);
        else if (type == BigDecimal.class) out = asBigDecimal(v);
        else if (type == BigInteger.class) out = asBigDecimal(v).toBigInteger();
        else if (type == Boolean.class || type == boolean.class) out = asBoolean(v);
        else if (type == byte[].class) out = asBytes(v);
        else if (type == UUID.class) out = asUuid(v);
        else if (type == Instant.class) out = asInstant(v);
        else if (type == Timestamp.class) out = Timestamp.from(asInstant(v));
        else if (type == java.sql.Date.class) out = asDate(v, null);
        else if (type == Time.class) out = asTime(v, null);
        else if (type == java.util.Date.class) out = java.util.Date.from(asInstant(v));
        else if (type == OffsetDateTime.class) out = asInstant(v).atOffset(ZoneOffset.UTC);
        else if (type == ZonedDateTime.class) out = asInstant(v).atZone(ZoneOffset.UTC);
        else if (type == LocalDateTime.class) out = LocalDateTime.ofInstant(asInstant(v), ZoneOffset.UTC);
        else if (type == LocalDate.class) out = LocalDateTime.ofInstant(asInstant(v), ZoneOffset.UTC).toLocalDate();
        else if (type == Array.class) {
            if (!(v instanceof List)) throw Errors.conversion(v, "ARRAY");
            out = new SkaidbArray((List<?>) v);
        } else if (type.isInstance(v)) out = v;
        else throw Errors.conversion(v, type.getName());
        if (type.isPrimitive()) {
            @SuppressWarnings("unchecked")
            T boxed = (T) out;
            return boxed;
        }
        return type.cast(out);
    }

    // ---- metadata ------------------------------------------------------------------

    static int sqlType(Object v) {
        if (v == null) return Types.NULL;
        if (v instanceof Boolean) return Types.BOOLEAN;
        if (v instanceof Long) return Types.BIGINT;
        if (v instanceof Double) return Types.DOUBLE;
        if (v instanceof BigDecimal) return Types.DECIMAL;
        if (v instanceof String) return Types.VARCHAR;
        if (v instanceof byte[]) return Types.VARBINARY;
        if (v instanceof Instant) return Types.TIMESTAMP;
        if (v instanceof List) return Types.ARRAY;
        return Types.OTHER;   // UUID, document
    }

    static String typeName(int sqlType, Object sample) {
        switch (sqlType) {
            case Types.BOOLEAN: return "BOOLEAN";
            case Types.BIGINT: return "BIGINT";
            case Types.DOUBLE: return "DOUBLE";
            case Types.DECIMAL: return "DECIMAL";
            case Types.VARCHAR: return "TEXT";
            case Types.VARBINARY: return "BYTES";
            case Types.TIMESTAMP: return "TIMESTAMP";
            case Types.ARRAY: return "ARRAY";
            case Types.OTHER:
                if (sample instanceof UUID) return "UUID";
                if (sample instanceof Map) return "DOCUMENT";
                return "ANY";
            default: return "ANY";
        }
    }

    static String className(int sqlType, Object sample) {
        switch (sqlType) {
            case Types.BOOLEAN: return Boolean.class.getName();
            case Types.BIGINT: return Long.class.getName();
            case Types.DOUBLE: return Double.class.getName();
            case Types.DECIMAL: return BigDecimal.class.getName();
            case Types.VARCHAR: return String.class.getName();
            case Types.VARBINARY: return byte[].class.getName();
            case Types.TIMESTAMP: return Timestamp.class.getName();
            case Types.ARRAY: return Array.class.getName();
            case Types.OTHER:
                if (sample instanceof UUID) return UUID.class.getName();
                if (sample instanceof Map) return Map.class.getName();
                return Object.class.getName();
            default: return Object.class.getName();
        }
    }

    // ---- JSON rendering for getString of arrays, documents and bytes ---------------

    static String json(Object v) {
        StringBuilder b = new StringBuilder();
        json(v, b);
        return b.toString();
    }

    private static void json(Object v, StringBuilder b) {
        if (v == null) { b.append("null"); return; }
        if (v instanceof Boolean || v instanceof Long) { b.append(v); return; }
        if (v instanceof Double) {
            double d = (Double) v;
            if (Double.isNaN(d) || Double.isInfinite(d)) quote(v.toString(), b); else b.append(d);
            return;
        }
        if (v instanceof BigDecimal) { b.append(((BigDecimal) v).toPlainString()); return; }
        if (v instanceof byte[]) {
            StringBuilder h = new StringBuilder("\\x");
            for (byte x : (byte[]) v) h.append(String.format("%02x", x & 0xff));
            if (b.length() == 0) { b.append(h); return; }    // top level: the bare hex form
            quote(h.toString(), b);
            return;
        }
        if (v instanceof List) {
            b.append('[');
            boolean first = true;
            for (Object o : (List<?>) v) {
                if (!first) b.append(',');
                first = false;
                json(o, b);
            }
            b.append(']');
            return;
        }
        if (v instanceof Map) {
            b.append('{');
            boolean first = true;
            for (Map.Entry<?, ?> e : ((Map<?, ?>) v).entrySet()) {
                if (!first) b.append(',');
                first = false;
                quote(String.valueOf(e.getKey()), b);
                b.append(':');
                json(e.getValue(), b);
            }
            b.append('}');
            return;
        }
        quote(v.toString(), b);   // String, UUID, Instant
    }

    private static void quote(String s, StringBuilder b) {
        b.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"': b.append("\\\""); break;
                case '\\': b.append("\\\\"); break;
                case '\n': b.append("\\n"); break;
                case '\r': b.append("\\r"); break;
                case '\t': b.append("\\t"); break;
                default:
                    if (c < 0x20) b.append(String.format("\\u%04x", (int) c));
                    else b.append(c);
            }
        }
        b.append('"');
    }
}
