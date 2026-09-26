package com.skaidb.jdbc;

import java.math.BigDecimal;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

/**
 * Column names come from the server; column TYPES do not (skaidb tables are
 * schema-less, so the wire carries a type per value, not per column). For a
 * materialised result each column's type is inferred from its values: the
 * one type all non-NULL values share, else OTHER ("ANY"). A streamed result
 * reports OTHER for every column.
 */
final class SkaidbResultSetMetaData implements ResultSetMetaData {
    private final String[] cols;
    private final int[] types;
    private final Object[] samples;
    private final int[] scales;

    SkaidbResultSetMetaData(String[] cols, List<Object[]> rows) {
        this.cols = cols;
        this.types = new int[cols.length];
        this.samples = new Object[cols.length];
        this.scales = new int[cols.length];
        for (int c = 0; c < cols.length; c++) {
            int t = Types.NULL;
            Object sample = null;
            if (rows == null) {
                t = Types.OTHER;
            } else {
                for (Object[] r : rows) {
                    Object v = c < r.length ? r[c] : null;
                    if (v == null) continue;
                    int vt = Values.sqlType(v);
                    if (vt == Types.OTHER && sample != null && sample.getClass() != v.getClass()) vt = -1;
                    if (t == Types.NULL) { t = vt; sample = v; }
                    else if (t != vt) { t = Types.OTHER; sample = null; break; }
                    if (v instanceof BigDecimal) scales[c] = Math.max(scales[c], ((BigDecimal) v).scale());
                }
                if (t == Types.NULL) t = Types.OTHER;   // no value to go by
            }
            types[c] = t;
            samples[c] = sample;
        }
    }

    private int idx(int column) throws SQLException {
        if (column < 1 || column > cols.length)
            throw new SQLException("column index " + column + " out of range 1.." + cols.length, "07009");
        return column - 1;
    }

    @Override public int getColumnCount() { return cols.length; }
    @Override public String getColumnName(int c) throws SQLException { return cols[idx(c)]; }
    @Override public String getColumnLabel(int c) throws SQLException { return cols[idx(c)]; }
    @Override public int getColumnType(int c) throws SQLException { return types[idx(c)]; }
    @Override public String getColumnTypeName(int c) throws SQLException { int i = idx(c); return Values.typeName(types[i], samples[i]); }
    @Override public String getColumnClassName(int c) throws SQLException { int i = idx(c); return Values.className(types[i], samples[i]); }
    @Override public boolean isAutoIncrement(int c) throws SQLException { idx(c); return false; }
    @Override public boolean isCaseSensitive(int c) throws SQLException { return types[idx(c)] == Types.VARCHAR || types[idx(c)] == Types.OTHER; }
    @Override public boolean isSearchable(int c) throws SQLException { idx(c); return true; }
    @Override public boolean isCurrency(int c) throws SQLException { idx(c); return false; }
    @Override public int isNullable(int c) throws SQLException { idx(c); return columnNullableUnknown; }

    @Override
    public boolean isSigned(int c) throws SQLException {
        int t = types[idx(c)];
        return t == Types.BIGINT || t == Types.DOUBLE || t == Types.DECIMAL;
    }

    @Override
    public int getColumnDisplaySize(int c) throws SQLException {
        switch (types[idx(c)]) {
            case Types.BOOLEAN: return 5;
            case Types.BIGINT: return 20;
            case Types.DOUBLE: return 25;
            case Types.DECIMAL: return 41;
            case Types.TIMESTAMP: return 29;
            default: return Integer.MAX_VALUE;
        }
    }

    @Override
    public int getPrecision(int c) throws SQLException {
        switch (types[idx(c)]) {
            case Types.BOOLEAN: return 1;
            case Types.BIGINT: return 19;
            case Types.DOUBLE: return 17;
            case Types.DECIMAL: return 38;
            case Types.TIMESTAMP: return 23;
            default: return 0;
        }
    }

    @Override
    public int getScale(int c) throws SQLException {
        int i = idx(c);
        return types[i] == Types.DECIMAL ? scales[i] : types[i] == Types.TIMESTAMP ? 3 : 0;
    }

    @Override public String getSchemaName(int c) throws SQLException { idx(c); return ""; }
    @Override public String getTableName(int c) throws SQLException { idx(c); return ""; }
    @Override public String getCatalogName(int c) throws SQLException { idx(c); return ""; }
    @Override public boolean isReadOnly(int c) throws SQLException { idx(c); return true; }
    @Override public boolean isWritable(int c) throws SQLException { idx(c); return false; }
    @Override public boolean isDefinitelyWritable(int c) throws SQLException { idx(c); return false; }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) return iface.cast(this);
        throw new SQLException("not a wrapper for " + iface.getName(), "HY000");
    }

    @Override public boolean isWrapperFor(Class<?> iface) { return iface.isInstance(this); }
}
