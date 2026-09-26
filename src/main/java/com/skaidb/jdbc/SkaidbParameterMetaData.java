package com.skaidb.jdbc;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.sql.Types;

/** The parameter count is exact; types are not declared by skaidb, so every parameter is OTHER / ANY. */
final class SkaidbParameterMetaData implements ParameterMetaData {
    private final int count;

    SkaidbParameterMetaData(int count) { this.count = count; }

    private void check(int i) throws SQLException {
        if (i < 1 || i > count) throw new SQLException("parameter index " + i + " out of range 1.." + count, "07009");
    }

    @Override public int getParameterCount() { return count; }
    @Override public int isNullable(int i) throws SQLException { check(i); return parameterNullableUnknown; }
    @Override public boolean isSigned(int i) throws SQLException { check(i); return true; }
    @Override public int getPrecision(int i) throws SQLException { check(i); return 0; }
    @Override public int getScale(int i) throws SQLException { check(i); return 0; }
    @Override public int getParameterType(int i) throws SQLException { check(i); return Types.OTHER; }
    @Override public String getParameterTypeName(int i) throws SQLException { check(i); return "ANY"; }
    @Override public String getParameterClassName(int i) throws SQLException { check(i); return Object.class.getName(); }
    @Override public int getParameterMode(int i) throws SQLException { check(i); return parameterModeIn; }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) return iface.cast(this);
        throw new SQLException("not a wrapper for " + iface.getName(), "HY000");
    }

    @Override public boolean isWrapperFor(Class<?> iface) { return iface.isInstance(this); }
}
