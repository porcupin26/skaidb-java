package com.skaidb.jdbc;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Map;

/** A skaidb array value. Elements may be of any type, so the base type is OTHER. */
final class SkaidbArray implements Array {
    private List<?> items;

    SkaidbArray(List<?> items) {
        this.items = items;
    }

    private List<?> items() throws SQLException {
        if (items == null) throw new SQLException("array has been freed", "HY000");
        return items;
    }

    @Override public String getBaseTypeName() { return "ANY"; }

    @Override public int getBaseType() { return Types.OTHER; }

    /** The elements as {@code Object[]}, each as ResultSet.getObject would return it. */
    @Override
    public Object getArray() throws SQLException {
        List<?> l = items();
        Object[] out = new Object[l.size()];
        for (int i = 0; i < out.length; i++) out[i] = Values.jdbcObject(l.get(i));
        return out;
    }

    @Override
    public Object getArray(Map<String, Class<?>> map) throws SQLException {
        if (map != null && !map.isEmpty()) throw Errors.unsupported("type maps");
        return getArray();
    }

    /** {@code count} elements from the 1-based {@code index}. */
    @Override
    public Object getArray(long index, int count) throws SQLException {
        Object[] all = (Object[]) getArray();
        if (index < 1 || count < 0 || index - 1 + count > all.length)
            throw new SQLException("array slice out of range", "2202E");
        Object[] out = new Object[count];
        System.arraycopy(all, (int) index - 1, out, 0, count);
        return out;
    }

    @Override
    public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
        if (map != null && !map.isEmpty()) throw Errors.unsupported("type maps");
        return getArray(index, count);
    }

    @Override public ResultSet getResultSet() throws SQLException { throw Errors.unsupported("Array.getResultSet"); }

    @Override
    public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
        throw Errors.unsupported("Array.getResultSet");
    }

    @Override
    public ResultSet getResultSet(long index, int count) throws SQLException {
        throw Errors.unsupported("Array.getResultSet");
    }

    @Override
    public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SQLException {
        throw Errors.unsupported("Array.getResultSet");
    }

    @Override public void free() { items = null; }

    /** The raw element list (for binding). */
    List<?> list() throws SQLException { return items(); }

    @Override public String toString() { return items == null ? "freed array" : Values.json(items); }
}
