package com.skaidb.jdbc;

import com.skaidb.Skaidb;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.BatchUpdateException;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

/**
 * A JDBC PreparedStatement: prepared on the server, parameters sent as typed
 * values (so arrays, documents, UUIDs and decimals bind losslessly), and
 * {@link #executeBatch()} sends every row in ONE {@code OP_EXECUTE_BATCH}
 * round trip. Statement kinds the server will not prepare (DDL, {@code USE})
 * fall back to client-side literal binding.
 */
public final class SkaidbPreparedStatement extends SkaidbStatement implements PreparedStatement {
    private final String sql;
    private final Skaidb.Query query;
    private final Object[] params;
    private final boolean[] set;
    private final List<Object[]> rows = new ArrayList<>();

    SkaidbPreparedStatement(SkaidbConnection conn, String sql) throws SQLException {
        super(conn);
        this.sql = sql;
        try {
            this.query = conn.nativeConnection().prepare(sql);
        } catch (RuntimeException e) {
            throw Errors.map(e);
        }
        int n = query.getParameterCount();
        this.params = new Object[n];
        this.set = new boolean[n];
    }

    private void bind(int index, Object nativeValue) throws SQLException {
        checkOpen();
        if (index < 1 || index > params.length)
            throw new SQLException("parameter index " + index + " out of range 1.." + params.length, "07009");
        params[index - 1] = nativeValue;
        set[index - 1] = true;
    }

    private Object[] boundRow() throws SQLException {
        for (int i = 0; i < set.length; i++) {
            if (!set[i]) throw new SQLException("no value specified for parameter " + (i + 1), "07001");
        }
        return params.clone();
    }

    // ---- execution ------------------------------------------------------------------

    @Override
    public boolean execute() throws SQLException {
        checkOpen();
        Object[] row = boundRow();
        resetResults();
        conn.beforeStatement();
        if (fetchSize > 0 && params.length == 0) {
            Skaidb.Connection c = conn.nativeConnection();
            return acceptStream(call(() -> c.stream(sql)));
        }
        return accept(call(() -> {
            for (int i = 0; i < row.length; i++) query.setObject(i + 1, row[i]);
            return query.execute();
        }));
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        if (!execute()) throw new SQLException("the statement returned no result set", "02000");
        return current;
    }

    @Override
    public int executeUpdate() throws SQLException {
        return (int) Math.min(Integer.MAX_VALUE, executeLargeUpdate());
    }

    @Override
    public long executeLargeUpdate() throws SQLException {
        if (execute()) {
            resetResults();
            throw new SQLException("the statement returned a result set; use executeQuery", "21000");
        }
        return updateCount;
    }

    @Override
    public void addBatch() throws SQLException {
        checkOpen();
        rows.add(boundRow());
    }

    @Override
    public void clearBatch() throws SQLException {
        checkOpen();
        rows.clear();
    }

    /**
     * All rows in ONE round trip. The server reports only the total, so each
     * element is {@link #SUCCESS_NO_INFO}; {@link #executeLargeUpdate()}-style
     * totals are available from {@link #getLargeUpdateCount()} afterwards.
     * Each row autocommits unless a transaction is open; if one fails the
     * rows before it stay applied and a {@link BatchUpdateException} is thrown.
     */
    @Override
    public int[] executeBatch() throws SQLException {
        long[] l = executeLargeBatch();
        int[] out = new int[l.length];
        for (int i = 0; i < l.length; i++) out[i] = (int) l[i];
        return out;
    }

    @Override
    public long[] executeLargeBatch() throws SQLException {
        checkOpen();
        resetResults();
        List<Object[]> todo = new ArrayList<>(rows);
        rows.clear();
        if (todo.isEmpty()) return new long[0];
        conn.beforeStatement();
        try {
            updateCount = call(() -> query.executeBatch(todo));
        } catch (SQLException e) {
            throw new BatchUpdateException(e.getMessage(), e.getSQLState(), e.getErrorCode(), new int[0], e);
        }
        long[] out = new long[todo.size()];
        java.util.Arrays.fill(out, SUCCESS_NO_INFO);
        return out;
    }

    @Override
    public void clearParameters() throws SQLException {
        checkOpen();
        java.util.Arrays.fill(params, null);
        java.util.Arrays.fill(set, false);
    }

    /** Null: the column list is known only once the statement has run. */
    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        checkOpen();
        return current == null ? null : current.getMetaData();
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        checkOpen();
        return new SkaidbParameterMetaData(params.length);
    }

    // ---- the Statement(String) forms do not apply ---------------------------------------

    private static SQLException notOnPrepared() {
        return new SQLException("a PreparedStatement runs its own SQL; use a Statement for other SQL", "HY000");
    }

    @Override public boolean execute(String sql) throws SQLException { throw notOnPrepared(); }
    @Override public ResultSet executeQuery(String sql) throws SQLException { throw notOnPrepared(); }
    @Override public int executeUpdate(String sql) throws SQLException { throw notOnPrepared(); }
    @Override public long executeLargeUpdate(String sql) throws SQLException { throw notOnPrepared(); }
    @Override public void addBatch(String sql) throws SQLException { throw notOnPrepared(); }

    // ---- setters --------------------------------------------------------------------------

    @Override public void setNull(int i, int sqlType) throws SQLException { bind(i, null); }
    @Override public void setNull(int i, int sqlType, String typeName) throws SQLException { bind(i, null); }
    @Override public void setBoolean(int i, boolean x) throws SQLException { bind(i, x); }
    @Override public void setByte(int i, byte x) throws SQLException { bind(i, (long) x); }
    @Override public void setShort(int i, short x) throws SQLException { bind(i, (long) x); }
    @Override public void setInt(int i, int x) throws SQLException { bind(i, (long) x); }
    @Override public void setLong(int i, long x) throws SQLException { bind(i, x); }
    @Override public void setFloat(int i, float x) throws SQLException { bind(i, (double) x); }
    @Override public void setDouble(int i, double x) throws SQLException { bind(i, x); }
    @Override public void setBigDecimal(int i, BigDecimal x) throws SQLException { bind(i, x); }
    @Override public void setString(int i, String x) throws SQLException { bind(i, x); }
    @Override public void setNString(int i, String x) throws SQLException { bind(i, x); }
    @Override public void setBytes(int i, byte[] x) throws SQLException { bind(i, x); }

    /** The start of that day in the JVM's default zone, as a timestamp. */
    @Override
    public void setDate(int i, Date x) throws SQLException {
        bind(i, x == null ? null : Values.startOfDay(x.toLocalDate(), null));
    }

    @Override
    public void setDate(int i, Date x, Calendar cal) throws SQLException {
        bind(i, x == null ? null : Values.startOfDay(x.toLocalDate(), cal));
    }

    @Override
    public void setTime(int i, Time x) throws SQLException {
        throw Errors.unsupported("TIME values (skaidb has no time-of-day type; bind a String)");
    }

    @Override
    public void setTime(int i, Time x, Calendar cal) throws SQLException { setTime(i, x); }

    @Override
    public void setTimestamp(int i, Timestamp x) throws SQLException { bind(i, x == null ? null : x.toInstant()); }

    /** A Timestamp is an absolute instant; the Calendar does not change it. */
    @Override
    public void setTimestamp(int i, Timestamp x, Calendar cal) throws SQLException { setTimestamp(i, x); }

    @Override public void setObject(int i, Object x) throws SQLException { bind(i, Values.toBind(x)); }
    @Override public void setObject(int i, Object x, int sqlType) throws SQLException { bind(i, Values.toBind(x, sqlType)); }

    @Override
    public void setObject(int i, Object x, int sqlType, int scaleOrLength) throws SQLException {
        Object v = Values.toBind(x, sqlType);
        if (v instanceof BigDecimal && (sqlType == java.sql.Types.DECIMAL || sqlType == java.sql.Types.NUMERIC))
            v = ((BigDecimal) v).setScale(scaleOrLength, java.math.RoundingMode.HALF_UP);
        bind(i, v);
    }

    @Override public void setArray(int i, Array x) throws SQLException { bind(i, x == null ? null : Values.toBind(x)); }

    @Override
    public void setAsciiStream(int i, InputStream x, int length) throws SQLException { setAsciiStream(i, x, (long) length); }

    @Override
    public void setAsciiStream(int i, InputStream x, long length) throws SQLException {
        bind(i, x == null ? null : new String(Values.readAll(x, length), StandardCharsets.US_ASCII));
    }

    @Override public void setAsciiStream(int i, InputStream x) throws SQLException { setAsciiStream(i, x, -1L); }

    @Override
    public void setBinaryStream(int i, InputStream x, int length) throws SQLException { setBinaryStream(i, x, (long) length); }

    @Override
    public void setBinaryStream(int i, InputStream x, long length) throws SQLException {
        bind(i, x == null ? null : Values.readAll(x, length));
    }

    @Override public void setBinaryStream(int i, InputStream x) throws SQLException { setBinaryStream(i, x, -1L); }

    @Override
    public void setCharacterStream(int i, Reader x, int length) throws SQLException { setCharacterStream(i, x, (long) length); }

    @Override
    public void setCharacterStream(int i, Reader x, long length) throws SQLException {
        bind(i, x == null ? null : Values.readAll(x, length));
    }

    @Override public void setCharacterStream(int i, Reader x) throws SQLException { setCharacterStream(i, x, -1L); }
    @Override public void setNCharacterStream(int i, Reader x, long length) throws SQLException { setCharacterStream(i, x, length); }
    @Override public void setNCharacterStream(int i, Reader x) throws SQLException { setCharacterStream(i, x, -1L); }

    @Override
    @Deprecated
    public void setUnicodeStream(int i, InputStream x, int length) throws SQLException {
        throw Errors.unsupported("setUnicodeStream");
    }

    @Override public void setURL(int i, URL x) throws SQLException { bind(i, x == null ? null : x.toString()); }
    @Override public void setRef(int i, Ref x) throws SQLException { throw Errors.unsupported("Ref"); }
    @Override public void setBlob(int i, Blob x) throws SQLException { throw Errors.unsupported("Blob (use setBytes)"); }
    @Override public void setBlob(int i, InputStream x, long length) throws SQLException { setBinaryStream(i, x, length); }
    @Override public void setBlob(int i, InputStream x) throws SQLException { setBinaryStream(i, x, -1L); }
    @Override public void setClob(int i, Clob x) throws SQLException { throw Errors.unsupported("Clob (use setString)"); }
    @Override public void setClob(int i, Reader x, long length) throws SQLException { setCharacterStream(i, x, length); }
    @Override public void setClob(int i, Reader x) throws SQLException { setCharacterStream(i, x, -1L); }
    @Override public void setNClob(int i, NClob x) throws SQLException { throw Errors.unsupported("NClob (use setString)"); }
    @Override public void setNClob(int i, Reader x, long length) throws SQLException { setCharacterStream(i, x, length); }
    @Override public void setNClob(int i, Reader x) throws SQLException { setCharacterStream(i, x, -1L); }
    @Override public void setRowId(int i, RowId x) throws SQLException { throw Errors.unsupported("RowId"); }
    @Override public void setSQLXML(int i, SQLXML x) throws SQLException { throw Errors.unsupported("SQLXML"); }
}
