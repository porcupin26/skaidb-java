package com.skaidb.jdbc;

import com.skaidb.Skaidb;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * A forward-only, read-only result set: either fully materialised (the
 * default) or streamed a chunk at a time (a statement with a fetch size above
 * zero). Column labels match case-insensitively, first match wins.
 */
public final class SkaidbResultSet implements ResultSet {
    private final SkaidbStatement stmt;
    private final String[] cols;
    private final Map<String, Integer> byName = new HashMap<>();
    private final List<Object[]> rows;          // materialised; null when streaming
    private final Skaidb.RowStream stream;      // streaming; null when materialised
    private final int maxRows;
    private int row = 0;                        // 1-based; 0 = before the first row
    private boolean after = false;
    private Object[] current;
    private boolean wasNull = false;
    private boolean closed = false;
    private SkaidbResultSetMetaData meta;

    SkaidbResultSet(SkaidbStatement stmt, String[] cols, List<Object[]> rows) {
        this.stmt = stmt;
        this.cols = cols;
        this.rows = rows;
        this.stream = null;
        this.maxRows = 0;
        index();
    }

    SkaidbResultSet(SkaidbStatement stmt, Skaidb.RowStream stream, int maxRows) {
        this.stmt = stmt;
        this.cols = stream.getColumnNames();
        this.rows = null;
        this.stream = stream;
        this.maxRows = maxRows;
        index();
    }

    private void index() {
        for (int i = 0; i < cols.length; i++) byName.putIfAbsent(cols[i].toLowerCase(Locale.ROOT), i + 1);
    }

    private void checkOpen() throws SQLException {
        if (closed) throw Errors.closed("result set");
    }

    // ---- navigation --------------------------------------------------------------

    @Override
    public boolean next() throws SQLException {
        checkOpen();
        if (after) return false;
        boolean has;
        if (rows != null) {
            has = row < rows.size();
            if (has) current = rows.get(row);
        } else if (maxRows > 0 && row >= maxRows) {
            stream.close();   // drains the rest so the connection is free again
            has = false;
        } else {
            try {
                has = stream.next();
            } catch (RuntimeException e) {
                after = true;
                current = null;
                throw Errors.map(e);
            }
            if (has) {
                Object[] r = new Object[cols.length];
                for (int i = 0; i < r.length; i++) r[i] = stream.getObject(i);
                current = r;
            }
        }
        if (has) {
            row++;
        } else {
            after = true;
            current = null;
        }
        return has;
    }

    @Override
    public void close() throws SQLException {
        if (closed) return;
        closed = true;
        current = null;
        if (stream != null) stream.close();
        if (stmt != null) stmt.resultClosed(this);
    }

    void closeQuietly() {
        try {
            close();
        } catch (SQLException | RuntimeException ignored) {
            // closing on behalf of the statement
        }
    }

    @Override public boolean isClosed() { return closed; }

    @Override
    public int getRow() throws SQLException {
        checkOpen();
        return after ? 0 : row;
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        checkOpen();
        if (rows == null) throw Errors.unsupported("isBeforeFirst on a streamed result set");
        return row == 0 && !rows.isEmpty();
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        checkOpen();
        if (rows == null) throw Errors.unsupported("isAfterLast on a streamed result set");
        return after && !rows.isEmpty();
    }

    @Override
    public boolean isFirst() throws SQLException {
        checkOpen();
        return !after && row == 1;
    }

    @Override
    public boolean isLast() throws SQLException {
        checkOpen();
        if (rows == null) throw Errors.unsupported("isLast on a streamed result set");
        return !after && row > 0 && row == rows.size();
    }

    private static SQLException forwardOnly() {
        return Errors.unsupported("moving a TYPE_FORWARD_ONLY result set other than by next()");
    }

    @Override public void beforeFirst() throws SQLException { throw forwardOnly(); }
    @Override public void afterLast() throws SQLException { throw forwardOnly(); }
    @Override public boolean first() throws SQLException { throw forwardOnly(); }
    @Override public boolean last() throws SQLException { throw forwardOnly(); }
    @Override public boolean absolute(int r) throws SQLException { throw forwardOnly(); }
    @Override public boolean relative(int r) throws SQLException { throw forwardOnly(); }
    @Override public boolean previous() throws SQLException { throw forwardOnly(); }

    // ---- values --------------------------------------------------------------------

    @Override
    public int findColumn(String label) throws SQLException {
        checkOpen();
        Integer i = label == null ? null : byName.get(label.toLowerCase(Locale.ROOT));
        if (i == null) throw new SQLException("no such column: " + label, "42703");
        return i;
    }

    private Object raw(int col) throws SQLException {
        checkOpen();
        if (current == null) throw new SQLException("no current row: call next() first", "24000");
        if (col < 1 || col > cols.length) throw new SQLException("column index " + col + " out of range 1.." + cols.length, "07009");
        Object v = current[col - 1];
        wasNull = v == null;
        return v;
    }

    @Override
    public boolean wasNull() throws SQLException {
        checkOpen();
        return wasNull;
    }

    @Override public Object getObject(int i) throws SQLException { return Values.jdbcObject(raw(i)); }
    @Override public Object getObject(String c) throws SQLException { return getObject(findColumn(c)); }
    @Override public <T> T getObject(int i, Class<T> type) throws SQLException { return Values.as(raw(i), type); }
    @Override public <T> T getObject(String c, Class<T> type) throws SQLException { return getObject(findColumn(c), type); }

    @Override
    public Object getObject(int i, Map<String, Class<?>> map) throws SQLException {
        if (map != null && !map.isEmpty()) throw Errors.unsupported("type maps");
        return getObject(i);
    }

    @Override public Object getObject(String c, Map<String, Class<?>> map) throws SQLException { return getObject(findColumn(c), map); }

    @Override public String getString(int i) throws SQLException { return Values.asString(raw(i)); }
    @Override public String getString(String c) throws SQLException { return getString(findColumn(c)); }
    @Override public String getNString(int i) throws SQLException { return getString(i); }
    @Override public String getNString(String c) throws SQLException { return getString(c); }
    @Override public boolean getBoolean(int i) throws SQLException { return Values.asBoolean(raw(i)); }
    @Override public boolean getBoolean(String c) throws SQLException { return getBoolean(findColumn(c)); }
    @Override public byte getByte(int i) throws SQLException { return (byte) Values.asLong(raw(i), Byte.MIN_VALUE, Byte.MAX_VALUE, "TINYINT"); }
    @Override public byte getByte(String c) throws SQLException { return getByte(findColumn(c)); }
    @Override public short getShort(int i) throws SQLException { return (short) Values.asLong(raw(i), Short.MIN_VALUE, Short.MAX_VALUE, "SMALLINT"); }
    @Override public short getShort(String c) throws SQLException { return getShort(findColumn(c)); }
    @Override public int getInt(int i) throws SQLException { return (int) Values.asLong(raw(i), Integer.MIN_VALUE, Integer.MAX_VALUE, "INTEGER"); }
    @Override public int getInt(String c) throws SQLException { return getInt(findColumn(c)); }
    @Override public long getLong(int i) throws SQLException { return Values.asLong(raw(i), Long.MIN_VALUE, Long.MAX_VALUE, "BIGINT"); }
    @Override public long getLong(String c) throws SQLException { return getLong(findColumn(c)); }
    @Override public float getFloat(int i) throws SQLException { return (float) Values.asDouble(raw(i)); }
    @Override public float getFloat(String c) throws SQLException { return getFloat(findColumn(c)); }
    @Override public double getDouble(int i) throws SQLException { return Values.asDouble(raw(i)); }
    @Override public double getDouble(String c) throws SQLException { return getDouble(findColumn(c)); }
    @Override public BigDecimal getBigDecimal(int i) throws SQLException { return Values.asBigDecimal(raw(i)); }
    @Override public BigDecimal getBigDecimal(String c) throws SQLException { return getBigDecimal(findColumn(c)); }

    @Override
    @Deprecated
    public BigDecimal getBigDecimal(int i, int scale) throws SQLException {
        BigDecimal d = getBigDecimal(i);
        return d == null ? null : d.setScale(scale, RoundingMode.HALF_UP);
    }

    @Override
    @Deprecated
    public BigDecimal getBigDecimal(String c, int scale) throws SQLException {
        BigDecimal d = getBigDecimal(c);
        return d == null ? null : d.setScale(scale, RoundingMode.HALF_UP);
    }

    @Override public byte[] getBytes(int i) throws SQLException { return Values.asBytes(raw(i)); }
    @Override public byte[] getBytes(String c) throws SQLException { return getBytes(findColumn(c)); }

    @Override
    public Timestamp getTimestamp(int i) throws SQLException {
        java.time.Instant t = Values.asInstant(raw(i));
        return t == null ? null : Timestamp.from(t);
    }

    @Override public Timestamp getTimestamp(String c) throws SQLException { return getTimestamp(findColumn(c)); }
    /** A timestamp is an absolute instant: the Calendar does not change it. */
    @Override public Timestamp getTimestamp(int i, Calendar cal) throws SQLException { return getTimestamp(i); }
    @Override public Timestamp getTimestamp(String c, Calendar cal) throws SQLException { return getTimestamp(c); }
    @Override public Date getDate(int i) throws SQLException { return Values.asDate(raw(i), null); }
    @Override public Date getDate(String c) throws SQLException { return getDate(findColumn(c)); }
    @Override public Date getDate(int i, Calendar cal) throws SQLException { return Values.asDate(raw(i), cal); }
    @Override public Date getDate(String c, Calendar cal) throws SQLException { return getDate(findColumn(c), cal); }
    @Override public Time getTime(int i) throws SQLException { return Values.asTime(raw(i), null); }
    @Override public Time getTime(String c) throws SQLException { return getTime(findColumn(c)); }
    @Override public Time getTime(int i, Calendar cal) throws SQLException { return Values.asTime(raw(i), cal); }
    @Override public Time getTime(String c, Calendar cal) throws SQLException { return getTime(findColumn(c), cal); }

    @Override
    public Array getArray(int i) throws SQLException {
        Object v = raw(i);
        if (v == null) return null;
        if (!(v instanceof List)) throw Errors.conversion(v, "ARRAY");
        return new SkaidbArray((List<?>) v);
    }

    @Override public Array getArray(String c) throws SQLException { return getArray(findColumn(c)); }

    @Override
    public InputStream getBinaryStream(int i) throws SQLException {
        byte[] b = getBytes(i);
        return b == null ? null : new ByteArrayInputStream(b);
    }

    @Override public InputStream getBinaryStream(String c) throws SQLException { return getBinaryStream(findColumn(c)); }

    @Override
    public InputStream getAsciiStream(int i) throws SQLException {
        String s = getString(i);
        return s == null ? null : new ByteArrayInputStream(s.getBytes(StandardCharsets.US_ASCII));
    }

    @Override public InputStream getAsciiStream(String c) throws SQLException { return getAsciiStream(findColumn(c)); }

    @Override
    @Deprecated
    public InputStream getUnicodeStream(int i) throws SQLException { throw Errors.unsupported("getUnicodeStream"); }

    @Override
    @Deprecated
    public InputStream getUnicodeStream(String c) throws SQLException { throw Errors.unsupported("getUnicodeStream"); }

    @Override
    public Reader getCharacterStream(int i) throws SQLException {
        String s = getString(i);
        return s == null ? null : new StringReader(s);
    }

    @Override public Reader getCharacterStream(String c) throws SQLException { return getCharacterStream(findColumn(c)); }
    @Override public Reader getNCharacterStream(int i) throws SQLException { return getCharacterStream(i); }
    @Override public Reader getNCharacterStream(String c) throws SQLException { return getCharacterStream(c); }

    @Override
    public URL getURL(int i) throws SQLException {
        String s = getString(i);
        if (s == null) return null;
        try {
            return new java.net.URI(s).toURL();
        } catch (MalformedURLException | java.net.URISyntaxException | IllegalArgumentException e) {
            throw new SQLDataException("not a URL: " + s, "22018", e);
        }
    }

    @Override public URL getURL(String c) throws SQLException { return getURL(findColumn(c)); }

    @Override public Ref getRef(int i) throws SQLException { throw Errors.unsupported("Ref"); }
    @Override public Ref getRef(String c) throws SQLException { throw Errors.unsupported("Ref"); }
    @Override public Blob getBlob(int i) throws SQLException { throw Errors.unsupported("Blob (use getBytes)"); }
    @Override public Blob getBlob(String c) throws SQLException { throw Errors.unsupported("Blob (use getBytes)"); }
    @Override public Clob getClob(int i) throws SQLException { throw Errors.unsupported("Clob (use getString)"); }
    @Override public Clob getClob(String c) throws SQLException { throw Errors.unsupported("Clob (use getString)"); }
    @Override public NClob getNClob(int i) throws SQLException { throw Errors.unsupported("NClob (use getString)"); }
    @Override public NClob getNClob(String c) throws SQLException { throw Errors.unsupported("NClob (use getString)"); }
    @Override public SQLXML getSQLXML(int i) throws SQLException { throw Errors.unsupported("SQLXML"); }
    @Override public SQLXML getSQLXML(String c) throws SQLException { throw Errors.unsupported("SQLXML"); }
    @Override public RowId getRowId(int i) throws SQLException { throw Errors.unsupported("RowId"); }
    @Override public RowId getRowId(String c) throws SQLException { throw Errors.unsupported("RowId"); }

    // ---- description ------------------------------------------------------------------

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        checkOpen();
        if (meta == null) meta = new SkaidbResultSetMetaData(cols, rows);
        return meta;
    }

    @Override
    public Statement getStatement() throws SQLException {
        checkOpen();
        return stmt;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkOpen();
        return null;
    }

    @Override public void clearWarnings() throws SQLException { checkOpen(); }
    @Override public String getCursorName() throws SQLException { throw Errors.unsupported("named cursors"); }
    @Override public int getType() throws SQLException { checkOpen(); return TYPE_FORWARD_ONLY; }
    @Override public int getConcurrency() throws SQLException { checkOpen(); return CONCUR_READ_ONLY; }
    @Override public int getHoldability() throws SQLException { checkOpen(); return HOLD_CURSORS_OVER_COMMIT; }
    @Override public int getFetchDirection() throws SQLException { checkOpen(); return FETCH_FORWARD; }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        checkOpen();
        if (direction != FETCH_FORWARD) throw Errors.unsupported("fetch direction " + direction);
    }

    @Override public int getFetchSize() throws SQLException { checkOpen(); return 0; }

    /** A hint the server does not take: chunk sizes are the server's. */
    @Override
    public void setFetchSize(int rows) throws SQLException {
        checkOpen();
        if (rows < 0) throw new SQLException("fetch size must be >= 0", "HY024");
    }

    // ---- read-only: no updates ---------------------------------------------------------

    private static SQLException readOnly() { return Errors.unsupported("updating a result set (CONCUR_READ_ONLY)"); }

    @Override public boolean rowUpdated() throws SQLException { throw readOnly(); }
    @Override public boolean rowInserted() throws SQLException { throw readOnly(); }
    @Override public boolean rowDeleted() throws SQLException { throw readOnly(); }
    @Override public void insertRow() throws SQLException { throw readOnly(); }
    @Override public void updateRow() throws SQLException { throw readOnly(); }
    @Override public void deleteRow() throws SQLException { throw readOnly(); }
    @Override public void refreshRow() throws SQLException { throw readOnly(); }
    @Override public void cancelRowUpdates() throws SQLException { throw readOnly(); }
    @Override public void moveToInsertRow() throws SQLException { throw readOnly(); }
    @Override public void moveToCurrentRow() throws SQLException { throw readOnly(); }
    @Override public void updateNull(int i) throws SQLException { throw readOnly(); }
    @Override public void updateNull(String c) throws SQLException { throw readOnly(); }
    @Override public void updateBoolean(int i, boolean x) throws SQLException { throw readOnly(); }
    @Override public void updateBoolean(String c, boolean x) throws SQLException { throw readOnly(); }
    @Override public void updateByte(int i, byte x) throws SQLException { throw readOnly(); }
    @Override public void updateByte(String c, byte x) throws SQLException { throw readOnly(); }
    @Override public void updateShort(int i, short x) throws SQLException { throw readOnly(); }
    @Override public void updateShort(String c, short x) throws SQLException { throw readOnly(); }
    @Override public void updateInt(int i, int x) throws SQLException { throw readOnly(); }
    @Override public void updateInt(String c, int x) throws SQLException { throw readOnly(); }
    @Override public void updateLong(int i, long x) throws SQLException { throw readOnly(); }
    @Override public void updateLong(String c, long x) throws SQLException { throw readOnly(); }
    @Override public void updateFloat(int i, float x) throws SQLException { throw readOnly(); }
    @Override public void updateFloat(String c, float x) throws SQLException { throw readOnly(); }
    @Override public void updateDouble(int i, double x) throws SQLException { throw readOnly(); }
    @Override public void updateDouble(String c, double x) throws SQLException { throw readOnly(); }
    @Override public void updateBigDecimal(int i, BigDecimal x) throws SQLException { throw readOnly(); }
    @Override public void updateBigDecimal(String c, BigDecimal x) throws SQLException { throw readOnly(); }
    @Override public void updateString(int i, String x) throws SQLException { throw readOnly(); }
    @Override public void updateString(String c, String x) throws SQLException { throw readOnly(); }
    @Override public void updateNString(int i, String x) throws SQLException { throw readOnly(); }
    @Override public void updateNString(String c, String x) throws SQLException { throw readOnly(); }
    @Override public void updateBytes(int i, byte[] x) throws SQLException { throw readOnly(); }
    @Override public void updateBytes(String c, byte[] x) throws SQLException { throw readOnly(); }
    @Override public void updateDate(int i, Date x) throws SQLException { throw readOnly(); }
    @Override public void updateDate(String c, Date x) throws SQLException { throw readOnly(); }
    @Override public void updateTime(int i, Time x) throws SQLException { throw readOnly(); }
    @Override public void updateTime(String c, Time x) throws SQLException { throw readOnly(); }
    @Override public void updateTimestamp(int i, Timestamp x) throws SQLException { throw readOnly(); }
    @Override public void updateTimestamp(String c, Timestamp x) throws SQLException { throw readOnly(); }
    @Override public void updateAsciiStream(int i, InputStream x, int n) throws SQLException { throw readOnly(); }
    @Override public void updateAsciiStream(String c, InputStream x, int n) throws SQLException { throw readOnly(); }
    @Override public void updateAsciiStream(int i, InputStream x, long n) throws SQLException { throw readOnly(); }
    @Override public void updateAsciiStream(String c, InputStream x, long n) throws SQLException { throw readOnly(); }
    @Override public void updateAsciiStream(int i, InputStream x) throws SQLException { throw readOnly(); }
    @Override public void updateAsciiStream(String c, InputStream x) throws SQLException { throw readOnly(); }
    @Override public void updateBinaryStream(int i, InputStream x, int n) throws SQLException { throw readOnly(); }
    @Override public void updateBinaryStream(String c, InputStream x, int n) throws SQLException { throw readOnly(); }
    @Override public void updateBinaryStream(int i, InputStream x, long n) throws SQLException { throw readOnly(); }
    @Override public void updateBinaryStream(String c, InputStream x, long n) throws SQLException { throw readOnly(); }
    @Override public void updateBinaryStream(int i, InputStream x) throws SQLException { throw readOnly(); }
    @Override public void updateBinaryStream(String c, InputStream x) throws SQLException { throw readOnly(); }
    @Override public void updateCharacterStream(int i, Reader x, int n) throws SQLException { throw readOnly(); }
    @Override public void updateCharacterStream(String c, Reader x, int n) throws SQLException { throw readOnly(); }
    @Override public void updateCharacterStream(int i, Reader x, long n) throws SQLException { throw readOnly(); }
    @Override public void updateCharacterStream(String c, Reader x, long n) throws SQLException { throw readOnly(); }
    @Override public void updateCharacterStream(int i, Reader x) throws SQLException { throw readOnly(); }
    @Override public void updateCharacterStream(String c, Reader x) throws SQLException { throw readOnly(); }
    @Override public void updateNCharacterStream(int i, Reader x, long n) throws SQLException { throw readOnly(); }
    @Override public void updateNCharacterStream(String c, Reader x, long n) throws SQLException { throw readOnly(); }
    @Override public void updateNCharacterStream(int i, Reader x) throws SQLException { throw readOnly(); }
    @Override public void updateNCharacterStream(String c, Reader x) throws SQLException { throw readOnly(); }
    @Override public void updateObject(int i, Object x, int n) throws SQLException { throw readOnly(); }
    @Override public void updateObject(String c, Object x, int n) throws SQLException { throw readOnly(); }
    @Override public void updateObject(int i, Object x) throws SQLException { throw readOnly(); }
    @Override public void updateObject(String c, Object x) throws SQLException { throw readOnly(); }
    @Override public void updateRef(int i, Ref x) throws SQLException { throw readOnly(); }
    @Override public void updateRef(String c, Ref x) throws SQLException { throw readOnly(); }
    @Override public void updateBlob(int i, Blob x) throws SQLException { throw readOnly(); }
    @Override public void updateBlob(String c, Blob x) throws SQLException { throw readOnly(); }
    @Override public void updateBlob(int i, InputStream x, long n) throws SQLException { throw readOnly(); }
    @Override public void updateBlob(String c, InputStream x, long n) throws SQLException { throw readOnly(); }
    @Override public void updateBlob(int i, InputStream x) throws SQLException { throw readOnly(); }
    @Override public void updateBlob(String c, InputStream x) throws SQLException { throw readOnly(); }
    @Override public void updateClob(int i, Clob x) throws SQLException { throw readOnly(); }
    @Override public void updateClob(String c, Clob x) throws SQLException { throw readOnly(); }
    @Override public void updateClob(int i, Reader x, long n) throws SQLException { throw readOnly(); }
    @Override public void updateClob(String c, Reader x, long n) throws SQLException { throw readOnly(); }
    @Override public void updateClob(int i, Reader x) throws SQLException { throw readOnly(); }
    @Override public void updateClob(String c, Reader x) throws SQLException { throw readOnly(); }
    @Override public void updateNClob(int i, NClob x) throws SQLException { throw readOnly(); }
    @Override public void updateNClob(String c, NClob x) throws SQLException { throw readOnly(); }
    @Override public void updateNClob(int i, Reader x, long n) throws SQLException { throw readOnly(); }
    @Override public void updateNClob(String c, Reader x, long n) throws SQLException { throw readOnly(); }
    @Override public void updateNClob(int i, Reader x) throws SQLException { throw readOnly(); }
    @Override public void updateNClob(String c, Reader x) throws SQLException { throw readOnly(); }
    @Override public void updateArray(int i, Array x) throws SQLException { throw readOnly(); }
    @Override public void updateArray(String c, Array x) throws SQLException { throw readOnly(); }
    @Override public void updateRowId(int i, RowId x) throws SQLException { throw readOnly(); }
    @Override public void updateRowId(String c, RowId x) throws SQLException { throw readOnly(); }
    @Override public void updateSQLXML(int i, SQLXML x) throws SQLException { throw readOnly(); }
    @Override public void updateSQLXML(String c, SQLXML x) throws SQLException { throw readOnly(); }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) return iface.cast(this);
        throw new SQLException("not a wrapper for " + iface.getName(), "HY000");
    }

    @Override public boolean isWrapperFor(Class<?> iface) { return iface.isInstance(this); }
}
