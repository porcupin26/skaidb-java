package com.skaidb.jdbc;

import com.skaidb.Skaidb;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLTransactionRollbackException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * A JDBC connection over one {@link Skaidb.Connection}. Seed failover, TLS,
 * the session database and reconnection after a transport failure all come
 * from the underlying connection.
 *
 * <p>Transactions: with {@code setAutoCommit(false)} the first statement
 * opens a transaction ({@code BEGIN}, or {@code BEGIN ATOMIC} with {@code
 * transaction=atomic}) and {@code commit()} / {@code rollback()} end it.
 * Plain {@code BEGIN} needs a standalone server; a cluster refuses it and
 * needs {@code transaction=atomic}. DDL is not transactional. If the
 * connection breaks mid-transaction the server has discarded it: the next
 * statement or {@code commit()} throws {@link SQLTransactionRollbackException}.
 */
public final class SkaidbConnection implements Connection {
    private final Skaidb.Connection conn;
    private final JdbcUrl cfg;
    private final String url;
    private volatile boolean closed = false;
    private boolean autoCommit = true;
    private boolean txnOpen = false;
    private long txnReconnects = 0;
    private boolean readOnly = false;
    private String catalog;
    private int holdability = ResultSet.HOLD_CURSORS_OVER_COMMIT;
    private final Properties clientInfo = new Properties();
    private SQLWarning warnings;

    SkaidbConnection(Skaidb.Connection conn, JdbcUrl cfg, String url) {
        this.conn = conn;
        this.cfg = cfg;
        this.url = url;
        this.catalog = cfg.database.isEmpty() ? null : cfg.database;
        if (cfg.readTimeoutMs > 0) conn.setReadTimeout(cfg.readTimeoutMs);
    }

    /** The underlying driver connection ({@code unwrap(Skaidb.Connection.class)} returns it too). */
    public Skaidb.Connection getSkaidbConnection() { return conn; }

    /**
     * Set the consistency level of later statements on this connection:
     * {@code one}, {@code quorum} or {@code all}.
     */
    public void setConsistency(String level) throws SQLException {
        checkOpen();
        conn.setConsistency(consistencyOf(level));
    }

    static int consistencyOf(String level) throws SQLException {
        switch (level == null ? "" : level.trim().toLowerCase(java.util.Locale.ROOT)) {
            case "one": return Skaidb.CONSISTENCY_ONE;
            case "quorum": return Skaidb.CONSISTENCY_QUORUM;
            case "all": return Skaidb.CONSISTENCY_ALL;
            default: throw new SQLException("consistency must be one, quorum or all, got " + level, "22023");
        }
    }

    String url() { return url; }
    JdbcUrl config() { return cfg; }

    void checkOpen() throws SQLException {
        if (closed) throw Errors.closed("connection");
    }

    /**
     * Called before every statement: opens the lazy transaction, and refuses
     * to run inside one whose connection has been lost (the statement would
     * otherwise autocommit on the re-dialled socket).
     */
    void beforeStatement() throws SQLException {
        checkOpen();
        if (txnOpen) {
            checkTransactionAlive();
        } else if (!autoCommit) {
            runControl(cfg.atomicTransactions ? "BEGIN ATOMIC" : "BEGIN");
            txnOpen = true;
            txnReconnects = conn.reconnects();
        }
    }

    private void checkTransactionAlive() throws SQLException {
        if (!conn.isUsable() || conn.reconnects() != txnReconnects) {
            txnOpen = false;
            throw new SQLTransactionRollbackException(
                "the connection was lost during the transaction; the server discarded it", "40003");
        }
    }

    private void runControl(String sql) throws SQLException {
        try {
            conn.executeRaw(sql);
        } catch (RuntimeException e) {
            throw Errors.map(e);
        }
    }

    Skaidb.Connection nativeConnection() { return conn; }

    // ---- statements ----------------------------------------------------------

    @Override
    public Statement createStatement() throws SQLException {
        checkOpen();
        return new SkaidbStatement(this);
    }

    @Override
    public Statement createStatement(int type, int concurrency) throws SQLException {
        checkCursor(type, concurrency, holdability);
        return createStatement();
    }

    @Override
    public Statement createStatement(int type, int concurrency, int holdability) throws SQLException {
        checkCursor(type, concurrency, holdability);
        return createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        checkOpen();
        return new SkaidbPreparedStatement(this, sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int type, int concurrency) throws SQLException {
        checkCursor(type, concurrency, holdability);
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int type, int concurrency, int holdability) throws SQLException {
        checkCursor(type, concurrency, holdability);
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        if (autoGeneratedKeys != Statement.NO_GENERATED_KEYS)
            throw Errors.unsupported("generated keys (use INSERT ... RETURNING)");
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw Errors.unsupported("generated keys (use INSERT ... RETURNING)");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throw Errors.unsupported("generated keys (use INSERT ... RETURNING)");
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw Errors.unsupported("CallableStatement (run CALL through a Statement or PreparedStatement)");
    }

    @Override
    public CallableStatement prepareCall(String sql, int type, int concurrency) throws SQLException {
        return prepareCall(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int type, int concurrency, int holdability) throws SQLException {
        return prepareCall(sql);
    }

    private void checkCursor(int type, int concurrency, int hold) throws SQLException {
        checkOpen();
        if (type != ResultSet.TYPE_FORWARD_ONLY) throw Errors.unsupported("scrollable result sets");
        if (concurrency != ResultSet.CONCUR_READ_ONLY) throw Errors.unsupported("updatable result sets");
        if (hold != ResultSet.HOLD_CURSORS_OVER_COMMIT && hold != ResultSet.CLOSE_CURSORS_AT_COMMIT)
            throw new SQLException("bad holdability " + hold, "HY092");
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        checkOpen();
        return sql;
    }

    // ---- transactions ----------------------------------------------------------

    @Override
    public void setAutoCommit(boolean on) throws SQLException {
        checkOpen();
        if (on == autoCommit) return;
        if (on && txnOpen) commit();   // JDBC: switching autocommit on commits
        autoCommit = on;
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        checkOpen();
        return autoCommit;
    }

    @Override
    public void commit() throws SQLException {
        checkOpen();
        if (autoCommit) throw new SQLException("commit() with autoCommit on", "25000");
        if (!txnOpen) return;
        checkTransactionAlive();
        txnOpen = false;
        runControl("COMMIT");
    }

    @Override
    public void rollback() throws SQLException {
        checkOpen();
        if (autoCommit) throw new SQLException("rollback() with autoCommit on", "25000");
        if (!txnOpen) return;
        txnOpen = false;
        if (!conn.isUsable() || conn.reconnects() != txnReconnects) return;   // already gone
        runControl("ROLLBACK");
    }

    @Override
    public Savepoint setSavepoint() throws SQLException { throw Errors.unsupported("savepoints"); }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException { throw Errors.unsupported("savepoints"); }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException { throw Errors.unsupported("savepoints"); }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException { throw Errors.unsupported("savepoints"); }

    /** READ COMMITTED: a transaction reads committed data plus its own buffered writes. */
    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        checkOpen();
        if (level != TRANSACTION_READ_COMMITTED) throw Errors.unsupported("transaction isolation level " + level);
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        checkOpen();
        return TRANSACTION_READ_COMMITTED;
    }

    // ---- state ----------------------------------------------------------------

    @Override
    public void close() throws SQLException {
        if (closed) return;
        try {
            if (txnOpen && conn.isUsable() && conn.reconnects() == txnReconnects) {
                try {
                    conn.executeRaw("ROLLBACK");
                } catch (RuntimeException ignored) {
                    // closing anyway; the server discards an unfinished transaction
                }
            }
        } finally {
            txnOpen = false;
            closed = true;
            conn.close();
        }
    }

    @Override
    public boolean isClosed() { return closed; }

    /** A round trip ({@code SELECT 1}) bounded by {@code timeout} seconds (0 = no bound). */
    @Override
    public boolean isValid(int timeout) throws SQLException {
        if (timeout < 0) throw new SQLException("timeout must be >= 0", "HY092");
        if (closed || !conn.isUsable()) return false;
        int before = conn.getReadTimeout();
        try {
            if (timeout > 0) conn.setReadTimeout(timeout * 1000);
            conn.executeRaw("SELECT 1");
            return true;
        } catch (RuntimeException e) {
            return false;
        } finally {
            try {
                conn.setReadTimeout(before);
            } catch (RuntimeException ignored) {
                // a closed connection answers false above
            }
        }
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        checkOpen();
        return new SkaidbDatabaseMetaData(this);
    }

    /** A hint only: skaidb has no read-only sessions. */
    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        checkOpen();
        this.readOnly = readOnly;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        checkOpen();
        return readOnly;
    }

    /** Switches the session database ({@code USE}); it is re-entered after a reconnect only if it came from the URL. */
    @Override
    public void setCatalog(String catalog) throws SQLException {
        checkOpen();
        if (catalog == null || catalog.equals(this.catalog)) return;
        runControl("USE \"" + catalog.replace("\"", "\"\"") + "\"");
        this.catalog = catalog;
    }

    /** The session database, or the server's current one when none was chosen. */
    @Override
    public String getCatalog() throws SQLException {
        checkOpen();
        if (catalog != null) return catalog;
        try {
            Skaidb.ResultSet rs = conn.executeRaw("SHOW DATABASES").getResultSet();
            while (rs != null && rs.next()) {
                if ("*".equals(rs.getString("current"))) return rs.getString("database");
            }
        } catch (RuntimeException e) {
            throw Errors.map(e);
        }
        return null;
    }

    /** skaidb has no schemas: ignored. */
    @Override
    public void setSchema(String schema) throws SQLException { checkOpen(); }

    @Override
    public String getSchema() throws SQLException {
        checkOpen();
        return null;
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        checkCursor(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, holdability);
        this.holdability = holdability;
    }

    @Override
    public int getHoldability() throws SQLException {
        checkOpen();
        return holdability;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkOpen();
        return warnings;
    }

    @Override
    public void clearWarnings() throws SQLException {
        checkOpen();
        warnings = null;
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        checkOpen();
        return Collections.emptyMap();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        if (map != null && !map.isEmpty()) throw Errors.unsupported("type maps");
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        if (value == null) clientInfo.remove(name);
        else clientInfo.setProperty(name, value);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        clientInfo.clear();
        if (properties != null) clientInfo.putAll(properties);
    }

    /** Kept locally; skaidb has no client-info properties to send them to. */
    @Override
    public String getClientInfo(String name) throws SQLException {
        checkOpen();
        return clientInfo.getProperty(name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        checkOpen();
        Properties p = new Properties();
        p.putAll(clientInfo);
        return p;
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        if (executor == null) throw new SQLException("executor is null", "HY009");
        closed = true;
        txnOpen = false;
        conn.close();
    }

    /** Bounds every read (the same as the {@code read_timeout} URL key). */
    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        checkOpen();
        if (milliseconds < 0) throw new SQLException("timeout must be >= 0", "HY092");
        conn.setReadTimeout(milliseconds);
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        checkOpen();
        return conn.getReadTimeout();
    }

    // ---- LOB and structured factories --------------------------------------------

    /** An array parameter value; any element type the driver binds. */
    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        checkOpen();
        return new SkaidbArray(elements == null ? null : Arrays.asList(elements.clone()));
    }

    @Override
    public Clob createClob() throws SQLException { throw Errors.unsupported("Clob"); }

    @Override
    public Blob createBlob() throws SQLException { throw Errors.unsupported("Blob"); }

    @Override
    public NClob createNClob() throws SQLException { throw Errors.unsupported("NClob"); }

    @Override
    public SQLXML createSQLXML() throws SQLException { throw Errors.unsupported("SQLXML"); }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw Errors.unsupported("Struct (bind a java.util.Map as a document)");
    }

    // ---- wrapper ------------------------------------------------------------------

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) return iface.cast(this);
        if (iface.isInstance(conn)) return iface.cast(conn);
        throw new SQLException("not a wrapper for " + iface.getName(), "HY000");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return iface.isInstance(this) || iface.isInstance(conn);
    }
}
