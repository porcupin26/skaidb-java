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
 * statement or {@code commit()} throws {@link SQLTransactionRollbackException},
 * and so does a metadata call or {@code setCatalog} made there — nothing
 * re-dials until that has been reported.
 * A streamed result set that is still open makes the connection busy, not
 * lost: another statement or {@code commit()} throws SQLState {@code HY010}
 * and the transaction stays open, and {@code rollback()} closes the result
 * set and rolls back.
 * A {@code commit()} the server refuses because another writer changed a row
 * the transaction writes throws it too, with SQLState {@code 40001}: nothing
 * was committed, the transaction is over and the connection is usable, so
 * the transaction can be run again.
 */
public final class SkaidbConnection implements Connection {
    private final Skaidb.Connection conn;
    private final JdbcUrl cfg;
    private final String url;
    private volatile boolean closed = false;
    private boolean autoCommit = true;
    private boolean txnOpen = false;
    private long txnReconnects = 0;
    /** The streamed result set that last took the wire; see {@link #busy()}. */
    private SkaidbResultSet streamed;
    private boolean readOnly = false;
    /** The session as the current request found it (see {@link #beginRequest}); null between requests. */
    private Skaidb.Session requestStart;
    private int holdability = ResultSet.HOLD_CURSORS_OVER_COMMIT;
    private final Properties clientInfo = new Properties();
    private SQLWarning warnings;
    /**
     * {@code conn.reconnects()} when the server refused a parameterised
     * stream (older than 0.330), else -1. A re-dial may reach another
     * member, so the refusal holds until the next one.
     */
    private long paramStreamsRefusedAt = -1;

    SkaidbConnection(Skaidb.Connection conn, JdbcUrl cfg, String url) {
        this.conn = conn;
        this.cfg = cfg;
        this.url = url;
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

    /**
     * Called before a statement the driver sends on its own account — a
     * metadata read, the {@code USE} of {@link #setCatalog}. It opens no
     * transaction, but inside one whose connection has been lost it is
     * refused like any other statement: sent, it would re-dial, and a
     * transaction's connection is not replaced behind its back.
     */
    void beforeControl() throws SQLException {
        checkOpen();
        if (txnOpen) checkTransactionAlive();
    }

    private void checkTransactionAlive() throws SQLException {
        if (!(conn.isUsable() || busy()) || conn.reconnects() != txnReconnects) {
            txnOpen = false;
            endLostTransaction();
            throw new SQLTransactionRollbackException(
                "the connection was lost during the transaction; the server discarded it", "40003");
        }
    }

    /**
     * The transaction is over for this connection, and so it has to be
     * for the core connection under it, which noted the {@code BEGIN} and
     * holds every statement back from a new session until the transaction
     * is ended: a {@code ROLLBACK} ends it there. Nothing is sent — the
     * connection that had the transaction is gone, and the core
     * connection answers for it.
     */
    private void endLostTransaction() {
        if (conn.isUsable()) return;   // on a session of its own already: nothing is held back
        try {
            conn.executeRaw("ROLLBACK");
        } catch (RuntimeException ignored) {
            // nothing was open, or the connection is closed
        }
    }

    /** A statement's streamed result set took the wire. */
    void streaming(SkaidbResultSet rs) { streamed = rs; }

    /**
     * True while a streamed result set owns the wire. The connection is
     * then unusable for anything else but not lost: the transaction is
     * still open on it, and a statement, COMMIT or ROLLBACK sent now is
     * refused before it leaves ({@code connection is busy streaming}).
     */
    private boolean busy() {
        if (streamed != null && !streamed.streaming()) streamed = null;
        return streamed != null;
    }

    private void runControl(String sql) throws SQLException {
        try {
            conn.executeRaw(sql);
        } catch (RuntimeException e) {
            throw failed(e);
        }
    }

    /**
     * The SQLException for a failure of the core connection. One that
     * reports the transaction's connection lost (SQLState 40003) — the
     * core connection found it gone before it sent the statement — is the
     * report {@link #checkTransactionAlive} would have made: the
     * transaction is over, here and for the core connection.
     */
    SQLException failed(RuntimeException e) {
        SQLException mapped = Errors.map(e);
        if (txnOpen && "40003".equals(mapped.getSQLState())) {
            txnOpen = false;
            endLostTransaction();
        }
        return mapped;
    }

    Skaidb.Connection nativeConnection() { return conn; }

    /** True while the server is known to refuse parameterised streams (see {@link #noteParameterStreamRefusal}). */
    boolean refusesParameterStreams() { return paramStreamsRefusedAt == conn.reconnects(); }

    /**
     * Whether {@code e} is the server refusing a parameterised stream
     * because it is older than 0.330 — it did not run the statement. If so,
     * that is remembered until the next re-dial.
     */
    boolean noteParameterStreamRefusal(SQLException e) {
        String m = e.getMessage() == null ? "" : e.getMessage();
        if (!m.startsWith("server does not support parameterised streaming")) return false;
        paramStreamsRefusedAt = conn.reconnects();
        return true;
    }

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
        // A COMMIT that is sent ends the transaction, whatever answers it.
        // Behind an open streamed result set it is not sent (HY010), and
        // the transaction stays open.
        if (!busy()) txnOpen = false;
        runControl("COMMIT");
    }

    @Override
    public void rollback() throws SQLException {
        checkOpen();
        if (autoCommit) throw new SQLException("rollback() with autoCommit on", "25000");
        if (!txnOpen) return;
        // A rollback ends what the transaction was reading too: closing a
        // streamed result set gives the wire back for the ROLLBACK, or
        // breaks the connection, and the server drops the transaction
        // with the socket. Skipping the ROLLBACK instead would leave the
        // transaction open for later statements to run in.
        if (busy()) streamed.closeQuietly();
        txnOpen = false;
        if (!conn.isUsable() || conn.reconnects() != txnReconnects) {
            endLostTransaction();   // already gone with its connection
            return;
        }
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

    // ---- request boundaries -------------------------------------------------------

    /**
     * A pool lends the connection out (JDBC 4.3). The session as it is
     * now — its database, its {@code SET CONSISTENCY} override, its scan
     * budget — is the one {@link #endRequest} puts the connection back to.
     * Not nested: until the request ends, further calls change nothing.
     */
    @Override
    public void beginRequest() throws SQLException {
        checkOpen();
        if (requestStart == null) requestStart = conn.session();
    }

    /**
     * The pool takes the connection back (JDBC 4.3): the next borrower
     * must find the session this request found, not the one it leaves. A
     * streamed result set still open is closed, a transaction still open
     * is rolled back, and what the request changed with {@code
     * setCatalog}, a {@code USE}, a {@code SET CONSISTENCY} or a {@code
     * SET SCAN BUDGET} is put back; a request that changed nothing costs
     * no round trip. If the session cannot be put back — the connection
     * is gone, or the server refuses (one older than 0.349 does not know
     * {@code SET CONSISTENCY DEFAULT}) — the connection is closed: it is
     * in nobody's session, and {@link #isClosed} and {@link #isValid}
     * tell the pool to drop it.
     */
    @Override
    public void endRequest() throws SQLException {
        Skaidb.Session start = requestStart;
        requestStart = null;
        if (closed || start == null) return;
        if (busy()) streamed.closeQuietly();
        boolean back;
        try {
            if (txnOpen) rollback();
            back = conn.restore(start);
        } catch (SQLException | RuntimeException e) {
            back = false;
        }
        if (!back) {
            txnOpen = false;
            closed = true;
            conn.close();
        }
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

    /**
     * A round trip ({@code SELECT 1}) bounded by {@code timeout} seconds (0
     * = no bound). While a streamed result set is open the connection
     * cannot be asked anything, and is not asked: it is valid — the
     * stream is being read from it — and the answer is {@code true}.
     */
    @Override
    public boolean isValid(int timeout) throws SQLException {
        if (timeout < 0) throw new SQLException("timeout must be >= 0", "HY092");
        if (closed) return false;
        if (busy()) return true;
        if (!conn.isUsable()) return false;
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

    /**
     * Switches the session database ({@code USE}); nothing is sent when
     * the connection is in it already. The connection enters it again
     * after a reconnect, as it does the URL's database or one a {@code
     * USE} statement entered: whichever came last.
     */
    @Override
    public void setCatalog(String catalog) throws SQLException {
        checkOpen();
        if (catalog == null || catalog.equals(conn.database())) return;
        beforeControl();
        runControl("USE \"" + catalog.replace("\"", "\"\"") + "\"");
    }

    /**
     * The database the connection is in: the URL's, or the one entered
     * last by {@link #setCatalog} or by a {@code USE} sent through a
     * statement. When none was chosen, the server's current one.
     */
    @Override
    public String getCatalog() throws SQLException {
        checkOpen();
        String entered = conn.database();
        if (!entered.isEmpty()) return entered;
        beforeControl();
        try {
            Skaidb.ResultSet rs = conn.executeRaw("SHOW DATABASES").getResultSet();
            while (rs != null && rs.next()) {
                if ("*".equals(rs.getString("current"))) return rs.getString("database");
            }
        } catch (RuntimeException e) {
            throw failed(e);
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
