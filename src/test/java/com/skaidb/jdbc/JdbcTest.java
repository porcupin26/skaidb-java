package com.skaidb.jdbc;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.skaidb.ConformanceServer;
import com.skaidb.Skaidb;
import java.math.BigDecimal;
import java.net.ServerSocket;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The JDBC layer through {@link DriverManager}, against the conformance
 * harness's fake server: the vectors' reference exchanges where they fit,
 * scripted ones (built from the wire spec) for the rest.
 */
class JdbcTest {

    static final int QUORUM = Skaidb.CONSISTENCY_QUORUM;

    static Properties creds() {
        Properties p = new Properties();
        p.setProperty("user", ConformanceServer.username());
        p.setProperty("password", ConformanceServer.password());
        return p;
    }

    static String url(ConformanceServer s, String rest) {
        return "jdbc:skaidb://127.0.0.1:" + s.port() + rest;
    }

    static ConformanceServer vectorCase(String name) throws Exception {
        return new ConformanceServer("ok", ConformanceServer.exchangesOf(ConformanceServer.caseNamed(name)));
    }

    static ConformanceServer.Exchange ex(byte[] request, byte[]... responses) {
        return new ConformanceServer.Exchange(request, List.of(responses));
    }

    static ConformanceServer.Exchange query(String sql, byte[]... responses) {
        return ex(ConformanceServer.queryRequest(sql, QUORUM), responses);
    }

    static void finish(ConformanceServer s) throws Exception {
        assertNull(s.finish());
    }

    // ---- registration and URLs --------------------------------------------------------

    @Test
    void discoveredThroughServiceLoaderAndDriverManager() throws SQLException {
        boolean found = false;
        for (Driver d : ServiceLoader.load(Driver.class)) found |= d instanceof SkaidbDriver;
        assertTrue(found, "META-INF/services/java.sql.Driver lists SkaidbDriver");
        assertInstanceOf(SkaidbDriver.class, DriverManager.getDriver("jdbc:skaidb://localhost"));
        SkaidbDriver d = new SkaidbDriver();
        assertTrue(d.acceptsURL("jdbc:skaidb://h:7000/db"));
        assertFalse(d.acceptsURL("jdbc:postgresql://h/db"));
        assertNull(d.connect("jdbc:postgresql://h/db", new Properties()), "a foreign URL is not ours");
        assertEquals(Skaidb.VERSION.split("\\.")[0], Integer.toString(d.getMajorVersion()));
        assertFalse(d.jdbcCompliant());
        assertTrue(d.getPropertyInfo("jdbc:skaidb://h", new Properties()).length >= 10);
    }

    @Test
    void urlParsing() throws SQLException {
        JdbcUrl u = JdbcUrl.parse(
            "jdbc:skaidb://h1:7001,h2,[::1]:7003/my%20db?user=ada&password=p%40ss+w%26rd&consistency=all&tls=true"
                + "&transaction=atomic&read_timeout=1500&fetch_size=100", null);
        assertEquals("my db", u.database);
        assertEquals("ada", u.values.get("user"));
        assertEquals("p@ss+w&rd", u.values.get("password"), "percent-decoded, '+' kept");
        assertEquals("all", u.values.get("consistency"));
        assertTrue(u.atomicTransactions);
        assertEquals(1500, u.readTimeoutMs);
        assertEquals(100, u.fetchSize);

        Properties over = new Properties();
        over.setProperty("password", "from-props");
        over.setProperty("hikari.whatever", "ignored");
        over.setProperty("CONSISTENCY", "one");
        JdbcUrl o = JdbcUrl.parse("jdbc:skaidb://h/?password=from-url&consistency=all", over);
        assertEquals("from-props", o.values.get("password"), "Properties override the URL");
        assertEquals("one", o.values.get("consistency"));
        assertEquals("", o.database);
        assertFalse(o.atomicTransactions);

        for (String bad : new String[] {
                "jdbc:skaidb:h", "jdbc:skaidb://", "jdbc:skaidb://h/?nope=1", "jdbc:skaidb://u:p@h/",
                "jdbc:skaidb://h/?consistency=eventual", "jdbc:skaidb://h/?transaction=serial",
                "jdbc:skaidb://h/?read_timeout=-1", "jdbc:skaidb://h/?tls=maybe", "jdbc:skaidb://h/?x=%zz",
                "jdbc:skaidb://h/?auth_mechanism=kerberos" }) {
            SQLException e = assertThrows(SQLException.class, () -> JdbcUrl.parse(bad, null), bad);
            assertEquals("08001", e.getSQLState(), bad);
        }
        // Checked when dialling, before any socket is opened.
        SQLException cert = assertThrows(SQLException.class,
            () -> DriverManager.getConnection("jdbc:skaidb://127.0.0.1:1/?auth_mechanism=certificate"));
        assertEquals("08001", cert.getSQLState());
        assertTrue(cert.getMessage().contains("client certificate"), cert.getMessage());
    }

    @Test
    void seedFailoverAndConsistencyFromTheUrl() throws Exception {
        int dead;
        try (ServerSocket s = new ServerSocket(0)) { dead = s.getLocalPort(); }
        ConformanceServer server = vectorCase("query_consistency_one");
        try (Connection c = DriverManager.getConnection(
                "jdbc:skaidb://127.0.0.1:" + dead + ",127.0.0.1:" + server.port() + "/?consistency=one", creds());
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT 1")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("x"));
            assertEquals(1, rs.getInt("X"), "labels match case-insensitively");
            assertFalse(rs.next());
        }
        finish(server);
    }

    // ---- statements -------------------------------------------------------------------

    @Test
    void queryThroughDriverManager() throws Exception {
        ConformanceServer server = vectorCase("query_rows_several");
        try (Connection c = DriverManager.getConnection(url(server, ""), creds());
             Statement st = c.createStatement()) {
            assertTrue(st.execute("SELECT id, name FROM t"));
            assertEquals(-1, st.getUpdateCount());
            ResultSet rs = st.getResultSet();
            ResultSetMetaData md = rs.getMetaData();
            assertEquals(2, md.getColumnCount());
            assertEquals("id", md.getColumnName(1));
            assertEquals(Types.BIGINT, md.getColumnType(1));
            assertEquals(Types.VARCHAR, md.getColumnType(2), "inferred from the non-NULL values");
            assertEquals("java.lang.Long", md.getColumnClassName(1));
            List<String> seen = new ArrayList<>();
            while (rs.next()) {
                long id = rs.getLong(1);
                String name = rs.getString("name");
                seen.add(id + "=" + (rs.wasNull() ? "NULL" : name));
                assertEquals(rs.getRow(), (int) id);
            }
            assertEquals(List.of("1=a", "2=NULL", "3=c"), seen);
            assertFalse(st.getMoreResults());
            assertEquals(-1, st.getUpdateCount());
        }
        finish(server);
    }

    @Test
    void preparedStatementBindsTypedParameters() throws Exception {
        ConformanceServer server = vectorCase("prepare_execute");
        try (Connection c = DriverManager.getConnection(url(server, "?user=" + ConformanceServer.username()),
                passwordOnly());
             PreparedStatement ps = c.prepareStatement(
                 "SELECT * FROM t WHERE a = ? AND b = ? AND c = ? AND d = ? AND e = ? AND f = ?")) {
            assertEquals(6, ps.getParameterMetaData().getParameterCount());
            ps.setInt(1, 7);
            ps.setString(2, "o'neil");
            ps.setDouble(3, 2.5);
            ps.setBoolean(4, true);
            ps.setNull(5, Types.VARCHAR);
            SQLException missing = assertThrows(SQLException.class, ps::executeQuery);
            assertEquals("07001", missing.getSQLState());
            ps.setBytes(6, new byte[] { (byte) 0xde, (byte) 0xad });
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean("ok"));
                assertEquals(Boolean.TRUE, rs.getObject(1));
            }
            assertEquals("07009", assertThrows(SQLException.class, () -> ps.setInt(7, 1)).getSQLState());
            assertThrows(SQLException.class, () -> ps.executeQuery("SELECT 1"));
        }
        finish(server);
    }

    static Properties passwordOnly() {
        Properties p = new Properties();
        p.setProperty("password", ConformanceServer.password());
        return p;
    }

    @Test
    void executeBatchIsOneRoundTrip() throws Exception {
        ConformanceServer server = vectorCase("execute_batch");
        try (Connection c = DriverManager.getConnection(url(server, ""), creds());
             PreparedStatement ps = c.prepareStatement("INSERT INTO t (id, name) VALUES (?, ?)")) {
            ps.setLong(1, 1);
            ps.setString(2, "x");
            ps.addBatch();
            ps.setObject(1, 2);
            ps.setObject(2, "y", Types.VARCHAR);
            ps.addBatch();
            ps.setLong(1, 3);
            ps.setNull(2, Types.VARCHAR);
            ps.addBatch();
            int[] counts = ps.executeBatch();
            assertArrayEquals(new int[] { Statement.SUCCESS_NO_INFO, Statement.SUCCESS_NO_INFO, Statement.SUCCESS_NO_INFO }, counts);
            assertEquals(3, ps.getLargeUpdateCount(), "the server's total");
            assertEquals(0, ps.executeBatch().length, "the batch was cleared");
        }
        finish(server);
        List<byte[]> reqs = server.requests();
        assertEquals(2, reqs.size(), "OP_PREPARE + ONE OP_EXECUTE_BATCH");
        assertEquals(7, reqs.get(1)[0]);
    }

    @Test
    void batchFailureIsABatchUpdateException() throws Exception {
        String sql = "INSERT INTO t (id) VALUES (?)";
        byte[] batch = ConformanceServer.batchRequest(3, QUORUM, new Object[] { 1L }, new Object[] { 1L });
        ConformanceServer server = new ConformanceServer("ok", List.of(
            ex(ConformanceServer.prepareRequest(sql), ConformanceServer.preparedResponse(3, 1)),
            ex(batch, ConformanceServer.errorResponse("unique violation: index \"pk\" already has 1 (batch row 2)"))));
        try (Connection c = DriverManager.getConnection(url(server, ""), creds());
             PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setLong(1, 1);
            ps.addBatch();
            ps.addBatch();
            BatchUpdateException e = assertThrows(BatchUpdateException.class, ps::executeBatch);
            assertEquals("23505", e.getSQLState());
            assertInstanceOf(SQLIntegrityConstraintViolationException.class, e.getCause());
        }
        finish(server);
    }

    @Test
    void updateCountsAndDdl() throws Exception {
        ConformanceServer server = new ConformanceServer("ok", List.of(
            query("UPDATE t SET x = 1", ConformanceServer.mutationResponse(42)),
            query("CREATE TABLE t (PRIMARY KEY (id))", ConformanceServer.ddlResponse()),
            query("SELECT 1", ConformanceServer.rowsResponse(new String[] { "x" }, new Object[] { 1L }))));
        try (Connection c = DriverManager.getConnection(url(server, ""), creds());
             Statement st = c.createStatement()) {
            assertEquals(42, st.executeUpdate("UPDATE t SET x = 1"));
            assertEquals(0, st.executeUpdate("CREATE TABLE t (PRIMARY KEY (id))"));
            SQLException e = assertThrows(SQLException.class, () -> st.executeUpdate("SELECT 1"));
            assertEquals("21000", e.getSQLState());
        }
        finish(server);
    }

    @Test
    void multipleResultSets() throws Exception {
        ConformanceServer server = vectorCase("result_sets");
        try (Connection c = DriverManager.getConnection(url(server, ""), creds());
             Statement st = c.createStatement()) {
            assertTrue(st.execute("CALL p()"));
            ResultSet a = st.getResultSet();
            assertTrue(a.next());
            assertEquals(1, a.getInt("a"));
            assertTrue(a.next());
            assertTrue(st.getMoreResults());
            assertTrue(a.isClosed());
            ResultSet b = st.getResultSet();
            assertTrue(b.next());
            assertEquals("x", b.getString("b"));
            assertNull(b.getString("c"));
            assertTrue(b.wasNull());
            assertFalse(st.getMoreResults());
        }
        finish(server);
    }

    @Test
    void fetchSizeStreams() throws Exception {
        ConformanceServer server = vectorCase("stream_rows");
        try (Connection c = DriverManager.getConnection(url(server, "?fetch_size=500"), creds());
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT id, name FROM t")) {
            assertEquals(500, st.getFetchSize());
            int n = 0;
            while (rs.next()) n++;
            assertEquals(3, n);
            assertThrows(SQLFeatureNotSupportedException.class, rs::isLast);
        }
        finish(server);

        ConformanceServer failing = vectorCase("stream_error_after_header");
        try (Connection c = DriverManager.getConnection(url(failing, ""), creds());
             Statement st = c.createStatement()) {
            st.setFetchSize(1);
            ResultSet rs = st.executeQuery("SELECT id, name FROM t");
            assertTrue(rs.next());
            assertTrue(rs.next());
            SQLException e = assertThrows(SQLException.class, rs::next);
            assertTrue(e.getMessage().contains("scan budget exceeded"));
            assertEquals("54000", e.getSQLState());
        }
        finish(failing);
    }

    // ---- errors -------------------------------------------------------------------------

    @Test
    void errorsMapToSqlStates() throws Exception {
        ConformanceServer server = vectorCase("query_error_then_reuse");
        try (Connection c = DriverManager.getConnection(url(server, ""), creds());
             Statement st = c.createStatement()) {
            SQLException e = assertThrows(SQLException.class, () -> st.executeQuery("SELECT nope"));
            assertTrue(e.getMessage().contains("boom"));
            assertEquals("HY000", e.getSQLState());
            ResultSet rs = st.executeQuery("SELECT 1");
            assertTrue(rs.next(), "the connection survives a statement error");
        }
        finish(server);

        ConformanceServer unknown = vectorCase("query_error");
        try (Connection c = DriverManager.getConnection(url(unknown, ""), creds())) {
            SQLException e = assertThrows(SQLException.class, () -> c.createStatement().executeQuery("SELECT nope"));
            assertInstanceOf(SQLSyntaxErrorException.class, e);
            assertEquals("42703", e.getSQLState());
        }
        finish(unknown);

        for (String outcome : new String[] { "denied", "bad_server_signature" }) {
            ConformanceServer s = new ConformanceServer(outcome, List.of());
            SQLException e = assertThrows(SQLException.class, () -> DriverManager.getConnection(url(s, ""), creds()));
            assertInstanceOf(SQLInvalidAuthorizationSpecException.class, e, outcome);
            assertEquals("28000", e.getSQLState());
            finish(s);
        }

        int dead;
        try (ServerSocket s = new ServerSocket(0)) { dead = s.getLocalPort(); }
        SQLException down = assertThrows(SQLException.class,
            () -> DriverManager.getConnection("jdbc:skaidb://127.0.0.1:" + dead, creds()));
        assertInstanceOf(SQLNonTransientConnectionException.class, down);
        assertEquals("08001", down.getSQLState());
    }

    @Test
    void serverMessagesMapToSubclasses() {
        Object[][] table = {
            { "unique violation: index \"u\" already has 1", SQLIntegrityConstraintViolationException.class, "23505" },
            { "not null violation: column \"a\" of table \"t\" is null", SQLIntegrityConstraintViolationException.class, "23502" },
            { "foreign key violation: constraint \"fk\": x", SQLIntegrityConstraintViolationException.class, "23503" },
            { "check violation: constraint \"c\" of table \"t\": x", SQLIntegrityConstraintViolationException.class, "23514" },
            { "permission denied: Select on Table(\"t\")", SQLSyntaxErrorException.class, "42501" },
            { "parse error: unexpected token", SQLSyntaxErrorException.class, "42601" },
            { "table \"nope\" does not exist", SQLSyntaxErrorException.class, "42P01" },
            { "no such table: nope", SQLSyntaxErrorException.class, "42P01" },
            { "type error: unknown function version()", SQLSyntaxErrorException.class, "42883" },
            { "connection is closed", SQLNonTransientConnectionException.class, "08003" },
            { "something else", SQLException.class, "HY000" },
        };
        for (Object[] row : table) {
            SQLException e = Errors.map(new Skaidb.SkaidbException((String) row[0]));
            assertEquals(row[1], e.getClass(), (String) row[0]);
            assertEquals(row[2], e.getSQLState(), (String) row[0]);
        }
        SQLException timeout = Errors.map(new Skaidb.SkaidbException("query failed: Read timed out",
            new java.net.SocketTimeoutException("Read timed out")));
        assertInstanceOf(java.sql.SQLTimeoutException.class, timeout);
        SQLException broken = Errors.map(new Skaidb.SkaidbException("query failed: reset", new java.io.IOException("reset")));
        assertInstanceOf(java.sql.SQLRecoverableException.class, broken);
        assertEquals("08006", broken.getSQLState());
    }

    // ---- transactions, DataSource, metadata ------------------------------------------------

    @Test
    void transactionsBeginLazilyAndCommit() throws Exception {
        ConformanceServer server = new ConformanceServer("ok", List.of(
            query("BEGIN", ConformanceServer.ddlResponse()),
            query("INSERT INTO t (id) VALUES (1)", ConformanceServer.mutationResponse(1)),
            query("COMMIT", ConformanceServer.ddlResponse()),
            query("BEGIN", ConformanceServer.ddlResponse()),
            query("INSERT INTO t (id) VALUES (2)", ConformanceServer.mutationResponse(1)),
            query("ROLLBACK", ConformanceServer.ddlResponse()),
            query("BEGIN", ConformanceServer.ddlResponse()),
            query("INSERT INTO t (id) VALUES (3)", ConformanceServer.mutationResponse(1)),
            query("ROLLBACK", ConformanceServer.ddlResponse())));
        try (Connection c = DriverManager.getConnection(url(server, ""), creds())) {
            assertTrue(c.getAutoCommit());
            assertThrows(SQLException.class, c::commit, "commit with autocommit on");
            c.setAutoCommit(false);
            c.commit();   // nothing open yet: no round trip
            Statement st = c.createStatement();
            assertEquals(1, st.executeUpdate("INSERT INTO t (id) VALUES (1)"));
            c.commit();
            assertEquals(1, st.executeUpdate("INSERT INTO t (id) VALUES (2)"));
            c.rollback();
            assertEquals(1, st.executeUpdate("INSERT INTO t (id) VALUES (3)"));
            assertThrows(SQLFeatureNotSupportedException.class, c::setSavepoint);
            assertEquals(Connection.TRANSACTION_READ_COMMITTED, c.getTransactionIsolation());
        }   // close() rolls the open transaction back
        finish(server);

        ConformanceServer atomic = new ConformanceServer("ok", List.of(
            query("BEGIN ATOMIC", ConformanceServer.ddlResponse()),
            query("DELETE FROM t", ConformanceServer.mutationResponse(2)),
            query("COMMIT", ConformanceServer.ddlResponse())));
        try (Connection c = DriverManager.getConnection(url(atomic, "?transaction=atomic"), creds())) {
            c.setAutoCommit(false);
            c.createStatement().executeUpdate("DELETE FROM t");
            c.setAutoCommit(true);   // commits
        }
        finish(atomic);
    }

    @Test
    void dataSourceForPools() throws Exception {
        ConformanceServer server = new ConformanceServer("ok", List.of(
            query("USE \"app\"", ConformanceServer.ddlResponse()),
            query("SELECT 1", ConformanceServer.rowsResponse(new String[] { "x" }, new Object[] { 1L }))));
        SkaidbDataSource ds = new SkaidbDataSource();
        ds.setUrl(url(server, "/app"));
        ds.setUser(ConformanceServer.username());
        ds.setPassword(ConformanceServer.password());
        try (Connection c = ds.getConnection()) {
            assertTrue(c.isValid(5), "isValid is a SELECT 1 round trip");
            assertEquals("app", c.getCatalog());
            assertTrue(c.isWrapperFor(Skaidb.Connection.class));
            assertTrue(c.unwrap(Skaidb.Connection.class).isUsable());
        }
        assertTrue(!new SkaidbDataSource().isWrapperFor(Connection.class));
        assertThrows(SQLException.class, () -> new SkaidbDataSource().getConnection());
        finish(server);
    }

    @Test
    void databaseMetaData() throws Exception {
        byte[] tables = ConformanceServer.rowsResponse(
            new String[] { "table", "primary_key", "replication", "nodes", "witness", "transition", "kind" },
            new Object[] { "people", "org, id", null, null, true, false, "row" },
            new Object[] { "events", "id", null, null, true, false, "timeseries" },
            new Object[] { "v_people", "", null, null, false, false, "view" });
        byte[] describe = ConformanceServer.rowsResponse(new String[] { "column", "key", "indexes" },
            new Object[] { "org", "primary key (1/2)", "" },
            new Object[] { "id", "primary key (2/2)", "" },
            new Object[] { "email", "", "people_email (secondary)" });
        ConformanceServer server = new ConformanceServer("ok", List.of(
            query("USE \"app\"", ConformanceServer.ddlResponse()),
            query("SHOW TABLES", tables),
            query("SHOW TABLES", tables),
            query("DESCRIBE \"people\"", describe),
            query("SHOW TABLES", tables),
            query("DESCRIBE \"people\"", describe),
            query("SHOW DATABASES", ConformanceServer.rowsResponse(new String[] { "database", "current" },
                new Object[] { "default", "" }, new Object[] { "app", "*" }))));
        try (Connection c = DriverManager.getConnection(url(server, "/app"), creds())) {
            DatabaseMetaData md = c.getMetaData();
            assertEquals("skaidb", md.getDatabaseProductName());
            assertEquals(Skaidb.VERSION, md.getDriverVersion());
            assertEquals("\"", md.getIdentifierQuoteString());

            List<String> names = new ArrayList<>();
            try (ResultSet rs = md.getTables(null, null, "%e%", new String[] { "TABLE", "TIMESERIES" })) {
                while (rs.next()) {
                    assertEquals("app", rs.getString("TABLE_CAT"));
                    names.add(rs.getString("TABLE_NAME") + ":" + rs.getString("TABLE_TYPE"));
                }
            }
            assertEquals(List.of("people:TABLE", "events:TIMESERIES"), names);
            assertFalse(md.getTables("other", null, null, null).next(), "other catalogs answer empty");

            List<String> cols = new ArrayList<>();
            try (ResultSet rs = md.getColumns(null, null, "people", null)) {
                while (rs.next()) cols.add(rs.getString("COLUMN_NAME") + ":" + rs.getInt("DATA_TYPE") + ":" + rs.getString("IS_NULLABLE"));
            }
            assertEquals(List.of("org:1111:NO", "id:1111:NO", "email:1111:"), cols);

            List<String> pk = new ArrayList<>();
            try (ResultSet rs = md.getPrimaryKeys(null, null, "people")) {
                while (rs.next()) pk.add(rs.getShort("KEY_SEQ") + "=" + rs.getString("COLUMN_NAME"));
            }
            assertEquals(List.of("1=org", "2=id"), pk);

            List<String> cats = new ArrayList<>();
            try (ResultSet rs = md.getCatalogs()) { while (rs.next()) cats.add(rs.getString(1)); }
            assertEquals(List.of("app", "default"), cats);

            assertFalse(md.getSchemas().next());
            assertFalse(md.getProcedures(null, null, "%").next());
            assertEquals(4, md.getImportedKeys(null, null, "people").findColumn("PKCOLUMN_NAME"));
            assertTrue(md.getTypeInfo().next());
        }
        finish(server);
    }

    // ---- values and unsupported features ----------------------------------------------------

    @Test
    void resultSetConversions() throws SQLException {
        UUID u = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");
        Instant t = Instant.ofEpochMilli(1_700_000_000_123L);
        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[] { 5_000_000_000L, "42", new BigDecimal("12.50"), u.toString(), t,
            List.of(1L, "a"), new java.util.LinkedHashMap<>(Map.of("k", 1L)), 2.75, true, null });
        SkaidbResultSet rs = new SkaidbResultSet(null,
            new String[] { "big", "num", "dec", "uid", "ts", "arr", "doc", "dbl", "flag", "nothing" }, rows);
        assertThrows(SQLException.class, () -> rs.getInt(1), "no current row yet");
        assertTrue(rs.next());
        assertEquals("22003", assertThrows(SQLDataException.class, () -> rs.getInt(1)).getSQLState());
        assertEquals(5_000_000_000L, rs.getLong(1));
        assertEquals(42, rs.getInt("num"), "numeric text converts");
        assertEquals(new BigDecimal("12.50"), rs.getBigDecimal("dec"));
        assertEquals(12, rs.getInt("dec"));
        assertEquals("12.50", rs.getString("dec"));
        assertEquals(u, rs.getObject("uid", UUID.class), "a UUID parses from text");
        assertEquals(Timestamp.from(t), rs.getObject("ts"), "getObject: JDBC's Timestamp");
        assertEquals(t, rs.getObject("ts", Instant.class));
        assertEquals(LocalDateTime.of(2023, 11, 14, 22, 13, 20, 123_000_000), rs.getObject("ts", LocalDateTime.class));
        assertEquals(t.atOffset(java.time.ZoneOffset.UTC), rs.getObject("ts", OffsetDateTime.class));
        assertArrayEquals(new Object[] { 1L, "a" }, (Object[]) rs.getArray("arr").getArray());
        assertEquals(List.of(1L, "a"), rs.getObject("arr", List.class));
        assertEquals("[1,\"a\"]", rs.getString("arr"));
        assertEquals("{\"k\":1}", rs.getString("doc"));
        assertEquals(Map.of("k", 1L), rs.getObject("doc", Map.class));
        assertEquals(2.75f, rs.getFloat("dbl"));
        assertEquals(2, rs.getInt("dbl"));
        assertTrue(rs.getBoolean("flag"));
        assertEquals(1, rs.getInt("flag"));
        assertEquals(0, rs.getInt("nothing"));
        assertTrue(rs.wasNull());
        assertNull(rs.getObject("nothing", Long.class));
        assertThrows(SQLDataException.class, () -> rs.getObject("nothing", long.class));
        assertThrows(SQLDataException.class, () -> rs.getInt("uid"));
        assertEquals("42703", assertThrows(SQLException.class, () -> rs.findColumn("nope")).getSQLState());
        assertThrows(SQLFeatureNotSupportedException.class, rs::previous);
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateInt(1, 1));
        assertTrue(rs.isFirst() && rs.isLast());
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(Types.DECIMAL, md.getColumnType(3));
        assertEquals(2, md.getScale(3));
        assertEquals(Types.TIMESTAMP, md.getColumnType(5));
        assertEquals(Types.ARRAY, md.getColumnType(6));
        assertEquals("DOCUMENT", md.getColumnTypeName(7));
        assertEquals(Types.OTHER, md.getColumnType(10), "no value to infer from");
        assertFalse(rs.next());
        assertTrue(rs.isAfterLast());
        rs.close();
        assertThrows(SQLException.class, rs::next);
    }

    @Test
    void bindConversions() throws SQLException {
        assertEquals(7L, Values.toBind(7));
        assertEquals(1.5, Values.toBind(1.5f));
        assertEquals(Instant.ofEpochMilli(1000), Values.toBind(new Timestamp(1000)));
        assertEquals(Instant.parse("2024-01-02T03:04:05Z"), Values.toBind(LocalDateTime.of(2024, 1, 2, 3, 4, 5)));
        assertEquals(Instant.parse("2024-01-02T00:00:00Z"), Values.toBind(java.time.LocalDate.of(2024, 1, 2)));
        assertEquals(List.of(1L, "x"), Values.toBind(new Object[] { 1, 'x' }));
        assertEquals(new BigDecimal("12345678901234567890"), Values.toBind(new java.math.BigInteger("12345678901234567890")));
        assertEquals(List.of(2L), Values.toBind(new SkaidbArray(List.of(2L))));
        assertEquals("12", Values.toBind(12, Types.VARCHAR));
        assertEquals(12L, Values.toBind("12", Types.BIGINT));
        assertEquals(true, Values.toBind("yes", Types.BOOLEAN));
        assertThrows(SQLDataException.class, () -> Values.toBind(new Object() { }));
        assertThrows(SQLDataException.class, () -> Values.toBind(java.sql.Time.valueOf("10:00:00")));
    }

    @Test
    void unsupportedFeaturesSaySo() throws Exception {
        ConformanceServer server = new ConformanceServer("ok", List.of());
        try (Connection c = DriverManager.getConnection(url(server, ""), creds())) {
            assertThrows(SQLFeatureNotSupportedException.class,
                () -> c.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY));
            assertThrows(SQLFeatureNotSupportedException.class,
                () -> c.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE));
            assertThrows(SQLFeatureNotSupportedException.class, () -> c.prepareCall("CALL p()"));
            assertThrows(SQLFeatureNotSupportedException.class,
                () -> c.prepareStatement("INSERT INTO t (id) VALUES (1)", Statement.RETURN_GENERATED_KEYS));
            assertThrows(SQLFeatureNotSupportedException.class, () -> c.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE));
            assertThrows(SQLFeatureNotSupportedException.class, c::createBlob);
            Statement st = c.createStatement();
            assertThrows(SQLFeatureNotSupportedException.class, st::cancel);
            assertFalse(st.getGeneratedKeys().next());
            st.close();
            assertThrows(SQLException.class, () -> st.executeQuery("SELECT 1"));
        }
        finish(server);
    }

    @Test
    void closedConnectionRefusesWork() throws Exception {
        ConformanceServer server = new ConformanceServer("ok", List.of());
        Connection c = DriverManager.getConnection(url(server, ""), creds());
        Statement st = c.createStatement();
        c.close();
        assertTrue(c.isClosed());
        assertFalse(c.isValid(1));
        assertEquals("08003", assertThrows(SQLException.class, c::createStatement).getSQLState());
        assertEquals("08003", assertThrows(SQLException.class, () -> st.executeQuery("SELECT 1")).getSQLState());
        finish(server);
    }
}
