package com.skaidb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The session database survives a re-dial: the database a connection is in
 * — from its {@code database} option, or entered with a {@code USE}
 * statement — is state of the server-side session, so the driver re-enters
 * it on a new socket before it sends anything else. Against the scripted
 * fake server over several connections: each test states every statement
 * each session may receive.
 */
class SessionDatabaseTest {

    static final int QUORUM = Skaidb.CONSISTENCY_QUORUM;
    static final String COUNT = "SELECT count(*) FROM notes";
    static final String INSERT = "INSERT INTO notes (id, body) VALUES (4, 'four')";
    /** How the driver enters database {@code app} on a dial. */
    static final String ENTER_APP = "USE \"app\"";

    static Skaidb.ConnectOptions options(ConformanceServer s) {
        return new Skaidb.ConnectOptions().host("127.0.0.1", s.port())
            .user(ConformanceServer.username()).password(ConformanceServer.password());
    }

    static ConformanceServer.Exchange q(String sql, byte[]... responses) {
        return new ConformanceServer.Exchange(ConformanceServer.queryRequest(sql, QUORUM), List.of(responses));
    }

    static byte[] ok() { return ConformanceServer.ddlResponse(); }
    static byte[] changed(long n) { return ConformanceServer.mutationResponse(n); }
    static byte[] error(String message) { return ConformanceServer.errorResponse(message); }
    static byte[] counted(long n) { return ConformanceServer.rowsResponse(new String[] { "count" }, new Object[] { n }); }

    /** An answered statement after which the server goes away, with the client idle. */
    static ConformanceServer.Exchange thenTheServerGoesAway() { return q("SELECT 1", counted(1)).thenHangUp(); }

    /** The statements connection {@code n} (the first is 0) received. */
    static List<String> sent(ConformanceServer s, int n) { return TransactionTest.statements(s.requests(n)); }

    static long count(Skaidb.Connection c) {
        Skaidb.ResultSet rs = c.query(COUNT);
        rs.next();
        return rs.getLong(1);
    }

    /**
     * Lose the connection while it sits idle: the server answers one last
     * statement and goes away. The driver finds that out before it sends
     * its next statement, which then runs on a new connection.
     */
    static void loseWhileIdle(Skaidb.Connection c, ConformanceServer server, int connectionsSoFar) throws Exception {
        c.query("SELECT 1");
        assertTrue(server.awaitEnded(connectionsSoFar, 2_000));
        assertTrue(ConformanceServer.awaitSeenClosed(c, 2_000));
        assertFalse(c.isUsable());
    }

    // ---- the database option -----------------------------------------------------

    @Test
    void theDatabaseOptionIsReEnteredOnTheNewConnection() throws Exception {
        ConformanceServer server = ConformanceServer.sessions(List.of(
            List.of(q(ENTER_APP, ok()), thenTheServerGoesAway()),
            List.of(q(ENTER_APP, ok()), q(COUNT, counted(3)), q(INSERT, changed(1)))));
        try (Skaidb.Connection c = Skaidb.connect(options(server).database("app"))) {
            loseWhileIdle(c, server, 1);
            assertEquals(3, count(c));
            assertEquals(1, c.execute(INSERT));
            assertEquals(1, c.reconnects());
        }
        assertNull(server.finish());
        assertEquals(List.of(ENTER_APP, COUNT, INSERT), sent(server, 1), "the database first, then the statements");
    }

    // ---- a USE statement -----------------------------------------------------------

    /** One way of sending a statement that takes no parameters. */
    interface Send {
        void run(Skaidb.Connection c, String sql);
    }

    @Test
    void aDatabaseEnteredWithUseIsReEnteredOnTheNewConnection() throws Exception {
        // Every way of sending a statement without parameters ...
        Map<String, Send> paths = new LinkedHashMap<>();
        paths.put("execute", (c, sql) -> c.execute(sql));
        paths.put("query", (c, sql) -> c.query(sql));
        paths.put("executeRaw", (c, sql) -> c.executeRaw(sql));
        paths.put("prepare", (c, sql) -> c.prepare(sql).executeUpdate());
        for (Map.Entry<String, Send> path : paths.entrySet()) reEnters(path.getKey(), path.getValue(), "USE app", ENTER_APP);
        // ... and the statement as written, with how the driver enters that
        // database again (whichStatementsEnterADatabase has every spelling).
        Send execute = paths.get("execute");
        reEnters("execute", execute, "  Use   Database\tapp  ;  ", ENTER_APP);
        reEnters("execute", execute, "USE DATABASE \"we \"\"quote\"\" it\";", "USE \"we \"\"quote\"\" it\"");
        // The server skips a -- comment, so the statement is a USE all the same.
        reEnters("execute", execute, "USE app -- the app database", ENTER_APP);
    }

    /** {@code use} is sent, the connection is lost while idle, and the next statements find {@code enter} sent first. */
    static void reEnters(String path, Send send, String use, String enter) throws Exception {
        String what = path + "(" + use + ")";
        ConformanceServer server = ConformanceServer.sessions(List.of(
            List.of(q(use, ok()), thenTheServerGoesAway()),
            List.of(q(enter, ok()), q(COUNT, counted(3)), q(INSERT, changed(1)))));
        try (Skaidb.Connection c = Skaidb.connect(options(server))) {
            send.run(c, use);
            loseWhileIdle(c, server, 1);
            assertEquals(3, count(c), what);
            assertEquals(1, c.execute(INSERT), what);
        }
        assertNull(server.finish(), what);
        assertEquals(List.of(enter, COUNT, INSERT), sent(server, 1), what + ": the database first, then the statements");
    }

    @Test
    void aUseSentThroughAStreamIsNotedToo() throws Exception {
        ConformanceServer server = ConformanceServer.sessions(List.of(
            List.of(new ConformanceServer.Exchange(ConformanceServer.streamRequest("USE app", QUORUM), List.of(ok())),
                thenTheServerGoesAway()),
            List.of(q(ENTER_APP, ok()), q(COUNT, counted(3)))));
        try (Skaidb.Connection c = Skaidb.connect(options(server))) {
            try (Skaidb.RowStream rows = c.stream("USE app")) {
                assertFalse(rows.next());
            }
            loseWhileIdle(c, server, 1);
            assertEquals(3, count(c));
        }
        assertNull(server.finish());
        assertEquals(List.of(ENTER_APP, COUNT), sent(server, 1));
    }

    @Test
    void theLastDatabaseEnteredIsTheOneReEntered() throws Exception {
        ConformanceServer server = ConformanceServer.sessions(List.of(
            // the option's database, then two USE statements: the last one counts
            List.of(q("USE \"reports\"", ok()), q("USE staging", ok()), q("USE app", ok()), thenTheServerGoesAway()),
            // ... on every later dial as well
            List.of(q(ENTER_APP, ok()), thenTheServerGoesAway()),
            List.of(q(ENTER_APP, ok()), q(COUNT, counted(3)))));
        try (Skaidb.Connection c = Skaidb.connect(options(server).database("reports"))) {
            c.execute("USE staging");
            c.execute("USE app");
            loseWhileIdle(c, server, 1);
            loseWhileIdle(c, server, 2);
            assertEquals(3, count(c));
            assertEquals(2, c.reconnects());
        }
        assertNull(server.finish());
        assertEquals(List.of(ENTER_APP, "SELECT 1"), sent(server, 1));
        assertEquals(List.of(ENTER_APP, COUNT), sent(server, 2));
    }

    @Test
    void aStatementFailsWhenItsDatabaseCannotBeReEntered() throws Exception {
        String gone = "database \"app\" does not exist";
        ConformanceServer server = ConformanceServer.sessions(List.of(
            List.of(q("USE app", ok()), thenTheServerGoesAway()),
            // The new session refuses the database: nothing else may be sent on it.
            List.of(q(ENTER_APP, error(gone))),
            // The next statement dials again, and this time is let in.
            List.of(q(ENTER_APP, ok()), q(COUNT, counted(3)))));
        try (Skaidb.Connection c = Skaidb.connect(options(server))) {
            c.execute("USE app");
            loseWhileIdle(c, server, 1);
            Skaidb.SkaidbException e = assertThrows(Skaidb.SkaidbException.class, () -> c.execute(INSERT));
            assertEquals(gone, e.getMessage(), "the statement fails with what the USE was answered");
            assertTrue(server.awaitEnded(2, 2_000), "that session is given up, socket and all");
            assertEquals(List.of(ENTER_APP), sent(server, 1), "the statement ran in no other database");
            assertFalse(c.isUsable());
            assertEquals(3, count(c));
        }
        assertNull(server.finish());
        assertEquals(List.of(ENTER_APP, COUNT), sent(server, 2));
    }

    // ---- a USE is not held hostage by the database it leaves -----------------------------

    static final String GONE = "database \"app\" does not exist";
    static final String BUDGET = "SET SCAN BUDGET ROWS 100";
    static final String ALL = "SET CONSISTENCY ALL";

    @Test
    void aUseIsNotHeldHostageByTheDatabaseItLeaves() throws Exception {
        // Every way of sending the USE that leaves the dropped database.
        Map<String, Send> paths = new LinkedHashMap<>();
        paths.put("execute", (c, sql) -> c.execute(sql));
        paths.put("query", (c, sql) -> c.query(sql));
        paths.put("executeRaw", (c, sql) -> c.executeRaw(sql));
        paths.put("prepare", (c, sql) -> c.prepare(sql).executeUpdate());
        paths.put("stream", (c, sql) -> c.stream(sql).close());
        for (Map.Entry<String, Send> path : paths.entrySet()) {
            String name = path.getKey();
            boolean streams = name.equals("stream");
            ConformanceServer.Exchange useOther = streams
                ? new ConformanceServer.Exchange(ConformanceServer.streamRequest("USE other", QUORUM), List.of(ok()))
                : q("USE other", ok());
            ConformanceServer server = ConformanceServer.sessions(List.of(
                // In app, with settings of its own; then app is dropped from elsewhere and the connection lost.
                List.of(q("USE app", ok()), q(BUDGET, ok()), q(ALL, ok()).thenHangUp()),
                // Any other statement must not run anywhere else: it fails at the USE that re-enters app.
                List.of(q(ENTER_APP, error(GONE))),
                // But a USE says where to go instead: the settings, then the USE itself — not app first.
                List.of(q(ALL, ok()), q(BUDGET, ok()), useOther, q(COUNT, counted(3)).thenHangUp()),
                // ... and that is the database a later dial enters.
                List.of(q("USE \"other\"", ok()), q(ALL, ok()), q(BUDGET, ok()), q(INSERT, changed(1)))));
            try (Skaidb.Connection c = Skaidb.connect(options(server))) {
                c.execute("USE app");
                c.execute(BUDGET);
                c.execute(ALL);
                assertTrue(server.awaitEnded(1, 2_000), name);
                assertTrue(ConformanceServer.awaitSeenClosed(c, 2_000), name);
                Skaidb.SkaidbException e = assertThrows(Skaidb.SkaidbException.class, () -> c.query("SELECT 1"), name);
                assertEquals(GONE, e.getMessage(), name);
                assertTrue(server.awaitEnded(2, 2_000), name);

                path.getValue().run(c, "USE other");
                assertEquals("other", c.database(), name);
                assertEquals(3, count(c), name + ": a statement after it runs in other");
                assertEquals(2, c.reconnects(), name);
                assertTrue(server.awaitEnded(3, 2_000), name);
                assertTrue(ConformanceServer.awaitSeenClosed(c, 2_000), name);
                assertEquals(1, c.execute(INSERT), name);
            }
            assertNull(server.finish(), name);
            assertEquals(List.of(ENTER_APP), sent(server, 1), name);
            assertEquals(List.of(ALL, BUDGET, (streams ? "STREAM " : "") + "USE other", COUNT), sent(server, 2),
                name + ": the settings and the USE, no USE of the old database");
        }
    }

    @Test
    void aUseRefusedOnTheNewConnectionLeavesTheSessionWhereItWas() throws Exception {
        ConformanceServer server = ConformanceServer.sessions(List.of(
            List.of(q("USE app", ok()).thenHangUp()),
            // The USE goes out first on the new connection, and is refused: that
            // session is in no database, and no statement may find it there.
            List.of(q("USE nowhere", error("database \"nowhere\" does not exist"))),
            // The connection is still in app: the next statement dials again, into it.
            List.of(q(ENTER_APP, ok()), q(COUNT, counted(3)))));
        try (Skaidb.Connection c = Skaidb.connect(options(server))) {
            c.execute("USE app");
            assertTrue(server.awaitEnded(1, 2_000));
            assertTrue(ConformanceServer.awaitSeenClosed(c, 2_000));
            Skaidb.SkaidbException e = assertThrows(Skaidb.SkaidbException.class, () -> c.execute("USE nowhere"));
            assertEquals("database \"nowhere\" does not exist", e.getMessage());
            assertEquals("app", c.database());
            assertTrue(server.awaitEnded(2, 2_000), "the session that is in no database is given up");
            assertFalse(c.isUsable());
            assertEquals(3, count(c));
            assertEquals(2, c.reconnects());
        }
        assertNull(server.finish());
        assertEquals(List.of(ENTER_APP, COUNT), sent(server, 2), "the statement ran in app, not in the default database");
    }

    @Test
    void onlyAUseTheServerAcceptedAndTheDriverCanReadIsNoted() throws Exception {
        ConformanceServer server = ConformanceServer.sessions(List.of(
            List.of(q(ENTER_APP, ok()),
                // refused: the session stays where it was
                q("USE nowhere", error("database \"nowhere\" does not exist")),
                // not one USE statement as the driver reads it (nor, today, as
                // a server does): left alone, whatever answers it
                q("USE other /* for the report */", ok()),
                thenTheServerGoesAway()),
            List.of(q(ENTER_APP, ok()), q(COUNT, counted(3)))));
        try (Skaidb.Connection c = Skaidb.connect(options(server).database("app"))) {
            assertThrows(Skaidb.SkaidbException.class, () -> c.execute("USE nowhere"));
            c.execute("USE other /* for the report */");
            loseWhileIdle(c, server, 1);
            assertEquals(3, count(c));
        }
        assertNull(server.finish());
        assertEquals(List.of(ENTER_APP, COUNT), sent(server, 1));
    }

    @Test
    void whichStatementsEnterADatabase() {
        Map<String, String> enters = new LinkedHashMap<>();
        enters.put("USE app", "app");
        enters.put("use app", "app");
        enters.put("Use App_2", "App_2");
        enters.put("USE _bi", "_bi");
        enters.put("\n\tUSE  app  ;  ", "app");
        enters.put("USE app;", "app");
        enters.put("USE DATABASE app", "app");
        enters.put("use database app ;", "app");
        enters.put("USE \"app\"", "app");
        enters.put("USE DATABASE \"my app\";", "my app");
        enters.put("USE \"a\"\"b\"", "a\"b");
        enters.put("USE \"database\"", "database");
        // DATABASE with no name after it is the name.
        enters.put("USE database", "database");
        enters.put("USE DATABASE;", "DATABASE");
        enters.put("USE DATABASE database", "database");
        // A -- comment runs to the end of its line and is not part of the statement ...
        enters.put("USE app -- the app database", "app");
        enters.put("USE app--the app database", "app");
        enters.put("USE app -- it's the app; \"really\"", "app");
        enters.put("USE app -- c ;", "app");
        enters.put("USE app; -- done", "app");
        enters.put("USE app -- c\n;", "app");
        enters.put("-- first a comment\nUSE app", "app");
        enters.put("USE -- which one\napp", "app");
        enters.put("USE DATABASE app -- c", "app");
        enters.put("USE DATABASE -- c\n\"app\" -- d\n; -- e", "app");
        // ... unless the two dashes are inside the quoted name.
        enters.put("USE \"a--b\"", "a--b");
        enters.put("USE \"a\"\"--b\" -- c", "a\"--b");
        for (Map.Entry<String, String> e : enters.entrySet()) assertEquals(e.getValue(), Skaidb.usedDatabase(e.getKey()), e.getKey());

        for (String other : new String[] {
                "", "USE", "USE ", "USE;", "USEapp", "USER app", "REUSE app", "SELECT 'USE app'", "USE 'app'",
                "USE app /* c */", "/* c */ USE app", "USE app; SELECT 1",
                // a comment hides what follows it on its line, a statement included
                "-- USE app", "USE -- x", "USE -- app\n", "USE app -- c\nnotes", "USE app - - c", "USE 'app' -- c",
                "-- c\nSELECT 1 -- USE app", "USE \"app -- c",
                "USE app;;", "USE a-b", "USE app.notes", "USE app notes", "USE database app notes", "USE \"app",
                "USE app\"", "USE \"app\"x", "USE \"a\"b\"", "USE \"\"", "USE DATABASE \"\"", "USE ?", "USE 9lives",
                "INSERT INTO notes (id) VALUES (1)" }) {
            assertNull(Skaidb.usedDatabase(other), other);
        }
    }

    // ---- pools -----------------------------------------------------------------------

    @Test
    void aPooledConnectionReEntersItsDatabaseWhenItReDials() throws Exception {
        ConformanceServer server = ConformanceServer.sessions(List.of(
            List.of(q(ENTER_APP, ok()), thenTheServerGoesAway()),
            List.of(q(ENTER_APP, ok()), q(COUNT, counted(3)), q(INSERT, changed(1)))));
        List<Skaidb.Connection> used = new ArrayList<>();
        try (Skaidb.Pool pool = new Skaidb.Pool(options(server).database("app"))) {
            // Lost while it is checked out: the same connection re-dials.
            pool.withConnection(c -> {
                used.add(c);
                try {
                    loseWhileIdle(c, server, 1);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
                assertEquals(3, count(c));
                return null;
            });
            // In the pool's database still, so it went back to the pool.
            assertEquals(1, (long) pool.withConnection(c -> {
                used.add(c);
                return c.execute(INSERT);
            }));
            assertSame(used.get(0), used.get(1));
        }
        assertNull(server.finish());
        assertEquals(List.of(ENTER_APP, COUNT, INSERT), sent(server, 1));
    }

    @Test
    void aBorrowersUseDoesNotReachTheNextBorrowerOfAPoolWithADatabase() throws Exception {
        ConformanceServer server = ConformanceServer.sessions(List.of(
            // The first borrower re-enters the pool's own database, the second another one.
            List.of(q(ENTER_APP, ok()), q("USE app", ok()), q("USE reports", ok())),
            List.of(q(ENTER_APP, ok()), q(COUNT, counted(3)))));
        List<Skaidb.Connection> used = new ArrayList<>();
        try (Skaidb.Pool pool = new Skaidb.Pool(options(server).database("app"))) {
            pool.withConnection(c -> {
                used.add(c);
                return c.execute("USE app");
            });
            assertEquals(1, pool.members().get(0).idle, "still in the pool's database: kept");
            pool.withConnection(c -> {
                used.add(c);
                return c.execute("USE reports");
            });
            assertSame(used.get(0), used.get(1));
            // A connection left in another database is not handed on: it is closed.
            assertEquals(0, pool.members().get(0).idle);
            assertTrue(server.awaitEnded(1, 2_000));
            assertEquals(3, (long) pool.withConnection(c -> {
                used.add(c);
                return count(c);
            }));
            assertNotSame(used.get(1), used.get(2));
        }
        assertNull(server.finish());
        assertEquals(List.of(ENTER_APP, COUNT), sent(server, 1), "the next borrower is in the pool's database");
    }
}
