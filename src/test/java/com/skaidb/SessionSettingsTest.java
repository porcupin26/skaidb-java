package com.skaidb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
 * A connection's session settings survive a re-dial. Besides its database,
 * a session carries a consistency override ({@code SET CONSISTENCY ...})
 * and a scan budget ({@code SET SCAN BUDGET ...}); both live on the server
 * side of the socket. The driver notes each one the server accepted, as it
 * notes a {@code USE}, and sends it again on a new connection — after the
 * session database and before the statement — so that no statement ever
 * runs under other settings than the application set. Against the scripted
 * fake server over several connections: each test states every statement
 * each session may receive.
 */
class SessionSettingsTest {

    static final String BUDGET = "SET SCAN BUDGET ROWS 1";
    static final String ALL = "SET CONSISTENCY ALL";
    static final String SCAN = "SELECT id FROM notes";
    static final String REFUSED = "resource limit: scan budget exceeded (2 rows examined)";

    static Skaidb.ConnectOptions options(ConformanceServer s) { return TransactionTest.options(s); }
    static ConformanceServer.Exchange q(String sql, byte[]... responses) { return TransactionTest.q(sql, responses); }
    static byte[] ok() { return ConformanceServer.ddlResponse(); }
    static byte[] error(String message) { return ConformanceServer.errorResponse(message); }
    static byte[] rows(long... ids) {
        Object[][] rows = new Object[ids.length][];
        for (int i = 0; i < ids.length; i++) rows[i] = new Object[] { ids[i] };
        return ConformanceServer.rowsResponse(new String[] { "id" }, rows);
    }
    static List<String> sent(ConformanceServer s, int n) { return TransactionTest.sent(s, n); }

    /** The server has gone away while the connection sat idle, and the driver can see it. */
    static void lost(Skaidb.Connection c, ConformanceServer server, int connectionsSoFar) throws Exception {
        assertTrue(server.awaitEnded(connectionsSoFar, 2_000));
        assertTrue(ConformanceServer.awaitSeenClosed(c, 2_000));
    }

    // ---- the scenario -------------------------------------------------------------------

    /** One way of sending a statement that takes no parameters. */
    interface Send {
        void run(Skaidb.Connection c, String sql);
    }

    @Test
    void bothSettingsAreSentAgainOnTheNewConnection() throws Exception {
        Map<String, Send> paths = new LinkedHashMap<>();
        paths.put("execute", (c, sql) -> c.execute(sql));
        paths.put("query", (c, sql) -> c.query(sql));
        paths.put("executeRaw", (c, sql) -> c.executeRaw(sql));
        paths.put("prepare", (c, sql) -> c.prepare(sql).executeUpdate());
        for (Map.Entry<String, Send> path : paths.entrySet()) {
            String name = path.getKey();
            ConformanceServer server = ConformanceServer.sessions(List.of(
                List.of(q(BUDGET, ok()), q(ALL, ok()), q(SCAN, error(REFUSED)).thenHangUp()),
                // The new session: the consistency override, then the scan budget, then the statement.
                List.of(q(ALL, ok()), q(BUDGET, ok()), q(SCAN, error(REFUSED)))));
            try (Skaidb.Connection c = Skaidb.connect(options(server))) {
                path.getValue().run(c, BUDGET);
                path.getValue().run(c, ALL);
                assertEquals(REFUSED, assertThrows(Skaidb.SkaidbException.class, () -> c.query(SCAN)).getMessage(), name);
                lost(c, server, 1);
                // The limit the application set still holds: the same statement fails the same way.
                assertEquals(REFUSED, assertThrows(Skaidb.SkaidbException.class, () -> c.query(SCAN)).getMessage(), name);
                assertEquals(1, c.reconnects(), name);
            }
            assertNull(server.finish(), name);
            assertEquals(List.of(ALL, BUDGET, SCAN), sent(server, 1), name + ": the settings first, then the statement");
        }
    }

    @Test
    void theSessionDatabaseComesFirstThenTheSettings() throws Exception {
        ConformanceServer server = ConformanceServer.sessions(List.of(
            List.of(q("USE \"app\"", ok()), q(BUDGET, ok()), q("USE reports", ok()), q(ALL, ok()).thenHangUp()),
            List.of(q("USE \"reports\"", ok()), q(ALL, ok()), q(BUDGET, ok()), q(SCAN, rows(1)))));
        try (Skaidb.Connection c = Skaidb.connect(options(server).database("app"))) {
            c.execute(BUDGET);
            c.execute("USE reports");
            c.execute(ALL);
            lost(c, server, 1);
            assertEquals(1, c.query(SCAN).getRowCount());
        }
        assertNull(server.finish());
        assertEquals(List.of("USE \"reports\"", ALL, BUDGET, SCAN), sent(server, 1));
    }

    @Test
    void aSettingPutBackToItsDefaultIsNotSentAgain() throws Exception {
        ConformanceServer server = ConformanceServer.sessions(List.of(
            List.of(q(BUDGET, ok()), q(ALL, ok()), q("SET SCAN BUDGET DEFAULT", ok()),
                q("set consistency default ;", ok()).thenHangUp()),
            List.of(q(SCAN, rows(1, 2, 3)))));
        try (Skaidb.Connection c = Skaidb.connect(options(server))) {
            c.execute(BUDGET);
            c.execute(ALL);
            c.execute("SET SCAN BUDGET DEFAULT");
            c.execute("set consistency default ;");
            lost(c, server, 1);
            assertEquals(3, c.query(SCAN).getRowCount());
        }
        assertNull(server.finish());
        assertEquals(List.of(SCAN), sent(server, 1), "nothing is set on the new session");
    }

    @Test
    void theLastSettingOfEachKindIsTheOneSentAgain() throws Exception {
        ConformanceServer server = ConformanceServer.sessions(List.of(
            List.of(q(ALL, ok()), q(BUDGET, ok()), q("SET CONSISTENCY ONE", ok()),
                q("SET SCAN BUDGET BYTES 4096 ROWS 50", ok()), q("SET SCAN BUDGET DEFAULT", ok()),
                // as written, less its comment and its semicolon
                q("set scan budget\trows 7 ; -- for the report", ok()).thenHangUp()),
            // ... on every later dial as well
            List.of(q("SET CONSISTENCY ONE", ok()), q("set scan budget\trows 7", ok()), q(SCAN, rows(1)).thenHangUp()),
            List.of(q("SET CONSISTENCY ONE", ok()), q("set scan budget\trows 7", ok()), q(SCAN, rows(1)))));
        try (Skaidb.Connection c = Skaidb.connect(options(server))) {
            c.execute(ALL);
            c.execute(BUDGET);
            c.execute("SET CONSISTENCY ONE");
            c.execute("SET SCAN BUDGET BYTES 4096 ROWS 50");
            c.execute("SET SCAN BUDGET DEFAULT");
            c.execute("set scan budget\trows 7 ; -- for the report");
            lost(c, server, 1);
            assertEquals(1, c.query(SCAN).getRowCount());
            lost(c, server, 2);
            assertEquals(1, c.query(SCAN).getRowCount());
            assertEquals(2, c.reconnects());
        }
        assertNull(server.finish());
    }

    @Test
    void aStatementFailsWhenASettingIsRefusedOnTheNewConnection() throws Exception {
        String refused = "parse error: SET CONSISTENCY takes ONE, QUORUM or ALL";
        ConformanceServer server = ConformanceServer.sessions(List.of(
            List.of(q(ALL, ok()), q(BUDGET, ok()).thenHangUp()),
            // The new session refuses a setting: nothing else may be sent on it.
            List.of(q(ALL, error(refused))),
            List.of(q(ALL, ok()), q(BUDGET, error("the budget is refused"))),
            // The next statement dials again, and this time both are taken.
            List.of(q(ALL, ok()), q(BUDGET, ok()), q(SCAN, error(REFUSED)))));
        try (Skaidb.Connection c = Skaidb.connect(options(server))) {
            c.execute(ALL);
            c.execute(BUDGET);
            lost(c, server, 1);
            Skaidb.SkaidbException e = assertThrows(Skaidb.SkaidbException.class, () -> c.query(SCAN));
            assertEquals(refused, e.getMessage(), "the statement fails with what the setting was answered");
            assertTrue(server.awaitEnded(2, 2_000), "that session is given up, socket and all");
            assertEquals(List.of(ALL), sent(server, 1), "the statement did not run under other settings");
            assertFalse(c.isUsable());
            assertEquals("the budget is refused", assertThrows(Skaidb.SkaidbException.class, () -> c.query(SCAN)).getMessage());
            assertTrue(server.awaitEnded(3, 2_000));
            assertEquals(List.of(ALL, BUDGET), sent(server, 2));
            assertEquals(REFUSED, assertThrows(Skaidb.SkaidbException.class, () -> c.query(SCAN)).getMessage());
            assertTrue(c.isUsable());
        }
        assertNull(server.finish());
        assertEquals(List.of(ALL, BUDGET, SCAN), sent(server, 3));
    }

    @Test
    void onlyASettingTheServerAcceptedAndTheDriverCanReadIsNoted() throws Exception {
        ConformanceServer server = ConformanceServer.sessions(List.of(
            List.of(q(BUDGET, ok()),
                // refused: the session keeps the budget it had
                q("SET SCAN BUDGET ROWS many", error("parse error: expected a number")),
                // an older server does not know DEFAULT: the override it had stays noted
                q(ALL, ok()), q("SET CONSISTENCY DEFAULT", error("parse error: SET CONSISTENCY takes ONE, QUORUM or ALL")),
                // not one such statement as the driver reads it: left alone, whatever answers it
                q("SET CONSISTENCY=ONE", ok()), q("SET SCAN_BUDGET ROWS 9", ok()), q("SET CONFIG storage.x = 1", ok())
                    .thenHangUp()),
            List.of(q(ALL, ok()), q(BUDGET, ok()), q(SCAN, error(REFUSED)))));
        try (Skaidb.Connection c = Skaidb.connect(options(server))) {
            c.execute(BUDGET);
            assertThrows(Skaidb.SkaidbException.class, () -> c.execute("SET SCAN BUDGET ROWS many"));
            c.execute(ALL);
            assertThrows(Skaidb.SkaidbException.class, () -> c.execute("SET CONSISTENCY DEFAULT"));
            c.execute("SET CONSISTENCY=ONE");
            c.execute("SET SCAN_BUDGET ROWS 9");
            c.execute("SET CONFIG storage.x = 1");
            lost(c, server, 1);
            assertEquals(REFUSED, assertThrows(Skaidb.SkaidbException.class, () -> c.query(SCAN)).getMessage());
        }
        assertNull(server.finish());
        assertEquals(List.of(ALL, BUDGET, SCAN), sent(server, 1));
    }

    @Test
    void aSettingSentWithAParameterOrThroughAStreamIsNotedToo() throws Exception {
        String prepared = "SET SCAN BUDGET ROWS ?";
        ConformanceServer server = ConformanceServer.sessions(List.of(
            List.of(
                // the server prepares it: the value travels typed, and is noted as the literal it stands for
                TransactionTest.ex(ConformanceServer.prepareRequest(prepared), ConformanceServer.preparedResponse(1, 1)),
                TransactionTest.ex(ConformanceServer.executeRequest(1, Skaidb.CONSISTENCY_QUORUM, 5L), ok()).thenHangUp()),
            List.of(q("SET SCAN BUDGET ROWS 5", ok()),
                // or it refuses to, and the driver binds the literal itself
                TransactionTest.ex(ConformanceServer.prepareRequest(prepared), error("statement cannot be prepared")),
                q("SET SCAN BUDGET ROWS 6", ok()).thenHangUp()),
            List.of(q("SET SCAN BUDGET ROWS 6", ok()),
                TransactionTest.ex(ConformanceServer.streamRequest("SET SCAN BUDGET ROWS 7", Skaidb.CONSISTENCY_QUORUM), ok())
                    .thenHangUp()),
            List.of(q("SET SCAN BUDGET ROWS 7", ok()), q(SCAN, rows(1)))));
        try (Skaidb.Connection c = Skaidb.connect(options(server))) {
            c.prepare(prepared).setLong(1, 5L).executeUpdate();
            lost(c, server, 1);
            c.prepare(prepared).setLong(1, 6L).executeUpdate();
            lost(c, server, 2);
            c.stream("SET SCAN BUDGET ROWS 7").close();
            lost(c, server, 3);
            assertEquals(1, c.query(SCAN).getRowCount());
        }
        assertNull(server.finish());
    }

    @Test
    void whichStatementsSetTheSession() {
        Map<String, String[]> sets = new LinkedHashMap<>();
        sets.put("SET CONSISTENCY ALL", new String[] { "consistency", "SET CONSISTENCY ALL" });
        sets.put("set consistency one", new String[] { "consistency", "set consistency one" });
        sets.put("  Set\tConsistency  QUORUM ; ", new String[] { "consistency", "Set\tConsistency  QUORUM" });
        sets.put("SET CONSISTENCY ALL -- every replica", new String[] { "consistency", "SET CONSISTENCY ALL" });
        sets.put("-- every replica\nSET CONSISTENCY ALL;", new String[] { "consistency", "SET CONSISTENCY ALL" });
        sets.put("SET CONSISTENCY DEFAULT", new String[] { "consistency", null });
        sets.put("set consistency default;", new String[] { "consistency", null });
        sets.put("SET SCAN BUDGET ROWS 1", new String[] { "scan budget", "SET SCAN BUDGET ROWS 1" });
        sets.put("SET SCAN BUDGET ROWS 10 BYTES 4096", new String[] { "scan budget", "SET SCAN BUDGET ROWS 10 BYTES 4096" });
        sets.put("set scan budget bytes 4096 rows 10 ;", new String[] { "scan budget", "set scan budget bytes 4096 rows 10" });
        sets.put("SET SCAN BUDGET ROWS 1 -- tight", new String[] { "scan budget", "SET SCAN BUDGET ROWS 1" });
        sets.put("SET SCAN BUDGET DEFAULT", new String[] { "scan budget", null });
        sets.put("SET  SCAN\nBUDGET  Default -- back", new String[] { "scan budget", null });
        // The driver reads no further than the kind: what follows is the server's to judge.
        sets.put("SET CONSISTENCY sometimes", new String[] { "consistency", "SET CONSISTENCY sometimes" });
        sets.put("SET SCAN BUDGET", new String[] { "scan budget", "SET SCAN BUDGET" });
        sets.put("SET CONSISTENCY", new String[] { "consistency", "SET CONSISTENCY" });
        for (Map.Entry<String, String[]> e : sets.entrySet()) {
            String[] got = Skaidb.sessionSetting(e.getKey());
            assertEquals(e.getValue()[0], got == null ? null : got[0], e.getKey());
            assertEquals(e.getValue()[1], got[1], e.getKey());
        }
        for (String other : new String[] {
                "", "SET", "SET SCAN", "SET SCANBUDGET ROWS 1", "SET SCAN_BUDGET ROWS 1",
                "SET CONSISTENCY=ALL", "SETCONSISTENCY ALL", "RESET CONSISTENCY", "SET CONFIG storage.scan_row_budget = 1",
                "SET CONSISTENCY ALL; SELECT 1", "SET SCAN BUDGET ROWS 1;;", "-- SET CONSISTENCY ALL",
                "SELECT 'SET CONSISTENCY ALL'", "USE app", "BEGIN" }) {
            assertNull(Skaidb.sessionSetting(other), other);
        }
    }

    // ---- a pooled connection that is re-dialled while a borrower holds it ---------------------

    @Test
    void aBorrowersSettingsAreSentAgainWhenItsConnectionReDials() throws Exception {
        ConformanceServer server = ConformanceServer.sessions(List.of(
            List.of(q("USE \"app\"", ok()), q(BUDGET, ok()), q(ALL, ok()).thenHangUp()),
            List.of(q("USE \"app\"", ok()), q(ALL, ok()), q(BUDGET, ok()), q(SCAN, error(REFUSED)))));
        List<Skaidb.Connection> used = new ArrayList<>();
        try (Skaidb.Pool pool = new Skaidb.Pool(options(server).database("app"))) {
            Skaidb.Connection c = pool.acquire();
            try {
                used.add(c);
                c.execute(BUDGET);
                c.execute(ALL);
                lost(c, server, 1);
                assertEquals(REFUSED, assertThrows(Skaidb.SkaidbException.class, () -> c.query(SCAN)).getMessage());
                assertEquals(1, c.reconnects());
            } finally {
                pool.release(c);
            }
            assertSame(used.get(0), pool.withConnection(conn -> conn), "re-dialled in place, and kept");
        }
        assertNull(server.finish());
    }
}
