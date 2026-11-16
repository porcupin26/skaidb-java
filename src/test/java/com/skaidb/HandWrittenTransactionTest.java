package com.skaidb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * A transaction the application writes itself — {@code BEGIN}, its
 * statements, {@code COMMIT} or {@code ROLLBACK}, each sent as an ordinary
 * statement — lives on the connection that began it exactly as the {@code
 * transaction} helper's does (PROTOCOL.md §6 rule 7): the driver notes the
 * {@code BEGIN} the server accepted, and until the transaction's {@code
 * COMMIT} or {@code ROLLBACK} a lost connection is not replaced. Against
 * the scripted fake server: each test states every statement each session
 * may receive.
 */
class HandWrittenTransactionTest {

    static final int QUORUM = Skaidb.CONSISTENCY_QUORUM;
    static final String LOST = "the connection was lost during the transaction";
    static final String ROW_1 = "INSERT INTO notes (id) VALUES (1)";
    static final String ROW_2 = "INSERT INTO notes (id) VALUES (2)";
    static final String ROW_3 = "INSERT INTO notes (id) VALUES (3)";
    static final String COUNT = "SELECT count(*) FROM notes";

    static Skaidb.Connection connect(ConformanceServer s) { return TransactionTest.connect(s); }
    static ConformanceServer.Exchange q(String sql, byte[]... responses) { return TransactionTest.q(sql, responses); }
    static byte[] ok() { return ConformanceServer.ddlResponse(); }
    static byte[] changed(long n) { return ConformanceServer.mutationResponse(n); }
    static byte[] counted(long n) { return ConformanceServer.rowsResponse(new String[] { "count" }, new Object[] { n }); }
    static List<String> sent(ConformanceServer s, int n) { return TransactionTest.sent(s, n); }

    static long count(Skaidb.Connection c) {
        Skaidb.ResultSet rs = c.query(COUNT);
        rs.next();
        return rs.getLong(1);
    }

    /** The driver's transport error for a statement it sent: the I/O failure is its cause. */
    static Skaidb.SkaidbException assertTransport(Skaidb.SkaidbException e) {
        assertEquals(Skaidb.SkaidbException.class, e.getClass());
        assertInstanceOf(IOException.class, e.getCause(), e.getMessage());
        assertFalse(e.getMessage().startsWith(LOST), e.getMessage());
        return e;
    }

    /** The loss of the transaction's connection, reported for a statement the driver did not send. */
    static Skaidb.SkaidbException assertLost(Skaidb.SkaidbException e) {
        assertEquals(Skaidb.SkaidbException.class, e.getClass());
        assertTrue(e.getMessage().startsWith(LOST), e.getMessage());
        assertInstanceOf(IOException.class, e.getCause(), e.getMessage());
        return e;
    }

    static Skaidb.SkaidbException thrown(org.junit.jupiter.api.function.Executable statement) {
        return assertThrows(Skaidb.SkaidbException.class, statement);
    }

    // ---- the scenario ---------------------------------------------------------------

    @Test
    void nothingOfAHandWrittenTransactionRunsOnANewSession() throws Exception {
        ConformanceServer server = ConformanceServer.sessions(List.of(
            // The server goes away under the transaction's second row ...
            List.of(q("BEGIN", ok()), q(ROW_1, changed(1)), q(ROW_2).thenHangUp()),
            // ... and is back for whoever dials next: a session with no transaction open.
            List.of(q(COUNT, counted(0)))));
        try (Skaidb.Connection c = connect(server)) {
            c.execute("BEGIN");
            assertEquals(1, c.execute(ROW_1));
            assertTransport(thrown(() -> c.execute(ROW_2)));
            // The application carries on. On a new session this row would be committed on its own.
            assertLost(thrown(() -> c.execute(ROW_3)));
            assertEquals(1, server.connections(), "a statement of the transaction re-dialled");
            // So would the COMMIT "succeed" there, with nothing to commit.
            Skaidb.SkaidbException commit = assertLost(thrown(() -> c.execute("COMMIT")));
            assertTrue(commit.getMessage().contains("nothing was committed"), commit.getMessage());
            assertEquals(1, server.connections(), "the COMMIT re-dialled");
            assertEquals(0, c.reconnects());

            // The COMMIT ended the transaction for the driver too: the next statement re-dials as usual.
            assertEquals(0, count(c), "none of the three rows exists");
            assertEquals(1, c.reconnects());
            assertTrue(c.isUsable());
        }
        assertNull(server.finish());
        assertEquals(List.of("BEGIN", ROW_1, ROW_2), sent(server, 0));
        assertEquals(List.of(COUNT), sent(server, 1), "nothing of the transaction reached the new session");
    }

    @Test
    void aRollbackAfterTheLossSucceedsWithoutBeingSent() throws Exception {
        ConformanceServer server = ConformanceServer.sessions(List.of(
            List.of(q("BEGIN", ok()), q(ROW_1, changed(1)), q(ROW_2).thenHangUp()),
            List.of(q(COUNT, counted(0)))));
        try (Skaidb.Connection c = connect(server)) {
            c.execute("BEGIN");
            c.execute(ROW_1);
            assertTransport(thrown(() -> c.execute(ROW_2)));
            assertLost(thrown(() -> c.execute(ROW_3)));
            // The server discarded the transaction with its connection: it is rolled back.
            assertEquals(-1, c.execute("ROLLBACK"));
            assertEquals(1, server.connections(), "the ROLLBACK was sent on a new session");
            assertEquals(0, c.reconnects());
            assertEquals(0, count(c));
            assertEquals(1, c.reconnects());
        }
        assertNull(server.finish());
        assertEquals(List.of("BEGIN", ROW_1, ROW_2), sent(server, 0));
        assertEquals(List.of(COUNT), sent(server, 1));
    }

    @Test
    void withNoLossATransactionIsSentAsItIsWritten() throws Exception {
        ConformanceServer server = ConformanceServer.sessions(List.of(List.of(
            q("BEGIN", ok()), q(ROW_1, changed(1)), q("COMMIT", ok()),
            q("BEGIN TRANSACTION", ok()), q(ROW_2, changed(1)), q("ROLLBACK", ok()),
            // with nothing open, COMMIT and ROLLBACK are ordinary statements: sent, and answered by the server
            q("ROLLBACK", ok()), q("COMMIT", TransactionTest.error("constraint violation: no transaction in progress")),
            q(COUNT, counted(1)))));
        try (Skaidb.Connection c = connect(server)) {
            assertEquals(-1, c.execute("BEGIN"));
            assertEquals(1, c.execute(ROW_1));
            assertEquals(-1, c.execute("COMMIT"));
            c.execute("BEGIN TRANSACTION");
            c.execute(ROW_2);
            c.execute("ROLLBACK");
            c.execute("ROLLBACK");
            assertEquals("constraint violation: no transaction in progress", thrown(() -> c.execute("COMMIT")).getMessage());
            assertEquals(1, count(c));
            assertEquals(0, c.reconnects());
        }
        assertNull(server.finish());
    }

    // ---- which statement the connection is lost under ----------------------------------

    @Test
    void aConnectionLostUnderTheCommitItselfLeavesTheOutcomeUnknown() throws Exception {
        ConformanceServer server = ConformanceServer.sessions(List.of(
            // The reply to COMMIT never arrives: the server may or may not have applied it.
            List.of(q("BEGIN", ok()), q(ROW_1, changed(1)), q("COMMIT").thenHangUp()),
            List.of(q(COUNT, counted(1)))));
        try (Skaidb.Connection c = connect(server)) {
            c.execute("BEGIN");
            c.execute(ROW_1);
            Skaidb.SkaidbException e = thrown(() -> c.execute("COMMIT"));
            assertInstanceOf(IOException.class, e.getCause());
            assertTrue(e.getMessage().startsWith("query failed: the connection was closed by the server"), e.getMessage());
            assertTrue(e.getMessage().contains("unknown"), "the outcome of a lost COMMIT is unknown: " + e.getMessage());
            assertFalse(e.getMessage().contains("nothing was committed"), e.getMessage());
            // The COMMIT was the transaction's end: what follows is an ordinary statement.
            assertEquals(1, count(c));
            assertEquals(1, c.reconnects());
        }
        assertNull(server.finish());
        assertEquals(List.of("BEGIN", ROW_1, "COMMIT"), sent(server, 0));
        assertEquals(List.of(COUNT), sent(server, 1), "no second COMMIT on the new session");
    }

    @Test
    void aConnectionLostUnderTheRollbackEndsTheTransactionToo() throws Exception {
        ConformanceServer server = ConformanceServer.sessions(List.of(
            List.of(q("BEGIN", ok()), q(ROW_1, changed(1)), q("ROLLBACK").thenHangUp()),
            List.of(q(COUNT, counted(0)))));
        try (Skaidb.Connection c = connect(server)) {
            c.execute("BEGIN");
            c.execute(ROW_1);
            // The statement the connection is lost under fails, as any does ...
            assertTransport(thrown(() -> c.execute("ROLLBACK")));
            // ... and the transaction is over either way: nothing is held back for it.
            assertEquals(0, count(c));
        }
        assertNull(server.finish());
        assertEquals(List.of(COUNT), sent(server, 1));
    }

    @Test
    void aConnectionLostUnderTheBeginOpensNothing() throws Exception {
        ConformanceServer server = ConformanceServer.sessions(List.of(
            List.of(q("BEGIN").thenHangUp()),
            List.of(q("BEGIN", ok()), q(ROW_1, changed(1)), q("COMMIT", ok()))));
        try (Skaidb.Connection c = connect(server)) {
            assertTransport(thrown(() -> c.execute("BEGIN")));
            // Nothing was open, so nothing forbids the re-dial: the application starts over.
            c.execute("BEGIN");
            c.execute(ROW_1);
            c.execute("COMMIT");
            assertEquals(1, c.reconnects());
        }
        assertNull(server.finish());
    }

    // ---- every way of sending a statement -------------------------------------------------

    @Test
    void everyKindOfStatementIsRefusedOnceTheTransactionsConnectionIsLost() throws Exception {
        String prepared = "INSERT INTO notes (id, body) VALUES (?, ?)";
        ConformanceServer server = ConformanceServer.sessions(List.of(
            List.of(q("BEGIN", ok()), q(ROW_1).thenHangUp()),
            List.of(q(COUNT, counted(0)))));
        try (Skaidb.Connection c = connect(server)) {
            c.execute("BEGIN");
            assertTransport(thrown(() -> c.execute(ROW_1)));
            assertLost(thrown(() -> c.execute(ROW_2)));
            assertLost(thrown(() -> c.query(COUNT)));
            assertLost(thrown(() -> c.executeRaw(ROW_2)));
            assertLost(thrown(() -> c.prepare(prepared).setLong(1, 7L).setString(2, "x").executeUpdate()));
            assertLost(thrown(() -> c.prepare(prepared).executeBatch(List.of(new Object[] { 7L, "x" }, new Object[] { 8L, "y" }))));
            assertLost(thrown(() -> c.stream(COUNT)));
            assertLost(thrown(() -> c.stream("SELECT id FROM notes WHERE id > ?", 1L)));
            // A second BEGIN is a statement of the lost transaction like any other,
            // and so is the helper's.
            assertLost(thrown(() -> c.execute("BEGIN")));
            assertLost(thrown(() -> c.transaction(conn -> conn.execute(ROW_3))));
            assertFalse(c.isUsable());
            assertEquals(1, server.connections(), "a statement of the transaction re-dialled");
            assertEquals(0, c.reconnects());
            // ROLLBACK through a stream ends it as well as through execute.
            try (Skaidb.RowStream rolledBack = c.stream("ROLLBACK")) {
                assertFalse(rolledBack.next());
                assertEquals(-1, rolledBack.getAffected());
            }
            assertEquals(1, server.connections());
            assertEquals(0, count(c));
        }
        assertNull(server.finish());
        assertEquals(List.of("BEGIN", ROW_1), sent(server, 0));
        assertEquals(List.of(COUNT), sent(server, 1));
    }

    @Test
    void aTransactionOpenedAndEndedThroughStreamsIsNotedToo() throws Exception {
        ConformanceServer.Exchange begin = TransactionTest.ex(ConformanceServer.streamRequest("BEGIN", QUORUM), ok());
        ConformanceServer server = ConformanceServer.sessions(List.of(
            List.of(begin, q(ROW_1).thenHangUp()),
            List.of(q(COUNT, counted(0)))));
        try (Skaidb.Connection c = connect(server)) {
            c.stream("BEGIN").close();
            assertTransport(thrown(() -> c.execute(ROW_1)));
            assertLost(thrown(() -> c.execute(ROW_2)));
            Skaidb.SkaidbException commit = assertLost(thrown(() -> c.stream("COMMIT")));
            assertTrue(commit.getMessage().contains("nothing was committed"), commit.getMessage());
            assertEquals(1, server.connections());
            assertEquals(0, count(c));
        }
        assertNull(server.finish());
    }

    // ---- what opens a noted transaction, and what ends it ---------------------------------

    @Test
    void onlyABeginTheServerAcceptedOpensATransaction() throws Exception {
        ConformanceServer server = ConformanceServer.sessions(List.of(
            // A cluster refuses a plain BEGIN: nothing is open, so the connection re-dials as ever.
            List.of(q("BEGIN", TransactionTest.error("BEGIN needs a standalone server; use BEGIN ATOMIC")),
                q(ROW_1).thenHangUp()),
            List.of(q(ROW_2, changed(1)))));
        try (Skaidb.Connection c = connect(server)) {
            thrown(() -> c.execute("BEGIN"));
            assertTransport(thrown(() -> c.execute(ROW_1)));
            assertEquals(1, c.execute(ROW_2));
            assertEquals(1, c.reconnects());
        }
        assertNull(server.finish());
    }

    @Test
    void aRefusedCommitHasEndedTheTransaction() throws Exception {
        ConformanceServer server = ConformanceServer.sessions(List.of(
            // A COMMIT refused for a conflict ends the transaction (PROTOCOL.md §6 rule 6) ...
            List.of(q("BEGIN", ok()), q(ROW_1, changed(1)), q("COMMIT", TransactionTest.conflict()),
                q(ROW_2).thenHangUp()),
            // ... so a later loss is an ordinary one: the next statement re-dials.
            List.of(q(ROW_3, changed(1)))));
        try (Skaidb.Connection c = connect(server)) {
            c.execute("BEGIN");
            c.execute(ROW_1);
            TransactionTest.assertConflict(() -> c.execute("COMMIT"));
            assertTransport(thrown(() -> c.execute(ROW_2)));
            assertEquals(1, c.execute(ROW_3));
        }
        assertNull(server.finish());
    }

    @Test
    void whichStatementsOpenAndEndATransaction() {
        for (String begin : new String[] {
                "BEGIN", "begin", " \n\tBegin ; ", "BEGIN;", "BEGIN TRANSACTION", "BEGIN ATOMIC",
                "BEGIN ATOMIC PARTITION (tenant = 'a')", "BEGIN -- the transfer", "-- the transfer\nBEGIN" }) {
            assertTrue(Skaidb.beginsTransaction(begin), begin);
            assertEquals(0, Skaidb.endsTransaction(begin), begin);
        }
        for (String other : new String[] {
                "", "BEGINNING", "BEGIN_", "SELECT 'BEGIN'", "-- BEGIN", "EXPLAIN BEGIN", "\"BEGIN\"",
                "INSERT INTO t (id) VALUES (1)", "COMMIT", "ROLLBACK" }) {
            assertFalse(Skaidb.beginsTransaction(other), other);
        }
        Map<String, Integer> ends = new LinkedHashMap<>();
        ends.put("COMMIT", Skaidb.ENDS_COMMIT);
        ends.put("commit", Skaidb.ENDS_COMMIT);
        ends.put("  Commit  ;  ", Skaidb.ENDS_COMMIT);
        ends.put("COMMIT;", Skaidb.ENDS_COMMIT);
        ends.put("COMMIT TRANSACTION", Skaidb.ENDS_COMMIT);
        ends.put("commit\ttransaction ;", Skaidb.ENDS_COMMIT);
        ends.put("COMMIT -- done", Skaidb.ENDS_COMMIT);
        ends.put("-- done\nCOMMIT -- c\n;", Skaidb.ENDS_COMMIT);
        ends.put("ROLLBACK", Skaidb.ENDS_ROLLBACK);
        ends.put("rollback;", Skaidb.ENDS_ROLLBACK);
        ends.put("ROLLBACK TRANSACTION", Skaidb.ENDS_ROLLBACK);
        ends.put("ROLLBACK -- never mind", Skaidb.ENDS_ROLLBACK);
        for (Map.Entry<String, Integer> e : ends.entrySet()) {
            assertEquals(e.getValue(), Skaidb.endsTransaction(e.getKey()), e.getKey());
        }
        // Anything the driver cannot read as exactly that statement is sent as any other.
        for (String other : new String[] {
                "", "COMMITTED", "COMMIT WORK", "COMMIT;;", "COMMIT; SELECT 1", "COMMIT TRANSACTION now",
                "ROLLBACK TO sp", "ROLLBACKS", "-- COMMIT", "SELECT 'COMMIT'", "COMMIT /* c */", "END", "BEGIN" }) {
            assertEquals(0, Skaidb.endsTransaction(other), other);
        }
    }

    // ---- the helper is its own ------------------------------------------------------------

    @Test
    void theHelpersTransactionIsNotNotedASecondTime() throws Exception {
        ConformanceServer server = ConformanceServer.sessions(List.of(
            List.of(q("BEGIN", ok()), q(ROW_1).thenHangUp()),
            // After the helper returned the connection is as after any loss: no ROLLBACK is owed first.
            List.of(q(ROW_2, changed(1)), q("BEGIN", ok()), q(ROW_3, changed(1)), q("COMMIT", ok()),
                // ... and a COMMIT the application sends with nothing open goes to the server.
                q("COMMIT", TransactionTest.error("constraint violation: no transaction in progress")))));
        try (Skaidb.Connection c = connect(server)) {
            thrown(() -> c.transaction(conn -> conn.execute(ROW_1)));
            assertEquals(1, c.execute(ROW_2));
            assertEquals(1L, (long) c.transaction(conn -> conn.execute(ROW_3)));
            assertEquals("constraint violation: no transaction in progress", thrown(() -> c.execute("COMMIT")).getMessage());
            assertEquals(1, c.reconnects());
        }
        assertNull(server.finish());
    }
}
