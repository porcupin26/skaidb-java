package com.skaidb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

/**
 * The typed transaction-conflict error and the retrying {@code transaction}
 * helper (PROTOCOL.md §6 rule 6), against the scripted fake server: it
 * answers only the requests of its script, in order and byte for byte, so
 * each test states every statement the driver may send — and {@link
 * #sent} reads back the ones it did.
 */
class TransactionTest {

    static final int QUORUM = Skaidb.CONSISTENCY_QUORUM;

    /** The server's text today. The driver matches the prefix only. */
    static final String CONFLICT = "transaction conflict: a row of table accounts was changed by another writer "
        + "after the transaction began — nothing was committed; run the transaction again";

    static final String UPDATE = "UPDATE accounts SET balance = balance - 25 WHERE id = 1";
    static final String PREPARED = "INSERT INTO ledger (id, amount) VALUES (?, ?)";

    static ConformanceServer server(ConformanceServer.Exchange... script) throws IOException {
        return new ConformanceServer("ok", List.of(script));
    }

    static Skaidb.ConnectOptions options(ConformanceServer s) {
        return new Skaidb.ConnectOptions().host("127.0.0.1", s.port())
            .user(ConformanceServer.username()).password(ConformanceServer.password());
    }

    static Skaidb.Connection connect(ConformanceServer s) { return Skaidb.connect(options(s)); }

    static ConformanceServer.Exchange ex(byte[] request, byte[]... responses) {
        return new ConformanceServer.Exchange(request, List.of(responses));
    }

    /** One {@code OP_QUERY} statement and its answer (none: the server stays silent). */
    static ConformanceServer.Exchange q(String sql, byte[]... responses) {
        return ex(ConformanceServer.queryRequest(sql, QUORUM), responses);
    }

    static byte[] ok() { return ConformanceServer.ddlResponse(); }
    static byte[] changed(long n) { return ConformanceServer.mutationResponse(n); }
    static byte[] conflict() { return ConformanceServer.errorResponse(CONFLICT); }
    static byte[] error(String message) { return ConformanceServer.errorResponse(message); }
    static byte[] oneRow(String col, Object value) {
        return ConformanceServer.rowsResponse(new String[] { col }, new Object[] { value });
    }

    /** The statements the fake server received, in order. */
    static List<String> sent(ConformanceServer s) { return statements(s.requests()); }

    /** The statements the fake server received on connection {@code n} (the first is 0). */
    static List<String> sent(ConformanceServer s, int n) { return statements(s.requests(n)); }

    static List<String> statements(List<byte[]> requests) {
        List<String> out = new ArrayList<>();
        for (byte[] request : requests) {
            Skaidb.Reader r = new Skaidb.Reader(request);
            int op = r.u8();
            if (op == 1) {
                r.u8();
                out.add(r.text());
            } else if (op == 2) {
                out.add("PREPARE " + r.text());
            } else if (op == 5) {
                r.u8();
                out.add("STREAM " + r.text());
            } else {
                out.add("opcode " + op);
            }
        }
        return out;
    }

    /** {@code times} runs of BEGIN, the statement, COMMIT. */
    static List<String> runs(int times) {
        List<String> out = new ArrayList<>();
        for (int i = 0; i < times; i++) Collections.addAll(out, "BEGIN", UPDATE, "COMMIT");
        return out;
    }

    /**
     * A conflict, caught as the statement error the driver has always
     * thrown, carrying the server's message unchanged.
     */
    static Skaidb.TransactionConflictException assertConflict(Executable statement) {
        Skaidb.SkaidbException e = assertThrows(Skaidb.SkaidbException.class, statement);
        assertEquals(Skaidb.TransactionConflictException.class, e.getClass());
        assertEquals(CONFLICT, e.getMessage());
        assertNull(e.getCause(), "a statement error, not a transport failure");
        return (Skaidb.TransactionConflictException) e;
    }

    // ---- the typed error ---------------------------------------------------------

    @Test
    void aConflictIsATypedStatementErrorOnEveryPath() throws Exception {
        String prepared = "UPDATE accounts SET balance = ? WHERE id = 1";
        ConformanceServer server = server(
            q("COMMIT", conflict()),                                                        // execute
            q("COMMIT", conflict()),                                                        // query
            q("COMMIT", conflict()),                                                        // executeRaw
            ex(ConformanceServer.prepareRequest(prepared), ConformanceServer.preparedResponse(7, 1)),
            ex(ConformanceServer.executeRequest(7, QUORUM, 5L), conflict()),                // a prepared statement
            ex(ConformanceServer.batchRequest(7, QUORUM, new Object[] { 1L }, new Object[] { 2L }), conflict()),
            ex(ConformanceServer.streamRequest("COMMIT", QUORUM), conflict()),              // a stream's first frame
            ex(ConformanceServer.streamRequest("SELECT id FROM accounts", QUORUM),          // ... and a later one
                ConformanceServer.rowsHeaderResponse("id"),
                ConformanceServer.rowsChunkResponse(new Object[] { 1L }), conflict()),
            q("SELECT nope", error("unknown column nope")),
            q("SELECT 1", oneRow("x", 1L)));
        try (Skaidb.Connection c = connect(server)) {
            assertConflict(() -> c.execute("COMMIT"));
            assertConflict(() -> c.query("COMMIT"));
            assertConflict(() -> c.executeRaw("COMMIT"));
            assertConflict(() -> c.prepare(prepared).setLong(1, 5L).executeUpdate());
            assertConflict(() -> c.prepare(prepared).executeBatch(List.of(new Object[] { 1L }, new Object[] { 2L })));
            assertConflict(() -> c.stream("COMMIT"));
            Skaidb.RowStream rows = c.stream("SELECT id FROM accounts");
            assertTrue(rows.next());
            assertConflict(rows::next);

            // Any other server error is the plain statement error, as before.
            Skaidb.SkaidbException other = assertThrows(Skaidb.SkaidbException.class, () -> c.query("SELECT nope"));
            assertEquals(Skaidb.SkaidbException.class, other.getClass());

            // A conflict is a statement error: the same socket goes on serving.
            assertTrue(c.isUsable());
            assertEquals(1, c.query("SELECT 1").getRowCount());
            assertEquals(0, c.reconnects());
        }
        assertNull(server.finish());
    }

    @Test
    void onlyTheMessagePrefixMakesAConflict() {
        assertInstanceOf(Skaidb.TransactionConflictException.class, Skaidb.serverError(CONFLICT));
        assertInstanceOf(Skaidb.TransactionConflictException.class,
            Skaidb.serverError("transaction conflict: worded differently one day"));
        for (String other : new String[] {
                "transaction conflict", "Transaction conflict: x", "error: transaction conflict: x",
                " transaction conflict: x", "no transaction in progress", "" }) {
            assertEquals(Skaidb.SkaidbException.class, Skaidb.serverError(other).getClass(), other);
            assertEquals(other, Skaidb.serverError(other).getMessage());
        }
        // The typed error is a subtype of the statement error, so a catch
        // written for SkaidbException still catches it.
        assertTrue(Skaidb.SkaidbException.class.isAssignableFrom(Skaidb.TransactionConflictException.class));
    }

    // ---- the helper --------------------------------------------------------------

    @Test
    void commitsOnceAndReturnsTheBodysValue() throws Exception {
        ConformanceServer server = server(
            q("BEGIN", ok()),
            q(UPDATE, changed(1)),
            q("SELECT balance FROM accounts WHERE id = 1", oneRow("balance", 75L)),
            q("COMMIT", ok()));
        AtomicInteger runs = new AtomicInteger();
        try (Skaidb.Connection c = connect(server)) {
            long balance = c.transaction(conn -> {
                runs.incrementAndGet();
                assertSame(c, conn, "the body gets the connection the transaction is open on");
                assertEquals(1L, conn.execute(UPDATE));
                Skaidb.ResultSet rs = conn.query("SELECT balance FROM accounts WHERE id = 1");
                rs.next();
                return rs.getLong("balance");
            });
            assertEquals(75L, balance);
        }
        assertNull(server.finish());
        assertEquals(1, runs.get());
        assertEquals(List.of("BEGIN", UPDATE, "SELECT balance FROM accounts WHERE id = 1", "COMMIT"), sent(server));
    }

    @Test
    void aConflictRunsTheBodyAgainAndTheLastRunsValueIsReturned() throws Exception {
        ConformanceServer server = server(
            q("BEGIN", ok()), q(UPDATE, changed(1)), q("COMMIT", conflict()),
            q("BEGIN", ok()), q(UPDATE, changed(1)), q("COMMIT", conflict()),
            q("BEGIN", ok()), q(UPDATE, changed(1)), q("COMMIT", ok()));
        AtomicInteger runs = new AtomicInteger();
        try (Skaidb.Connection c = connect(server)) {
            String value = c.transaction(conn -> {
                conn.execute(UPDATE);
                return "run " + runs.incrementAndGet();
            }, 3);
            assertEquals("run 3", value);
            assertTrue(c.isUsable());
            assertEquals(0, c.reconnects(), "every run on the one socket");
        }
        assertNull(server.finish());
        assertEquals(3, runs.get());
        assertEquals(runs(3), sent(server));
    }

    @Test
    void aConflictOnEveryCommitIsThrownAfterTheLastAttempt() throws Exception {
        ConformanceServer server = server(
            q("BEGIN", ok()), q(UPDATE, changed(1)), q("COMMIT", conflict()),
            q("BEGIN", ok()), q(UPDATE, changed(1)), q("COMMIT", conflict()),
            q("SELECT 1", oneRow("x", 1L)));
        AtomicInteger runs = new AtomicInteger();
        try (Skaidb.Connection c = connect(server)) {
            assertConflict(() -> c.transaction(conn -> {
                runs.incrementAndGet();
                return conn.execute(UPDATE);
            }, 2));
            assertEquals(2, runs.get(), "exactly two runs");
            // Nothing is open any more and the connection goes on serving.
            assertTrue(c.isUsable());
            assertEquals(1, c.query("SELECT 1").getRowCount());
        }
        assertNull(server.finish());
        List<String> want = runs(2);
        want.add("SELECT 1");
        assertEquals(want, sent(server));
    }

    @Test
    void byDefaultTheBodyRunsAtMostThreeTimesAfterBegin() throws Exception {
        ConformanceServer server = server(
            q("BEGIN", ok()), q(UPDATE, changed(1)), q("COMMIT", conflict()),
            q("BEGIN", ok()), q(UPDATE, changed(1)), q("COMMIT", conflict()),
            q("BEGIN", ok()), q(UPDATE, changed(1)), q("COMMIT", conflict()));
        AtomicInteger runs = new AtomicInteger();
        try (Skaidb.Connection c = connect(server)) {
            assertConflict(() -> c.transaction(conn -> {
                runs.incrementAndGet();
                return conn.execute(UPDATE);
            }));
        }
        assertNull(server.finish());
        assertEquals(3, runs.get());
        assertEquals(runs(3), sent(server));
    }

    @Test
    void aBodyErrorRollsBackAndIsThrownAsItIs() throws Exception {
        ConformanceServer server = server(
            // the body throws its own exception
            q("BEGIN", ok()), q(UPDATE, changed(1)), q("ROLLBACK", ok()),
            // ... and the ROLLBACK itself is refused (a server before 0.347 with nothing open)
            q("BEGIN", ok()), q("ROLLBACK", error("no transaction in progress")),
            // a statement of the body fails
            q("BEGIN", ok()), q("INSERT INTO accounts (id) VALUES (1)", error("unique violation: accounts.id")),
            q("ROLLBACK", error("no transaction in progress")),
            // the body throws a conflict: rolled back like any other error, never run again
            q("BEGIN", ok()), q("ROLLBACK", ok()),
            q("SELECT 1", oneRow("x", 1L)));
        AtomicInteger runs = new AtomicInteger();
        try (Skaidb.Connection c = connect(server)) {
            IllegalStateException mine = new IllegalStateException("not enough funds");
            assertSame(mine, assertThrows(IllegalStateException.class, () -> c.transaction(conn -> {
                runs.incrementAndGet();
                conn.execute(UPDATE);
                throw mine;
            })));
            assertSame(mine, assertThrows(IllegalStateException.class, () -> c.transaction(conn -> {
                runs.incrementAndGet();
                throw mine;
            })), "a refused ROLLBACK never replaces the body's error");

            Skaidb.SkaidbException refused = assertThrows(Skaidb.SkaidbException.class, () -> c.transaction(conn -> {
                runs.incrementAndGet();
                return conn.execute("INSERT INTO accounts (id) VALUES (1)");
            }));
            assertEquals("unique violation: accounts.id", refused.getMessage());

            Skaidb.TransactionConflictException thrown = new Skaidb.TransactionConflictException(CONFLICT);
            assertSame(thrown, assertConflict(() -> c.transaction(conn -> {
                runs.incrementAndGet();
                throw thrown;
            }, 3)));
            assertEquals(4, runs.get(), "one run each: a body error is never retried");

            assertTrue(c.isUsable());
            assertEquals(1, c.query("SELECT 1").getRowCount());
        }
        assertNull(server.finish());
        assertEquals(List.of(
            "BEGIN", UPDATE, "ROLLBACK",
            "BEGIN", "ROLLBACK",
            "BEGIN", "INSERT INTO accounts (id) VALUES (1)", "ROLLBACK",
            "BEGIN", "ROLLBACK",
            "SELECT 1"), sent(server));
    }

    @Test
    void aRefusedBeginSendsNothingElse() throws Exception {
        ConformanceServer server = server(
            q("BEGIN", error("BEGIN needs a standalone server; use BEGIN ATOMIC")),
            q("SELECT 1", oneRow("x", 1L)));
        AtomicInteger runs = new AtomicInteger();
        try (Skaidb.Connection c = connect(server)) {
            Skaidb.SkaidbException e = assertThrows(Skaidb.SkaidbException.class, () -> c.transaction(conn -> {
                runs.incrementAndGet();
                return conn.execute(UPDATE);   // would autocommit
            }));
            assertEquals("BEGIN needs a standalone server; use BEGIN ATOMIC", e.getMessage());
            assertEquals(0, runs.get(), "the body never runs");
            assertEquals(1, c.query("SELECT 1").getRowCount());
        }
        assertNull(server.finish());
        assertEquals(List.of("BEGIN", "SELECT 1"), sent(server), "no COMMIT, no ROLLBACK");
    }

    @Test
    void aCommitRefusedForAnotherReasonIsNotRetried() throws Exception {
        ConformanceServer server = server(
            q("BEGIN", ok()), q(UPDATE, changed(1)), q("COMMIT", error("check violation: balance >= 0")),
            q("SELECT 1", oneRow("x", 1L)));
        AtomicInteger runs = new AtomicInteger();
        try (Skaidb.Connection c = connect(server)) {
            Skaidb.SkaidbException e = assertThrows(Skaidb.SkaidbException.class, () -> c.transaction(conn -> {
                runs.incrementAndGet();
                return conn.execute(UPDATE);
            }, 3));
            assertEquals(Skaidb.SkaidbException.class, e.getClass());
            assertEquals("check violation: balance >= 0", e.getMessage());
            assertEquals(1, runs.get());
            assertEquals(1, c.query("SELECT 1").getRowCount());
        }
        assertNull(server.finish());
        assertEquals(List.of("BEGIN", UPDATE, "COMMIT", "SELECT 1"), sent(server), "no second BEGIN, no ROLLBACK");
    }

    @Test
    void badArgumentsAreRefusedBeforeAnythingIsSent() throws Exception {
        ConformanceServer server = server();
        AtomicInteger runs = new AtomicInteger();
        Skaidb.TransactionBody<Long> body = conn -> {
            runs.incrementAndGet();
            return conn.execute(UPDATE);
        };
        try (Skaidb.Connection c = connect(server)) {
            for (int attempts : new int[] { 0, -1, Integer.MIN_VALUE }) {
                Skaidb.SkaidbException e = assertThrows(Skaidb.SkaidbException.class, () -> c.transaction(body, attempts));
                assertEquals("transaction attempts must be >= 1, got " + attempts, e.getMessage());
            }
            assertThrows(Skaidb.SkaidbException.class, () -> c.transaction(body, 0, "BEGIN ATOMIC"));
            assertThrows(Skaidb.SkaidbException.class, () -> c.transaction((Skaidb.TransactionBody<Long>) null));
            assertThrows(Skaidb.SkaidbException.class, () -> c.transaction(body, 3, null));
            assertTrue(c.isUsable());
        }
        assertNull(server.finish());
        assertEquals(0, runs.get());
        assertEquals(List.of(), sent(server), "not one statement");
    }

    @Test
    void aCustomBeginIsSentVerbatim() throws Exception {
        // A '?' in it is text, not a placeholder.
        String begin = "BEGIN ATOMIC PARTITION (tenant = 'why?')";
        ConformanceServer server = server(
            q(begin, ok()), q(UPDATE, changed(1)), q("COMMIT", conflict()),
            q(begin, ok()), q(UPDATE, changed(1)), q("COMMIT", ok()));
        try (Skaidb.Connection c = connect(server)) {
            long changed = c.transaction(conn -> conn.execute(UPDATE), 2, begin);
            assertEquals(1L, changed);
        }
        assertNull(server.finish());
        assertEquals(List.of(begin, UPDATE, "COMMIT", begin, UPDATE, "COMMIT"), sent(server));
    }

    // ---- a transport failure is not a conflict --------------------------------------

    @Test
    void aTransportFailureInTheBodyIsNotRetriedAndNothingFollowsOnANewSocket() throws Exception {
        // The server never answers the body's statement: the read times out.
        ConformanceServer server = server(q("BEGIN", ok()), q(UPDATE));
        AtomicInteger runs = new AtomicInteger();
        try (Skaidb.Connection c = connect(server)) {
            c.setReadTimeout(200);
            Skaidb.SkaidbException e = assertThrows(Skaidb.SkaidbException.class, () -> c.transaction(conn -> {
                runs.incrementAndGet();
                return conn.execute(UPDATE);
            }, 3));
            assertEquals(Skaidb.SkaidbException.class, e.getClass());
            assertInstanceOf(java.net.SocketTimeoutException.class, e.getCause());
            assertEquals(1, runs.get());
            // The server discarded the transaction with the socket: a
            // ROLLBACK would only re-dial to roll back nothing.
            assertFalse(c.isUsable());
            assertEquals(0, c.reconnects());
        }
        assertNull(server.finish());
        assertEquals(List.of("BEGIN", UPDATE), sent(server));
    }

    @Test
    void aBodyThatSwallowsATransportFailureGetsNoCommit() throws Exception {
        ConformanceServer server = server(q("BEGIN", ok()), q(UPDATE));
        try (Skaidb.Connection c = connect(server)) {
            c.setReadTimeout(200);
            Skaidb.SkaidbException e = assertThrows(Skaidb.SkaidbException.class, () -> c.transaction(conn -> {
                try {
                    conn.execute(UPDATE);
                } catch (Skaidb.SkaidbException swallowed) {
                    // carries on as if the transaction were still there
                }
                return "done";
            }));
            assertTrue(e.getMessage().startsWith("the connection was lost during the transaction"), e.getMessage());
            assertEquals(0, c.reconnects(), "COMMIT was not sent on a re-dialled socket");
        }
        assertNull(server.finish());
        assertEquals(List.of("BEGIN", UPDATE), sent(server));
    }

    // ---- the transaction never leaves its session (PROTOCOL.md §6 rule 7) -----------

    static final String LOST = "the connection was lost during the transaction";
    static final String DEBIT = "UPDATE accounts SET balance = balance - 25 WHERE id = 2";
    static final String CREDIT = "INSERT INTO ledger (id, amount) VALUES (7, 25)";

    /** The driver's transport error: the I/O failure is its cause. */
    static void assertTransport(Skaidb.SkaidbException e) {
        assertEquals(Skaidb.SkaidbException.class, e.getClass());
        assertInstanceOf(IOException.class, e.getCause(), e.getMessage());
    }

    /** The loss of the transaction's connection, reported for a statement the driver did not send. */
    static void assertLost(Skaidb.SkaidbException e) {
        assertTransport(e);
        assertTrue(e.getMessage().startsWith(LOST), e.getMessage());
    }

    @Test
    void noStatementOfTheBodyRunsOnANewSessionAfterTheConnectionIsLost() throws Exception {
        ConformanceServer server = ConformanceServer.sessions(List.of(
            // The server goes away under the body's second statement ...
            List.of(q("BEGIN", ok()), q(UPDATE, changed(1)), q(DEBIT).thenHangUp()),
            // ... and is back for whoever dials next: a session with no transaction open.
            List.of(q("SELECT 1", oneRow("x", 1L)))));
        List<Skaidb.SkaidbException> raised = new ArrayList<>();
        try (Skaidb.Connection c = connect(server)) {
            Skaidb.SkaidbException reported = assertThrows(Skaidb.SkaidbException.class, () -> c.transaction(conn -> {
                conn.execute(UPDATE);
                try {
                    conn.execute(DEBIT);
                } catch (Skaidb.SkaidbException e) {
                    raised.add(e);   // swallowed: the body carries on
                }
                try {
                    conn.execute(CREDIT);   // on a new session this write would autocommit
                } catch (Skaidb.SkaidbException e) {
                    raised.add(e);
                }
                return "done";
            }));
            assertEquals(1, server.connections(), "a statement of the body re-dialled");
            assertEquals(2, raised.size(), "the statement after the loss raises too");
            assertTransport(raised.get(0));
            assertLost(raised.get(1));
            // The helper reports the loss; it sent neither COMMIT nor ROLLBACK anywhere.
            assertLost(reported);
            assertEquals(0, c.reconnects());

            // The connection is as after any connection loss: its next statement re-dials.
            assertFalse(c.isUsable());
            assertEquals(1, c.query("SELECT 1").getRowCount());
            assertEquals(1, c.reconnects());
            assertTrue(c.isUsable());
        }
        assertNull(server.finish());
        assertEquals(List.of("BEGIN", UPDATE, DEBIT), sent(server, 0));
        assertEquals(List.of("SELECT 1"), sent(server, 1), "nothing of the transaction reached the new session");
    }

    @Test
    void everyKindOfStatementIsRefusedOnceTheTransactionsConnectionIsLost() throws Exception {
        ConformanceServer server = ConformanceServer.sessions(List.of(
            List.of(q("BEGIN", ok()), q(UPDATE).thenHangUp()),
            List.of(q("SELECT 1", oneRow("x", 1L)))));
        try (Skaidb.Connection c = connect(server)) {
            assertLost(assertThrows(Skaidb.SkaidbException.class, () -> c.transaction(conn -> {
                assertTransport(assertThrows(Skaidb.SkaidbException.class, () -> conn.execute(UPDATE)));
                assertLost(assertThrows(Skaidb.SkaidbException.class, () -> conn.execute(CREDIT)));
                assertLost(assertThrows(Skaidb.SkaidbException.class, () -> conn.query("SELECT balance FROM accounts")));
                assertLost(assertThrows(Skaidb.SkaidbException.class, () -> conn.executeRaw(CREDIT)));
                assertLost(assertThrows(Skaidb.SkaidbException.class,
                    () -> conn.prepare(PREPARED).setLong(1, 7L).setLong(2, 25L).executeUpdate()));
                assertLost(assertThrows(Skaidb.SkaidbException.class,
                    () -> conn.prepare(PREPARED).executeBatch(List.of(new Object[] { 7L, 25L }, new Object[] { 8L, 5L }))));
                assertLost(assertThrows(Skaidb.SkaidbException.class, () -> conn.stream("SELECT id FROM ledger")));
                assertLost(assertThrows(Skaidb.SkaidbException.class, () -> conn.stream("SELECT id FROM ledger WHERE id > ?", 1L)));
                // A transaction opened inside the lost one is refused at its begin.
                assertLost(assertThrows(Skaidb.SkaidbException.class, () -> conn.transaction(inner -> inner.execute(CREDIT))));
                assertEquals(0, conn.reconnects());
                return null;
            })));
            assertEquals(1, server.connections(), "a statement of the body re-dialled");
            assertEquals(1, c.query("SELECT 1").getRowCount());
        }
        assertNull(server.finish());
        assertEquals(List.of("BEGIN", UPDATE), sent(server, 0));
        assertEquals(List.of("SELECT 1"), sent(server, 1));
    }

    @Test
    void aPoolsTransactionStaysOnItsSessionToo() throws Exception {
        ConformanceServer server = ConformanceServer.sessions(List.of(
            List.of(q("BEGIN", ok()), q(UPDATE, changed(1)), q(DEBIT).thenHangUp()),
            List.of(q("SELECT 1", oneRow("x", 1L)))));
        List<Skaidb.Connection> used = new ArrayList<>();
        try (Skaidb.Pool pool = new Skaidb.Pool(options(server))) {
            assertLost(assertThrows(Skaidb.SkaidbException.class, () -> pool.transaction(conn -> {
                used.add(conn);
                conn.execute(UPDATE);
                assertTransport(assertThrows(Skaidb.SkaidbException.class, () -> conn.execute(DEBIT)));
                assertLost(assertThrows(Skaidb.SkaidbException.class, () -> conn.execute(CREDIT)));
                return null;
            })));
            assertEquals(1, server.connections(), "a statement of the body re-dialled");
            // The lost connection did not go back to the pool: the next checkout dials.
            assertEquals(0, pool.members().get(0).idle);
            assertEquals(1, (int) pool.withConnection(conn -> {
                used.add(conn);
                return conn.query("SELECT 1").getRowCount();
            }));
            assertFalse(used.get(0) == used.get(1));
        }
        assertNull(server.finish());
        assertEquals(List.of("BEGIN", UPDATE, DEBIT), sent(server, 0));
        assertEquals(List.of("SELECT 1"), sent(server, 1));
    }

    @Test
    void aConnectionLostUnderTheCommitIsReportedAsThatFailure() throws Exception {
        // The reply to COMMIT never arrives: the server may or may not have applied it.
        ConformanceServer server = ConformanceServer.sessions(List.of(
            List.of(q("BEGIN", ok()), q(UPDATE, changed(1)), q("COMMIT").thenHangUp()),
            List.of(q("SELECT 1", oneRow("x", 1L)))));
        AtomicInteger runs = new AtomicInteger();
        try (Skaidb.Connection c = connect(server)) {
            Skaidb.SkaidbException e = assertThrows(Skaidb.SkaidbException.class, () -> c.transaction(conn -> {
                runs.incrementAndGet();
                return conn.execute(UPDATE);
            }, 3));
            assertTransport(e);
            assertTrue(e.getMessage().startsWith("query failed"), e.getMessage());
            assertFalse(e.getMessage().contains("discarded"), "the outcome of a lost COMMIT is unknown: " + e.getMessage());
            assertEquals(1, runs.get(), "not run again");
            assertEquals(1, server.connections(), "no ROLLBACK, no second COMMIT on a new session");
            assertEquals(1, c.query("SELECT 1").getRowCount());
        }
        assertNull(server.finish());
        assertEquals(List.of("BEGIN", UPDATE, "COMMIT"), sent(server, 0));
        assertEquals(List.of("SELECT 1"), sent(server, 1));
    }

    // ---- a broken connection closes its socket at once ------------------------------

    /** How long the fake server waits to see the driver's socket close. */
    static final long SOON = 2_000;

    @Test
    void aReadTimeoutInsideATransactionClosesTheSocketAtOnce() throws Exception {
        // The server never answers the body's statement: the read times out.
        ConformanceServer server = server(q("BEGIN", ok()), q(UPDATE));
        try (Skaidb.Connection c = connect(server)) {
            c.setReadTimeout(200);
            Skaidb.SkaidbException e = assertThrows(Skaidb.SkaidbException.class,
                () -> c.transaction(conn -> conn.execute(UPDATE)));
            assertInstanceOf(java.net.SocketTimeoutException.class, e.getCause());
            assertFalse(c.isUsable());
            // Nothing more is asked of the connection — no statement, no
            // close() — and the server already sees the socket go: it drops
            // the session, and the transaction left open on it, now.
            assertTrue(server.awaitEnded(1, SOON), "the dead socket stayed open, and the transaction with it");
            assertEquals(0, c.reconnects());
        }
        assertNull(server.finish());
        assertEquals(List.of("BEGIN", UPDATE), sent(server));
    }

    /** One way of breaking a connection: the script, and the call that fails on it. */
    static final class Break {
        final String name;
        final List<ConformanceServer.Exchange> script;
        final java.util.function.Consumer<Skaidb.Connection> call;
        final boolean raises;
        Break(String name, java.util.function.Consumer<Skaidb.Connection> call, boolean raises,
              ConformanceServer.Exchange... script) {
            this.name = name;
            this.script = List.of(script);
            this.call = call;
            this.raises = raises;
        }
    }

    @Test
    void howeverAConnectionBreaksItsSocketIsClosedAtOnce() throws Exception {
        String select = "SELECT id FROM accounts";
        byte[] stream = ConformanceServer.streamRequest(select, QUORUM);
        byte[] header = ConformanceServer.rowsHeaderResponse("id");
        byte[] chunk = ConformanceServer.rowsChunkResponse(new Object[] { 1L });
        List<Break> breaks = List.of(
            new Break("a statement's answer times out", c -> c.query("SELECT 1"), true, q("SELECT 1")),
            new Break("a prepare's answer times out", c -> c.prepare(PREPARED).setLong(1, 7L).setLong(2, 25L).executeUpdate(),
                true, ex(ConformanceServer.prepareRequest(PREPARED))),
            new Break("a stream's first frame times out", c -> c.stream(select), true, ex(stream)),
            new Break("a stream's next frame times out", c -> c.stream(select).next(), true, ex(stream, header)),
            // The socket is healthy in the next two: only the position on it is lost.
            new Break("a stream request is answered with a frame no stream starts with", c -> c.stream(select),
                true, ex(stream, ConformanceServer.preparedResponse(1, 0))),
            new Break("a frame that is not the stream's arrives in it", c -> c.stream(select).next(),
                true, ex(stream, header, ok())),
            new Break("an abandoned stream cannot be drained", c -> {
                Skaidb.RowStream rows = c.stream(select);
                assertTrue(rows.next());
                rows.close();   // never throws; the rest of the stream does not arrive
            }, false, ex(stream, header, chunk)));
        for (Break b : breaks) {
            ConformanceServer server = new ConformanceServer("ok", b.script);
            try (Skaidb.Connection c = connect(server)) {
                c.setReadTimeout(200);
                if (b.raises) assertThrows(Skaidb.SkaidbException.class, () -> b.call.accept(c), b.name);
                else b.call.accept(c);
                assertFalse(c.isUsable(), b.name);
                assertTrue(server.awaitEnded(1, SOON), b.name + ": the socket stayed open");
                assertEquals(0, c.reconnects(), b.name);
            }
            assertNull(server.finish(), b.name);
        }
    }

    // ---- threads and pools ---------------------------------------------------------

    @Test
    void anotherThreadsStatementWaitsForTheTransaction() throws Exception {
        ConformanceServer server = server(
            q("BEGIN", ok()), q(UPDATE, changed(1)), q("COMMIT", ok()),
            q("SELECT 2", oneRow("x", 2L)));
        AtomicReference<Throwable> failure = new AtomicReference<>();
        try (Skaidb.Connection c = connect(server)) {
            Thread other = new Thread(() -> {
                try {
                    c.query("SELECT 2");
                } catch (Throwable t) {
                    failure.set(t);
                }
            }, "other-statement");
            c.transaction(conn -> {
                other.start();
                // The other thread is parked on the connection's monitor
                // before the transaction sends anything more.
                long deadline = System.nanoTime() + 10_000_000_000L;
                while (other.getState() != Thread.State.BLOCKED) {
                    assertTrue(other.isAlive() && System.nanoTime() < deadline, "the other statement did not wait");
                    Thread.onSpinWait();
                }
                return conn.execute(UPDATE);
            });
            other.join(10_000);
            assertFalse(other.isAlive());
        }
        assertNull(failure.get());
        assertNull(server.finish());
        assertEquals(List.of("BEGIN", UPDATE, "COMMIT", "SELECT 2"), sent(server));
    }

    @Test
    void aPoolPinsOneConnectionForTheWholeTransaction() throws Exception {
        ConformanceServer server = server(
            q("BEGIN ATOMIC", ok()), q(UPDATE, changed(1)), q("COMMIT", conflict()),
            q("BEGIN ATOMIC", ok()), q(UPDATE, changed(1)), q("COMMIT", ok()),
            q("BEGIN", ok()), q("ROLLBACK", ok()),
            q("SELECT 1", oneRow("x", 1L)));
        List<Skaidb.Connection> used = new ArrayList<>();
        try (Skaidb.Pool pool = new Skaidb.Pool(options(server))) {
            long changed = pool.transaction(conn -> {
                used.add(conn);
                return conn.execute(UPDATE);
            }, 2, "BEGIN ATOMIC");
            assertEquals(1L, changed);
            assertEquals(2, used.size());
            assertSame(used.get(0), used.get(1), "both runs on the connection checked out for the transaction");

            // The conflict said nothing about the member, and the connection came back.
            Skaidb.Pool.Member member = pool.members().get(0);
            assertEquals("ready", member.state);
            assertEquals(0, member.inUse);
            assertEquals(1, member.idle);
            synchronized (pool) {
                assertEquals(0, pool.members.get(member.endpoint).errors);
            }

            // A body error rolls back and returns the connection too.
            IllegalStateException mine = new IllegalStateException("no");
            assertSame(mine, assertThrows(IllegalStateException.class, () -> pool.transaction(conn -> {
                used.add(conn);
                throw mine;
            })));
            assertSame(used.get(0), used.get(2));

            Skaidb.Connection again = pool.acquire();
            try {
                assertSame(used.get(0), again, "returned to the pool after, and usable");
                assertEquals(1, again.query("SELECT 1").getRowCount());
                assertEquals(0, again.reconnects());
            } finally {
                pool.release(again);
            }
        }
        assertNull(server.finish());
        assertEquals(List.of(
            "BEGIN ATOMIC", UPDATE, "COMMIT", "BEGIN ATOMIC", UPDATE, "COMMIT",
            "BEGIN", "ROLLBACK", "SELECT 1"), sent(server));
    }

    @Test
    void aPoolRefusesBadArgumentsBeforeItChecksOutAConnection() throws Exception {
        int dead;
        try (ServerSocket s = new ServerSocket(0)) { dead = s.getLocalPort(); }
        AtomicInteger runs = new AtomicInteger();
        try (Skaidb.Pool pool = new Skaidb.Pool("skaidb://u:p@127.0.0.1:" + dead)) {
            // Nothing listens there: a checkout would fail with "no reachable endpoint".
            Skaidb.SkaidbException e = assertThrows(Skaidb.SkaidbException.class, () -> pool.transaction(conn -> {
                runs.incrementAndGet();
                return null;
            }, 0));
            assertEquals("transaction attempts must be >= 1, got 0", e.getMessage());
            assertEquals(0, pool.members().get(0).inUse);
        }
        assertEquals(0, runs.get());
    }
}
