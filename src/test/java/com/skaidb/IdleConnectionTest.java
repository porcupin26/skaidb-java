package com.skaidb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * A connection the server closed while it sat idle costs no statement. A
 * restart, a failover or an idle timeout closes idle connections; over a
 * real network the next WRITE to such a socket still succeeds — the reset
 * for it has not come back yet — and only the read fails, after which the
 * statement cannot be sent again (it may have run). So the driver looks at
 * the socket before it writes: on an idle connection the server sends
 * nothing, and anything readable there — the end of the stream, a reset,
 * stray bytes — means the connection is gone. It is then replaced exactly
 * as after a failed write, before anything was sent on it.
 *
 * <p>The scripted server makes that deterministic on loopback with a
 * half-close: it shuts down only its sending side, goes on reading, and
 * counts every byte that still arrives.
 */
class IdleConnectionTest {

    static final int QUORUM = Skaidb.CONSISTENCY_QUORUM;
    static final String COUNT = "SELECT count(*) FROM notes";
    static final String INSERT = "INSERT INTO notes (id) VALUES (4)";
    static final String PREPARED = "INSERT INTO notes (id, body) VALUES (?, ?)";

    /** How long the tests give the server's close to reach the client. */
    static final long ARRIVES = 100;

    static Skaidb.ConnectOptions options(ConformanceServer s) { return TransactionTest.options(s); }
    static ConformanceServer.Exchange q(String sql, byte[]... responses) { return TransactionTest.q(sql, responses); }
    static ConformanceServer.Exchange ex(byte[] request, byte[]... responses) { return TransactionTest.ex(request, responses); }
    static byte[] ok() { return ConformanceServer.ddlResponse(); }
    static byte[] changed(long n) { return ConformanceServer.mutationResponse(n); }
    static byte[] counted(long n) { return ConformanceServer.rowsResponse(new String[] { "count" }, new Object[] { n }); }
    static List<String> sent(ConformanceServer s, int n) { return TransactionTest.sent(s, n); }

    /** One way of running a statement, and the script that answers it. */
    static final class Way {
        final List<ConformanceServer.Exchange> script;
        final java.util.function.Consumer<Skaidb.Connection> run;
        Way(java.util.function.Consumer<Skaidb.Connection> run, ConformanceServer.Exchange... script) {
            this.script = List.of(script);
            this.run = run;
        }
    }

    /** Reads and writes, on every path a statement takes. */
    static Map<String, Way> ways() {
        Map<String, Way> ways = new LinkedHashMap<>();
        ways.put("a read (query)", new Way(c -> assertEquals(1, c.query(COUNT).getRowCount()), q(COUNT, counted(3))));
        ways.put("a write (execute)", new Way(c -> assertEquals(1, c.execute(INSERT)), q(INSERT, changed(1))));
        ways.put("executeRaw", new Way(c -> assertEquals(1, c.executeRaw(INSERT).getAffected()), q(INSERT, changed(1))));
        ways.put("a statement with parameters", new Way(
            c -> assertEquals(1, c.prepare(PREPARED).setLong(1, 4L).setString(2, "four").executeUpdate()),
            ex(ConformanceServer.prepareRequest(PREPARED), ConformanceServer.preparedResponse(1, 2)),
            ex(ConformanceServer.executeRequest(1, QUORUM, 4L, "four"), changed(1))));
        ways.put("a batch", new Way(
            c -> assertEquals(2, c.prepare(PREPARED).executeBatch(List.of(new Object[] { 4L, "four" }, new Object[] { 5L, "five" }))),
            ex(ConformanceServer.prepareRequest(PREPARED), ConformanceServer.preparedResponse(1, 2)),
            ex(ConformanceServer.batchRequest(1, QUORUM, new Object[] { 4L, "four" }, new Object[] { 5L, "five" }), changed(2))));
        ways.put("a stream", new Way(c -> {
            try (Skaidb.RowStream rows = c.stream(COUNT)) {
                assertTrue(rows.next());
                assertFalse(rows.next());
            }
        }, ex(ConformanceServer.streamRequest(COUNT, QUORUM), ConformanceServer.rowsHeaderResponse("count"),
            ConformanceServer.rowsChunkResponse(new Object[] { 3L }), ConformanceServer.rowsEndResponse())));
        ways.put("a stream with parameters", new Way(c -> {
            try (Skaidb.RowStream rows = c.stream("SELECT id FROM notes WHERE id > ?", 1L)) {
                assertTrue(rows.next());
                assertFalse(rows.next());
            }
        }, ex(ConformanceServer.streamRequest("SELECT id FROM notes WHERE id > ?", QUORUM, 1L),
            ConformanceServer.rowsHeaderResponse("id"),
            ConformanceServer.rowsChunkResponse(new Object[] { 2L }), ConformanceServer.rowsEndResponse())));
        ways.put("the transaction helper", new Way(
            c -> assertEquals(1L, (long) c.transaction(conn -> conn.execute(INSERT))),
            q("BEGIN", ok()), q(INSERT, changed(1)), q("COMMIT", ok())));
        ways.put("a hand-written BEGIN", new Way(c -> {
            c.execute("BEGIN");
            c.execute(INSERT);
            c.execute("COMMIT");
        }, q("BEGIN", ok()), q(INSERT, changed(1)), q("COMMIT", ok())));
        return ways;
    }

    // ---- one connection ------------------------------------------------------------------

    @Test
    void aConnectionHalfClosedWhileIdleCostsNoStatement() throws Exception {
        for (Map.Entry<String, Way> way : ways().entrySet()) {
            String name = way.getKey();
            ConformanceServer server = ConformanceServer.sessions(List.of(
                // Handshake and Hello are answered; then the server stops
                // sending, keeps the socket open and reads on.
                List.of(ConformanceServer.idle().thenHalfClose()),
                way.getValue().script));
            try (Skaidb.Connection c = Skaidb.connect(options(server))) {
                Thread.sleep(ARRIVES);
                // No error reaches the application: nothing was sent on the dead connection.
                way.getValue().run.accept(c);
                assertEquals(1, c.reconnects(), name);
                assertTrue(c.isUsable(), name);
            }
            assertNull(server.finish(), name);
            assertEquals(0, server.bytesAfterHalfClose(0), name + ": bytes written to the connection the server had closed");
            assertEquals(2, server.connections(), name);
        }
    }

    @Test
    void aConnectionResetOrClosedWhileIdleCostsNoStatementEither() throws Exception {
        Map<String, ConformanceServer.Exchange> ends = new LinkedHashMap<>();
        ends.put("reset", ConformanceServer.idle().thenReset());
        ends.put("closed", ConformanceServer.idle().thenHangUp());
        // ... and after a statement, not only right after the Hello.
        ends.put("reset after a statement", q(COUNT, counted(3)).thenReset());
        ends.put("closed after a statement", q(COUNT, counted(3)).thenHangUp());
        ends.put("half-closed after a statement", q(COUNT, counted(3)).thenHalfClose());
        for (Map.Entry<String, ConformanceServer.Exchange> end : ends.entrySet()) {
            String name = end.getKey();
            ConformanceServer server = ConformanceServer.sessions(List.of(
                List.of(end.getValue()),
                List.of(q(INSERT, changed(1)), q(COUNT, counted(4)))));
            try (Skaidb.Connection c = Skaidb.connect(options(server))) {
                if (name.contains("after a statement")) assertEquals(1, c.query(COUNT).getRowCount(), name);
                assertTrue(server.awaitEnded(name.contains("half") ? 0 : 1, 2_000), name);
                Thread.sleep(ARRIVES);
                assertEquals(1, c.execute(INSERT), name);
                assertEquals(1, c.query(COUNT).getRowCount(), name);
                assertEquals(1, c.reconnects(), name);
            }
            assertNull(server.finish(), name);
            assertEquals(List.of(INSERT, COUNT), sent(server, 1), name);
        }
    }

    @Test
    void theNewConnectionHasTheSessionOfTheOldOne() throws Exception {
        // PROTOCOL.md §6 rules 9 and 10, as after a failed write: the session
        // database is entered first, and a prepared statement is prepared
        // again instead of being executed under the old connection's id.
        ConformanceServer server = ConformanceServer.sessions(List.of(
            List.of(q("USE \"app\"", ok()),
                ex(ConformanceServer.prepareRequest(PREPARED), ConformanceServer.preparedResponse(7, 2)),
                ex(ConformanceServer.executeRequest(7, QUORUM, 1L, "one"), changed(1)).thenHalfClose()),
            List.of(q("USE \"app\"", ok()),
                ex(ConformanceServer.prepareRequest(PREPARED), ConformanceServer.preparedResponse(1, 2)),
                ex(ConformanceServer.executeRequest(1, QUORUM, 2L, "two"), changed(1)))));
        try (Skaidb.Connection c = Skaidb.connect(options(server).database("app"))) {
            Skaidb.Query insert = c.prepare(PREPARED);
            assertEquals(1, insert.setLong(1, 1L).setString(2, "one").executeUpdate());
            Thread.sleep(ARRIVES);
            assertEquals(1, insert.setLong(1, 2L).setString(2, "two").executeUpdate());
            assertEquals(1, c.reconnects());
        }
        assertNull(server.finish());
        assertEquals(0, server.bytesAfterHalfClose(0));
    }

    @Test
    void aHealthyIdleConnectionIsLeftAlone() throws Exception {
        // The look costs no round trip and finds nothing: every statement on the one socket.
        List<ConformanceServer.Exchange> script = new ArrayList<>();
        for (int i = 0; i < 50; i++) script.add(q(COUNT, counted(i)));
        ConformanceServer server = new ConformanceServer("ok", script);
        try (Skaidb.Connection c = Skaidb.connect(options(server))) {
            for (int i = 0; i < 50; i++) {
                if (i % 10 == 0) Thread.sleep(20);
                assertEquals(1, c.query(COUNT).getRowCount());
            }
            assertEquals(0, c.reconnects());
        }
        assertNull(server.finish());
        assertEquals(50, server.requests().size());
    }

    @Test
    void aDialThatFindsItsOwnSocketGoneFailsOnceAndIsNotRepeatedFromInside() throws Exception {
        // A member that is shutting down takes the login and closes: the
        // new connection is gone before its session database is entered.
        ConformanceServer server = ConformanceServer.sessions(List.of(
            List.of(q("USE \"app\"", ok()), ConformanceServer.idle().thenHalfClose()),
            List.of(ConformanceServer.idle().thenHangUp()),
            List.of(q("USE \"app\"", ok()), q(COUNT, counted(3)))));
        try (Skaidb.Connection c = Skaidb.connect(options(server).database("app"))) {
            Thread.sleep(ARRIVES);
            // One dial for the statement, and its failure is the statement's.
            Skaidb.SkaidbException e = assertThrows(Skaidb.SkaidbException.class, () -> c.query(COUNT));
            assertInstanceOf(IOException.class, e.getCause(), e.getMessage());
            assertEquals(2, server.connections(), "the dial dialled again from inside itself");
            assertFalse(c.isUsable());
            // The next statement dials again, as after any failed dial.
            assertEquals(1, c.query(COUNT).getRowCount());
            assertEquals(2, c.reconnects());
        }
        assertNull(server.finish());
    }

    // ---- a pool ----------------------------------------------------------------------------

    @Test
    void aPoolDoesNotHandOutAConnectionTheServerClosedWhileItSatIdle() throws Exception {
        Map<String, ConformanceServer.Exchange> ends = new LinkedHashMap<>();
        ends.put("half-closed", q(COUNT, counted(3)).thenHalfClose());
        ends.put("closed", q(COUNT, counted(3)).thenHangUp());
        ends.put("reset", q(COUNT, counted(3)).thenReset());
        for (Map.Entry<String, ConformanceServer.Exchange> end : ends.entrySet()) {
            String name = end.getKey();
            ConformanceServer server = ConformanceServer.sessions(List.of(
                List.of(end.getValue()),
                List.of(q(INSERT, changed(1)), q(COUNT, counted(4)))));
            List<Skaidb.Connection> used = new ArrayList<>();
            try (Skaidb.Pool pool = new Skaidb.Pool(options(server))) {
                pool.withConnection(c -> {
                    used.add(c);
                    return c.query(COUNT);
                });
                assertEquals(1, pool.members().get(0).idle, name);
                Thread.sleep(ARRIVES);
                // The checkout gives a working connection, and a statement on it succeeds.
                Skaidb.Connection c = pool.acquire();
                try {
                    used.add(c);
                    assertTrue(c.isUsable(), name);
                    assertNotSame(used.get(0), c, name + ": the dead connection was handed out");
                    assertFalse(used.get(0).isUsable(), name + ": and it is closed, not kept");
                    assertEquals(0, c.reconnects(), name);
                    assertEquals(1, c.execute(INSERT), name);
                } finally {
                    pool.release(c);
                }
                assertEquals(1, (int) pool.withConnection(conn -> conn.query(COUNT).getRowCount()), name);
            }
            assertNull(server.finish(), name);
            if (name.equals("half-closed")) assertEquals(0, server.bytesAfterHalfClose(0), name);
            assertEquals(List.of(INSERT, COUNT), sent(server, 1), name);
        }
    }

    // ---- inside a transaction nothing re-dials (PROTOCOL.md §6 rule 7) ---------------------

    @Test
    void insideTheHelperADeadConnectionIsALostTransactionNotAReDial() throws Exception {
        ConformanceServer server = ConformanceServer.sessions(List.of(
            List.of(q("BEGIN", ok()), q(INSERT, changed(1)).thenHalfClose()),
            List.of(q(COUNT, counted(3)))));
        List<Skaidb.SkaidbException> raised = new ArrayList<>();
        try (Skaidb.Connection c = Skaidb.connect(options(server))) {
            Skaidb.SkaidbException reported = assertThrows(Skaidb.SkaidbException.class, () -> c.transaction(conn -> {
                conn.execute(INSERT);
                sleep(ARRIVES);
                for (String sql : new String[] { "INSERT INTO notes (id) VALUES (5)", "INSERT INTO notes (id) VALUES (6)" }) {
                    try {
                        conn.execute(sql);
                    } catch (Skaidb.SkaidbException e) {
                        raised.add(e);
                    }
                }
                return null;
            }));
            assertEquals(2, raised.size());
            for (Skaidb.SkaidbException e : raised) HandWrittenTransactionTest.assertLost(e);
            HandWrittenTransactionTest.assertLost(reported);
            assertEquals(1, server.connections(), "a statement of the body re-dialled");
            assertEquals(0, c.reconnects());
            assertFalse(c.isUsable());
            // After the helper, as after any loss.
            assertEquals(1, c.query(COUNT).getRowCount());
        }
        assertNull(server.finish());
        assertEquals(0, server.bytesAfterHalfClose(0), "nothing of the body was sent to the connection the server had closed");
        assertEquals(List.of("BEGIN", INSERT), sent(server, 0));
        assertEquals(List.of(COUNT), sent(server, 1));
    }

    @Test
    void insideAHandWrittenTransactionADeadConnectionIsALostTransactionToo() throws Exception {
        for (String end : new String[] { "COMMIT", "ROLLBACK" }) {
            ConformanceServer server = ConformanceServer.sessions(List.of(
                List.of(q("BEGIN", ok()), q(INSERT, changed(1)).thenHalfClose()),
                List.of(q(COUNT, counted(3)))));
            try (Skaidb.Connection c = Skaidb.connect(options(server))) {
                c.execute("BEGIN");
                c.execute(INSERT);
                Thread.sleep(ARRIVES);
                if (end.equals("COMMIT")) {
                    // The loss is found before the COMMIT is written, not under it: nothing was committed.
                    Skaidb.SkaidbException e = HandWrittenTransactionTest.assertLost(
                        assertThrows(Skaidb.SkaidbException.class, () -> c.execute("COMMIT")));
                    assertTrue(e.getMessage().contains("nothing was committed"), e.getMessage());
                } else {
                    HandWrittenTransactionTest.assertLost(
                        assertThrows(Skaidb.SkaidbException.class, () -> c.execute("INSERT INTO notes (id) VALUES (5)")));
                    assertEquals(-1, c.execute("ROLLBACK"));
                }
                assertEquals(1, server.connections(), end);
                assertEquals(1, c.query(COUNT).getRowCount(), end);
                assertEquals(1, c.reconnects(), end);
            }
            assertNull(server.finish(), end);
            assertEquals(0, server.bytesAfterHalfClose(0), end);
            assertEquals(List.of(COUNT), sent(server, 1), end);
        }
    }

    static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    // ---- a connection that is streaming is not idle ------------------------------------------

    @Test
    void aStreamingConnectionIsNotLookedAt() throws Exception {
        // The rest of the stream is waiting in the socket: readable, and nothing wrong with it.
        ConformanceServer server = new ConformanceServer("ok", List.of(
            ex(ConformanceServer.streamRequest("SELECT id FROM notes", QUORUM), ConformanceServer.rowsHeaderResponse("id"),
                ConformanceServer.rowsChunkResponse(new Object[] { 1L }),
                ConformanceServer.rowsChunkResponse(new Object[] { 2L }), ConformanceServer.rowsEndResponse()),
            q(COUNT, counted(2))));
        try (Skaidb.Connection c = Skaidb.connect(options(server))) {
            Skaidb.RowStream rows = c.stream("SELECT id FROM notes");
            assertTrue(rows.next());
            Thread.sleep(ARRIVES);
            // Busy, as ever: not taken for dead and replaced under the stream.
            for (org.junit.jupiter.api.function.Executable other : List.<org.junit.jupiter.api.function.Executable>of(
                    () -> c.query(COUNT), () -> c.stream(COUNT),
                    () -> c.prepare(PREPARED).setLong(1, 1L).setString(2, "x").executeUpdate())) {
                Skaidb.SkaidbException busy = assertThrows(Skaidb.SkaidbException.class, other);
                assertTrue(busy.getMessage().startsWith("connection is busy streaming"), busy.getMessage());
            }
            assertEquals(0, c.reconnects());
            assertTrue(rows.next());
            assertEquals(2L, rows.getObject(0));
            assertFalse(rows.next());
            assertEquals(1, c.query(COUNT).getRowCount());
            assertEquals(0, c.reconnects());
        }
        assertNull(server.finish());
    }

    // ---- TLS: the look is at the transport, not at the decrypted stream ----------------------

    static Skaidb.ConnectOptions tlsOptions(ConformanceServer s) {
        return options(s).tlsCa(ConformanceServer.tls("ca.pem"));
    }

    @Test
    void underTlsAHealthyConnectionIsLeftAloneAndItsStreamIntact() throws Exception {
        // The look reads the socket under the TLS session. A byte taken
        // from there would be a byte of a TLS record: every later statement
        // would fail to decrypt.
        List<ConformanceServer.Exchange> script = new ArrayList<>();
        for (int i = 0; i < 40; i++) script.add(q(COUNT, counted(i)));
        ConformanceServer server = ConformanceServer.tlsSessions(List.of(script));
        try (Skaidb.Connection c = Skaidb.connect(tlsOptions(server))) {
            for (int i = 0; i < 40; i++) {
                if (i % 10 == 0) Thread.sleep(20);
                Skaidb.ResultSet rs = c.query(COUNT);
                assertTrue(rs.next());
                assertEquals(i, rs.getLong(1));
            }
            assertEquals(0, c.reconnects(), "a healthy TLS connection was taken for a dead one");
        }
        assertNull(server.finish());
    }

    @Test
    void underTlsAConnectionClosedWhileIdleCostsNoStatement() throws Exception {
        Map<String, ConformanceServer.Exchange> ends = new LinkedHashMap<>();
        ends.put("half-closed", q(COUNT, counted(3)).thenHalfClose());
        ends.put("closed", q(COUNT, counted(3)).thenHangUp());
        ends.put("reset", q(COUNT, counted(3)).thenReset());
        ends.put("half-closed after the Hello", ConformanceServer.idle().thenHalfClose());
        for (Map.Entry<String, ConformanceServer.Exchange> end : ends.entrySet()) {
            String name = end.getKey();
            ConformanceServer server = ConformanceServer.tlsSessions(List.of(
                List.of(end.getValue()),
                List.of(q(INSERT, changed(1)), q(COUNT, counted(4)))));
            try (Skaidb.Connection c = Skaidb.connect(tlsOptions(server))) {
                if (!name.contains("Hello")) assertEquals(1, c.query(COUNT).getRowCount(), name);
                Thread.sleep(ARRIVES);
                assertEquals(1, c.execute(INSERT), name);
                assertEquals(1, c.query(COUNT).getRowCount(), name);
                assertEquals(1, c.reconnects(), name);
            }
            assertNull(server.finish(), name);
            if (name.startsWith("half")) assertEquals(0, server.bytesAfterHalfClose(0), name);
            assertEquals(List.of(INSERT, COUNT), sent(server, 1), name);
        }
    }

    // ---- threads -------------------------------------------------------------------------------

    @Test
    void aThreadsInterruptStatusCostsNeitherAStatementNorTheConnection() throws Exception {
        ConformanceServer server = ConformanceServer.sessions(List.of(
            List.of(q(COUNT, counted(3)), q(INSERT, changed(1)).thenHangUp()),
            List.of(q(COUNT, counted(4)))));
        try (Skaidb.Connection c = Skaidb.connect(options(server))) {
            Thread.currentThread().interrupt();
            try {
                // An application that goes on after it was interrupted — its
                // clean-up, say — still talks to the database ...
                assertEquals(1, c.query(COUNT).getRowCount());
                assertEquals(1, c.execute(INSERT));
                assertEquals(0, c.reconnects(), "the interrupt status closed the connection");
                assertTrue(Thread.currentThread().isInterrupted(), "and the status is the application's to read");
                // ... and to a new connection, when the old one is gone.
                Thread.interrupted();   // the test's own waiting is not the driver's
                assertTrue(server.awaitEnded(1, 2_000));
                sleep(ARRIVES);
                Thread.currentThread().interrupt();
                assertEquals(1, c.query(COUNT).getRowCount());
                assertEquals(1, c.reconnects());
                assertTrue(Thread.currentThread().isInterrupted());
            } finally {
                Thread.interrupted();
            }
        }
        assertNull(server.finish());
    }

    @Test
    void anInterruptWhileAStatementWaitsForTheServerEndsTheWait() throws Exception {
        ConformanceServer server = ConformanceServer.sessions(List.of(
            List.of(q(COUNT)),   // never answered
            List.of(q(COUNT, counted(3)))));
        try (Skaidb.Connection c = Skaidb.connect(options(server))) {
            AtomicReference<Throwable> failure = new AtomicReference<>();
            AtomicReference<Boolean> status = new AtomicReference<>();
            Thread waiting = new Thread(() -> {
                try {
                    c.query(COUNT);
                } catch (Throwable t) {
                    failure.set(t);
                }
                status.set(Thread.currentThread().isInterrupted());
            }, "waiting-statement");
            waiting.start();
            long deadline = System.nanoTime() + 5_000_000_000L;
            while (server.requests().isEmpty() && System.nanoTime() < deadline) Thread.sleep(5);
            Thread.sleep(50);   // the statement is on the wire; its thread is in the read
            waiting.interrupt();
            waiting.join(5_000);
            assertFalse(waiting.isAlive(), "the statement still waits");
            // A transport error like any other: the statement may or may not have run.
            Skaidb.SkaidbException e = assertInstanceOf(Skaidb.SkaidbException.class, failure.get());
            assertInstanceOf(IOException.class, e.getCause());
            assertEquals("query failed: the thread was interrupted while it waited for the server, "
                + "and the connection was closed", e.getMessage());
            assertTrue(status.get(), "the interrupt status stays set for the application");
            assertFalse(c.isUsable());
            assertTrue(server.awaitEnded(1, 2_000), "the socket stayed open");
            // The connection re-dials on its next statement, as after any transport error.
            assertEquals(1, c.query(COUNT).getRowCount());
            assertEquals(1, c.reconnects());
        }
        assertNull(server.finish());
    }

    @Test
    void closeFromAnotherThreadEndsAStatementThatWaitsForTheServer() throws Exception {
        for (boolean tls : new boolean[] { false, true }) {
            // The server never answers the statement.
            List<List<ConformanceServer.Exchange>> script = List.of(List.of(q(COUNT)));
            ConformanceServer server = tls ? ConformanceServer.tlsSessions(script) : ConformanceServer.sessions(script);
            Skaidb.Connection c = Skaidb.connect(tls ? tlsOptions(server) : options(server));
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread waiting = new Thread(() -> {
                try {
                    c.query(COUNT);
                } catch (Throwable t) {
                    failure.set(t);
                }
            }, "waiting-statement");
            waiting.start();
            long deadline = System.nanoTime() + 5_000_000_000L;
            while (server.requests().isEmpty() && System.nanoTime() < deadline) Thread.sleep(5);
            Thread.sleep(50);   // the statement is on the wire; its thread is in the read
            Thread closer = new Thread(c::close, "closer");
            closer.start();
            closer.join(5_000);
            assertFalse(closer.isAlive(), "close() is stuck behind the read (tls=" + tls + ")");
            waiting.join(5_000);
            assertFalse(waiting.isAlive(), "the statement still waits (tls=" + tls + ")");
            assertInstanceOf(Skaidb.SkaidbException.class, failure.get());
            assertInstanceOf(IOException.class, failure.get().getCause());
            assertTrue(server.awaitEnded(1, 2_000), "the socket stayed open (tls=" + tls + ")");
            assertNull(server.finish());
        }
    }

    // ---- frames of any size -----------------------------------------------------------------------

    static long directMemory() {
        for (java.lang.management.BufferPoolMXBean pool
                : java.lang.management.ManagementFactory.getPlatformMXBeans(java.lang.management.BufferPoolMXBean.class)) {
            if (pool.getName().equals("direct")) return pool.getMemoryUsed();
        }
        throw new IllegalStateException("no direct buffer pool");
    }

    @Test
    void aLargeFrameLeavesNoDirectMemoryBehind() throws Exception {
        // 16 MB out and 16 MB back. The JDK copies what a socket reads and
        // writes through a direct buffer it keeps per thread, as large as
        // the largest piece ever handed to it in one call.
        char[] big = new char[16 * 1024 * 1024];
        java.util.Arrays.fill(big, 'x');
        String text = new String(big);
        String insert = "INSERT INTO notes (id, body) VALUES (1, '" + text + "')";
        ConformanceServer server = new ConformanceServer("ok", List.of(
            q(insert, changed(1)),
            q("SELECT body FROM notes", ConformanceServer.rowsResponse(new String[] { "body" }, new Object[] { text }))));
        AtomicReference<Throwable> failure = new AtomicReference<>();
        long[] grown = new long[1];
        Thread t = new Thread(() -> {
            try (Skaidb.Connection c = Skaidb.connect(options(server))) {
                long before = directMemory();
                assertEquals(1, c.execute(insert));
                Skaidb.ResultSet rs = c.query("SELECT body FROM notes");
                assertTrue(rs.next());
                assertEquals(text.length(), rs.getString(1).length());
                grown[0] = directMemory() - before;
            } catch (Throwable e) {
                failure.set(e);
            }
        }, "large-frames");
        t.start();
        t.join(60_000);
        assertNull(failure.get());
        assertNull(server.finish());
        assertTrue(grown[0] < 1024 * 1024, "the two frames left " + grown[0] + " bytes of direct memory with the thread");
    }
}
