package com.skaidb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * A pool hands every borrower its own session state. A pooled connection
 * reaches each borrower in the pool's session — in the pool's database
 * (its {@code database} option, else {@code default}, where every new
 * connection starts), with no transaction open, no consistency override
 * and no scan budget of an earlier borrower. When a connection comes back
 * and the driver's notes say its borrower moved it, the pool puts it back
 * before the next borrower gets it, sending only what is needed; a
 * connection that cannot be put back is closed instead of pooled. Against
 * the scripted fake server, with a pool that keeps one connection: each
 * test states every statement each session may receive.
 */
class PoolSessionTest {

    static final String BUDGET = "SET SCAN BUDGET ROWS 1";
    static final String ALL = "SET CONSISTENCY ALL";
    static final String COUNT = "SELECT count(*) FROM notes";
    static final String NO_BUDGET = "SET SCAN BUDGET DEFAULT";
    static final String NO_OVERRIDE = "SET CONSISTENCY DEFAULT";

    static Skaidb.ConnectOptions options(ConformanceServer s) { return TransactionTest.options(s); }
    static ConformanceServer.Exchange q(String sql, byte[]... responses) { return TransactionTest.q(sql, responses); }
    static byte[] ok() { return ConformanceServer.ddlResponse(); }
    static byte[] error(String message) { return ConformanceServer.errorResponse(message); }
    static byte[] counted(long n) { return ConformanceServer.rowsResponse(new String[] { "count" }, new Object[] { n }); }
    static List<String> sent(ConformanceServer s, int n) { return TransactionTest.sent(s, n); }

    static Skaidb.Pool pool(Skaidb.ConnectOptions o) { return new Skaidb.Pool(o, new Skaidb.PoolOptions().maxSize(1)); }

    static long count(Skaidb.Connection c) {
        Skaidb.ResultSet rs = c.query(COUNT);
        rs.next();
        return rs.getLong(1);
    }

    // ---- the scenario ----------------------------------------------------------------------

    @Test
    void theNextBorrowerGetsThePoolsSessionNotTheLastBorrowers() throws Exception {
        ConformanceServer server = ConformanceServer.sessions(List.of(List.of(
            // borrower 1
            q("USE other", ok()), q(BUDGET, ok()), q(ALL, ok()),
            // the pool, when the connection comes back: the database every new connection starts in ...
            q("USE \"default\"", ok()), q(NO_BUDGET, ok()), q(NO_OVERRIDE, ok()),
            // borrower 2: not in other, not refused for a scan budget
            q(COUNT, counted(3)))));
        List<Skaidb.Connection> used = new ArrayList<>();
        try (Skaidb.Pool pool = pool(options(server))) {
            pool.withConnection(c -> {
                used.add(c);
                c.execute("USE other");
                c.execute(BUDGET);
                return c.execute(ALL);
            });
            assertEquals(List.of("USE other", BUDGET, ALL, "USE \"default\"", NO_BUDGET, NO_OVERRIDE), sent(server, 0),
                "put back when it is returned, before anyone can borrow it");
            assertEquals(1, pool.members().get(0).idle);
            assertEquals(3, (long) pool.withConnection(c -> {
                used.add(c);
                assertEquals("", c.database(), "no database of a borrower's choosing");
                return count(c);
            }));
            assertSame(used.get(0), used.get(1), "the same connection, put back rather than replaced");
        }
        assertNull(server.finish());
    }

    @Test
    void aPoolWithADatabaseOptionPutsItsConnectionsBackInThatDatabase() throws Exception {
        ConformanceServer server = ConformanceServer.sessions(List.of(List.of(
            q("USE \"app\"", ok()),
            // still in the pool's database: nothing to put back
            q("USE app", ok()),
            q("USE reports", ok()), q("USE \"app\"", ok()),
            q(COUNT, counted(3)))));
        List<Skaidb.Connection> used = new ArrayList<>();
        try (Skaidb.Pool pool = pool(options(server).database("app"))) {
            pool.withConnection(c -> c.execute("USE app"));
            assertEquals(2, server.requests().size());
            pool.withConnection(c -> {
                used.add(c);
                return c.execute("USE reports");
            });
            assertEquals(3, (long) pool.withConnection(c -> {
                used.add(c);
                assertEquals("app", c.database());
                return count(c);
            }));
            assertSame(used.get(0), used.get(1));
        }
        assertNull(server.finish());
    }

    @Test
    void aBorrowerThatChangedNothingCostsNoRoundTripAndOnlyWhatIsNeededIsSent() throws Exception {
        ConformanceServer server = ConformanceServer.sessions(List.of(List.of(
            q(COUNT, counted(3)),
            q(BUDGET, ok()), q(NO_BUDGET, ok()),
            q(ALL, ok()), q(NO_OVERRIDE, ok()),
            // set and cleared by the borrower itself: nothing left to put back
            q(BUDGET, ok()), q(NO_BUDGET, ok()), q(ALL, ok()), q(NO_OVERRIDE, ok()),
            q("USE other", ok()), q("USE \"default\"", ok()),
            // a borrower in the database every connection starts in has not moved it
            q("USE default", ok()),
            q(COUNT, counted(3)))));
        try (Skaidb.Pool pool = pool(options(server))) {
            pool.withConnection(PoolSessionTest::count);
            assertEquals(1, server.requests().size(), "nothing was changed, nothing is sent");
            pool.withConnection(c -> c.execute(BUDGET));
            assertEquals(3, server.requests().size());
            pool.withConnection(c -> c.execute(ALL));
            assertEquals(5, server.requests().size());
            pool.withConnection(c -> {
                c.execute(BUDGET);
                c.execute(NO_BUDGET);
                c.execute(ALL);
                return c.execute(NO_OVERRIDE);
            });
            assertEquals(9, server.requests().size());
            pool.withConnection(c -> c.execute("USE other"));
            assertEquals(11, server.requests().size());
            pool.withConnection(c -> c.execute("USE default"));
            assertEquals(12, server.requests().size());
            pool.withConnection(PoolSessionTest::count);
        }
        assertNull(server.finish());
    }

    // ---- a connection that cannot be put back is closed ---------------------------------------

    @Test
    void aConnectionThatCannotBePutBackIsClosedAndTheNextBorrowerGetsANewOne() throws Exception {
        // What an older server answers SET CONSISTENCY DEFAULT with.
        String older = "parse error: SET CONSISTENCY takes ONE, QUORUM or ALL";
        String[][] refusals = {
            { ALL, NO_OVERRIDE, older },
            { BUDGET, NO_BUDGET, "the budget stays" },
            { "USE other", "USE \"default\"", "database \"default\" does not exist" },
        };
        for (String[] refusal : refusals) {
            String name = refusal[1];
            ConformanceServer server = ConformanceServer.sessions(List.of(
                List.of(q(refusal[0], ok()), q(refusal[1], error(refusal[2]))),
                List.of(q(COUNT, counted(3)))));
            List<Skaidb.Connection> used = new ArrayList<>();
            try (Skaidb.Pool pool = pool(options(server))) {
                pool.withConnection(c -> {
                    used.add(c);
                    return c.execute(refusal[0]);
                });
                assertEquals(0, pool.members().get(0).idle, name + ": kept as the last borrower left it");
                assertTrue(server.awaitEnded(1, 2_000), name + ": not closed");
                assertFalse(used.get(0).isUsable(), name);
                assertEquals(3, (long) pool.withConnection(c -> {
                    used.add(c);
                    return count(c);
                }));
                assertNotSame(used.get(0), used.get(1), name);
            }
            assertNull(server.finish(), name);
            assertEquals(List.of(COUNT), sent(server, 1), name + ": a new connection, in the pool's session");
        }
    }

    @Test
    void aConnectionThatComesBackWithATransactionOpenIsRolledBackFirst() throws Exception {
        String insert = "INSERT INTO notes (id) VALUES (9)";
        ConformanceServer server = ConformanceServer.sessions(List.of(
            List.of(q("BEGIN", ok()), q(insert, ConformanceServer.mutationResponse(1)), q("USE other", ok()),
                // the pool: the transaction first, then the rest
                q("ROLLBACK", ok()), q("USE \"default\"", ok()),
                // the next borrower's statement is committed on its own, as it expects
                q(insert, ConformanceServer.mutationResponse(1)),
                // ... and when the ROLLBACK is refused the connection is not kept
                q("BEGIN", ok()), q("ROLLBACK", error("constraint violation: no transaction in progress"))),
            List.of(q(COUNT, counted(1)))));
        List<Skaidb.Connection> used = new ArrayList<>();
        try (Skaidb.Pool pool = pool(options(server))) {
            pool.withConnection(c -> {
                used.add(c);
                c.execute("BEGIN");
                c.execute(insert);
                return c.execute("USE other");   // returned in the middle of its transaction
            });
            assertEquals(1, (long) pool.withConnection(c -> {
                used.add(c);
                return c.execute(insert);
            }));
            assertSame(used.get(0), used.get(1));
            pool.withConnection(c -> c.execute("BEGIN"));
            assertTrue(server.awaitEnded(1, 2_000), "a connection whose transaction could not be rolled back was kept");
            assertEquals(1, (long) pool.withConnection(c -> {
                used.add(c);
                return count(c);
            }));
            assertNotSame(used.get(0), used.get(2));
        }
        assertNull(server.finish());
    }

    // ---- the notes go with the borrower ----------------------------------------------------------

    @Test
    void aReDialForTheNextBorrowerRestoresThePoolsSessionNotTheLastBorrowers() throws Exception {
        ConformanceServer server = ConformanceServer.sessions(List.of(
            List.of(q("USE \"app\"", ok()),
                q("USE other", ok()), q(BUDGET, ok()), q(ALL, ok()),
                q("USE \"app\"", ok()), q(NO_BUDGET, ok()), q(NO_OVERRIDE, ok()),
                // borrower 2 holds the connection when the server goes away
                q(COUNT, counted(3)).thenHangUp()),
            // Its re-dial: the pool's database, and nothing of borrower 1.
            List.of(q("USE \"app\"", ok()), q(COUNT, counted(3)))));
        try (Skaidb.Pool pool = pool(options(server).database("app"))) {
            pool.withConnection(c -> {
                c.execute("USE other");
                c.execute(BUDGET);
                return c.execute(ALL);
            });
            pool.withConnection(c -> {
                assertEquals(3, count(c));
                try {
                    assertTrue(server.awaitEnded(1, 2_000));
                    assertTrue(ConformanceServer.awaitSeenClosed(c, 2_000));
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
                assertEquals(3, count(c));
                assertEquals(1, c.reconnects());
                return null;
            });
        }
        assertNull(server.finish());
        assertEquals(List.of("USE \"app\"", COUNT), sent(server, 1));
    }

    @Test
    void aConnectionLostInItsBorrowersHandsIsNotPutBackOverANewSession() throws Exception {
        ConformanceServer server = ConformanceServer.sessions(List.of(
            List.of(q("USE other", ok()), q(BUDGET, ok()).thenHangUp()),
            List.of(q(COUNT, counted(3)))));
        List<Skaidb.Connection> used = new ArrayList<>();
        try (Skaidb.Pool pool = pool(options(server))) {
            pool.withConnection(c -> {
                used.add(c);
                c.execute("USE other");
                c.execute(BUDGET);
                try {
                    assertTrue(server.awaitEnded(1, 2_000));
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
                return null;
            });
            // Gone when it came back: nothing was dialled to put a session back that nobody has.
            assertEquals(1, server.connections());
            assertEquals(0, pool.members().get(0).idle);
            assertEquals(3, (long) pool.withConnection(c -> {
                used.add(c);
                return count(c);
            }));
            assertNotSame(used.get(0), used.get(1));
        }
        assertNull(server.finish());
        assertEquals(List.of(COUNT), sent(server, 1));
    }

    // ---- the same for a pool of one's own -------------------------------------------------------

    @Test
    void aConnectionCanBePutBackToASessionItWasInBefore() throws Exception {
        ConformanceServer server = ConformanceServer.sessions(List.of(List.of(
            q("USE \"app\"", ok()), q("SET SCAN BUDGET ROWS 5000", ok()),
            // changed ...
            q("USE other", ok()), q(BUDGET, ok()), q(ALL, ok()),
            // ... and put back: the budget is the one it had, not the default
            q("USE \"app\"", ok()), q("SET SCAN BUDGET ROWS 5000", ok()), q(NO_OVERRIDE, ok()),
            q(COUNT, counted(3)))));
        try (Skaidb.Connection c = Skaidb.connect(options(server).database("app"))) {
            c.execute("SET SCAN BUDGET ROWS 5000");
            Skaidb.Session before = c.session();
            assertEquals("app", before.database);
            assertEquals("SET SCAN BUDGET ROWS 5000", before.scanBudget);
            assertNull(before.consistency);
            c.execute("USE other");
            c.execute(BUDGET);
            c.execute(ALL);
            assertTrue(c.restore(before));
            assertEquals("app", c.database());
            assertTrue(c.restore(before), "already there: nothing is sent");
            assertEquals(3, count(c));
            assertThrows(Skaidb.SkaidbException.class, () -> c.restore(null));
        }
        assertNull(server.finish());
    }
}
