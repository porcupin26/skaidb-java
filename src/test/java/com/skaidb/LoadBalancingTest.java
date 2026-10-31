package com.skaidb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import javax.crypto.Mac;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Pool load balancing (PROTOCOL.md §6.1): policies, idle order, health
 * ejection and re-admission, member discovery ({@code SHOW ENDPOINTS}) and
 * read-your-writes affinity — against fake members that each serve many
 * connections.
 */
class LoadBalancingTest {

    /**
     * A fake skaidb member: SCRAM per the conformance vectors on every
     * connection, then {@code SELECT …} answers one row naming the member
     * (after {@code delayMs}), {@code SHOW ENDPOINTS} answers
     * {@code listing}, {@code SHOW DATABASES} (the pool's probe) answers no
     * rows, anything else is a one-row mutation. {@code refuse} closes each
     * new connection before the handshake; {@code oldServer} answers SHOW
     * ENDPOINTS with a parse error.
     */
    static final class Member implements AutoCloseable {
        final String name;
        volatile long delayMs = 0;
        volatile boolean refuse = false;
        volatile boolean oldServer = false;
        volatile Supplier<List<Object[]>> listing;
        final List<String> statements = new CopyOnWriteArrayList<>();
        final int port;
        final String endpoint;
        private final ServerSocket server;
        private final List<Socket> sockets = new CopyOnWriteArrayList<>();

        Member(String name) throws IOException {
            this.name = name;
            server = new ServerSocket();
            server.setReuseAddress(true);
            server.bind(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0), 64);
            port = server.getLocalPort();
            endpoint = "127.0.0.1:" + port;
            Thread t = new Thread(this::accept, "member-" + name);
            t.setDaemon(true);
            t.start();
        }

        private void accept() {
            while (!server.isClosed()) {
                Socket s;
                try {
                    s = server.accept();
                } catch (IOException e) {
                    return;
                }
                if (refuse) {
                    try { s.close(); } catch (IOException ignored) {}
                    continue;
                }
                sockets.add(s);
                try { s.setTcpNoDelay(true); } catch (IOException ignored) {}   // latencies are measured
                Thread t = new Thread(() -> serve(s), "member-" + name + "-conn");
                t.setDaemon(true);
                t.start();
            }
        }

        private void serve(Socket s) {
            try (Socket sock = s) {
                DataInputStream in = new DataInputStream(sock.getInputStream());
                OutputStream out = sock.getOutputStream();
                Map<String, Object> auth = Json.obj(ConformanceServer.V.get("auth"));
                Map<String, Object> challenge = Json.obj(auth.get("challenge"));
                Skaidb.Reader start = new Skaidb.Reader(readFrame(in));
                if (start.u8() != 10) return;
                String user = start.text();
                String clientNonce = start.text();
                byte[] salt = ConformanceServer.unhex((String) challenge.get("salt"));
                int iterations = ((Long) challenge.get("iterations")).intValue();
                String serverNonce = clientNonce + challenge.get("server_nonce_suffix");
                writeFrame(out, new Skaidb.Buf().u8(11).u32(salt.length).raw(salt).u32(iterations)
                    .str(serverNonce).toBytes());
                if (readFrame(in)[0] != 12) return;
                byte[] am = String.join("\0", user, clientNonce, serverNonce,
                    ConformanceServer.hex(salt), Integer.toString(iterations)).getBytes(StandardCharsets.UTF_8);
                byte[] salted = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256").generateSecret(
                    new PBEKeySpec(((String) auth.get("password")).toCharArray(), salt, iterations, 256)).getEncoded();
                byte[] serverSig = hmac(hmac(salted, "Server Key".getBytes(StandardCharsets.UTF_8)), am);
                writeFrame(out, ConformanceServer.concat(new byte[] { 13, 1 }, serverSig));
                for (;;) {
                    Skaidb.Reader r = new Skaidb.Reader(readFrame(in));
                    if (r.u8() != 1) {   // Hello and anything else: an ack
                        writeFrame(out, ConformanceServer.ddlResponse());
                        continue;
                    }
                    r.u8();
                    String sql = r.text();
                    statements.add(sql);
                    Supplier<List<Object[]>> l = listing;
                    if (sql.equals("SHOW ENDPOINTS")) {
                        if (oldServer || l == null) {
                            writeFrame(out, ConformanceServer.errorResponse("parse error: expected TABLES, found ENDPOINTS"));
                        } else {
                            writeFrame(out, ConformanceServer.rowsResponse(
                                new String[] { "endpoint", "state", "local" }, l.get().toArray(new Object[0][])));
                        }
                    } else if (sql.equals("SHOW DATABASES")) {
                        writeFrame(out, ConformanceServer.rowsResponse(new String[] { "name" }));
                    } else if (sql.startsWith("SELECT")) {
                        long d = delayMs;
                        if (d > 0) Thread.sleep(d);
                        writeFrame(out, ConformanceServer.rowsResponse(new String[] { "member" }, new Object[][] { { name } }));
                    } else {
                        writeFrame(out, ConformanceServer.mutationResponse(1));
                    }
                }
            } catch (Exception e) {
                // the client went away, or the member closed
            }
        }

        @Override public void close() {
            try { server.close(); } catch (IOException ignored) {}
            for (Socket s : sockets) {
                try { s.close(); } catch (IOException ignored) {}
            }
        }

        private static byte[] readFrame(DataInputStream in) throws IOException {
            byte[] b = new byte[in.readInt()];
            in.readFully(b);
            return b;
        }

        private static void writeFrame(OutputStream out, byte[] payload) throws IOException {
            byte[] frame = new byte[4 + payload.length];   // one write: one segment
            frame[0] = (byte) (payload.length >>> 24);
            frame[1] = (byte) (payload.length >>> 16);
            frame[2] = (byte) (payload.length >>> 8);
            frame[3] = (byte) payload.length;
            System.arraycopy(payload, 0, frame, 4, payload.length);
            out.write(frame);
            out.flush();
        }

        private static byte[] hmac(byte[] key, byte[] msg) throws Exception {
            Mac m = Mac.getInstance("HmacSHA256");
            m.init(new SecretKeySpec(key, "HmacSHA256"));
            return m.doFinal(msg);
        }
    }

    private final List<AutoCloseable> cleanup = new ArrayList<>();

    @AfterEach
    void closeAll() throws Exception {
        Collections.reverse(cleanup);
        for (AutoCloseable c : cleanup) c.close();
    }

    private List<Member> members(String... names) throws IOException {
        List<Member> out = new ArrayList<>();
        for (String n : names) {
            Member m = new Member(n);
            cleanup.add(m);
            out.add(m);
        }
        return out;
    }

    private static Skaidb.ConnectOptions connectTo(Member... seeds) {
        Skaidb.ConnectOptions o = new Skaidb.ConnectOptions()
            .user(ConformanceServer.username()).password(ConformanceServer.password());
        for (Member m : seeds) o.host(m.endpoint);
        return o;
    }

    private Skaidb.Pool pool(Skaidb.ConnectOptions co, Skaidb.PoolOptions po) {
        Skaidb.Pool p = new Skaidb.Pool(co, po);
        cleanup.add(p);
        return p;
    }

    private Skaidb.Pool pool(List<Member> seeds, Skaidb.PoolOptions po) {
        return pool(connectTo(seeds.toArray(new Member[0])), po);
    }

    private static Skaidb.PoolOptions roundRobin() {
        return new Skaidb.PoolOptions().loadBalancing(Skaidb.LoadBalancing.ROUND_ROBIN);
    }

    private static String who(Skaidb.Pool p) { return who(p, null); }

    private static String who(Skaidb.Pool p, Object affinity) {
        return p.withConnection(affinity, c -> {
            Skaidb.ResultSet rs = c.query("SELECT member()");
            rs.next();
            return rs.getString(1);
        });
    }

    private static List<String> serve(Skaidb.Pool p, int n, Object affinity) {
        List<String> out = new ArrayList<>();
        for (int i = 0; i < n; i++) out.add(who(p, affinity));
        return out;
    }

    private static Set<String> distinct(List<String> served) { return new HashSet<>(served); }

    private static Skaidb.Pool.Member row(Skaidb.Pool p, Member m) {
        for (Skaidb.Pool.Member r : p.members()) if (r.endpoint.equals(m.endpoint)) return r;
        throw new AssertionError("no member " + m.endpoint + " in " + p.members());
    }

    private static String state(Skaidb.Pool p, Member m) { return row(p, m).state; }

    private static Skaidb.Pool.MemberState internal(Skaidb.Pool p, Member m) {
        synchronized (p) { return p.members.get(m.endpoint); }
    }

    private static void await(String what, Supplier<Boolean> done) throws InterruptedException {
        long deadline = System.nanoTime() + 5_000_000_000L;
        while (!done.get()) {
            assertTrue(System.nanoTime() < deadline, what);
            Thread.sleep(50);
        }
    }

    // ---- policies -----------------------------------------------------------

    @Test
    void roundRobinRotatesOverEveryMember() throws IOException {
        List<Member> ms = members("a", "b", "c");
        // The DSN form, anonymous: the client then skips the signature check.
        String dsn = "skaidb://anonymous@" + ms.get(0).endpoint + "," + ms.get(1).endpoint + "," + ms.get(2).endpoint;
        Skaidb.Pool p = new Skaidb.Pool(dsn, new Skaidb.PoolOptions().loadBalancing("round_robin"));
        cleanup.add(p);
        List<String> served = serve(p, 9, null);
        List<String> sorted = new ArrayList<>(served);
        Collections.sort(sorted);
        assertEquals(Arrays.asList("a", "a", "a", "b", "b", "b", "c", "c", "c"), sorted);
        // Strict rotation: three in a row are three different members.
        for (int i = 0; i < 9; i += 3) assertEquals(3, distinct(served.subList(i, i + 3)).size(), served.toString());
    }

    @Test
    void leastLatencyPrefersTheFasterMember() throws IOException {
        List<Member> ms = members("a", "b");
        ms.get(1).delayMs = 20;
        Skaidb.Pool p = pool(ms, new Skaidb.PoolOptions()
            .loadBalancing(Skaidb.LoadBalancing.LEAST_LATENCY).ejectLatencyFactor(0));
        List<String> served = serve(p, 60, null);
        assertTrue(Collections.frequency(served, "a") > 48, served.toString());
        assertTrue(served.contains("b"), "b was measured at least once");
    }

    @Test
    void leastLatencyReMeasuresAMemberItHasNotUsedLately() throws IOException {
        List<Member> ms = members("a", "b");
        Member b = ms.get(1);
        b.delayMs = 20;
        Skaidb.Pool p = pool(ms, new Skaidb.PoolOptions()
            .loadBalancing(Skaidb.LoadBalancing.LEAST_LATENCY).ejectLatencyFactor(0));
        serve(p, 20, null);
        // b is slow, then recovers: an idle pool notices once b's average
        // goes stale instead of never asking b again.
        b.delayMs = 0;
        synchronized (p) { internal(p, b).lastSample -= 10_000_000_000L; }
        assertTrue(serve(p, 3, null).contains("b"));
    }

    @Test
    void randomPolicyKeepsOneReusedConnection() throws IOException {
        Skaidb.Pool p = pool(members("a", "b", "c"), new Skaidb.PoolOptions());
        // One connection, reused: random never targets a member.
        assertEquals(1, distinct(serve(p, 5, null)).size());
    }

    @Test
    void unknownPolicyIsRefused() {
        assertThrows(Skaidb.SkaidbException.class, () -> new Skaidb.PoolOptions().loadBalancing("nearest"));
        assertThrows(Skaidb.SkaidbException.class, () -> new Skaidb.PoolOptions().idleOrder("random"));
        assertThrows(Skaidb.SkaidbException.class, () -> new Skaidb.PoolOptions().maxSize(0));
        assertThrows(Skaidb.SkaidbException.class, () -> new Skaidb.PoolOptions().ejectErrors(0));
        assertThrows(Skaidb.SkaidbException.class, () -> new Skaidb.PoolOptions().ejectBackoff(Duration.ofSeconds(-1)));
        assertEquals(Skaidb.LoadBalancing.LEAST_LATENCY, Skaidb.LoadBalancing.parse("LEAST_LATENCY"));
        assertEquals(Skaidb.IdleOrder.FIFO, Skaidb.IdleOrder.parse("fifo"));
    }

    // ---- idle order ---------------------------------------------------------

    private Skaidb.Connection[] three(Skaidb.Pool p) {
        Skaidb.Connection[] conns = { p.acquire(), p.acquire(), p.acquire() };
        for (Skaidb.Connection c : conns) p.release(c);
        return conns;
    }

    @Test
    void lifoHandsOutTheMostRecentlyReturned() throws IOException {
        Skaidb.Pool p = pool(members("a"), new Skaidb.PoolOptions());
        Skaidb.Connection[] conns = three(p);
        assertSame(conns[2], p.acquire());
    }

    @Test
    void fifoHandsOutTheLongestIdle() throws IOException {
        Skaidb.Pool p = pool(members("a"), new Skaidb.PoolOptions().idleOrder(Skaidb.IdleOrder.FIFO));
        Skaidb.Connection[] conns = three(p);
        List<Skaidb.Connection> got = Arrays.asList(p.acquire(), p.acquire(), p.acquire());
        assertEquals(Arrays.asList(conns), got);
    }

    // ---- health ejection ------------------------------------------------------

    @Test
    void aFailingMemberIsEjectedThenReadmittedByTheProbe() throws Exception {
        List<Member> ms = members("a", "b", "c");
        Member b = ms.get(1);
        b.refuse = true;
        Skaidb.Pool p = pool(ms, roundRobin().ejectErrors(2).ejectBackoff(Duration.ofMillis(300)));
        assertFalse(serve(p, 12, null).contains("b"));
        assertEquals("ejected", state(p, b));
        // Healthy again: the background probe re-admits it after the backoff.
        b.refuse = false;
        await("never re-admitted", () -> state(p, b).equals("ready"));
        assertTrue(serve(p, 6, null).contains("b"));
    }

    @Test
    void aProbeThatFailsDoublesTheBackoff() throws Exception {
        List<Member> ms = members("a", "b", "c");
        Member b = ms.get(1);
        b.refuse = true;
        Skaidb.Pool p = pool(ms, roundRobin().ejectErrors(1).ejectBackoff(Duration.ofMillis(200)));
        serve(p, 6, null);
        Skaidb.Pool.MemberState m = internal(p, b);
        await("the probe never ran", () -> { synchronized (p) { return m.ejections >= 2; } });
        assertEquals("ejected", state(p, b));
        synchronized (p) { assertTrue(p.backoffFor(m) >= 400_000_000L); }
    }

    @Test
    void aConsistentlySlowMemberIsEjected() throws IOException {
        List<Member> ms = members("a", "b", "c");
        Member b = ms.get(1);
        b.delayMs = 50;
        Skaidb.Pool p = pool(ms, roundRobin().ejectLatencyFloor(Duration.ofMillis(20)).ejectBackoff(Duration.ofSeconds(30)));
        serve(p, 45, null);
        assertEquals("ejected", state(p, b));
        assertFalse(serve(p, 6, null).contains("b"));
    }

    @Test
    void anEjectedMemberIsNotEjectedAgainByLateRoundTrips() throws IOException {
        List<Member> ms = members("a", "b", "c");
        Member b = ms.get(1);
        b.delayMs = 50;
        Skaidb.Pool p = pool(ms, roundRobin().ejectLatencyFloor(Duration.ofMillis(20)).ejectBackoff(Duration.ofSeconds(30)));
        // A connection to b checked out before the ejection, kept out.
        Skaidb.Connection late = null;
        List<Skaidb.Connection> others = new ArrayList<>();
        while (late == null) {
            Skaidb.Connection c = p.acquire();
            if (c.connectedEp.equals(b.endpoint)) late = c;
            else others.add(c);
        }
        for (Skaidb.Connection c : others) p.release(c);
        serve(p, 45, null);
        assertEquals("ejected", state(p, b));
        Skaidb.Pool.MemberState m = internal(p, b);
        long until;
        synchronized (p) { until = m.ejectedUntil; }
        // Slow round trips still finishing on it.
        for (int i = 0; i < 5; i++) late.query("SELECT member()");
        synchronized (p) {
            assertEquals(1, m.ejections);
            assertEquals(until, m.ejectedUntil);
        }
        p.release(late);
        assertFalse(late.isUsable(), "returned to an ejected member: closed, not kept");
    }

    @Test
    void oneSlowStatementDoesNotEjectAMember() throws IOException {
        List<Member> ms = members("a", "b", "c");
        Member b = ms.get(1);
        Skaidb.Pool p = pool(ms, roundRobin().ejectLatencyFloor(Duration.ofMillis(50)));
        serve(p, 36, null);
        b.delayMs = 500;
        while (!who(p).equals("b")) {
            // until b has served the slow one
        }
        b.delayMs = 0;
        serve(p, 6, null);
        assertEquals("ready", state(p, b));
    }

    /**
     * Connections to each member outside the pool, by member name: stand-ins
     * to feed the pool's latency measurements round trips of a chosen length.
     */
    private Map<String, Skaidb.Connection> standIns(Skaidb.Pool p, List<Member> ms) {
        Map<String, Skaidb.Connection> out = new HashMap<>();
        for (Member m : ms) {
            Skaidb.Connection c = Skaidb.connect(connectTo(m));
            cleanup.add(c);
            synchronized (p) { c.poolEp = c.connectedEp; }
            out.put(m.name, c);
        }
        return out;
    }

    /** The pool's WARNING lines logged while {@code body} runs. */
    private static List<String> poolWarnings(Runnable body) {
        Logger log = Logger.getLogger("com.skaidb.pool");
        List<String> lines = new CopyOnWriteArrayList<>();
        SimpleFormatter text = new SimpleFormatter();
        Handler capture = new Handler() {
            @Override public void publish(LogRecord r) {
                if (r.getLevel().intValue() >= Level.WARNING.intValue()) lines.add(text.formatMessage(r));
            }
            @Override public void flush() {}
            @Override public void close() {}
        };
        log.addHandler(capture);
        try {
            body.run();
        } finally {
            log.removeHandler(capture);
        }
        return lines;
    }

    @Test
    void aSlowFirstRoundTripDoesNotEjectAMember() throws IOException {
        // The first statements on b stall in the client — a CPU-bound
        // application starting up holds the thread — and b answers fast from
        // then on: the fastest of its first round trips seeds its average, so
        // b stays in rotation. (Seeded by its first round trip alone, b's
        // average still read 3.8 s when it was first compared with the
        // others, and b was ejected.)
        List<Member> ms = members("a", "b", "c");
        Member b = ms.get(1);
        Skaidb.Pool p = pool(ms, roundRobin().ejectLatencyFloor(Duration.ofMillis(50)));
        Map<String, Skaidb.Connection> to = standIns(p, ms);
        p.roundTrip(to.get("b"), 6_600_000_000L);
        p.roundTrip(to.get("b"), 3_000_000_000L);
        for (int i = 0; i < 12; i++) {
            for (String name : new String[] { "a", "b", "c" }) p.roundTrip(to.get(name), 200_000);
        }
        assertEquals("ready", state(p, b));
        assertEquals(0.2, row(p, b).latencyMs);
    }

    @Test
    void aMemberSlowFromItsFirstRoundTripIsEjectedAndTheWarningNamesItsSlowest() throws IOException {
        List<Member> ms = members("a", "b", "c");
        Member b = ms.get(1);
        Skaidb.Pool p = pool(ms, roundRobin().ejectLatencyFloor(Duration.ofMillis(50)).ejectBackoff(Duration.ofSeconds(30)));
        Map<String, Skaidb.Connection> to = standIns(p, ms);
        List<String> warnings = poolWarnings(() -> {
            for (int i = 0; i < 12; i++) {
                p.roundTrip(to.get("a"), 200_000);
                p.roundTrip(to.get("c"), 200_000);
                p.roundTrip(to.get("b"), i == 5 ? 900_000_000 : 500_000_000);
            }
        });
        assertEquals("ejected", state(p, b));
        List<String> ejected = new ArrayList<>();
        for (String w : warnings) if (w.contains("ejecting")) ejected.add(w);
        assertEquals(1, ejected.size(), warnings.toString());
        assertTrue(ejected.get(0).matches(".*latency 5\\d\\d\\.\\d ms vs median 0\\.2 ms;.*"), ejected.get(0));
        assertTrue(ejected.get(0).contains("slowest of the last 10 round trips 900.0 ms"), ejected.get(0));
    }

    @Test
    void aReadmittedMemberIsSeededAfresh() throws Exception {
        List<Member> ms = members("a", "b", "c");
        Member b = ms.get(1);
        Skaidb.Pool p = pool(ms, roundRobin().ejectErrors(1).ejectBackoff(Duration.ofMillis(100)));
        Map<String, Skaidb.Connection> to = standIns(p, ms);
        Skaidb.Pool.MemberState m = internal(p, b);
        for (long t : new long[] { 4_000_000, 3_000_000, 2_000_000 }) p.roundTrip(to.get("b"), t);
        assertEquals(2.0, row(p, b).latencyMs);
        // Ejected, then re-admitted by the probe: measured afresh.
        p.failed(b.endpoint);
        assertEquals("ejected", state(p, b));
        await("never re-admitted", () -> state(p, b).equals("ready"));
        synchronized (p) {
            assertFalse(m.seeded);
            assertTrue(m.firsts.isEmpty(), m.firsts.toString());
            assertTrue(m.recent.isEmpty(), m.recent.toString());
            assertEquals(0, m.samples);
        }
        p.roundTrip(to.get("b"), 7_000_000);
        assertNull(row(p, b).latencyMs, "not measured until its first three round trips");
        p.roundTrip(to.get("b"), 9_000_000);
        p.roundTrip(to.get("b"), 8_000_000);
        assertEquals(7.0, row(p, b).latencyMs);
    }

    @Test
    void theLastUsableMemberIsNeverEjected() throws IOException {
        List<Member> ms = members("a", "b", "c");
        Member a = ms.get(0);
        ms.get(1).refuse = true;
        ms.get(2).refuse = true;
        Skaidb.Pool p = pool(ms, roundRobin().ejectErrors(1));
        for (int i = 0; i < 6; i++) assertEquals("a", who(p));
        a.refuse = true;
        // Idle connections to a still work (how many depends on the dial
        // order above); once they are used up, every dial fails.
        List<Skaidb.Connection> held = new ArrayList<>();
        try {
            assertThrows(Skaidb.SkaidbException.class, () -> {
                for (int i = 0; i < 12; i++) held.add(p.acquire());
            });
            assertEquals("ready", state(p, a));
        } finally {
            for (Skaidb.Connection c : held) c.close();
        }
    }

    @Test
    void aRefusedLoginIsNotAMemberFault() {
        assertFalse(Skaidb.memberFault(new Skaidb.SkaidbException("authentication denied: bad password")));
        assertFalse(Skaidb.memberFault(new Skaidb.SkaidbException("server signature mismatch (mutual auth failed)")));
        assertTrue(Skaidb.memberFault(new Skaidb.SkaidbException("authentication denied: too many connections: 500")));
        assertTrue(Skaidb.memberFault(new Skaidb.SkaidbException("bad handshake challenge")));
        assertTrue(Skaidb.memberFault(new java.net.ConnectException("Connection refused")));
        assertTrue(Skaidb.memberFault(new java.io.EOFException()));
    }

    @Test
    void aMemberThatClosesBeforeTheChallengeFallsThroughToTheNextSeed() throws IOException {
        List<Member> ms = members("a", "b");
        ms.get(0).refuse = true;   // a capacity refusal: the socket closes before AuthChallenge
        for (int i = 0; i < 5; i++) {
            try (Skaidb.Connection c = Skaidb.connect(connectTo(ms.get(0), ms.get(1)))) {
                Skaidb.ResultSet rs = c.query("SELECT member()");
                rs.next();
                assertEquals("b", rs.getString(1));
            }
        }
    }

    // ---- discovery ------------------------------------------------------------

    /** Endpoint -> state as every member lists it. */
    private final Map<String, String> states = Collections.synchronizedMap(new TreeMap<>());

    private List<Member> cluster() throws IOException {
        List<Member> ms = members("a", "b", "c");
        states.put(ms.get(0).endpoint, "ready");
        states.put(ms.get(1).endpoint, "ready");
        states.put(ms.get(2).endpoint, "resyncing");
        for (Member me : ms) {
            me.listing = () -> {
                List<Object[]> rows = new ArrayList<>();
                synchronized (states) {
                    for (Map.Entry<String, String> e : states.entrySet())
                        rows.add(new Object[] { e.getKey(), e.getValue(), e.getKey().equals(me.endpoint) });
                }
                return rows;
            };
        }
        return ms;
    }

    private static void relist(Skaidb.Pool p) {
        Skaidb.Connection c = p.acquire();
        try {
            p.listThrough(c);
        } finally {
            p.release(c);
        }
    }

    @Test
    void membersAreDiscoveredAndResyncingOnesGetNoConnections() throws IOException {
        List<Member> ms = cluster();
        Member c = ms.get(2);
        Skaidb.Pool p = pool(connectTo(ms.get(0)), roundRobin().discover(true));
        assertEquals(new HashSet<>(Arrays.asList("a", "b")), distinct(serve(p, 8, null)));
        assertEquals("resyncing", state(p, c));
        // c catches up: the next refresh lets it in, measured afresh.
        Skaidb.Pool.MemberState m = internal(p, c);
        synchronized (p) {
            m.ewma = 9.9e9;
            m.seeded = true;
            m.recent.add(9_900_000_000L);
            m.samples = 50;
        }
        states.put(c.endpoint, "ready");
        relist(p);
        synchronized (p) {
            assertEquals(0, m.samples);
            assertFalse(m.seeded);
            assertTrue(m.recent.isEmpty(), m.recent.toString());
        }
        assertNull(row(p, c).latencyMs);
        assertTrue(serve(p, 6, null).contains("c"));
    }

    @Test
    void aListingWithAMemberNotReadyRefreshesSooner() throws IOException {
        List<Member> ms = cluster();
        Skaidb.Pool p = pool(connectTo(ms.get(0)), roundRobin().discover(true).refreshInterval(Duration.ofSeconds(60)));
        who(p);
        synchronized (p) { assertTrue(p.nextRefresh - System.nanoTime() <= 5_000_000_000L); }   // c resyncing
        states.put(ms.get(2).endpoint, "ready");
        relist(p);
        synchronized (p) { assertTrue(p.nextRefresh - System.nanoTime() > 50_000_000_000L); }
    }

    @Test
    void aSeedGivenByAnotherNameIsNotCountedTwice() throws IOException {
        List<Member> ms = cluster();
        Member a = ms.get(0);
        Skaidb.ConnectOptions co = new Skaidb.ConnectOptions().host("localhost", a.port)
            .user(ConformanceServer.username()).password(ConformanceServer.password());
        Skaidb.Pool p = pool(co, roundRobin().discover(true));
        who(p);
        List<Skaidb.Pool.Member> rows = p.members();
        assertEquals(3, rows.size(), rows.toString());
        Skaidb.Pool.Member seed = rows.stream().filter(r -> r.seed).findFirst().orElseThrow(AssertionError::new);
        assertEquals("localhost:" + a.port, seed.endpoint);
        assertEquals(a.endpoint, seed.canonical);
    }

    @Test
    void aMemberThatLeavesTheListingIsDropped() throws IOException {
        List<Member> ms = cluster();
        Member b = ms.get(1);
        Skaidb.Pool p = pool(connectTo(ms.get(0)), roundRobin().discover(true));
        who(p);
        states.remove(b.endpoint);
        relist(p);
        assertTrue(p.members().stream().noneMatch(r -> r.endpoint.equals(b.endpoint)), p.members().toString());
    }

    @Test
    void aSeedThatLeavesTheListingIsKeptAsUnlisted() throws IOException {
        List<Member> ms = cluster();
        Member a = ms.get(0), b = ms.get(1);
        Skaidb.Pool p = pool(connectTo(a, b), roundRobin().discover(true));
        serve(p, 4, null);
        states.remove(b.endpoint);
        relist(p);
        assertEquals("unlisted", state(p, b));
        assertTrue(row(p, b).seed);
    }

    @Test
    void anOldServerTurnsDiscoveryOffAndKeepsTheSeeds() throws IOException {
        List<Member> ms = cluster();
        Member a = ms.get(0);
        a.oldServer = true;
        Skaidb.Pool p = pool(connectTo(a), roundRobin().discover(true));
        assertEquals(Collections.singleton("a"), distinct(serve(p, 4, null)));
        synchronized (p) { assertTrue(p.discoveryOff); }
        assertEquals(1, p.members().size());
    }

    @Test
    void aStandaloneServerTurnsDiscoveryOffAndKeepsTheSeeds() throws IOException {
        Member a = members("a").get(0);
        a.listing = Collections::emptyList;   // standalone: no rows
        Skaidb.Pool p = pool(connectTo(a), roundRobin().discover(true));
        assertEquals(Collections.singleton("a"), distinct(serve(p, 4, null)));
        synchronized (p) { assertTrue(p.discoveryOff); }
        assertEquals(1, p.members().size());
        assertEquals(1, a.statements.stream().filter(s -> s.equals("SHOW ENDPOINTS")).count());
    }

    // ---- read-your-writes -------------------------------------------------------

    @Test
    void readsFollowTheMemberThatTookTheWrite() throws Exception {
        List<Member> ms = members("a", "b", "c");
        Skaidb.Pool p = pool(ms, roundRobin().readYourWrites(Duration.ofMillis(400)));
        String wrote;
        Skaidb.Connection conn = p.acquire("alice");
        try {
            conn.execute("INSERT INTO t VALUES (1)");
            wrote = conn.connectedEp;
        } finally {
            p.release(conn);
        }
        String writer = ms.stream().filter(m -> m.endpoint.equals(wrote)).findFirst()
            .orElseThrow(AssertionError::new).name;
        assertEquals(Collections.singleton(writer), distinct(serve(p, 6, "alice")));
        // Another key, or no key, still rotates.
        assertEquals(3, distinct(serve(p, 3, "bob")).size());
        Thread.sleep(500);
        assertEquals(3, distinct(serve(p, 3, "alice")).size());
    }

    @Test
    void readsDoNotSetAffinity() throws IOException {
        Skaidb.Pool p = pool(members("a", "b", "c"), roundRobin().readYourWrites(Duration.ofSeconds(5)));
        who(p, "alice");
        assertEquals(3, distinct(serve(p, 3, "alice")).size());
    }

    @Test
    void statementClassification() {
        for (String sql : new String[] { "SELECT 1", "  select 1", "(SELECT 1)", "-- c\nSHOW TABLES",
                "/* x */ EXPLAIN SELECT 1", "WITH a AS (SELECT 1) SELECT * FROM a",
                "DESCRIBE t", "DESC t", "VALUES (1)", "use app" }) {
            assertFalse(Skaidb.isWrite(sql), sql);
        }
        for (String sql : new String[] { "INSERT INTO t VALUES (1)", "update t set a = 1", "DELETE FROM t",
                "CREATE TABLE t (id INT PRIMARY KEY)", "COMMIT", "CALL p()", "selectx" }) {
            assertTrue(Skaidb.isWrite(sql), sql);
        }
    }
}
