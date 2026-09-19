package com.skaidb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * An in-process skaidb that speaks just enough of the wire protocol
 * (PROTOCOL.md §§1-3) to drive the client end to end: the four-frame
 * handshake, Hello, OP_QUERY, prepare/execute/batch, and streaming. No
 * SQL is interpreted; it answers by opcode.
 */
class FakeServerTest {

    /** What the fake saw, for assertions. */
    static final class Seen {
        volatile String user;
        volatile String helloName;
        volatile String helloVersion;
        final List<String> sql = new CopyOnWriteArrayList<>();
        final List<Integer> levels = new CopyOnWriteArrayList<>();
        final List<Object[]> params = new CopyOnWriteArrayList<>();
        volatile int connections = 0;
        final List<Socket> sockets = new CopyOnWriteArrayList<>();
    }

    private ServerSocket server;
    private Thread acceptor;
    private final Seen seen = new Seen();
    /** How many chunks a stream answer carries; each holds 3 rows. */
    private volatile int streamChunks = 2;

    @BeforeEach
    void start() throws IOException {
        server = listen(0);
        startAcceptor();
    }

    /** Serve whatever {@link #server} currently is, one thread per client. */
    private void startAcceptor() {
        acceptor = new Thread(() -> {
            try {
                while (!server.isClosed()) {
                    Socket s = server.accept();
                    seen.connections++;
                    seen.sockets.add(s);
                    Thread t = new Thread(() -> serve(s));
                    t.setDaemon(true);
                    t.start();
                }
            } catch (IOException e) {
                // closed
            }
        });
        acceptor.setDaemon(true);
        acceptor.start();
    }

    @AfterEach
    void stop() throws IOException {
        server.close();
        for (Socket s : seen.sockets) s.close();
    }

    private static ServerSocket listen(int port) throws IOException {
        ServerSocket ss = new ServerSocket();
        ss.setReuseAddress(true);
        ss.bind(new java.net.InetSocketAddress(java.net.InetAddress.getLoopbackAddress(), port), 50);
        return ss;
    }

    private String dsn() {
        // Empty password: the client then skips the server-signature check,
        // which lets the fake answer with a zero proof.
        return "skaidb://anonymous@127.0.0.1:" + server.getLocalPort();
    }

    // ---- the fake ----------------------------------------------------------

    private static byte[] readFrame(DataInputStream in) throws IOException {
        int len = in.readInt();
        byte[] b = new byte[len];
        in.readFully(b);
        return b;
    }

    private static void writeFrame(OutputStream out, byte[] payload) throws IOException {
        out.write(new byte[] {
            (byte) (payload.length >>> 24), (byte) (payload.length >>> 16),
            (byte) (payload.length >>> 8), (byte) payload.length });
        out.write(payload);
        out.flush();
    }

    private static Skaidb.Buf cell(Skaidb.Buf b, Object v) {
        byte[] enc = Skaidb.encodeValue(v);
        return b.u32(enc.length).raw(enc);
    }

    private static byte[] rowsFrame(String[] cols, List<Object[]> rows) {
        Skaidb.Buf b = new Skaidb.Buf();
        b.u8(0).u32(cols.length);
        for (String c : cols) b.str(c);
        b.u32(rows.size());
        for (Object[] r : rows) {
            b.u32(r.length);
            for (Object v : r) cell(b, v);
        }
        return b.toBytes();
    }

    private void serve(Socket s) {
        try (Socket sock = s) {
            DataInputStream in = new DataInputStream(sock.getInputStream());
            OutputStream out = sock.getOutputStream();

            // 1. AuthStart
            Skaidb.Reader r = new Skaidb.Reader(readFrame(in));
            if (r.u8() != 10) return;
            seen.user = r.text();
            r.text(); // client nonce
            // 2. AuthChallenge
            Skaidb.Buf ch = new Skaidb.Buf();
            ch.u8(11).u32(16).raw(new byte[16]).u32(1).str("srv-nonce");
            writeFrame(out, ch.toBytes());
            // 3. AuthFinish
            r = new Skaidb.Reader(readFrame(in));
            if (r.u8() != 12) return;
            r.take(32);
            // 4. AuthOutcome Ok + 32-byte signature
            Skaidb.Buf ok = new Skaidb.Buf();
            ok.u8(13).u8(1).raw(new byte[32]);
            writeFrame(out, ok.toBytes());

            int nextStmt = 100;
            java.util.Map<Integer, Integer> stmtParams = new java.util.HashMap<>();
            while (true) {
                byte[] frame;
                try { frame = readFrame(in); } catch (IOException eof) { return; }
                r = new Skaidb.Reader(frame);
                int op = r.u8();
                switch (op) {
                    case 8: { // Hello
                        seen.helloName = r.text();
                        seen.helloVersion = r.text();
                        writeFrame(out, new Skaidb.Buf().u8(2).toBytes());
                        break;
                    }
                    case 1: { // Query
                        int level = r.u8();
                        String sql = r.text();
                        seen.levels.add(level);
                        seen.sql.add(sql);
                        writeFrame(out, answer(sql, null));
                        break;
                    }
                    case 2: { // Prepare
                        String sql = r.text();
                        if (sql.startsWith("CREATE")) {
                            writeFrame(out, new Skaidb.Buf().u8(3).str("cannot prepare DDL").toBytes());
                            break;
                        }
                        int id = nextStmt++;
                        stmtParams.put(id, Skaidb.countPlaceholders(sql));
                        seen.sql.add("PREPARE " + sql);
                        writeFrame(out, new Skaidb.Buf().u8(4).u32(id).u16(stmtParams.get(id)).toBytes());
                        break;
                    }
                    case 3: { // Execute prepared
                        int level = r.u8();
                        int id = r.u32();
                        int n = r.u16();
                        Object[] p = new Object[n];
                        for (int i = 0; i < n; i++) p[i] = Skaidb.decodeValue(new Skaidb.Reader(r.blob()));
                        seen.levels.add(level);
                        seen.params.add(p);
                        writeFrame(out, answer("EXEC " + id, p));
                        break;
                    }
                    case 7: { // Batch
                        r.u8(); r.u32();
                        int nrows = r.u32();
                        long total = 0;
                        for (int i = 0; i < nrows; i++) {
                            int n = r.u16();
                            for (int j = 0; j < n; j++) r.blob();
                            total++;
                        }
                        writeFrame(out, new Skaidb.Buf().u8(1).i64(total).toBytes());
                        break;
                    }
                    case 5: { // Stream
                        r.u8();
                        String sql = r.text();
                        seen.sql.add("STREAM " + sql);
                        if (sql.startsWith("FAIL")) {
                            writeFrame(out, new Skaidb.Buf().u8(3).str("boom").toBytes());
                            break;
                        }
                        if (!sql.startsWith("SELECT")) {
                            writeFrame(out, answer(sql, null));   // one frame, no stream
                            break;
                        }
                        Skaidb.Buf h = new Skaidb.Buf();
                        h.u8(5).u32(2).str("id").str("name");
                        writeFrame(out, h.toBytes());
                        for (int c = 0; c < streamChunks; c++) {
                            Skaidb.Buf ck = new Skaidb.Buf();
                            ck.u8(6).u32(3);
                            for (int i = 0; i < 3; i++) {
                                ck.u32(2);
                                cell(ck, (long) (c * 3 + i));
                                cell(ck, "row" + (c * 3 + i));
                            }
                            writeFrame(out, ck.toBytes());
                        }
                        writeFrame(out, new Skaidb.Buf().u8(7).toBytes());
                        break;
                    }
                    default:
                        writeFrame(out, new Skaidb.Buf().u8(3).str("unknown opcode " + op).toBytes());
                }
            }
        } catch (IOException e) {
            // client went away
        }
    }

    /** Answer by the SQL's first word: no parsing beyond that. */
    private static byte[] answer(String sql, Object[] params) {
        if (sql.startsWith("SELECT") || sql.startsWith("EXEC")) {
            List<Object[]> rows = new ArrayList<>();
            if (params == null) {
                rows.add(new Object[] { 1L, "Ada" });
                rows.add(new Object[] { 2L, "Linus" });
            } else {
                rows.add(new Object[] { (long) params.length, params.length > 0 ? params[0] : null });
            }
            return rowsFrame(new String[] { "id", "name" }, rows);
        }
        if (sql.startsWith("INSERT")) return new Skaidb.Buf().u8(1).i64(1).toBytes();
        if (sql.startsWith("CREATE") || sql.startsWith("USE")) return new Skaidb.Buf().u8(2).toBytes();
        if (sql.startsWith("MULTI")) {
            Skaidb.Buf b = new Skaidb.Buf();
            b.u8(8).u32(2);
            b.u32(1).str("a").u32(1).u32(1); cell(b, 1L);
            b.u32(1).str("b").u32(2).u32(1); cell(b, "x"); b.u32(1); cell(b, "y");
            return b.toBytes();
        }
        return new Skaidb.Buf().u8(3).str("syntax error near " + sql).toBytes();
    }

    // ---- tests -------------------------------------------------------------

    @Test
    void handshakeHelloAndPlainQueries() {
        try (Skaidb.Connection conn = Skaidb.connect(dsn() + "/?consistency=one")) {
            assertEquals("anonymous", seen.user);
            assertEquals("java", seen.helloName);
            assertEquals(Skaidb.VERSION, seen.helloVersion);
            assertNotNull(seen.helloVersion);
            assertTrue(conn.isUsable());

            Skaidb.ResultSet rs = conn.query("SELECT id, name FROM t");
            assertEquals(Skaidb.CONSISTENCY_ONE, seen.levels.get(0));
            assertEquals(2, rs.getRowCount());
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("Ada", rs.getString("name"));
            assertTrue(rs.next());
            assertEquals("Linus", rs.getString(2));
            assertFalse(rs.next());

            assertEquals(1L, conn.execute("INSERT INTO t VALUES (1)"));
            assertEquals(-1L, conn.execute("CREATE TABLE t (PRIMARY KEY (id))"));

            Skaidb.SkaidbException e = assertThrows(Skaidb.SkaidbException.class,
                () -> conn.execute("GARBAGE"));
            assertTrue(e.getMessage().contains("syntax error"), e.getMessage());
            // A statement error leaves the connection usable (§6).
            assertTrue(conn.isUsable());
            assertEquals(1L, conn.execute("INSERT INTO t VALUES (2)"));

            conn.setConsistency(Skaidb.CONSISTENCY_ALL);
            conn.query("SELECT 1");
            assertEquals(Skaidb.CONSISTENCY_ALL, seen.levels.get(seen.levels.size() - 1));
        }
        assertEquals(1, seen.connections);
    }

    @Test
    void sessionDatabaseIssuesUse() {
        try (Skaidb.Connection conn = Skaidb.connect(dsn() + "/app")) {
            assertEquals("USE \"app\"", seen.sql.get(0));
            assertTrue(conn.isUsable());
        }
    }

    @Test
    void typedParametersGoThroughServerSidePrepare() {
        try (Skaidb.Connection conn = Skaidb.connect(dsn())) {
            java.util.Map<String, Object> doc = new java.util.LinkedHashMap<>();
            doc.put("tags", java.util.Arrays.asList("a", "b"));
            java.util.UUID id = java.util.UUID.randomUUID();
            Skaidb.ResultSet rs = conn.prepare("SELECT * FROM t WHERE a = ? AND b = ? AND c = ?")
                .setObject(1, doc).setObject(2, id).setConsistency(Skaidb.CONSISTENCY_ALL)
                .setNull(3)
                .executeQuery();
            assertTrue(rs.next());
            assertEquals(3L, rs.getLong("id"));
            assertEquals(doc, rs.getObject("name"));
            Object[] p = seen.params.get(0);
            assertEquals(doc, p[0]);
            assertEquals(id, p[1]);
            assertEquals(null, p[2]);
            assertEquals(Skaidb.CONSISTENCY_ALL, seen.levels.get(seen.levels.size() - 1));
            // The connection's own level was not touched by the per-statement one.
            conn.query("SELECT 1");
            assertEquals(Skaidb.CONSISTENCY_QUORUM, seen.levels.get(seen.levels.size() - 1));

            // The same text prepares once per connection.
            conn.prepare("SELECT * FROM t WHERE a = ? AND b = ? AND c = ?").setInt(1, 1).setInt(2, 2).setInt(3, 3).executeQuery();
            long prepares = seen.sql.stream().filter(s -> s.startsWith("PREPARE")).count();
            assertEquals(1, prepares);

            // Parameter index checks are local and 1-based.
            Skaidb.Query q = conn.prepare("SELECT ?");
            assertThrows(Skaidb.SkaidbException.class, () -> q.setInt(0, 1));
            assertThrows(Skaidb.SkaidbException.class, () -> q.setInt(2, 1));

            // Statements the server refuses to prepare fall back to text binding.
            assertEquals(-1L, conn.prepare("CREATE TABLE ? (PRIMARY KEY (id))").setString(1, "x").executeUpdate());
            assertTrue(seen.sql.contains("CREATE TABLE 'x' (PRIMARY KEY (id))"));

            // A batch is one round-trip and returns the total.
            List<Object[]> rows = new ArrayList<>();
            for (int i = 0; i < 5; i++) rows.add(new Object[] { (long) i, "n" + i });
            assertEquals(5L, conn.prepare("INSERT INTO t (a, b) VALUES (?, ?)").executeBatch(rows));
            assertThrows(Skaidb.SkaidbException.class,
                () -> conn.prepare("INSERT INTO t (a, b) VALUES (?, ?)").executeBatch(List.<Object[]>of(new Object[] { 1 })));
        }
    }

    @Test
    void multipleResultSets() {
        try (Skaidb.Connection conn = Skaidb.connect(dsn())) {
            Skaidb.ResultSet rs = conn.query("MULTI");
            assertTrue(rs.next());
            assertEquals(1L, rs.getLong("a"));
            assertFalse(rs.next());
            assertTrue(rs.nextResultSet());
            assertEquals(2, rs.getRowCount());
            assertTrue(rs.next());
            assertEquals("x", rs.getString("b"));
            assertFalse(rs.nextResultSet());
        }
    }

    @Test
    void streamingOwnsTheWireUntilTheEnd() {
        try (Skaidb.Connection conn = Skaidb.connect(dsn())) {
            long n = 0;
            try (Skaidb.RowStream s = conn.stream("SELECT id, name FROM big")) {
                assertFalse(conn.isUsable(), "a pool must not lend a streaming connection");
                Skaidb.SkaidbException busy = assertThrows(Skaidb.SkaidbException.class,
                    () -> conn.query("SELECT 1"));
                assertTrue(busy.getMessage().contains("busy streaming"), busy.getMessage());
                while (s.next()) {
                    assertEquals(n, ((Number) s.getObject(0)).longValue());
                    assertEquals("row" + n, s.getObject("name"));
                    n++;
                }
                assertEquals(6, n);
                // RowsEnd was read: the wire is free before close() runs.
                assertTrue(conn.isUsable());
            }
            assertTrue(conn.isUsable());
            assertEquals(2, conn.query("SELECT 1").getRowCount());
        }
    }

    @Test
    void abandonedStreamIsDrainedAndTheConnectionReused() {
        streamChunks = 4;
        try (Skaidb.Connection conn = Skaidb.connect(dsn())) {
            try (Skaidb.RowStream s = conn.stream("SELECT id, name FROM big")) {
                assertTrue(s.next());
                assertEquals(0L, s.getObject(0));
                // walk away after one row: close() drains the remaining frames
            }
            assertTrue(conn.isUsable());
            assertEquals(2, conn.query("SELECT 1").getRowCount());
            assertEquals(1, seen.connections, "drained, not re-dialled");

            // A stream that fails up front leaves the wire free as well.
            Skaidb.SkaidbException e = assertThrows(Skaidb.SkaidbException.class, () -> conn.stream("FAIL"));
            assertEquals("boom", e.getMessage());
            assertTrue(conn.isUsable());
            // A non-row statement streamed answers in one frame and owns nothing.
            try (Skaidb.RowStream s = conn.stream("INSERT INTO t VALUES (1)")) {
                assertFalse(s.next());
                assertTrue(conn.isUsable());
            }
        }
    }

    @Test
    void reconnectsAfterTheServerDropsTheSocket() throws Exception {
        try (Skaidb.Connection conn = Skaidb.connect(dsn())) {
            conn.query("SELECT 1");
            assertEquals(1, seen.connections);
            // Drop every server-side socket, then listen again on the same port.
            int port = server.getLocalPort();
            server.close();
            for (Socket s : seen.sockets) s.close();
            Thread.sleep(50);
            assertThrows(Skaidb.SkaidbException.class, () -> conn.query("SELECT 2"));
            assertFalse(conn.isUsable());
            server = listen(port);
            startAcceptor();
            // The next statement re-dials (handshake + Hello again) and succeeds.
            assertEquals(2, conn.query("SELECT 3").getRowCount());
            assertTrue(conn.isUsable());
            assertEquals(2, seen.connections);
        }
    }

    @Test
    void poolHandsOutUsableConnectionsOnly() {
        try (Skaidb.Pool pool = new Skaidb.Pool(dsn(), 2)) {
            String v = pool.withConnection(c -> {
                Skaidb.ResultSet rs = c.query("SELECT name FROM t");
                rs.next();
                return rs.getString("name");
            });
            assertEquals("Ada", v);
            assertEquals(1, seen.connections);
            Skaidb.Connection a = pool.acquire();   // the idle one
            Skaidb.Connection b = pool.acquire();   // a fresh one
            Skaidb.Connection c = pool.acquire();   // maxsize bounds IDLE, not checked-out
            assertEquals(3, seen.connections);
            pool.release(a); pool.release(b); pool.release(c);
            // One of the three was surplus and closed.
            assertFalse(a.isUsable() && b.isUsable() && c.isUsable());
            // A connection mid-stream is never returned to the idle set.
            Skaidb.Connection d = pool.acquire();
            Skaidb.RowStream s = d.stream("SELECT id FROM big");
            pool.release(d);
            assertFalse(d.isUsable());
            s.close();
        }
        assertThrows(Skaidb.SkaidbException.class, () -> new Skaidb.Pool(dsn(), 0));
    }

    @Test
    void unreachableSeedsFailClearly() throws IOException {
        server.close();
        Skaidb.SkaidbException e = assertThrows(Skaidb.SkaidbException.class,
            () -> Skaidb.connect("skaidb://u:p@127.0.0.1:" + server.getLocalPort()));
        assertTrue(e.getMessage().startsWith("no reachable endpoint"), e.getMessage());
    }
}
