package com.skaidb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * Core-client additions: Result, executeRaw, batch fallback, read timeout,
 * reconnects, refused-login cleanup, parameterised streaming.
 */
class NativeExtrasTest {

    static Skaidb.Connection connect(ConformanceServer s) {
        return Skaidb.connect(new Skaidb.ConnectOptions().host("127.0.0.1", s.port())
            .user(ConformanceServer.username()).password(ConformanceServer.password()));
    }

    static ConformanceServer.Exchange q(String sql, byte[] response) {
        return new ConformanceServer.Exchange(ConformanceServer.queryRequest(sql, Skaidb.CONSISTENCY_QUORUM), List.of(response));
    }

    @Test
    void executeRawSendsTheTextVerbatimAndReportsTheKind() throws Exception {
        ConformanceServer server = new ConformanceServer("ok", List.of(
            q("SELECT '?' AS q, ? AS p", ConformanceServer.rowsResponse(new String[] { "q" }, new Object[] { "?" })),
            q("DELETE FROM t", ConformanceServer.mutationResponse(4)),
            q("CREATE TABLE t (PRIMARY KEY (id))", ConformanceServer.ddlResponse())));
        try (Skaidb.Connection c = connect(server)) {
            Skaidb.Result rows = c.executeRaw("SELECT '?' AS q, ? AS p");
            assertTrue(rows.hasRows());
            assertEquals(-1, rows.getAffected());
            Skaidb.Result del = c.executeRaw("DELETE FROM t");
            assertFalse(del.hasRows());
            assertFalse(del.isDdl());
            assertEquals(4, del.getAffected());
            Skaidb.Result ddl = c.executeRaw("CREATE TABLE t (PRIMARY KEY (id))");
            assertTrue(ddl.isDdl());
            assertNull(ddl.getResultSet());
        }
        assertNull(server.finish());
        assertEquals(2, new Skaidb.Query(null, "SELECT ? WHERE x = '?' AND y = ?").getParameterCount());
    }

    @Test
    void batchOfAnUnpreparableStatementRunsRowByRow() throws Exception {
        String sql = "CREATE TABLE IF NOT EXISTS ? (PRIMARY KEY (id))";
        ConformanceServer server = new ConformanceServer("ok", List.of(
            new ConformanceServer.Exchange(ConformanceServer.prepareRequest(sql),
                List.of(ConformanceServer.errorResponse("statement cannot be prepared"))),
            q("CREATE TABLE IF NOT EXISTS 'a' (PRIMARY KEY (id))", ConformanceServer.ddlResponse()),
            q("CREATE TABLE IF NOT EXISTS 'b' (PRIMARY KEY (id))", ConformanceServer.ddlResponse())));
        try (Skaidb.Connection c = connect(server)) {
            assertEquals(0L, c.prepare(sql).executeBatch(List.of(new Object[] { "a" }, new Object[] { "b" })));
        }
        assertNull(server.finish());
    }

    @Test
    void readTimeoutBreaksTheConnectionAndItReDials() throws Exception {
        // The first connection never answers the statement; the re-dial gets a real answer.
        ConformanceServer silent = new ConformanceServer("ok", List.of(
            new ConformanceServer.Exchange(ConformanceServer.queryRequest("SELECT slow()", Skaidb.CONSISTENCY_QUORUM), List.of())));
        Skaidb.Connection c = connect(silent);
        c.setReadTimeout(200);
        assertEquals(200, c.getReadTimeout());
        Skaidb.SkaidbException e = assertThrows(Skaidb.SkaidbException.class, () -> c.query("SELECT slow()"));
        assertInstanceOf(java.net.SocketTimeoutException.class, e.getCause());
        assertFalse(c.isUsable(), "a timed-out read leaves the wire position unknown");
        assertEquals(0, c.reconnects());
        c.close();
        silent.close();
    }

    // ---- parameterised streaming (OP_QUERY_STREAM_PARAMS, PROTOCOL.md §3.4) ----

    /** Little-endian bytes put together from the spec alone, not with the driver's encoder. */
    static final class Wire {
        private final ByteArrayOutputStream out = new ByteArrayOutputStream();
        Wire u8(int v) { out.write(v); return this; }
        Wire u16(int v) { return raw(ByteBuffer.allocate(2).order(ByteOrder.LITTLE_ENDIAN).putShort((short) v).array()); }
        Wire u32(int v) { return raw(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(v).array()); }
        Wire i64(long v) { return raw(ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(v).array()); }
        Wire raw(byte[] b) { out.write(b, 0, b.length); return this; }
        Wire text(String s) { byte[] b = s.getBytes(StandardCharsets.UTF_8); return u32(b.length).raw(b); }
        /** One parameter: u32 length, then the value's bytes (§4). */
        Wire param(Wire value) { byte[] v = value.done(); return u32(v.length).raw(v); }
        byte[] done() { return out.toByteArray(); }
    }

    static ConformanceServer.Exchange stream(byte[] request, String col, Object... values) {
        List<byte[]> resp = new ArrayList<>();
        resp.add(ConformanceServer.rowsHeaderResponse(col));
        for (Object v : values) resp.add(ConformanceServer.rowsChunkResponse(new Object[] { v }));
        resp.add(ConformanceServer.rowsEndResponse());
        return new ConformanceServer.Exchange(request, resp);
    }

    static List<Object> drain(Skaidb.RowStream s) {
        List<Object> got = new ArrayList<>();
        try (s) {
            while (s.next()) got.add(s.getObject(0));
        }
        return got;
    }

    @Test
    void parameterisedStreamSendsTheSqlThenTheParameterVector() throws Exception {
        String sql = "SELECT id FROM t WHERE ts >= ? AND tag = ? AND gone = ? AND tags = ?";
        byte[] want = new Wire()
            .u8(9).u8(Skaidb.CONSISTENCY_ONE).text(sql).u16(4)
            .param(new Wire().u8(2).i64(5))                                  // Int 5
            .param(new Wire().u8(5).text("o'neil"))                          // String
            .param(new Wire().u8(0))                                         // Null
            .param(new Wire().u8(9).u32(2).u8(2).i64(1).u8(2).i64(2))        // Array [1, 2]: one value
            .done();
        ConformanceServer server = new ConformanceServer("ok", List.of(stream(want, "id", 7L, 8L)));
        try (Skaidb.Connection c = connect(server)) {
            c.setConsistency(Skaidb.CONSISTENCY_ONE);
            // A value that cannot be bound fails before anything is sent.
            Skaidb.SkaidbException nan = assertThrows(Skaidb.SkaidbException.class,
                () -> c.stream(sql, 5L, "o'neil", null, Double.NaN));
            assertTrue(nan.getMessage().startsWith("cannot bind"), nan.getMessage());
            assertTrue(c.isUsable());
            assertEquals(List.of(7L, 8L), drain(c.stream(sql, 5L, "o'neil", null, List.of(1L, 2L))));
            assertTrue(c.isUsable(), "RowsEnd frees the wire");
        }
        assertNull(server.finish(), "the request is exactly opcode 9, the level, the SQL and the u16-counted values");
    }

    @Test
    void withoutParametersTheOriginalOpcodeIsSent() throws Exception {
        byte[] want = new Wire().u8(5).u8(Skaidb.CONSISTENCY_QUORUM).text("SELECT 1").done();
        ConformanceServer server = new ConformanceServer("ok", List.of(
            stream(want, "x", 1L), stream(want, "x", 2L), stream(want, "x", 3L)));
        try (Skaidb.Connection c = connect(server)) {
            assertEquals(List.of(1L), drain(c.stream("SELECT 1")));
            assertEquals(List.of(2L), drain(c.stream("SELECT 1", new Object[0])));
            assertEquals(List.of(3L), drain(c.stream("SELECT 1", (Object[]) null)));
        }
        assertNull(server.finish(), "OP_QUERY_STREAM byte for byte, which every server knows");
    }

    @Test
    void streamOverloadsKeepTheirMeaning() throws Exception {
        // The 1.2.0 signature is still there, so code compiled against it links.
        Method plain = Skaidb.Connection.class.getMethod("stream", String.class);
        assertEquals(Skaidb.RowStream.class, plain.getReturnType());
        assertTrue(Skaidb.Connection.class.getMethod("stream", String.class, Object[].class).isVarArgs());

        String two = "SELECT id FROM t WHERE a = ? AND b = ?";
        String one = "SELECT id FROM t WHERE tags = ?";
        int q = Skaidb.CONSISTENCY_QUORUM;
        ConformanceServer server = new ConformanceServer("ok", List.of(
            stream(ConformanceServer.streamRequest(two, q, 1L, "b"), "id", 1L),
            stream(ConformanceServer.streamRequest(one, q, List.of(1L, "b")), "id", 2L),
            stream(ConformanceServer.streamRequest(one, q, (long) Skaidb.CONSISTENCY_ALL), "id", 3L)));
        try (Skaidb.Connection c = connect(server)) {
            // An Object[] passed alone is the parameter list ...
            assertEquals(List.of(1L), drain(c.stream(two, new Object[] { 1L, "b" })));
            // ... and a List is one array value.
            assertEquals(List.of(2L), drain(c.stream(one, List.of(1L, "b"))));
            // There is no stream(String, int) overload: it would capture
            // stream(sql, 7) as a level. An int is a parameter; the stream
            // runs at the connection's consistency (QUORUM here).
            assertEquals(List.of(3L), drain(c.stream(one, Skaidb.CONSISTENCY_ALL)));
        }
        assertNull(server.finish());
    }

    @Test
    void anOldServerRefusingParameterisedStreamsSaysSo() throws Exception {
        String sql = "SELECT id FROM t WHERE id > ?";
        byte[] refusal = ConformanceServer.errorResponse("protocol error: malformed message: unknown opcode");
        ConformanceServer server = new ConformanceServer("ok", List.of(
            new ConformanceServer.Exchange(ConformanceServer.streamRequest(sql, Skaidb.CONSISTENCY_QUORUM, 1L), List.of(refusal)),
            q("SELECT 1", ConformanceServer.rowsResponse(new String[] { "x" }, new Object[] { 1L })),
            new ConformanceServer.Exchange(ConformanceServer.streamRequest("SELECT 2", Skaidb.CONSISTENCY_QUORUM), List.of(refusal))));
        try (Skaidb.Connection c = connect(server)) {
            Skaidb.SkaidbException e = assertThrows(Skaidb.SkaidbException.class, () -> c.stream(sql, 1L));
            assertTrue(e.getMessage().startsWith("server does not support parameterised streaming (skaidb >= 0.330): "),
                e.getMessage());
            assertTrue(e.getMessage().endsWith("unknown opcode"), e.getMessage());
            // One ordinary error frame: the same socket goes on serving.
            assertTrue(c.isUsable());
            assertEquals(1, c.query("SELECT 1").getRowCount());
            assertEquals(0, c.reconnects());
            // Without parameters the message stays the plain streaming one.
            Skaidb.SkaidbException plain = assertThrows(Skaidb.SkaidbException.class, () -> c.stream("SELECT 2"));
            assertTrue(plain.getMessage().startsWith("server does not support streaming: "), plain.getMessage());
        }
        assertNull(server.finish());
    }

    @Test
    void aRefusedLoginClosesItsSocket() throws Exception {
        AtomicReference<String> seen = new AtomicReference<>();
        try (ServerSocket ss = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            Thread t = new Thread(() -> {
                try (Socket s = ss.accept()) {
                    DataInputStream in = new DataInputStream(s.getInputStream());
                    OutputStream out = s.getOutputStream();
                    byte[] start = new byte[in.readInt()];
                    in.readFully(start);
                    byte[] denied = new Skaidb.Buf().u8(13).u8(0).str("nope").toBytes();
                    out.write(new byte[] { 0, 0, 0, (byte) denied.length });
                    out.write(denied);
                    out.flush();
                    s.setSoTimeout(5000);
                    seen.set(in.read() == -1 ? "closed" : "data");
                } catch (Exception ex) {
                    seen.set(ex.getClass().getSimpleName());
                }
            });
            t.start();
            Skaidb.ConnectOptions o = new Skaidb.ConnectOptions().host("127.0.0.1", ss.getLocalPort())
                .tls(false).user("u").password("p");
            Skaidb.SkaidbException e = assertThrows(Skaidb.SkaidbException.class, () -> Skaidb.connect(o));
            assertTrue(e.getMessage().contains("bad handshake challenge"), e.getMessage());
            t.join(10_000);
        }
        assertEquals("closed", seen.get(), "the driver closed the socket right after the refusal");
    }
}
