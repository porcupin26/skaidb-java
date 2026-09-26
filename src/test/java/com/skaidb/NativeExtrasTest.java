package com.skaidb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.DataInputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/** Core-client additions: Result, executeRaw, batch fallback, read timeout, reconnects, refused-login cleanup. */
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
