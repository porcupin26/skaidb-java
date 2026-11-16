package com.skaidb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * A prepared statement's id is an index on the connection that issued it
 * (PROTOCOL.md §3.3): on another connection it is unknown, or names another
 * statement prepared there since. So the driver never executes under an id
 * it did not get from the socket it executes on.
 */
class PreparedStatementTest {

    static final int QUORUM = Skaidb.CONSISTENCY_QUORUM;
    /** The same parameters, a different effect. */
    static final String UPDATE = "UPDATE t SET v = ? WHERE id = ?";
    static final String DELETE = "DELETE FROM t WHERE v = ? AND id = ?";

    static ConformanceServer.Exchange ex(byte[] request, byte[]... responses) {
        return new ConformanceServer.Exchange(request, List.of(responses));
    }

    static ConformanceServer.Exchange prepare(String sql, int id) {
        return ex(ConformanceServer.prepareRequest(sql), ConformanceServer.preparedResponse(id, 2));
    }

    static ConformanceServer.Exchange execute(int id, Object... params) {
        return ex(ConformanceServer.executeRequest(id, QUORUM, params), ConformanceServer.mutationResponse(1));
    }

    @Test
    void aStatementHeldAcrossAReDialIsPreparedAgainAndRunsAsItself() throws Exception {
        ConformanceServer server = ConformanceServer.sessions(List.of(
            List.of(prepare(UPDATE, 1), execute(1, "u", 1L), prepare(DELETE, 2), execute(2, "d", 2L),
                // the server goes away while the connection sits idle
                ex(ConformanceServer.queryRequest("SELECT 1", QUORUM), ConformanceServer.ddlResponse()).thenHangUp()),
            // On the new connection DELETE comes first and takes the id UPDATE had on the old one.
            List.of(prepare(DELETE, 1), execute(1, "d", 2L), prepare(UPDATE, 2), execute(2, "u", 1L),
                // each is prepared once there: again under its new id, and a batch as well
                execute(2, "u", 1L),
                ex(ConformanceServer.batchRequest(2, QUORUM, new Object[] { "v", 3L }, new Object[] { "w", 4L }),
                    ConformanceServer.mutationResponse(2)))));
        try (Skaidb.Connection c = Skaidb.connect(new Skaidb.ConnectOptions().host("127.0.0.1", server.port())
                .user(ConformanceServer.username()).password(ConformanceServer.password()))) {
            // The statement objects the application keeps, with their values.
            Skaidb.Query update = c.prepare(UPDATE).setString(1, "u").setLong(2, 1);
            Skaidb.Query delete = c.prepare(DELETE).setString(1, "d").setLong(2, 2);
            assertEquals(1, update.executeUpdate());
            assertEquals(1, delete.executeUpdate());

            c.execute("SELECT 1");
            assertTrue(server.awaitEnded(1, 2_000));
            assertTrue(ConformanceServer.awaitSeenClosed(c, 2_000));

            // Through the objects obtained before the loss: DELETE first.
            assertEquals(1, delete.executeUpdate());
            assertEquals(1, update.executeUpdate(), "UPDATE ran as UPDATE, under the id the new connection gave it");
            assertEquals(1, update.executeUpdate());
            assertEquals(2, update.executeBatch(List.of(new Object[] { "v", 3L }, new Object[] { "w", 4L })));
            assertEquals(1, c.reconnects());
        }
        // Byte for byte as scripted: no execute went out under an id of the old connection.
        assertNull(server.finish());
        assertEquals(List.of(), server.closes());
    }

    // ---- two threads on one connection ---------------------------------------------

    /** A server by opcode: ids from 100 on each connection, and a count of every execute that went wrong. */
    static final class Counting implements AutoCloseable {
        final ServerSocket listener = new ServerSocket();
        final List<Socket> sockets = new CopyOnWriteArrayList<>();
        final AtomicLong executes = new AtomicLong();
        /** Executes under an id that names the OTHER statement on the connection they arrived on. */
        final AtomicLong asAnotherStatement = new AtomicLong();
        /** Executes under an id the connection they arrived on never issued. */
        final AtomicLong underAnUnknownId = new AtomicLong();

        Counting() throws IOException {
            listener.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 50);
            Thread acceptor = new Thread(() -> {
                try {
                    while (true) {
                        Socket s = listener.accept();
                        s.setTcpNoDelay(true);
                        sockets.add(s);
                        Thread t = new Thread(() -> serve(s), "counting-server");
                        t.setDaemon(true);
                        t.start();
                    }
                } catch (IOException e) {
                    // closed
                }
            }, "counting-acceptor");
            acceptor.setDaemon(true);
            acceptor.start();
        }

        /** Cut every connection: the client finds out at its next round trip. */
        void cut() {
            for (Socket s : sockets) {
                try { s.close(); } catch (IOException ignored) {}
                sockets.remove(s);
            }
        }

        @Override public void close() throws IOException {
            listener.close();
            cut();
        }

        private static byte[] readFrame(DataInputStream in) throws IOException {
            byte[] b = new byte[in.readInt()];
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

        private void serve(Socket s) {
            try (Socket sock = s) {
                DataInputStream in = new DataInputStream(sock.getInputStream());
                OutputStream out = sock.getOutputStream();
                readFrame(in);   // AuthStart: an empty password skips the signature check
                writeFrame(out, new Skaidb.Buf().u8(11).u32(16).raw(new byte[16]).u32(1).str("srv-nonce").toBytes());
                readFrame(in);
                writeFrame(out, new Skaidb.Buf().u8(13).u8(1).raw(new byte[32]).toBytes());
                int next = 100;
                Map<Integer, String> prepared = new ConcurrentHashMap<>();
                while (true) {
                    Skaidb.Reader r = new Skaidb.Reader(readFrame(in));
                    int op = r.u8();
                    if (op == 2) {
                        int id = next++;
                        prepared.put(id, r.text());
                        writeFrame(out, new Skaidb.Buf().u8(4).u32(id).u16(2).toBytes());
                    } else if (op == 3) {
                        r.u8();
                        int id = r.u32();
                        r.u16();
                        // Each statement's first value names the statement it was bound for.
                        Object boundFor = Skaidb.decodeValue(new Skaidb.Reader(r.blob()));
                        String sql = prepared.get(id);
                        executes.incrementAndGet();
                        if (sql == null) {
                            underAnUnknownId.incrementAndGet();
                            writeFrame(out, new Skaidb.Buf().u8(3).str("unknown prepared statement id " + id).toBytes());
                        } else {
                            if (!sql.equals(boundFor)) asAnotherStatement.incrementAndGet();
                            writeFrame(out, new Skaidb.Buf().u8(1).i64(1).toBytes());
                        }
                    } else {
                        writeFrame(out, new Skaidb.Buf().u8(2).toBytes());
                    }
                }
            } catch (IOException | RuntimeException e) {
                // cut
            }
        }
    }

    @Test
    void threadsSharingAConnectionNeverExecuteUnderAnotherConnectionsId() throws Exception {
        AtomicBoolean stop = new AtomicBoolean();
        try (Counting server = new Counting();
             Skaidb.Connection c = Skaidb.connect("skaidb://anonymous@127.0.0.1:" + server.listener.getLocalPort())) {
            // Two threads, each with its own statement, on the one connection ...
            Thread[] workers = new Thread[2];
            for (int i = 0; i < workers.length; i++) {
                String sql = i == 0 ? UPDATE : DELETE;
                workers[i] = new Thread(() -> {
                    Skaidb.Query q = c.prepare(sql);
                    for (long n = 0; !stop.get(); n++) {
                        try {
                            q.setString(1, sql).setLong(2, n).executeUpdate();
                        } catch (Skaidb.SkaidbException e) {
                            // the connection was cut: the next statement re-dials
                        }
                    }
                }, "worker-" + i);
                workers[i].setDaemon(true);
                workers[i].start();
            }
            // ... which is cut over and over. After each re-dial the two
            // statements are prepared in whichever order the threads arrive,
            // so an id carried over from the connection before names the
            // other statement as often as not.
            // For a second and a half — longer on a slow machine, until
            // the connection has re-dialled a few hundred times.
            long start = System.nanoTime();
            java.util.Random random = new java.util.Random(7);
            for (long ran = 0; ran < 1_500_000_000L || (c.reconnects() < 300 && ran < 10_000_000_000L);
                    ran = System.nanoTime() - start) {
                Thread.sleep(0, 200_000 + random.nextInt(800_000));
                server.cut();
            }
            stop.set(true);
            for (Thread w : workers) w.join(10_000);
            assertTrue(c.reconnects() > 0 && server.executes.get() > 0,
                "nothing was tested: " + c.reconnects() + " re-dials, " + server.executes + " executes");
            assertEquals(0, server.asAnotherStatement.get(), "statements executed as another one, of " + server.executes);
            assertEquals(0, server.underAnUnknownId.get(), "executes under an id the connection never issued");
        }
    }
}
