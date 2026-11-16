package com.skaidb;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.crypto.Mac;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * The scripted fake server of conformance/README.md: one connection, the
 * SCRAM handshake verified independently of the driver (JCE PBKDF2 + HMAC,
 * never the driver's own code), then the given exchanges replayed byte for
 * byte. Any deviation is kept in {@link #error()}.
 *
 * <p>{@link #sessions} scripts several connections, served one after the
 * other — what a driver that re-dials talks to — and an exchange {@link
 * Exchange#thenHangUp() can end} its connection from the server's side.
 */
public final class ConformanceServer implements AutoCloseable {

    /** conformance/vectors.json, parsed once. */
    public static final Map<String, Object> V = load();

    private static Map<String, Object> load() {
        try {
            return Json.obj(Json.parse(new String(
                Files.readAllBytes(Paths.get("conformance", "vectors.json")), StandardCharsets.UTF_8)));
        } catch (IOException e) {
            throw new IllegalStateException("cannot read conformance/vectors.json", e);
        }
    }

    /** How a scripted connection ends from the server's side, if it does. */
    private enum End { NONE, HANG_UP, HALF_CLOSE, RESET }

    /** One scripted request and the frames sent back for it. */
    public static final class Exchange {
        /** Null for {@link ConformanceServer#idle()}: nothing is asked. */
        final byte[] request;
        final List<byte[]> responses;
        /** What the server does to the socket once the responses are out. */
        final End end;
        public Exchange(byte[] request, List<byte[]> responses) {
            this(request, responses, End.NONE);
        }
        private Exchange(byte[] request, List<byte[]> responses, End end) {
            this.request = request;
            this.responses = responses;
            this.end = end;
        }
        /**
         * This exchange, after which the server closes the socket: the
         * connection is lost — under this request when it has no responses,
         * otherwise while the client sits idle after them. The last
         * exchange of its connection's script.
         */
        public Exchange thenHangUp() { return new Exchange(request, responses, End.HANG_UP); }
        /**
         * This exchange, after which the server shuts down only the
         * SENDING side of the socket and goes on reading — and counting,
         * see {@link ConformanceServer#bytesAfterHalfClose} — whatever
         * still arrives, until the client closes its side. The client
         * reads the end of the stream while its writes still succeed: what
         * a connection closed while it sat idle looks like over a real
         * network, where the reset for a write comes back too late to be
         * seen. The last exchange of its connection's script.
         */
        public Exchange thenHalfClose() { return new Exchange(request, responses, End.HALF_CLOSE); }
        /**
         * This exchange, after which the server resets the connection
         * (closes it with {@code SO_LINGER} 0) instead of closing it
         * cleanly. The last exchange of its connection's script.
         */
        public Exchange thenReset() { return new Exchange(request, responses, End.RESET); }
    }

    /**
     * No request at all: the connection sits idle after its handshake and
     * Hello. Followed by {@link Exchange#thenHangUp}, {@link
     * Exchange#thenHalfClose} or {@link Exchange#thenReset}, the server
     * ends the connection there, with nothing asked of it.
     */
    public static Exchange idle() { return new Exchange(null, List.of()); }

    private final ServerSocket listener;
    private final Thread thread;
    private final String outcome;
    /** One script per connection, in the order the connections are accepted. */
    private final List<List<Exchange>> scripts;
    private final List<byte[]> requests = new CopyOnWriteArrayList<>();
    /** The requests of each accepted connection. */
    private final List<List<byte[]>> received = new CopyOnWriteArrayList<>();
    /** The statement ids OP_CLOSE named. */
    private final List<Integer> closes = new CopyOnWriteArrayList<>();
    /** The bytes each accepted connection sent after the server half-closed it. */
    private final List<java.util.concurrent.atomic.AtomicLong> afterHalfClose = new CopyOnWriteArrayList<>();
    /** Connections that have ended; guarded by this. */
    private int ended = 0;
    private volatile String error;
    private volatile String authStartUser;

    /** {@code outcome} is one of auth.outcomes' names: ok, bad_server_signature, denied. */
    public ConformanceServer(String outcome, List<Exchange> exchanges) throws IOException {
        this(outcome, List.of(exchanges), 1);
    }

    /**
     * A server for several connections, one after the other, each replaying
     * the script of its turn; every login is accepted. A connection ends
     * when the client closes its socket or its script hangs up, and once
     * the last one has ended nothing listens any more.
     */
    public static ConformanceServer sessions(List<List<Exchange>> scripts) throws IOException {
        return new ConformanceServer("ok", scripts, scripts.size());
    }

    /**
     * {@link #sessions} behind TLS: the listener presents the test server
     * certificate of {@code src/test/resources/tls} (it carries {@code
     * DNS:skaidb}; trust {@code ca.pem}) and asks for no client
     * certificate. The login is the SCRAM one, inside the TLS session.
     */
    public static ConformanceServer tlsSessions(List<List<Exchange>> scripts) throws Exception {
        javax.net.ssl.SSLContext ctx = javax.net.ssl.SSLContext.getInstance("TLS");
        ctx.init(Skaidb.clientKeyManagers(tls("server.pem"), tls("server.key")), null, null);
        return new ConformanceServer("ok", scripts, scripts.size(), ctx.getServerSocketFactory().createServerSocket());
    }

    /** A file of the throwaway test CA under {@code src/test/resources/tls}. */
    public static String tls(String name) { return Paths.get("src", "test", "resources", "tls", name).toString(); }

    private ConformanceServer(String outcome, List<List<Exchange>> scripts, int backlog) throws IOException {
        this(outcome, scripts, backlog, new ServerSocket());
    }

    private ConformanceServer(String outcome, List<List<Exchange>> scripts, int backlog, ServerSocket unbound)
            throws IOException {
        this.outcome = outcome;
        this.scripts = new ArrayList<>(scripts);
        listener = unbound;
        listener.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), backlog);
        thread = new Thread(this::run, "conformance-server");
        thread.setDaemon(true);
        thread.start();
    }

    /** The exchanges of one vectors.json case. */
    public static List<Exchange> exchangesOf(Map<String, Object> testCase) {
        List<Exchange> out = new ArrayList<>();
        for (Object e : Json.arr(testCase.get("exchanges"))) {
            Map<String, Object> ex = Json.obj(e);
            List<byte[]> resp = new ArrayList<>();
            for (Object r : Json.arr(ex.get("responses"))) resp.add(unhex((String) r));
            out.add(new Exchange(unhex((String) ex.get("request")), resp));
        }
        return out;
    }

    /** The vectors.json case with this name. */
    public static Map<String, Object> caseNamed(String name) {
        for (Object c : Json.arr(V.get("cases"))) {
            if (name.equals(Json.obj(c).get("name"))) return Json.obj(c);
        }
        throw new IllegalArgumentException("no case " + name);
    }

    public int port() { return listener.getLocalPort(); }
    public static String username() { return (String) auth().get("username"); }
    public static String password() { return (String) auth().get("password"); }
    /** Null when the script ran exactly as written. */
    public String error() { return error; }
    /** The non-ignorable requests received, in order. */
    public List<byte[]> requests() { return requests; }
    /**
     * The prepared-statement ids the client closed (OP_CLOSE), in order. An
     * OP_CLOSE may arrive at any point and is answered, never scripted.
     */
    public List<Integer> closes() { return closes; }
    /** How many connections were accepted. */
    public int connections() { return received.size(); }
    /** The non-ignorable requests connection {@code n} (the first is 0) sent, in order. */
    public List<byte[]> requests(int n) { return received.get(n); }
    /**
     * How many bytes connection {@code n} (the first is 0) sent after the
     * server half-closed it ({@link Exchange#thenHalfClose}): 0 when the
     * client wrote nothing more to it.
     */
    public long bytesAfterHalfClose(int n) { return afterHalfClose.get(n).get(); }

    /**
     * Wait until {@code n} connections have ended — the client closed its
     * socket, or the script hung up. False when {@code millis} pass first.
     */
    public synchronized boolean awaitEnded(int n, long millis) throws InterruptedException {
        long deadline = System.nanoTime() + millis * 1_000_000L;
        while (ended < n) {
            long left = (deadline - System.nanoTime()) / 1_000_000L;
            if (left <= 0) return false;
            wait(left);
        }
        return true;
    }
    /**
     * Wait until {@code c} can see that the server ended its connection —
     * the close has arrived at the client's socket — as the driver itself
     * finds out before its next statement. False when {@code millis} pass
     * first. The connection is then marked broken, as that statement would
     * have marked it; it re-dials when it is used.
     */
    public static boolean awaitSeenClosed(Skaidb.Connection c, long millis) throws InterruptedException {
        long deadline = System.nanoTime() + millis * 1_000_000L;
        while (c.stillUsable()) {
            if (System.nanoTime() - deadline > 0) return false;
            Thread.sleep(1);
        }
        return true;
    }

    /** The username the client put in AuthStart. */
    public String authStartUser() { return authStartUser; }

    /** Wait for the connection to end (the client closed it) and stop listening. */
    public String finish() throws InterruptedException {
        thread.join(10_000);
        if (thread.isAlive()) error = error != null ? error : "fake server still running after 10 s";
        close();
        return error;
    }

    @Override public void close() {
        try { listener.close(); } catch (IOException ignored) {}
    }

    private static Map<String, Object> auth() { return Json.obj(V.get("auth")); }

    private void run() {
        try {
            for (List<Exchange> script : scripts) {
                try (Socket s = listener.accept()) {
                    s.setTcpNoDelay(true);   // a frame is two writes: do not wait for an ACK between them
                    List<byte[]> got = new CopyOnWriteArrayList<>();
                    afterHalfClose.add(new java.util.concurrent.atomic.AtomicLong());
                    received.add(got);
                    serve(s, new DataInputStream(s.getInputStream()), s.getOutputStream(), new ArrayList<>(script), got);
                } catch (Exception e) {
                    if (error == null) error = e.getClass().getSimpleName() + ": " + e.getMessage();
                }
                synchronized (this) {
                    ended++;
                    notifyAll();
                }
                if (error != null) return;
            }
        } finally {
            // Nothing is scripted past this point: a further dial is refused
            // at once instead of waiting for a handshake that never comes.
            close();
        }
    }

    /**
     * End the connection as {@code ex} says; true when it did. A
     * half-closed connection is read to its end here, every byte counted.
     */
    private boolean end(Socket s, DataInputStream in, Exchange ex, List<Exchange> pending) throws IOException {
        if (ex.end == End.NONE) return false;
        if (!pending.isEmpty()) error = "the script goes on after the server ended the connection";
        if (ex.end == End.RESET) {
            s.setSoLinger(true, 0);
        } else if (ex.end == End.HALF_CLOSE) {
            s.shutdownOutput();
            java.util.concurrent.atomic.AtomicLong count = afterHalfClose.get(afterHalfClose.size() - 1);
            byte[] sink = new byte[4096];
            try {
                for (int n; (n = in.read(sink)) > 0; ) count.addAndGet(n);
            } catch (IOException e) {
                // the client reset the socket instead of closing it: nothing more arrives either way
            }
        }
        return true;
    }

    private void serve(Socket s, DataInputStream in, OutputStream out, List<Exchange> pending, List<byte[]> got)
            throws Exception {
        Map<String, Object> challenge = Json.obj(auth().get("challenge"));
        byte[] start = readFrame(in);
        if (start == null || start[0] != 10) throw new IllegalStateException("expected AuthStart");
        Skaidb.Reader r = new Skaidb.Reader(Arrays.copyOfRange(start, 1, start.length));
        byte[] user = r.blob();
        byte[] clientNonce = r.blob();
        authStartUser = new String(user, StandardCharsets.UTF_8);
        byte[] salt = unhex((String) challenge.get("salt"));
        int iterations = ((Long) challenge.get("iterations")).intValue();
        byte[] suffix = ((String) challenge.get("server_nonce_suffix")).getBytes(StandardCharsets.UTF_8);
        byte[] serverNonce = concat(clientNonce, suffix);
        writeFrame(out, new Skaidb.Buf().u8(11).u32(salt.length).raw(salt).u32(iterations)
            .u32(serverNonce.length).raw(serverNonce).toBytes());

        byte[] finish = readFrame(in);
        if (finish == null || finish[0] != 12) throw new IllegalStateException("expected AuthFinish");
        byte[] am = String.join("\0",
                new String(user, StandardCharsets.UTF_8), new String(clientNonce, StandardCharsets.UTF_8),
                new String(serverNonce, StandardCharsets.UTF_8), hex(salt), Integer.toString(iterations))
            .getBytes(StandardCharsets.UTF_8);
        SecretKeyFactory f = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
        byte[] salted = f.generateSecret(new PBEKeySpec(password().toCharArray(), salt, iterations, 256)).getEncoded();
        byte[] clientKey = hmac(salted, "Client Key".getBytes(StandardCharsets.UTF_8));
        byte[] clientSig = hmac(MessageDigest.getInstance("SHA-256").digest(clientKey), am);
        byte[] proof = new byte[32];
        for (int i = 0; i < 32; i++) proof[i] = (byte) (clientKey[i] ^ clientSig[i]);
        if (!Arrays.equals(Arrays.copyOfRange(finish, 1, finish.length), proof))
            throw new IllegalStateException("client proof did not verify");
        byte[] serverSig = hmac(hmac(salted, "Server Key".getBytes(StandardCharsets.UTF_8)), am);

        if (outcome.equals("ok")) {
            writeFrame(out, concat(new byte[] { 13, 1 }, serverSig));
        } else if (outcome.equals("bad_server_signature")) {
            byte[] bad = new byte[32];
            Arrays.fill(bad, (byte) 0xaa);
            writeFrame(out, concat(new byte[] { 13, 1 }, bad));
            return;
        } else {
            for (Object o : Json.arr(auth().get("outcomes"))) {
                if (outcome.equals(Json.obj(o).get("name")))
                    writeFrame(out, unhex((String) Json.obj(o).get("payload")));
            }
            return;
        }

        byte[] ddl = unhex((String) Json.obj(V.get("ignorable_requests")).get("ddl_payload"));
        boolean greeted = false;
        while (true) {
            // idle(): nothing is asked — the connection is ended as it sits
            // there, once its Hello has been answered.
            if (greeted && !pending.isEmpty() && pending.get(0).request == null) {
                if (end(s, in, pending.remove(0), pending)) return;
                continue;
            }
            byte[] req = readFrame(in);
            if (req == null) {
                if (!pending.isEmpty() && pending.get(0).request != null)
                    error = "never received request " + hex(pending.get(0).request);
                return;
            }
            if (req.length > 0 && (req[0] == 4 || req[0] == 8)) {
                if (req[0] == 4) closes.add(new Skaidb.Reader(Arrays.copyOfRange(req, 1, req.length)).u32());
                writeFrame(out, ddl);
                greeted |= req[0] == 8;
                continue;
            }
            requests.add(req);
            got.add(req);
            if (pending.isEmpty()) {
                error = "unexpected extra request " + hex(req);
                return;
            }
            Exchange ex = pending.remove(0);
            if (ex.request == null || !Arrays.equals(req, ex.request)) {
                error = "request mismatch:\n  got  " + hex(req) + "\n  want "
                    + (ex.request == null ? "nothing: the connection sits idle" : hex(ex.request));
                return;
            }
            for (byte[] resp : ex.responses) writeFrame(out, resp);
            if (end(s, in, ex, pending)) return;
        }
    }

    // ---- frames and bytes, independent of the driver ------------------------

    private static byte[] readFrame(DataInputStream in) throws IOException {
        int len;
        try {
            len = in.readInt();
        } catch (EOFException e) {
            return null;
        }
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

    private static byte[] hmac(byte[] key, byte[] msg) throws Exception {
        Mac m = Mac.getInstance("HmacSHA256");
        m.init(new SecretKeySpec(key, "HmacSHA256"));
        return m.doFinal(msg);
    }

    static byte[] concat(byte[] a, byte[] b) {
        byte[] o = Arrays.copyOf(a, a.length + b.length);
        System.arraycopy(b, 0, o, a.length, b.length);
        return o;
    }

    public static byte[] unhex(String h) {
        byte[] b = new byte[h.length() / 2];
        for (int i = 0; i < b.length; i++) b[i] = (byte) Integer.parseInt(h.substring(2 * i, 2 * i + 2), 16);
        return b;
    }

    public static String hex(byte[] b) {
        StringBuilder sb = new StringBuilder();
        for (byte x : b) sb.append(String.format("%02x", x & 0xff));
        return sb.toString();
    }

    // ---- building extra scripted frames (for tests beyond the vectors) --------

    /** OP_QUERY request bytes (§3.1). */
    public static byte[] queryRequest(String sql, int level) {
        byte[] body = sql.getBytes(StandardCharsets.UTF_8);
        return new Skaidb.Buf().u8(1).u8(level).u32(body.length).raw(body).toBytes();
    }

    /** OP_PREPARE request bytes (§3.3). */
    public static byte[] prepareRequest(String sql) {
        byte[] body = sql.getBytes(StandardCharsets.UTF_8);
        return new Skaidb.Buf().u8(2).u32(body.length).raw(body).toBytes();
    }

    /** OP_EXECUTE request bytes for these Java values (§3.3). */
    public static byte[] executeRequest(int stmtId, int level, Object... params) {
        Skaidb.Buf b = new Skaidb.Buf().u8(3).u8(level).u32(stmtId).u16(params.length);
        for (Object p : params) {
            byte[] v = Skaidb.encodeValue(p);
            b.u32(v.length).raw(v);
        }
        return b.toBytes();
    }

    /** OP_EXECUTE_BATCH request bytes for these rows of Java values (§3.3). */
    public static byte[] batchRequest(int stmtId, int level, Object[]... rows) {
        Skaidb.Buf b = new Skaidb.Buf().u8(7).u8(level).u32(stmtId).u32(rows.length);
        for (Object[] row : rows) {
            b.u16(row.length);
            for (Object p : row) {
                byte[] v = Skaidb.encodeValue(p);
                b.u32(v.length).raw(v);
            }
        }
        return b.toBytes();
    }

    /**
     * A streaming request (§3.4): {@code OP_QUERY_STREAM} without parameters,
     * {@code OP_QUERY_STREAM_PARAMS} with these Java values.
     */
    public static byte[] streamRequest(String sql, int level, Object... params) {
        byte[] body = sql.getBytes(StandardCharsets.UTF_8);
        if (params.length == 0) return new Skaidb.Buf().u8(5).u8(level).u32(body.length).raw(body).toBytes();
        Skaidb.Buf b = new Skaidb.Buf().u8(9).u8(level).u32(body.length).raw(body).u16(params.length);
        for (Object p : params) {
            byte[] v = Skaidb.encodeValue(p);
            b.u32(v.length).raw(v);
        }
        return b.toBytes();
    }

    /** A RowsHeader response: a stream's columns. */
    public static byte[] rowsHeaderResponse(String... cols) {
        Skaidb.Buf b = new Skaidb.Buf().u8(5).u32(cols.length);
        for (String c : cols) b.str(c);
        return b.toBytes();
    }

    /** A RowsChunk response with these Java values. */
    public static byte[] rowsChunkResponse(Object[]... rows) {
        Skaidb.Buf b = new Skaidb.Buf().u8(6).u32(rows.length);
        for (Object[] row : rows) {
            b.u32(row.length);
            for (Object v : row) {
                byte[] enc = Skaidb.encodeValue(v);
                b.u32(enc.length).raw(enc);
            }
        }
        return b.toBytes();
    }

    /** A RowsEnd response. */
    public static byte[] rowsEndResponse() { return new byte[] { 7 }; }

    /** A Prepared response. */
    public static byte[] preparedResponse(int stmtId, int nparams) {
        return new Skaidb.Buf().u8(4).u32(stmtId).u16(nparams).toBytes();
    }

    /** A Rows response with these Java values. */
    public static byte[] rowsResponse(String[] cols, Object[]... rows) {
        Skaidb.Buf b = new Skaidb.Buf().u8(0).u32(cols.length);
        for (String c : cols) b.str(c);
        b.u32(rows.length);
        for (Object[] row : rows) {
            b.u32(row.length);
            for (Object v : row) {
                byte[] enc = Skaidb.encodeValue(v);
                b.u32(enc.length).raw(enc);
            }
        }
        return b.toBytes();
    }

    /**
     * A ResultSets response (a CALL whose body EMITted): the set count, then
     * each set laid out as a Rows body — these Rows responses minus their tag.
     */
    public static byte[] resultSetsResponse(byte[]... rowsResponses) {
        Skaidb.Buf b = new Skaidb.Buf().u8(8).u32(rowsResponses.length);
        for (byte[] rows : rowsResponses) b.raw(Arrays.copyOfRange(rows, 1, rows.length));
        return b.toBytes();
    }

    /** A Mutation response. */
    public static byte[] mutationResponse(long affected) {
        return new Skaidb.Buf().u8(1).i64(affected).toBytes();
    }

    /** A Ddl response. */
    public static byte[] ddlResponse() { return new byte[] { 2 }; }

    /** An Error response. */
    public static byte[] errorResponse(String message) {
        return new Skaidb.Buf().u8(3).str(message).toBytes();
    }
}
