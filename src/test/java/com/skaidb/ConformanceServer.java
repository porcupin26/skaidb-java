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

    /** One scripted request and the frames sent back for it. */
    public static final class Exchange {
        final byte[] request;
        final List<byte[]> responses;
        public Exchange(byte[] request, List<byte[]> responses) {
            this.request = request;
            this.responses = responses;
        }
    }

    private final ServerSocket listener;
    private final Thread thread;
    private final String outcome;
    private final List<Exchange> pending;
    private final List<byte[]> requests = new CopyOnWriteArrayList<>();
    private volatile String error;
    private volatile String authStartUser;

    /** {@code outcome} is one of auth.outcomes' names: ok, bad_server_signature, denied. */
    public ConformanceServer(String outcome, List<Exchange> exchanges) throws IOException {
        this.outcome = outcome;
        this.pending = new ArrayList<>(exchanges);
        listener = new ServerSocket();
        listener.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 1);
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
        try (Socket s = listener.accept()) {
            serve(new DataInputStream(s.getInputStream()), s.getOutputStream());
        } catch (Exception e) {
            if (error == null) error = e.getClass().getSimpleName() + ": " + e.getMessage();
        }
    }

    private void serve(DataInputStream in, OutputStream out) throws Exception {
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
        while (true) {
            byte[] req = readFrame(in);
            if (req == null) {
                if (!pending.isEmpty()) error = "never received request " + hex(pending.get(0).request);
                return;
            }
            if (req.length > 0 && (req[0] == 4 || req[0] == 8)) {
                writeFrame(out, ddl);
                continue;
            }
            requests.add(req);
            if (pending.isEmpty()) {
                error = "unexpected extra request " + hex(req);
                return;
            }
            Exchange ex = pending.remove(0);
            if (!Arrays.equals(req, ex.request)) {
                error = "request mismatch:\n  got  " + hex(req) + "\n  want " + hex(ex.request);
                return;
            }
            for (byte[] resp : ex.responses) writeFrame(out, resp);
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
