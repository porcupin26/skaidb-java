package com.skaidb;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.List;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;
import org.junit.jupiter.api.Test;

/**
 * Certificate login (wire mechanism EXTERNAL, PROTOCOL.md §2.4) and client
 * certificates, against a real TLS listener that requires a client
 * certificate. The PEM files under src/test/resources/tls are a throwaway
 * test CA: server cert with DNS:skaidb, client certs with CN=app.
 */
class CertificateAuthTest {

    static String res(String name) { return Paths.get("src", "test", "resources", "tls", name).toString(); }

    // ---- frame builder and options -----------------------------------------

    @Test
    void externalAuthStartBytes() {
        assertEquals("0a" + "00000000" + "00000000" + "02", ConformanceServer.hex(Skaidb.externalAuthStart("")));
        assertEquals("0a" + "03000000" + "617070" + "00000000" + "02",
            ConformanceServer.hex(Skaidb.externalAuthStart("app")));
    }

    @Test
    void certificateLoginNeedsAClientCertificate() {
        Skaidb.SkaidbException e = assertThrows(Skaidb.SkaidbException.class,
            () -> Skaidb.parseDsn("skaidb://h/?auth_mechanism=certificate&tls=true"));
        assertTrue(e.getMessage().contains("client certificate"), e.getMessage());
        assertThrows(Skaidb.SkaidbException.class,
            () -> Skaidb.parseDsn("skaidb://h/?tls_client_cert=/a.pem"));
        assertThrows(Skaidb.SkaidbException.class,
            () -> Skaidb.parseDsn("skaidb://h/?auth_mechanism=kerberos"));
        assertThrows(Skaidb.SkaidbException.class,
            () -> Skaidb.connect(new Skaidb.ConnectOptions().host("h", 1).authMechanism("certificate")));
    }

    @Test
    void dsnWithAClientCertificate() {
        Skaidb.ConnectOptions o = Skaidb.parseDsn(
            "skaidb://db1/app?tls_ca=/ca.pem&tls_client_cert=/c.pem&tls_client_key=/c.key&auth_mechanism=certificate");
        assertTrue(o.tls, "a client certificate implies TLS");
        assertEquals("/c.pem", o.tlsClientCert);
        assertEquals("/c.key", o.tlsClientKey);
        assertEquals(Skaidb.AUTH_CERTIFICATE, o.authMechanism);
        assertEquals("", o.user, "no user given: the certificate names it");
        assertEquals("app", Skaidb.parseDsn(
            "skaidb://app@db1/?tls_client_cert=/c.pem&tls_client_key=/c.key&auth_mechanism=certificate").user);
        // A certificate alone opens TLS; the login stays SCRAM.
        Skaidb.ConnectOptions scram = Skaidb.parseDsn("skaidb://u:p@db1/?tls_client_cert=/c.pem&tls_client_key=/c.key");
        assertEquals(Skaidb.AUTH_SCRAM, scram.authMechanism);
        assertEquals("u", scram.user);
    }

    @Test
    void privateKeyFormats() throws Exception {
        PrivateKey pk8 = Skaidb.loadPrivateKey(res("client.key"));
        PrivateKey pk1 = Skaidb.loadPrivateKey(res("client-pkcs1.key"));
        assertEquals("RSA", pk8.getAlgorithm());
        assertArrayEquals(pk8.getEncoded(), pk1.getEncoded(), "PKCS#1 wraps to the same PKCS#8 key");
        assertEquals("EC", Skaidb.loadPrivateKey(res("client-ec.key")).getAlgorithm());
        Skaidb.SkaidbException e = assertThrows(Skaidb.SkaidbException.class,
            () -> Skaidb.loadPrivateKey(res("ec-sec1.key")));
        assertTrue(e.getMessage().contains("openssl pkcs8"), e.getMessage());
        assertThrows(Skaidb.SkaidbException.class, () -> Skaidb.loadPrivateKey(res("ca.pem")));
    }

    // ---- end to end over TLS ---------------------------------------------------

    /** A TLS listener requiring a client certificate that answers EXTERNAL as scripted. */
    static final class TlsServer implements AutoCloseable {
        final SSLServerSocket listener;
        final Thread thread;
        volatile String peerCn;
        volatile byte[] authStart;
        volatile String error;

        TlsServer(byte[] outcome) throws Exception {
            SSLContext ctx = SSLContext.getInstance("TLS");
            KeyStore trust = KeyStore.getInstance(KeyStore.getDefaultType());
            trust.load(null, null);
            try (InputStream in = Files.newInputStream(Paths.get(res("ca.pem")))) {
                trust.setCertificateEntry("ca", CertificateFactory.getInstance("X.509").generateCertificate(in));
            }
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(trust);
            ctx.init(Skaidb.clientKeyManagers(res("server.pem"), res("server.key")), tmf.getTrustManagers(), null);
            listener = (SSLServerSocket) ctx.getServerSocketFactory().createServerSocket(0, 1, InetAddress.getLoopbackAddress());
            listener.setNeedClientAuth(true);
            thread = new Thread(() -> serve(outcome));
            thread.setDaemon(true);
            thread.start();
        }

        int port() { return listener.getLocalPort(); }

        private void serve(byte[] outcome) {
            try (SSLSocket s = (SSLSocket) listener.accept()) {
                s.startHandshake();
                X509Certificate peer = (X509Certificate) s.getSession().getPeerCertificates()[0];
                peerCn = peer.getSubjectX500Principal().getName();
                DataInputStream in = new DataInputStream(s.getInputStream());
                OutputStream out = s.getOutputStream();
                authStart = read(in);
                write(out, outcome);
                if (outcome[1] != 1) return;
                while (true) {
                    byte[] req;
                    try {
                        req = read(in);
                    } catch (IOException eof) {
                        return;
                    }
                    if (req[0] == 8 || req[0] == 4) { write(out, new byte[] { 2 }); continue; }
                    write(out, ConformanceServer.rowsResponse(new String[] { "who" }, new Object[] { "app" }));
                }
            } catch (Exception e) {
                error = e.toString();
            }
        }

        static byte[] read(DataInputStream in) throws IOException {
            byte[] b = new byte[in.readInt()];
            in.readFully(b);
            return b;
        }

        static void write(OutputStream out, byte[] p) throws IOException {
            out.write(new byte[] { (byte) (p.length >>> 24), (byte) (p.length >>> 16), (byte) (p.length >>> 8), (byte) p.length });
            out.write(p);
            out.flush();
        }

        String finish() throws InterruptedException {
            thread.join(10_000);
            close();
            return error;
        }

        @Override public void close() {
            try { listener.close(); } catch (IOException ignored) {}
        }
    }

    static byte[] okOutcome() {
        return ConformanceServer.concat(new byte[] { 13, 1 }, new byte[32]);   // zero signature: never verified
    }

    Skaidb.ConnectOptions certOptions(int port, String cert, String key) {
        return new Skaidb.ConnectOptions().host("127.0.0.1", port)
            .tlsCa(res("ca.pem")).tlsClientCert(res(cert)).tlsClientKey(res(key))
            .authMechanism(Skaidb.AUTH_CERTIFICATE);
    }

    @Test
    void certificateLoginOverTls() throws Exception {
        for (String[] pair : List.of(new String[] { "client.pem", "client.key" },
                                     new String[] { "client.pem", "client-pkcs1.key" },
                                     new String[] { "client-ec.pem", "client-ec.key" })) {
            TlsServer server = new TlsServer(okOutcome());
            try (Skaidb.Connection conn = Skaidb.connect(certOptions(server.port(), pair[0], pair[1]))) {
                Skaidb.ResultSet rs = conn.query("SELECT current_user() AS who");
                assertTrue(rs.next());
                assertEquals("app", rs.getString("who"));
            }
            assertNull(server.finish(), pair[1]);
            assertEquals("CN=app", server.peerCn, "the client certificate was presented");
            assertArrayEquals(Skaidb.externalAuthStart(""), server.authStart, "EXTERNAL AuthStart, no user");
        }
    }

    @Test
    void certificateLoginAssertsTheUser() throws Exception {
        TlsServer server = new TlsServer(okOutcome());
        Skaidb.connect(certOptions(server.port(), "client.pem", "client.key").user("app")).close();
        assertNull(server.finish());
        assertArrayEquals(Skaidb.externalAuthStart("app"), server.authStart);
    }

    @Test
    void certificateLoginThroughJdbcSendsNoDefaultUser() throws Exception {
        TlsServer server = new TlsServer(okOutcome());
        String url = "jdbc:skaidb://127.0.0.1:" + server.port() + "/?tls_ca=" + res("ca.pem")
            + "&tls_client_cert=" + res("client.pem") + "&tls_client_key=" + res("client.key")
            + "&auth_mechanism=certificate";
        try (java.sql.Connection c = java.sql.DriverManager.getConnection(url);
             java.sql.ResultSet rs = c.createStatement().executeQuery("SELECT current_user() AS who")) {
            assertTrue(rs.next());
            assertEquals("app", rs.getString(1));
        }
        assertNull(server.finish());
        assertArrayEquals(Skaidb.externalAuthStart(""), server.authStart,
            "no user given: AuthStart carries an EMPTY name, never a default like anonymous");
    }

    @Test
    void certificateLoginDenied() throws Exception {
        byte[] denied = new Skaidb.Buf().u8(13).u8(0).str("certificate CN app maps to no role").toBytes();
        TlsServer server = new TlsServer(denied);
        Skaidb.SkaidbException e = assertThrows(Skaidb.SkaidbException.class,
            () -> Skaidb.connect(certOptions(server.port(), "client.pem", "client.key")));
        assertTrue(e.getMessage().startsWith("authentication denied: certificate CN app maps to no role"), e.getMessage());
        assertNull(server.finish());
    }

    @Test
    void untrustedServerIsRefused() throws Exception {
        TlsServer server = new TlsServer(okOutcome());
        // The JVM trust store does not know the test CA.
        Skaidb.ConnectOptions o = new Skaidb.ConnectOptions().host("127.0.0.1", server.port()).tls(true)
            .tlsClientCert(res("client.pem")).tlsClientKey(res("client.key")).authMechanism("certificate");
        assertThrows(Skaidb.SkaidbException.class, () -> Skaidb.connect(o));
        server.finish();
    }
}
