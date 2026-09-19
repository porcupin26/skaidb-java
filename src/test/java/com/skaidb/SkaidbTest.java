package com.skaidb;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** Pure-function tests: no socket, no server. */
class SkaidbTest {

    // ---- version -----------------------------------------------------------

    @Test
    void versionIsThePackageVersion() {
        assertNotNull(Skaidb.VERSION);
        assertTrue(Skaidb.VERSION.matches("\\d+\\.\\d+\\.\\d+(-[0-9A-Za-z.]+)?"), Skaidb.VERSION);
        assertEquals(Skaidb.VERSION, Skaidb.version());
        // Surefire passes ${project.version}; the Hello frame must announce
        // exactly the version of the artifact it ships in.
        String expected = System.getProperty("skaidb.expected.version");
        if (expected != null) assertEquals(expected, Skaidb.VERSION);
    }

    // ---- placeholders ------------------------------------------------------

    @Test
    void countsPlaceholdersOutsideStringLiterals() {
        assertEquals(0, Skaidb.countPlaceholders("SELECT 1"));
        assertEquals(2, Skaidb.countPlaceholders("INSERT INTO t (a, b) VALUES (?, ?)"));
        assertEquals(1, Skaidb.countPlaceholders("SELECT * FROM t WHERE a = '?' AND b = ?"));
        assertEquals(1, Skaidb.countPlaceholders("SELECT 'it''s ?' , ?"));
        assertEquals(0, Skaidb.countPlaceholders("SELECT '?' || '?'"));
    }

    // ---- client-side binding (PROTOCOL.md §5) -----------------------------

    @Test
    void bindQuotesEveryScalar() {
        assertEquals("SELECT 1", Skaidb.bind("SELECT 1", new Object[0]));
        assertEquals("VALUES ('Ada', 1, 2.5, TRUE, NULL)",
            Skaidb.bind("VALUES (?, ?, ?, ?, ?)", new Object[] { "Ada", 1L, 2.5, true, null }));
        assertEquals("'it''s'", Skaidb.quote("it's"));
        assertEquals("'O''''Brien'", Skaidb.quote("O''Brien"));
        assertEquals("'deadbeef'", Skaidb.quote(new byte[] { (byte) 0xde, (byte) 0xad, (byte) 0xbe, (byte) 0xef }));
        assertEquals("1700000000000", Skaidb.quote(Instant.ofEpochMilli(1_700_000_000_000L)));
        assertEquals("'123e4567-e89b-12d3-a456-426614174000'",
            Skaidb.quote(UUID.fromString("123e4567-e89b-12d3-a456-426614174000")));
        assertEquals("-3", Skaidb.quote(-3));
        assertEquals("FALSE", Skaidb.quote(false));
        assertEquals("1.25", Skaidb.quote(new BigDecimal("1.25")));
    }

    @Test
    void bindLeavesPlaceholdersInsideStringsAlone() {
        assertEquals("SELECT 'a?b', 'x'", Skaidb.bind("SELECT 'a?b', ?", new Object[] { "x" }));
        assertEquals("SELECT 'it''s ?', 7", Skaidb.bind("SELECT 'it''s ?', ?", new Object[] { 7 }));
    }

    @Test
    void bindRefusesWhatHasNoLiteralForm() {
        assertThrows(Skaidb.SkaidbException.class, () -> Skaidb.quote(Double.NaN));
        assertThrows(Skaidb.SkaidbException.class, () -> Skaidb.quote(Double.POSITIVE_INFINITY));
        assertThrows(Skaidb.SkaidbException.class, () -> Skaidb.quote(Arrays.asList(1, 2)));
        assertThrows(Skaidb.SkaidbException.class, () -> Skaidb.quote(new LinkedHashMap<String, Object>()));
        assertThrows(Skaidb.SkaidbException.class, () -> Skaidb.quote(new int[] { 1 }));
        assertThrows(Skaidb.SkaidbException.class,
            () -> Skaidb.bind("VALUES (?, ?)", new Object[] { 1 }));
    }

    // ---- typed values (PROTOCOL.md §4) --------------------------------------

    private static byte[] le64(long v) {
        byte[] b = new byte[8];
        for (int i = 0; i < 8; i++) b[i] = (byte) (v >>> (8 * i));
        return b;
    }

    private static byte[] cat(byte[]... parts) {
        int n = 0;
        for (byte[] p : parts) n += p.length;
        byte[] out = new byte[n];
        int at = 0;
        for (byte[] p : parts) { System.arraycopy(p, 0, out, at, p.length); at += p.length; }
        return out;
    }

    @Test
    void encodesScalarsExactlyAsTheSpecSays() {
        assertArrayEquals(new byte[] { 0 }, Skaidb.encodeValue(null));
        assertArrayEquals(new byte[] { 1, 1 }, Skaidb.encodeValue(true));
        assertArrayEquals(new byte[] { 1, 0 }, Skaidb.encodeValue(false));
        assertArrayEquals(cat(new byte[] { 2 }, le64(-2)), Skaidb.encodeValue(-2));
        assertArrayEquals(cat(new byte[] { 2 }, le64(Long.MAX_VALUE)), Skaidb.encodeValue(Long.MAX_VALUE));
        assertArrayEquals(cat(new byte[] { 3 }, le64(Double.doubleToLongBits(1.5))), Skaidb.encodeValue(1.5));
        assertArrayEquals(new byte[] { 5, 3, 0, 0, 0, 'A', 'd', 'a' }, Skaidb.encodeValue("Ada"));
        assertArrayEquals(new byte[] { 6, 2, 0, 0, 0, 9, 8 }, Skaidb.encodeValue(new byte[] { 9, 8 }));
        assertArrayEquals(cat(new byte[] { 8 }, le64(1_700_000_000_000L)),
            Skaidb.encodeValue(Instant.ofEpochMilli(1_700_000_000_000L)));
        // Uuid: 16 raw bytes in RFC 4122 order
        byte[] u = Skaidb.encodeValue(UUID.fromString("00112233-4455-6677-8899-aabbccddeeff"));
        assertEquals(17, u.length);
        assertEquals(7, u[0]);
        assertEquals(0x00, u[1] & 0xff);
        assertEquals(0x11, u[2] & 0xff);
        assertEquals(0xff, u[16] & 0xff);
        // Decimal: i128 LE mantissa + u32 scale. 1.25 = 125 / 10^2
        byte[] d = Skaidb.encodeValue(new BigDecimal("1.25"));
        assertEquals(1 + 16 + 4, d.length);
        assertEquals(4, d[0]);
        assertEquals(125, d[1]);
        assertEquals(2, d[17]);
        // negative mantissa sign-extends through all 16 bytes
        byte[] neg = Skaidb.encodeValue(new BigDecimal("-1"));
        for (int i = 1; i <= 16; i++) assertEquals((byte) 0xff, neg[i], "byte " + i);
        assertEquals(0, neg[17]);
    }

    @Test
    void encodesCollectionsRecursively() {
        // Array of [1, "a"]: tag 9, count 2, then each value
        assertArrayEquals(cat(new byte[] { 9, 2, 0, 0, 0, 2 }, le64(1), new byte[] { 5, 1, 0, 0, 0, 'a' }),
            Skaidb.encodeValue(Arrays.asList(1, "a")));
        assertArrayEquals(Skaidb.encodeValue(Arrays.asList(1L, 2L)), Skaidb.encodeValue(new long[] { 1, 2 }));
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("k", true);
        assertArrayEquals(new byte[] { 10, 1, 0, 0, 0, 1, 0, 0, 0, 'k', 1, 1 }, Skaidb.encodeValue(m));
        Map<Object, Object> badKey = new LinkedHashMap<>();
        badKey.put(1, 1);
        assertThrows(Skaidb.SkaidbException.class, () -> Skaidb.encodeValue(badKey));
        assertThrows(Skaidb.SkaidbException.class, () -> Skaidb.encodeValue(new Object()));
        assertThrows(Skaidb.SkaidbException.class, () -> Skaidb.encodeValue(Double.NaN));
    }

    @Test
    void decodesEveryTypeAndRoundTrips() {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("z", 1L);
        doc.put("a", Arrays.asList("x", null, 2.5));
        Object[] samples = {
            null, true, false, 0L, -1L, Long.MIN_VALUE, 3.25, new BigDecimal("-12345.678"),
            "héllo ☃", new byte[] { 0, 1, 2 }, UUID.randomUUID(), Instant.ofEpochMilli(-1000),
            Arrays.asList(1L, "two", Arrays.asList(3L)), doc,
        };
        for (Object v : samples) {
            Object back = Skaidb.decodeValue(new Skaidb.Reader(Skaidb.encodeValue(v)));
            if (v instanceof byte[]) assertArrayEquals((byte[]) v, (byte[]) back);
            else assertEquals(v, back, String.valueOf(v));
        }
        // Document keys keep insertion order
        @SuppressWarnings("unchecked")
        Map<String, Object> back = (Map<String, Object>) Skaidb.decodeValue(new Skaidb.Reader(Skaidb.encodeValue(doc)));
        assertEquals(Arrays.asList("z", "a"), new java.util.ArrayList<>(back.keySet()));
        assertTrue(back instanceof LinkedHashMap);
    }

    @Test
    void decodeRejectsGarbage() {
        assertThrows(Skaidb.SkaidbException.class, () -> Skaidb.decodeValue(new Skaidb.Reader(new byte[] { 42 })));
        assertThrows(Skaidb.SkaidbException.class, () -> Skaidb.decodeValue(new Skaidb.Reader(new byte[] { 2, 1, 2 })));
        assertThrows(Skaidb.SkaidbException.class, () -> Skaidb.decodeValue(new Skaidb.Reader(new byte[] { 5, 9, 0, 0, 0, 'x' })));
    }

    // ---- DSN ----------------------------------------------------------------

    @Test
    void parsesAFullDsn() {
        Skaidb.Dsn d = Skaidb.parseDsn(
            "skaidb://ada:s3cret@h1:7001,h2,h3:7003/app?consistency=all&tls_ca=/etc/ca.pem&tls_server_name=node1");
        assertEquals(Arrays.asList("h1:7001", "h2:7000", "h3:7003"), d.seeds);
        assertEquals("ada", d.user);
        assertEquals("s3cret", d.password);
        assertEquals("app", d.database);
        assertEquals(Skaidb.CONSISTENCY_ALL, d.consistency);
        assertTrue(d.tls);
        assertEquals("/etc/ca.pem", d.tlsCa);
        assertEquals("node1", d.tlsServerName);
        assertFalse(d.tlsInsecure);
    }

    @Test
    void dsnDefaults() {
        Skaidb.Dsn d = Skaidb.parseDsn("skaidb://localhost");
        assertEquals(List.of("localhost:7000"), d.seeds);
        assertEquals("anonymous", d.user);
        assertEquals("", d.password);
        assertEquals("", d.database);
        assertEquals(Skaidb.CONSISTENCY_QUORUM, d.consistency);
        assertFalse(d.tls);
        assertEquals("skaidb", d.tlsServerName);

        assertEquals(Skaidb.CONSISTENCY_ONE, Skaidb.parseDsn("skaidb://h/?consistency=one").consistency);
        assertEquals(Skaidb.CONSISTENCY_QUORUM, Skaidb.parseDsn("skaidb://h/?consistency=QUORUM").consistency);
        assertTrue(Skaidb.parseDsn("skaidb://h/?tls=true").tls);
        assertTrue(Skaidb.parseDsn("skaidb://h/?tls=1").tls);
        Skaidb.Dsn insecure = Skaidb.parseDsn("skaidb://h/?tls_insecure=true");
        assertTrue(insecure.tls);
        assertTrue(insecure.tlsInsecure);
        assertEquals("", Skaidb.parseDsn("skaidb://u@h:7000/").database);
        assertEquals("u", Skaidb.parseDsn("skaidb://u@h:7000/").user);
    }

    @Test
    void dsnErrors() {
        assertThrows(Skaidb.SkaidbException.class, () -> Skaidb.parseDsn("postgres://h"));
        assertThrows(Skaidb.SkaidbException.class, () -> Skaidb.parseDsn("skaidb://"));
        assertThrows(Skaidb.SkaidbException.class, () -> Skaidb.parseDsn("skaidb://h/?consistency=eventual"));
        assertThrows(Skaidb.SkaidbException.class, () -> Skaidb.parseDsn("skaidb://h:7000/?x=%zz"));
    }

    // ---- ResultSet ----------------------------------------------------------

    @Test
    void resultSetIsOneBasedAndCoerces() {
        Skaidb.ResultSet rs = new Skaidb.ResultSet(new String[] { "id", "name", "score", "ok", "nothing" },
            new java.util.ArrayList<>(List.<Object[]>of(
                new Object[] { 1L, "Ada", 2.5, true, null },
                new Object[] { 2L, "Linus", 7.0, false, null })));
        assertEquals(2, rs.getRowCount());
        assertArrayEquals(new String[] { "id", "name", "score", "ok", "nothing" }, rs.getColumnNames());
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(1L, rs.getLong("id"));
        assertEquals("Ada", rs.getString(2));
        assertEquals("Ada", rs.getObject("name"));
        assertEquals(2.5, rs.getDouble("score"));
        assertEquals(2, rs.getInt("score"));
        assertEquals("2.5", rs.getString("score"));
        assertTrue(rs.getBoolean(4));
        assertTrue(rs.isNull("nothing"));
        assertNull(rs.getString("nothing"));
        assertThrows(Skaidb.SkaidbException.class, () -> rs.getInt("nothing"));
        assertThrows(Skaidb.SkaidbException.class, () -> rs.getObject("nope"));
        assertTrue(rs.next());
        assertEquals("Linus", rs.getString("name"));
        assertFalse(rs.next());
        assertFalse(rs.nextResultSet());
    }
}
