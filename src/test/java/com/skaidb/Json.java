package com.skaidb;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A minimal JSON reader for the conformance vectors, so the test suite needs
 * no JSON library. Objects become LinkedHashMap, arrays ArrayList, integers
 * Long, other numbers Double.
 */
public final class Json {
    private final String s;
    private int i = 0;

    private Json(String s) { this.s = s; }

    public static Object parse(String text) {
        Json j = new Json(text);
        Object v = j.value();
        j.ws();
        if (j.i != j.s.length()) throw j.err("trailing data");
        return v;
    }

    /** {@code (Map<String, Object>) o}, without an unchecked warning at every call site. */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> obj(Object o) { return (Map<String, Object>) o; }

    @SuppressWarnings("unchecked")
    public static List<Object> arr(Object o) { return (List<Object>) o; }

    private IllegalArgumentException err(String m) {
        return new IllegalArgumentException(m + " at offset " + i);
    }

    private void ws() {
        while (i < s.length() && Character.isWhitespace(s.charAt(i))) i++;
    }

    private Object value() {
        ws();
        if (i >= s.length()) throw err("unexpected end");
        char c = s.charAt(i);
        switch (c) {
            case '{': {
                i++;
                Map<String, Object> m = new LinkedHashMap<>();
                ws();
                if (s.charAt(i) == '}') { i++; return m; }
                while (true) {
                    ws();
                    String k = string();
                    ws();
                    if (s.charAt(i++) != ':') throw err("expected :");
                    m.put(k, value());
                    ws();
                    char d = s.charAt(i++);
                    if (d == '}') return m;
                    if (d != ',') throw err("expected , or }");
                }
            }
            case '[': {
                i++;
                List<Object> l = new ArrayList<>();
                ws();
                if (s.charAt(i) == ']') { i++; return l; }
                while (true) {
                    l.add(value());
                    ws();
                    char d = s.charAt(i++);
                    if (d == ']') return l;
                    if (d != ',') throw err("expected , or ]");
                }
            }
            case '"': return string();
            case 't': expect("true"); return Boolean.TRUE;
            case 'f': expect("false"); return Boolean.FALSE;
            case 'n': expect("null"); return null;
            default: return number();
        }
    }

    private void expect(String word) {
        if (!s.startsWith(word, i)) throw err("expected " + word);
        i += word.length();
    }

    private Object number() {
        int start = i;
        while (i < s.length() && "+-0123456789.eE".indexOf(s.charAt(i)) >= 0) i++;
        String t = s.substring(start, i);
        if (t.isEmpty()) throw err("unexpected character");
        if (t.matches("-?\\d+")) return Long.parseLong(t);
        return Double.parseDouble(t);
    }

    private String string() {
        if (s.charAt(i++) != '"') throw err("expected string");
        StringBuilder b = new StringBuilder();
        while (true) {
            char c = s.charAt(i++);
            if (c == '"') return b.toString();
            if (c != '\\') { b.append(c); continue; }
            char e = s.charAt(i++);
            switch (e) {
                case '"': b.append('"'); break;
                case '\\': b.append('\\'); break;
                case '/': b.append('/'); break;
                case 'b': b.append('\b'); break;
                case 'f': b.append('\f'); break;
                case 'n': b.append('\n'); break;
                case 'r': b.append('\r'); break;
                case 't': b.append('\t'); break;
                case 'u': b.append((char) Integer.parseInt(s.substring(i, i + 4), 16)); i += 4; break;
                default: throw err("bad escape");
            }
        }
    }
}
