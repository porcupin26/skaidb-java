# Changelog

All notable changes to the skaidb Java driver. The driver has its own
version series, independent of the skaidb server; see the README's
compatibility section for which server versions each release speaks to.

## 1.0.0

First standalone release. The driver previously lived in the skaidb
monorepo under `drivers/java`; its history is carried over unchanged.

- Published as `com.github.porcupin26:skaidb-java` (via JitPack); a Maven
  Wrapper (`./mvnw`, Maven 3.9.9) is checked in.
- The version the driver reports to the server in its Hello frame is now
  derived from the package metadata (`Skaidb.VERSION`), never a literal.
- `SkaidbException` and the internal `Unpreparable` carry a `serialVersionUID`.
- DSN parsing is factored into a package-private `parseDsn`, so it is
  covered by tests without a socket.
- JUnit 5 test suite: parameter counting and client-side binding, value
  encode/decode against the wire spec, DSN parsing, and an in-process fake
  server covering the SCRAM handshake, Hello, prepared statements,
  streaming and the abandon/drain rule.
- Sources and Javadoc jars are attached to every build.
- `examples/` holds the runnable examples.
