# Changelog

All notable changes to the skaidb Java driver. The driver has its own
version series, independent of the skaidb server; see the README's
compatibility section for which server versions each release speaks to.

## 1.0.2

Release automation: published from GitHub Actions. No driver changes.

- Pushing a tag `vX.Y.Z` now runs the release workflow
  (`.github/workflows/release.yml`) on GitHub-hosted runners: it checks the
  tag against `pom.xml`, builds and tests on Temurin 17, creates the GitHub
  Release with the jar, `-sources.jar`, `-javadoc.jar`, pom and checksums
  attached and this section as the notes, then has JitPack build the tag
  and verifies that `com.github.porcupin26:skaidb-java:vX.Y.Z` resolves.

## 1.0.1

Republished under a new tag because JitPack cached a failed v1.0.0 build;
no code changes.

- The first commit under tag `v1.0.0` built with JitPack's stock Maven,
  which is too old for `maven-compiler-plugin` 3.13.0; JitPack cached that
  failure and never re-resolves a tag, so
  `com.github.porcupin26:skaidb-java:v1.0.0` does not resolve. Use
  `v1.0.1` or newer.

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
