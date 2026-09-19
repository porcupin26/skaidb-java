# Releasing skaidb-java

The artifact is served by [JitPack](https://jitpack.io/#porcupin26/skaidb-java),
which builds a git tag on the first request for it and caches the result.
There is nothing to upload.

1. Update `<version>` in `pom.xml` (and `<scm><tag>`), keeping the
   `FALLBACK_VERSION` literal in `Skaidb.java` in step — it is only used by
   a hand-built class tree with no manifest, but should not drift.
2. Add the entry to `CHANGELOG.md`.
3. `mvn -B verify` locally; CI must be green on `main`.
4. Tag and push: `git tag vX.Y.Z && git push origin main vX.Y.Z`.
5. Warm JitPack (optional, the first consumer would otherwise wait for the
   build): fetch
   `https://jitpack.io/com/github/porcupin26/skaidb-java/vX.Y.Z/skaidb-java-vX.Y.Z.pom`
   and check the log at
   `https://jitpack.io/com/github/porcupin26/skaidb-java/vX.Y.Z/build.log`.

JitPack forces the coordinates `com.github.porcupin26:skaidb-java:<tag>`;
the tag is used verbatim as the version (so `v1.0.0`, not `1.0.0`). The
build runs on the JDK pinned in `jitpack.yml` (OpenJDK 17) with
`mvn install -DskipTests`; sources and Javadoc jars are attached because
the plugins are bound to the `package` phase.

## Maven Central

Publishing to Maven Central would let consumers drop the JitPack
repository block, but requires a Sonatype (Central Portal) account with a
verified namespace, GPG-signed artifacts and the deploy plugin; none of that
is set up. If it is added later, keep the same `groupId`/`artifactId` so
JitPack consumers can switch by removing the repository.
