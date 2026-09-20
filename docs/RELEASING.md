# Releasing skaidb-java

The artifact is served by [JitPack](https://jitpack.io/#porcupin26/skaidb-java),
which builds a git tag on request and caches the result. Pushing the tag is
the whole procedure: the `Release` workflow
(`.github/workflows/release.yml`) does the rest on GitHub-hosted runners.
There is nothing to upload and no secret to configure.

1. Update `<version>` in `pom.xml` (and `<scm><tag>`), keeping the
   `FALLBACK_VERSION` literal in `Skaidb.java` in step — it is only used by
   a hand-built class tree with no manifest, but the workflow refuses a
   release where the three disagree.
2. Add the `## X.Y.Z` entry to `CHANGELOG.md`; that section becomes the
   GitHub Release notes.
3. `./mvnw -B verify` locally; CI must be green on `main`.
4. Tag and push: `git tag vX.Y.Z && git push origin main vX.Y.Z`.

The workflow then, in order:

- checks that the tag is `v` + the pom version and that `<scm><tag>`,
  `FALLBACK_VERSION` and the CHANGELOG heading agree;
- runs `./mvnw -B -ntp verify` on Temurin 17 (tests, jar, `-sources.jar`,
  `-javadoc.jar`) and checks the jar manifest's `Implementation-Version`;
- creates the GitHub Release `vX.Y.Z` with those jars, the pom and a
  `SHA256SUMS` attached and the CHANGELOG section as the notes (a tag with
  a `-suffix` is marked a pre-release);
- requests the tag's pom from JitPack — the request that makes JitPack
  build the tag — polls
  `https://jitpack.io/api/builds/com.github.porcupin26/skaidb-java/vX.Y.Z`
  for up to 10 minutes and fails with the build log if JitPack reports
  `Error`; then downloads the jar from JitPack and checks its version.

The job summary links the JitPack build page and log
(`https://jitpack.io/com/github/porcupin26/skaidb-java/vX.Y.Z/build.log`).

If the JitPack job times out (JitPack was slow), re-run the failed jobs:
every step is idempotent. If JitPack reports `Error`, that tag is lost —
JitPack caches a tag's first result for good — so fix the cause and release
the next patch version (this is what happened to `v1.0.0`).

JitPack forces the coordinates `com.github.porcupin26:skaidb-java:<tag>`;
the tag is used verbatim as the version (so `v1.0.0`, not `1.0.0`). The
build runs on the JDK pinned in `jitpack.yml` (OpenJDK 17, the JDK the
release workflow builds with) with `./mvnw install -DskipTests` (the
wrapper pins Maven 3.9.9, because JitPack's own Maven predates the plugin
versions used here); sources and Javadoc jars are attached because the
plugins are bound to the `package` phase.

## Maven Central

Publishing to Maven Central would let consumers drop the JitPack
repository block, but requires a Sonatype (Central Portal) account with a
verified namespace, GPG-signed artifacts and the deploy plugin; none of that
is set up. If it is added later, keep the same `groupId`/`artifactId` so
JitPack consumers can switch by removing the repository.
