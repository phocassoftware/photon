# Embedded Photon runtime

This fork contains an experimental Java embedding path for the `dp-query-engine` batch job that
needs to run Photon in-process. The Phocas-specific API is intentionally limited to forward
address lookup; structured and reverse geocoding remain existing upstream functionality and are
not part of this integration. It is separate from the existing command-line and HTTP server path.

## Maven dependency

The fork publishes the Java component to the private GitHub Maven repository. The version
keeps the upstream Photon version and adds the `-phocas` suffix. For example, the fork of
Photon 1.3.0 is available as:

```xml
<repository>
    <id>github-photon</id>
    <url>https://maven.pkg.github.com/phocassoftware/photon</url>
</repository>

<dependency>
    <groupId>de.komoot.photon</groupId>
    <artifactId>photon</artifactId>
    <version>1.3.0-phocas</version>
</dependency>
```

Configure a GitHub token with package-read access in the Maven server entry matching the
repository id. The GitHub Release also contains the executable fat JAR used by the existing
command-line installation; the Maven artifact is the regular Java component with its runtime
dependencies declared transitively.

Releases are dispatched manually from `master`. Select `current` for the first release of an
upstream version (for example, `1.3.0-phocas`). If that fork release already exists but the
upstream version has not changed, select `currentIncrement`; it creates the next available
suffix such as `1.3.0-phocas-1`. Patch, minor, and major releases are based on the latest
`-phocas` release.

## Runtime ownership

Each job owns one `EmbeddedPhotonRuntime` and closes it when the job finishes. The runtime
starts an OpenSearch node inside that JVM and executes queries through the node's in-process
transport client. It does not call Photon's HTTP API or a localhost endpoint. The loopback
HTTP listener created by the OpenSearch runner is an internal implementation detail and is
bound to an OS-assigned port.

The embedded entrypoint disables OpenSearchRunner's Log4j2 configuration so the host application
owns the JVM logging configuration.

Do not open the same OpenSearch node directory from multiple runtimes. A node owns mutable
metadata and transient files in addition to the Lucene index. Instead, prepare one immutable
reference dataset and create a private runtime tree for each job:

```java
var preparedData = new PhotonDatasetPreparer(cacheDirectory, lockTimeout, pollInterval)
        .prepare(datasetId, source::materialize);

PhotonRuntimeMaterializer.materialize(preparedData, runtimeRoot.resolve("photon_data"));
try (var photon = EmbeddedPhotonRuntime.open(runtimeRoot, clusterName)) {
    var result = photon.search(address, countryCodes, 5, queryTimeout);
}
```

The forward-search method applies the same strict-then-lenient search path as Photon's normal
forward endpoint, validates backend completeness on every search, and returns only the coordinate,
country-code and formatted-address fields needed by the adapter. Coordinates are latitude/longitude
and `countryCode` is ISO alpha-2 when present.

The convenience method rejects indexed results without coordinates as incomplete. A successful
complete search with no hits is represented by an empty `hits()` list and is safe for the caller to
interpret as a no-match.

`source` is supplied by the embedding application. The fork deliberately has no AWS or S3
dependency: a caller can download and validate an artifact into the staging directory,
while tests can use a local fixture.

`PhotonRuntimeMaterializer` copies node metadata and transient files, and hard-links only
read-only Lucene index files. The reference dataset and each runtime must be on the same
filesystem for this to preserve file-backed page sharing. The reference index files must be
read-only before materialisation; the helper never changes their permissions.

`PhotonDatasetPreparer` coordinates first-job preparation with a stable filesystem lock,
rechecks after waiting, cleans failed staging, and publishes only a completion-marked
dataset through an atomic directory move. The lock is held only during preparation and
publication, not while a job performs lookups.

This API is intentionally a small PR-1 contract for forward geocoding. The Nexus provider adapter, S3 source
implementation, job retry policy and host/deployment configuration remain consumers of it.
