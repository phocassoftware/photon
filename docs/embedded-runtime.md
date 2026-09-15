# Embedded Photon runtime

This fork contains an experimental Java embedding path for a batch job that needs to run
Photon in-process. It is separate from the existing command-line and HTTP server path.

## Runtime ownership

Each job owns one `EmbeddedPhotonRuntime` and closes it when the job finishes. The runtime
starts an OpenSearch node inside that JVM and executes queries through the node's in-process
transport client. It does not call Photon's HTTP API or a localhost endpoint. The loopback
HTTP listener created by the OpenSearch runner is an internal implementation detail and is
bound to an OS-assigned port.

Do not open the same OpenSearch node directory from multiple runtimes. A node owns mutable
metadata and transient files in addition to the Lucene index. Instead, prepare one immutable
reference dataset and create a private runtime tree for each job:

```java
var preparedData = new PhotonDatasetPreparer(cacheDirectory, lockTimeout, pollInterval)
        .prepare(datasetId, source::materialize);

PhotonRuntimeMaterializer.materialize(preparedData, runtimeRoot.resolve("photon_data"));
try (var photon = EmbeddedPhotonRuntime.open(runtimeRoot, clusterName)) {
    var query = new SearchQueryBuilder(address, false, false).build();
    var result = photon.search(query, 1, queryTimeout);
}
```

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

This API is intentionally a small PR-1 contract. The Nexus provider adapter, S3 source
implementation, job retry policy and host/deployment configuration remain consumers of it.
