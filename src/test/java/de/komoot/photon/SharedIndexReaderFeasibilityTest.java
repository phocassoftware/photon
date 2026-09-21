package de.komoot.photon;

import de.komoot.photon.config.PhotonDBConfig;
import de.komoot.photon.config.PhotonDBLayoutConfig;
import de.komoot.photon.embedded.PhotonRuntimeMaterializer;
import de.komoot.photon.json.JsonReader;
import de.komoot.photon.nominatim.ImportThread;
import de.komoot.photon.nominatim.model.AddressType;
import de.komoot.photon.nominatim.model.NameMap;
import de.komoot.photon.opensearch.IncompleteSearchException;
import de.komoot.photon.opensearch.PhotonIndex;
import de.komoot.photon.opensearch.SearchQueryBuilder;
import de.komoot.photon.query.StructuredSearchRequest;
import org.codelibs.opensearch.runner.OpenSearchRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch._types.query_dsl.Query;
import org.opensearch.index.query.QueryBuilders;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

/**
 * Feasibility checks for the agreed four-job topology.
 *
 * These tests deliberately separate two questions: the embedded OpenSearch node ownership
 * model, and whether the immutable Lucene shard files themselves can be read by multiple
 * independent JVMs. Passing the second check does not make multiple OpenSearch nodes safe.
 */
@Isolated
class SharedIndexReaderFeasibilityTest {
    private static final String CLUSTER_NAME = "photon-feasibility";
    private static final int READER_COUNT = 4;
    private static final int OPEN_SEARCH_RUNTIME_COUNT = 4;
    private static final Duration PROCESS_TIMEOUT = Duration.ofSeconds(45);
    private static final String ANDORRA_FIXTURE = "/fixtures/andorra-release-260906.jsonl";

    @Test
    void embeddedClientCanQueryPhotonIndexWithoutAnHttpRequest(@TempDir Path dataDirectory) throws Exception {
        final var server = makeServer(dataDirectory);
        try {
            addBerlinDocument(server);

            final var response = server.getEmbeddedClient()
                    .prepareSearch(PhotonIndex.NAME)
                    .setQuery(QueryBuilders.matchQuery("collector.field.name", "berlin"))
                    .setSize(1)
                    .get();

            assertThat(response.getHits().getTotalHits().value()).isEqualTo(1);
            assertThat(response.getHits().getHits()).hasSize(1);
            assertThat(response.getHits().getHits()[0].getSourceAsString()).contains("berlin");
        } finally {
            server.shutdown();
        }
    }

    @Test
    void ordinaryEmbeddedRuntimesCannotOwnTheSameNodeDirectory(@TempDir Path dataDirectory) throws Exception {
        final var config = new PhotonDBConfig(dataDirectory.toString(), CLUSTER_NAME, List.of());
        final var first = new Server(config, true);
        try {
            first.recreateIndex(new DatabaseProperties(), new PhotonDBLayoutConfig().getNormalizationFilters());
            first.refreshIndexes();

            assertThatThrownBy(() -> new Server(config, false))
                    .isInstanceOf(RuntimeException.class)
                    .satisfies(error -> assertThat(allMessages(error)).containsIgnoringCase("lock"));
        } finally {
            first.shutdown();
        }
    }

    @Test
    void increasingLocalStorageNodeLimitDoesNotShareTheExistingPhotonIndex(@TempDir Path dataDirectory)
            throws Exception {
        final var server = makeServer(dataDirectory);
        try {
            final var runner = makeRunnerWithMultipleLocalStorageNodes(dataDirectory);
            try {
                runner.ensureYellow();

                assertThat(runner.client().admin().indices().prepareExists(PhotonIndex.NAME).get().isExists())
                        .as("a second node slot must not see the first node's local Photon index")
                        .isFalse();
            } finally {
                runner.close();
            }
        } finally {
            server.shutdown();
        }
    }

    @Test
    void privateOpenSearchNodesCanReadHardLinkedLuceneFiles(@TempDir Path dataDirectory) throws Exception {
        final var server = makeServer(dataDirectory);
        try {
            addBerlinDocument(server);
        } finally {
            server.shutdown();
        }

        final var sourceData = dataDirectory.resolve("photon_data");
        final var runtimeRoots = new ArrayList<Path>();
        runtimeRoots.add(dataDirectory);
        makeImmutableLuceneIndexFilesReadOnly(sourceData);
        final var indexDigestsBeforeStart = digestImmutableLuceneIndexFiles(sourceData);
        for (int i = 1; i < OPEN_SEARCH_RUNTIME_COUNT; i++) {
            final var cloneRoot = Files.createDirectory(dataDirectory.resolve("hard-linked-node-" + i));
            PhotonRuntimeMaterializer.materialize(sourceData, cloneRoot.resolve("photon_data"));
            runtimeRoots.add(cloneRoot);
        }

        final var runtimes = new ArrayList<Server>();
        try {
            for (var runtimeRoot : runtimeRoots) {
                runtimes.add(new Server(
                        new PhotonDBConfig(runtimeRoot.toString(), CLUSTER_NAME, List.of()), false));
            }

            for (var runtime : runtimes) {
                assertThat(searchBerlinWithEmbeddedNode(runtime)).isTrue();
            }

            final var sourceIndexFile = findFirstLuceneIndexFile(sourceData);
            for (var runtimeRoot : runtimeRoots.subList(1, runtimeRoots.size())) {
                final var runtimeIndexFile = runtimeRoot.resolve(
                        "photon_data").resolve(sourceData.relativize(sourceIndexFile));
                assertThat(Files.isSameFile(sourceIndexFile, runtimeIndexFile))
                        .as("per-node index paths should still address the same file inode")
                        .isTrue();
            }
            assertThat(digestImmutableLuceneIndexFiles(sourceData))
                    .as("OpenSearch startup must not modify shared read-only Lucene files")
                    .isEqualTo(indexDigestsBeforeStart);
        } finally {
            for (var runtime : runtimes.reversed()) {
                runtime.shutdown();
            }
        }
    }

    @Test
    void existingPhotonQueryDslCanRunThroughAnEmbeddedClientWithoutHttp(@TempDir Path dataDirectory) throws Exception {
        final var server = makeServer(dataDirectory);
        try {
            addBerlinDocument(server);

            final var query = new SearchQueryBuilder("1 Alexanderplatz Berlin", false, false);
            query.addImportance(30.0f);
            final var response = server.getEmbeddedClient()
                    .prepareSearch(PhotonIndex.NAME)
                    .setQuery(QueryBuilders.wrapperQuery(serializeQuery(query.build())))
                    .setSize(1)
                    .get();

            assertThat(response.getHits().getTotalHits().value()).isEqualTo(1);
            assertThat(response.getHits().getHits()).hasSize(1);
            assertThat(response.getHits().getHits()[0].getSourceAsString()).contains("Alexanderplatz");
        } finally {
            server.shutdown();
        }
    }

    @Test
    void embeddedRuntimeExecutesPhotonQueryWithoutHttp(@TempDir Path dataDirectory) throws Exception {
        final var server = makeServer(dataDirectory);
        try {
            addBerlinDocument(server);
        } finally {
            server.shutdown();
        }

        final var sourceData = dataDirectory.resolve("photon_data");
        makeImmutableLuceneIndexFilesReadOnly(sourceData);
        final var runtimeRoot = Files.createDirectory(dataDirectory.resolve("embedded-runtime"));
        PhotonRuntimeMaterializer.materialize(sourceData, runtimeRoot.resolve("photon_data"));

        try (var runtime = EmbeddedPhotonRuntime.open(runtimeRoot, CLUSTER_NAME)) {
            assertThat(runtime.isReady()).isTrue();

            final var query = new SearchQueryBuilder("1 Alexanderplatz Berlin", false, false);
            query.addImportance(30.0f);
            final var result = runtime.search(query.build(), 1, Duration.ofSeconds(1));

            assertThat(result.totalHits()).isEqualTo(1);
            assertThat(result.hits()).hasSize(1);
            assertThat(result.hits().getFirst().getCoordinates())
                    .containsExactly(13.38886, 52.51704);
        }
    }

    @Test
    void embeddedRuntimeExecutesForwardGeocodingSearchWithoutHttp(@TempDir Path dataDirectory) throws Exception {
        final var server = makeServer(dataDirectory);
        try {
            addBerlinDocument(server);
        } finally {
            server.shutdown();
        }

        final var sourceData = dataDirectory.resolve("photon_data");
        makeImmutableLuceneIndexFilesReadOnly(sourceData);
        final var runtimeRoot = Files.createDirectory(dataDirectory.resolve("embedded-geocoding-runtime"));
        PhotonRuntimeMaterializer.materialize(sourceData, runtimeRoot.resolve("photon_data"));

        try (var runtime = EmbeddedPhotonRuntime.open(runtimeRoot, CLUSTER_NAME)) {
            var result = runtime.search(
                    "1 Alexanderplatz Berlin", List.of("DE"), 1, Duration.ofSeconds(1));

            assertThat(result.totalHits()).isEqualTo(1);
            assertThat(result.hits()).singleElement().satisfies(hit -> {
                assertThat(hit.latitude()).isEqualTo(52.51704);
                assertThat(hit.longitude()).isEqualTo(13.38886);
                assertThat(hit.countryCode()).isEqualTo("DE");
                assertThat(hit.formattedAddress()).isEqualTo("berlin, 1 Alexanderplatz, Germany");
            });
        }
    }

    @Test
    void embeddedRuntimeExecutesStructuredGeocodingSearchWithoutHttp(@TempDir Path dataDirectory) throws Exception {
        final var server = makeServer(dataDirectory);
        try {
            addBerlinDocument(server);
        } finally {
            server.shutdown();
        }

        final var sourceData = dataDirectory.resolve("photon_data");
        makeImmutableLuceneIndexFilesReadOnly(sourceData);
        final var runtimeRoot = Files.createDirectory(dataDirectory.resolve("embedded-structured-geocoding-runtime"));
        PhotonRuntimeMaterializer.materialize(sourceData, runtimeRoot.resolve("photon_data"));

        try (var runtime = EmbeddedPhotonRuntime.open(runtimeRoot, CLUSTER_NAME)) {
            var request = new StructuredSearchRequest();
            request.setCountryCode("DE");
            request.setCity("Berlin");
            request.setStreet("Alexanderplatz");
            request.setHouseNumber("1");

            var result = runtime.searchStructured(request, 1, Duration.ofSeconds(1));

            assertThat(result.totalHits()).isEqualTo(1);
            assertThat(result.hits()).singleElement().satisfies(hit -> {
                assertThat(hit.latitude()).isEqualTo(52.51704);
                assertThat(hit.longitude()).isEqualTo(13.38886);
                assertThat(hit.countryCode()).isEqualTo("DE");
                assertThat(hit.formattedAddress()).isEqualTo("berlin, 1 Alexanderplatz, Germany");
            });
        }
    }

    @Test
    void embeddedRuntimeGeocodesAnAddressFromARealPhotonDump(@TempDir Path dataDirectory) throws Exception {
        final var server = makeServer(dataDirectory);
        try {
            importAndorraFixture(server);
        } finally {
            server.shutdown();
        }

        final var sourceData = dataDirectory.resolve("photon_data");
        makeImmutableLuceneIndexFilesReadOnly(sourceData);
        final var runtimeRoot = Files.createDirectory(dataDirectory.resolve("embedded-real-data-runtime"));
        PhotonRuntimeMaterializer.materialize(sourceData, runtimeRoot.resolve("photon_data"));

        try (var runtime = EmbeddedPhotonRuntime.open(runtimeRoot, CLUSTER_NAME)) {
            final var result = runtime.search(
                    "Carrer de la Llacuna, Andorra la Vella", List.of("AD"), 1, Duration.ofSeconds(1));

            assertThat(result.totalHits()).isPositive();
            assertThat(result.hits()).singleElement().satisfies(hit -> {
                assertThat(hit.latitude()).isCloseTo(42.5086053, within(0.000001));
                assertThat(hit.longitude()).isCloseTo(1.5224384, within(0.000001));
                assertThat(hit.countryCode()).isEqualTo("AD");
                if (hit.formattedAddress() != null) {
                    assertThat(hit.formattedAddress())
                            .contains("Carrer de la Llacuna")
                            .contains("Andorra la Vella");
                }
            });
        }
    }

    @Test
    void embeddedRuntimeUsesForwardLenientFallback(@TempDir Path dataDirectory) throws Exception {
        final var server = makeServer(dataDirectory);
        try {
            addBerlinDocument(server);
        } finally {
            server.shutdown();
        }

        final var sourceData = dataDirectory.resolve("photon_data");
        makeImmutableLuceneIndexFilesReadOnly(sourceData);
        final var runtimeRoot = Files.createDirectory(dataDirectory.resolve("embedded-fallback-runtime"));
        PhotonRuntimeMaterializer.materialize(sourceData, runtimeRoot.resolve("photon_data"));

        try (var runtime = EmbeddedPhotonRuntime.open(runtimeRoot, CLUSTER_NAME)) {
            var result = runtime.search(
                    "1 Alexanderplaz Berlin", List.of("DE"), 1, Duration.ofSeconds(1));

            assertThat(result.totalHits()).isEqualTo(1);
            assertThat(result.hits()).singleElement().satisfies(hit -> {
                assertThat(hit.countryCode()).isEqualTo("DE");
                assertThat(hit.formattedAddress()).isEqualTo("berlin, 1 Alexanderplatz, Germany");
            });
        }
    }

    @Test
    void embeddedRuntimeRejectsForwardResultWithoutCoordinates(@TempDir Path dataDirectory) throws Exception {
        final var server = makeServer(dataDirectory);
        try {
            final var importer = server.createImporter(new DatabaseProperties());
            importer.add(List.of(new PhotonDoc()
                    .placeId("1001").osmType("N").osmId(1001).tagKey("place").tagValue("city")
                    .categories(List.of("osm.place.city"))
                    .importance(0.6).addressType(AddressType.CITY)
                    .names(NameMap.makeForPlace(Map.of("name", "No Coordinate"), List.of("en")))));
            importer.finish();
            server.refreshIndexes();
        } finally {
            server.shutdown();
        }

        final var sourceData = dataDirectory.resolve("photon_data");
        makeImmutableLuceneIndexFilesReadOnly(sourceData);
        final var runtimeRoot = Files.createDirectory(dataDirectory.resolve("embedded-invalid-runtime"));
        PhotonRuntimeMaterializer.materialize(sourceData, runtimeRoot.resolve("photon_data"));

        try (var runtime = EmbeddedPhotonRuntime.open(runtimeRoot, CLUSTER_NAME)) {
            assertThatThrownBy(() -> runtime.search(
                    "No Coordinate", List.of(), 1, Duration.ofSeconds(1)))
                    .isInstanceOf(IncompleteSearchException.class)
                    .hasMessageContaining("coordinates");
        }
    }

    @Test
    void materializerRejectsSymlinksOutsideTheReferenceDataset(@TempDir Path dataDirectory) throws Exception {
        final var referenceData = Files.createDirectory(dataDirectory.resolve("reference"));
        final var externalFile = Files.writeString(dataDirectory.resolve("external"), "not part of Photon");
        Files.createSymbolicLink(referenceData.resolve("escape"), externalFile);

        assertThatThrownBy(() -> PhotonRuntimeMaterializer.materialize(
                referenceData, dataDirectory.resolve("runtime")))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("escapes");
        assertThat(dataDirectory.resolve("runtime")).doesNotExist();
    }

    @Test
    void materializerRejectsRuntimePathThroughSymlinkedParent(@TempDir Path dataDirectory) throws Exception {
        final var referenceData = Files.createDirectory(dataDirectory.resolve("reference"));
        final var runtimeParent = Files.createSymbolicLink(
                dataDirectory.resolve("runtime-parent"), referenceData);

        assertThatThrownBy(() -> PhotonRuntimeMaterializer.materialize(
                referenceData, runtimeParent.resolve("photon_data")))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("separate trees");
        assertThat(referenceData.resolve("photon_data")).doesNotExist();
    }

    @Test
    void materializerRejectsAbsoluteSymlinksInsideTheReferenceDataset(@TempDir Path dataDirectory) throws Exception {
        final var referenceData = Files.createDirectory(dataDirectory.resolve("reference"));
        final var internalFile = Files.writeString(referenceData.resolve("internal"), "part of Photon");
        Files.createSymbolicLink(referenceData.resolve("absolute-link"), internalFile);

        assertThatThrownBy(() -> PhotonRuntimeMaterializer.materialize(
                referenceData, dataDirectory.resolve("runtime")))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("absolute symlinks");
        assertThat(dataDirectory.resolve("runtime")).doesNotExist();
    }

    @Test
    void fourIndependentJvmsCanRunPhotonOverTheSameShardFiles(@TempDir Path dataDirectory) throws Exception {
        final var server = makeServer(dataDirectory);
        try {
            addBerlinDocument(server);
        } finally {
            // The OpenSearch node must not be changing the files while the read-only
            // multi-process check is running.
            server.shutdown();
        }

        final var sourceData = dataDirectory.resolve("photon_data");
        makeImmutableLuceneIndexFilesReadOnly(sourceData);
        final var coordinationDirectory = Files.createDirectory(dataDirectory.resolve("reader-coordination"));
        final var processes = new ArrayList<Process>();
        try {
            for (int i = 0; i < READER_COUNT; i++) {
                processes.add(startReader(
                        sourceData,
                        dataDirectory.resolve("embedded-runtime-" + i),
                        coordinationDirectory,
                        Integer.toString(i)));
            }

            awaitMarkers(coordinationDirectory, "ready-", READER_COUNT);
            assertThat(readMarkers(coordinationDirectory, "ready-", READER_COUNT)).containsOnly("1");

            processes.getFirst().destroyForcibly();
            assertThat(processes.getFirst().waitFor(PROCESS_TIMEOUT.toSeconds(), TimeUnit.SECONDS)).isTrue();

            Files.createFile(coordinationDirectory.resolve("release"));
            for (Process process : processes.subList(1, processes.size())) {
                assertThat(process.waitFor(PROCESS_TIMEOUT.toSeconds(), TimeUnit.SECONDS)).isTrue();
                assertThat(process.exitValue()).isZero();
            }
            assertThat(List.of(
                    Files.readString(coordinationDirectory.resolve("complete-1")),
                    Files.readString(coordinationDirectory.resolve("complete-2")),
                    Files.readString(coordinationDirectory.resolve("complete-3"))))
                    .containsOnly("1");
        } finally {
            createMarkerIfAbsent(coordinationDirectory.resolve("release"));
            for (Process process : processes) {
                if (process.isAlive()) {
                    process.destroyForcibly();
                }
            }
        }
    }

    private static Server makeServer(Path dataDirectory) throws IOException {
        final var server = new Server(
                new PhotonDBConfig(dataDirectory.toString(), CLUSTER_NAME, List.of()), true);
        final var properties = new DatabaseProperties();
        properties.setImportDate(new Date());
        server.recreateIndex(properties, new PhotonDBLayoutConfig().getNormalizationFilters());
        server.refreshIndexes();
        return server;
    }

    private static OpenSearchRunner makeRunnerWithMultipleLocalStorageNodes(Path dataDirectory) {
        final var runner = new OpenSearchRunner();
        runner.onBuild((number, settingsBuilder) -> {
            settingsBuilder.put("http.cors.enabled", false);
            settingsBuilder.put("discovery.type", "single-node");
            settingsBuilder.putList("discovery.seed_hosts", "127.0.0.1:9201");
            settingsBuilder.put("indices.query.bool.max_clause_count", "30000");
            settingsBuilder.put("index.codec", "best_compression");
            settingsBuilder.put("node.max_local_storage_nodes", "4");
        }).build(OpenSearchRunner.newConfigs()
                .basePath(dataDirectory.resolve("photon_data").toString())
                .clusterName(CLUSTER_NAME)
                .numOfNode(1));
        return runner;
    }

    private static boolean searchBerlinWithEmbeddedNode(Server server) {
        final var response = server.getEmbeddedClient()
                .prepareSearch(PhotonIndex.NAME)
                .setQuery(QueryBuilders.matchQuery("collector.field.name", "berlin"))
                .setSize(1)
                .get();
        return response.getHits().getTotalHits().value() == 1
                && response.getHits().getHits().length == 1;
    }

    private static byte[] serializeQuery(Query query)
            throws IOException {
        final var output = new ByteArrayOutputStream();
        final var mapper = new JacksonJsonpMapper();
        try (var generator = mapper.jsonProvider().createGenerator(output)) {
            mapper.serialize(query, generator);
        }
        return output.toByteArray();
    }

    private static boolean isInsideLuceneIndex(Path source, Path path) {
        final var relative = source.relativize(path);
        for (var element : relative) {
            if (element.toString().equals("index")) {
                return true;
            }
        }
        return false;
    }

    private static boolean isImmutableLuceneIndexFile(Path source, Path path) {
        if (!isInsideLuceneIndex(source, path)) {
            return false;
        }

        final var name = path.getFileName().toString();
        return !name.endsWith(".lock") && !name.startsWith("pending_");
    }

    private static Path findFirstLuceneIndexFile(Path dataDirectory) throws IOException {
        try (var paths = Files.walk(dataDirectory)) {
            return paths
                    .filter(path -> Files.isRegularFile(path))
                    .filter(path -> isImmutableLuceneIndexFile(dataDirectory, path))
                    .findFirst()
                    .orElseThrow(() -> new IOException("No Lucene index files found under " + dataDirectory));
        }
    }

    private static void makeImmutableLuceneIndexFilesReadOnly(Path dataDirectory) throws IOException {
        final var readOnly = Set.of(
                PosixFilePermission.OWNER_READ,
                PosixFilePermission.GROUP_READ,
                PosixFilePermission.OTHERS_READ);
        try (var paths = Files.walk(dataDirectory)) {
            for (var path : paths
                    .filter(path -> Files.isRegularFile(path))
                    .filter(path -> isImmutableLuceneIndexFile(dataDirectory, path))
                    .toList()) {
                Files.setPosixFilePermissions(path, readOnly);
            }
        }
    }

    private static Map<Path, String> digestImmutableLuceneIndexFiles(Path dataDirectory) throws IOException {
        final var digests = new HashMap<Path, String>();
        try (var paths = Files.walk(dataDirectory)) {
            for (var path : paths
                    .filter(path -> Files.isRegularFile(path))
                    .filter(path -> isImmutableLuceneIndexFile(dataDirectory, path))
                    .toList()) {
                digests.put(dataDirectory.relativize(path), sha256(path));
            }
        }
        return Map.copyOf(digests);
    }

    private static String sha256(Path path) throws IOException {
        try {
            return java.util.HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(path)));
        } catch (NoSuchAlgorithmException e) {
            throw new AssertionError(e);
        }
    }

    private static void addBerlinDocument(Server server) throws IOException {
        final var importer = server.createImporter(new DatabaseProperties());
        var berlin = new PhotonDoc()
                .placeId("1000").osmType("N").osmId(1000).tagKey("place").tagValue("city")
                .categories(List.of("osm.place.city"))
                .importance(0.6).addressType(AddressType.CITY)
                .houseNumber("1")
                .countryCode("de")
                .addAddresses(Map.of("street", "Alexanderplatz", "city", "Berlin"), Set.of("en"))
                .centroid(new GeometryFactory().createPoint(new Coordinate(13.38886, 52.51704)))
                .names(NameMap.makeForPlace(Map.of("name", "berlin"), List.of("en", "de", "fr", "it")));
        berlin.setCountry(Map.of("default", "Germany"));
        importer.add(List.of(berlin));
        importer.finish();
        server.refreshIndexes();
    }

    private static void importAndorraFixture(Server server) throws IOException {
        final var importThread = new ImportThread(server.createImporter(new DatabaseProperties()));
        try (var fixture = SharedIndexReaderFeasibilityTest.class.getResourceAsStream(ANDORRA_FIXTURE)) {
            assertThat(fixture).as("the real Photon fixture must be on the test classpath").isNotNull();

            final var reader = new JsonReader(fixture);
            reader.setLanguages(Set.of("en"));
            reader.readHeader();
            reader.readFile(importThread);
        } finally {
            importThread.finish();
        }

        assertThat(importThread.hasErrors()).isFalse();
        server.refreshIndexes();
    }

    private static Process startReader(Path sourceDataDirectory, Path runtimeRoot,
                                       Path coordinationDirectory, String readerId) throws IOException {
        final var javaExecutable = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        return new ProcessBuilder(
                javaExecutable,
                "-Xms256m",
                "-Xmx512m",
                "-cp", System.getProperty("java.class.path"),
                SharedIndexReaderProcess.class.getName(),
                sourceDataDirectory.toString(),
                runtimeRoot.toString(),
                coordinationDirectory.toString(),
                readerId
        ).redirectErrorStream(true)
                .redirectOutput(coordinationDirectory.resolve("reader-" + readerId + ".log").toFile())
                .start();
    }

    private static void awaitMarkers(Path directory, String prefix, int count) throws IOException, InterruptedException {
        final long deadline = System.nanoTime() + PROCESS_TIMEOUT.toNanos();
        while (markerCount(directory, prefix) < count) {
            if (System.nanoTime() >= deadline) {
                throw new IOException("Timed out waiting for " + count + " " + prefix + " markers: "
                        + readerLogs(directory));
            }
            Thread.sleep(25);
        }
    }

    private static String readerLogs(Path directory) throws IOException {
        try (var paths = Files.list(directory)) {
            return paths
                    .filter(path -> path.getFileName().toString().startsWith("reader-")
                            && path.getFileName().toString().endsWith(".log"))
                    .sorted()
                    .map(path -> {
                        try {
                            return path.getFileName() + "=" + Files.readString(path);
                        } catch (IOException e) {
                            return path.getFileName() + "=<unreadable: " + e.getMessage() + ">";
                        }
                    })
                    .collect(Collectors.joining(" | "));
        }
    }

    private static List<String> readMarkers(Path directory, String prefix, int count) throws IOException {
        final var values = new ArrayList<String>();
        for (int i = 0; i < count; i++) {
            values.add(Files.readString(directory.resolve(prefix + i)));
        }
        return values;
    }

    private static long markerCount(Path directory, String prefix) throws IOException {
        try (Stream<Path> paths = Files.list(directory)) {
            return paths.filter(path -> path.getFileName().toString().startsWith(prefix)).count();
        }
    }

    private static void createMarkerIfAbsent(Path marker) throws IOException {
        if (!Files.exists(marker)) {
            Files.createFile(marker);
        }
    }

    private static String allMessages(Throwable error) {
        final var messages = new StringBuilder();
        for (Throwable current = error; current != null; current = current.getCause()) {
            messages.append(current).append('\n');
        }
        return messages.toString();
    }
}
