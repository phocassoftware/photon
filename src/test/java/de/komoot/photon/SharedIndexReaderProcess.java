package de.komoot.photon;

import de.komoot.photon.embedded.PhotonRuntimeMaterializer;
import de.komoot.photon.opensearch.SearchQueryBuilder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;

/**
 * Child process used by {@link SharedIndexReaderFeasibilityTest}.
 *
 * Keeping this in a separate JVM makes the shared-file check meaningful: four independent
 * embedded Photon runtimes must open hard-linked shard files without sharing a Java heap or
 * an OpenSearch node directory.
 */
public final class SharedIndexReaderProcess {
    private static final Duration WAIT_TIMEOUT = Duration.ofSeconds(30);

    private SharedIndexReaderProcess() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            throw new IllegalArgumentException(
                    "Expected <source-data-dir> <runtime-root> <coordination-dir> <reader-id>");
        }

        final var sourceDataDirectory = Path.of(args[0]);
        final var runtimeRoot = Path.of(args[1]);
        final var coordinationDirectory = Path.of(args[2]);
        final var readerId = args[3];
        final var ready = coordinationDirectory.resolve("ready-" + readerId);
        final var complete = coordinationDirectory.resolve("complete-" + readerId);
        final var release = coordinationDirectory.resolve("release");

        PhotonRuntimeMaterializer.materialize(sourceDataDirectory, runtimeRoot.resolve("photon_data"));
        try (var runtime = EmbeddedPhotonRuntime.open(runtimeRoot, "photon-feasibility")) {
            final var query = new SearchQueryBuilder("1 Alexanderplatz Berlin", false, false).build();
            final var initialHits = runtime.search(query, 1, Duration.ofSeconds(10)).totalHits();
            writeMarker(ready, Long.toString(initialHits));

            waitFor(release);

            final var finalHits = runtime.search(query, 1, Duration.ofSeconds(10)).totalHits();
            writeMarker(complete, Long.toString(finalHits));
        }
    }

    private static void writeMarker(Path marker, String value) throws IOException {
        final var temporary = marker.resolveSibling("." + marker.getFileName() + ".tmp");
        Files.writeString(temporary, value);
        Files.move(temporary, marker, StandardCopyOption.ATOMIC_MOVE);
    }

    private static void waitFor(Path marker) throws IOException, InterruptedException {
        final long deadline = System.nanoTime() + WAIT_TIMEOUT.toNanos();
        while (!Files.exists(marker)) {
            if (System.nanoTime() >= deadline) {
                throw new IOException("Timed out waiting for " + marker);
            }
            Thread.sleep(25);
        }
    }
}
