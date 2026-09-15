package de.komoot.photon.embedded;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;

/**
 * Child process used by {@link PhotonDatasetPreparerTest} to exercise the OS-backed lock.
 */
public final class PhotonDatasetPreparerProcess {
    private static final Duration LOCK_WAIT = Duration.ofSeconds(5);
    private static final Duration POLL_INTERVAL = Duration.ofMillis(10);

    private PhotonDatasetPreparerProcess() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            throw new IllegalArgumentException("Expected <cache-dir> <coordination-dir> <worker-id>");
        }

        final var cacheDirectory = Path.of(args[0]);
        final var coordinationDirectory = Path.of(args[1]);
        final var workerId = args[2];
        final var ready = coordinationDirectory.resolve("ready-" + workerId);
        final var sourceMarker = coordinationDirectory.resolve("source-" + workerId);

        final var preparer = new PhotonDatasetPreparer(cacheDirectory, LOCK_WAIT, POLL_INTERVAL);
        final var prepared = preparer.prepare("world-1", staging -> {
            Files.createFile(sourceMarker);
            Thread.sleep(250);
            Files.createDirectories(staging);
            Files.writeString(staging.resolve("marker"), "ready");
        });

        if (!Files.readString(prepared.resolve("marker")).equals("ready")) {
            throw new IOException("Prepared dataset is incomplete.");
        }
        writeMarker(ready);
    }

    private static void writeMarker(Path marker) throws IOException {
        final var temporary = marker.resolveSibling("." + marker.getFileName() + ".tmp");
        Files.writeString(temporary, "1");
        Files.move(temporary, marker, StandardCopyOption.ATOMIC_MOVE);
    }
}
