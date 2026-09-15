package de.komoot.photon.embedded;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PhotonDatasetPreparerTest {
    private static final Duration LOCK_WAIT = Duration.ofSeconds(5);
    private static final Duration POLL_INTERVAL = Duration.ofMillis(10);
    private static final int PROCESS_COUNT = 4;

    @Test
    void concurrentCallersMaterializeADatasetOnlyOnce(@TempDir Path cacheDirectory) throws Exception {
        final var preparer = new PhotonDatasetPreparer(cacheDirectory, LOCK_WAIT, POLL_INTERVAL);
        final var materializations = new AtomicInteger();
        final var executor = Executors.newFixedThreadPool(2);
        try {
            final var first = executor.submit(() -> preparer.prepare("world-1", staging -> {
                materializations.incrementAndGet();
                Thread.sleep(100);
                writeFixture(staging);
            }));
            final var second = executor.submit(() -> preparer.prepare("world-1", staging -> {
                materializations.incrementAndGet();
                writeFixture(staging);
            }));

            assertThat(first.get(LOCK_WAIT.toSeconds(), TimeUnit.SECONDS))
                    .isEqualTo(second.get(LOCK_WAIT.toSeconds(), TimeUnit.SECONDS));
            assertThat(materializations).hasValue(1);
            assertThat(Files.readString(first.get().resolve("marker"))).isEqualTo("ready");
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void completedDatasetIsReusedWithoutCallingTheSource(@TempDir Path cacheDirectory) throws Exception {
        final var preparer = new PhotonDatasetPreparer(cacheDirectory, LOCK_WAIT, POLL_INTERVAL);
        final var materializations = new AtomicInteger();
        final PhotonDatasetPreparer.DatasetSource source = staging -> {
            materializations.incrementAndGet();
            writeFixture(staging);
        };

        final var first = preparer.prepare("world-1", source);
        final var second = preparer.prepare("world-1", source);

        assertThat(second).isEqualTo(first);
        assertThat(materializations).hasValue(1);
    }

    @Test
    void failedPreparationDoesNotPublishTheDataset(@TempDir Path cacheDirectory) throws Exception {
        final var preparer = new PhotonDatasetPreparer(cacheDirectory, LOCK_WAIT, POLL_INTERVAL);

        assertThatThrownBy(() -> preparer.prepare("world-1", staging -> {
            Files.createDirectories(staging);
            throw new IOException("download failed");
        }))
                .isInstanceOf(IOException.class)
                .hasMessage("download failed");

        final var prepared = preparer.prepare("world-1", PhotonDatasetPreparerTest::writeFixture);
        assertThat(Files.exists(prepared.resolve("marker"))).isTrue();
        assertThat(Files.exists(cacheDirectory.resolve("datasets/world-1/.complete"))).isTrue();
    }

    @Test
    void rejectsPathTraversalDatasetIds(@TempDir Path cacheDirectory) {
        final var preparer = new PhotonDatasetPreparer(cacheDirectory, LOCK_WAIT, POLL_INTERVAL);

        assertThatThrownBy(() -> preparer.prepare("../world-1", PhotonDatasetPreparerTest::writeFixture))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void replacesAnIncompleteDatasetDirectory(@TempDir Path cacheDirectory) throws Exception {
        final var incompleteDataDirectory = cacheDirectory.resolve("datasets/world-1/photon_data");
        Files.createDirectories(incompleteDataDirectory);
        Files.writeString(incompleteDataDirectory.resolve("stale-file"), "incomplete");

        final var preparer = new PhotonDatasetPreparer(cacheDirectory, LOCK_WAIT, POLL_INTERVAL);
        final var prepared = preparer.prepare("world-1", PhotonDatasetPreparerTest::writeFixture);

        assertThat(Files.readString(prepared.resolve("marker"))).isEqualTo("ready");
        assertThat(prepared.resolve("stale-file")).doesNotExist();
    }

    @Test
    void independentProcessesMaterializeADatasetOnlyOnce(@TempDir Path cacheDirectory) throws Exception {
        final var coordinationDirectory = Files.createDirectory(cacheDirectory.resolve("coordination"));
        final var processes = new ArrayList<Process>();
        try {
            for (int i = 0; i < PROCESS_COUNT; i++) {
                processes.add(startPreparerProcess(cacheDirectory, coordinationDirectory, Integer.toString(i)));
            }

            awaitMarkers(coordinationDirectory, "ready-", PROCESS_COUNT);
            assertThat(sourceMarkerCount(coordinationDirectory)).isEqualTo(1);
            for (Process process : processes) {
                assertThat(process.waitFor(LOCK_WAIT.toSeconds(), TimeUnit.SECONDS)).isTrue();
                assertThat(process.exitValue()).isZero();
            }
        } finally {
            for (Process process : processes) {
                if (process.isAlive()) {
                    process.destroyForcibly();
                }
            }
        }
    }

    private static void writeFixture(Path stagingDataDirectory) throws IOException {
        Files.createDirectories(stagingDataDirectory);
        Files.writeString(stagingDataDirectory.resolve("marker"), "ready");
    }

    private static Process startPreparerProcess(Path cacheDirectory, Path coordinationDirectory,
                                                String workerId) throws IOException {
        final var javaExecutable = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        return new ProcessBuilder(
                javaExecutable,
                "-Xms64m",
                "-Xmx128m",
                "-cp", System.getProperty("java.class.path"),
                PhotonDatasetPreparerProcess.class.getName(),
                cacheDirectory.toString(),
                coordinationDirectory.toString(),
                workerId
        ).redirectErrorStream(true)
                .redirectOutput(coordinationDirectory.resolve("preparer-" + workerId + ".log").toFile())
                .start();
    }

    private static void awaitMarkers(Path directory, String prefix, int count)
            throws IOException, InterruptedException {
        final long deadline = System.nanoTime() + LOCK_WAIT.toNanos();
        while (markerCount(directory, prefix) < count) {
            if (System.nanoTime() >= deadline) {
                throw new IOException("Timed out waiting for " + prefix + " markers: " + directory);
            }
            Thread.sleep(25);
        }
    }

    private static long markerCount(Path directory, String prefix) throws IOException {
        try (var paths = Files.list(directory)) {
            return paths.filter(path -> path.getFileName().toString().startsWith(prefix)).count();
        }
    }

    private static long sourceMarkerCount(Path directory) throws IOException {
        return markerCount(directory, "source-");
    }
}
