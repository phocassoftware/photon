package de.komoot.photon.embedded;

import org.jspecify.annotations.NullMarked;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.Comparator;

/**
 * Prepares one immutable Photon dataset on a shared local filesystem.
 *
 * <p>Every job may call this class. A stable OS lock ensures that only one
 * caller materialises a missing dataset; other callers wait and recheck the
 * completion marker. The lock is held only while preparing and publishing the
 * dataset, not while a job is reading it.</p>
 */
@NullMarked
public final class PhotonDatasetPreparer {
    private static final String LOCK_FILE = ".photon-dataset-preparation.lock";
    private static final String DATASETS_DIRECTORY = "datasets";
    private static final String PHOTON_DATA_DIRECTORY = "photon_data";
    private static final String COMPLETION_MARKER = ".complete";

    private final Path cacheDirectory;
    private final Duration lockWaitTimeout;
    private final Duration pollInterval;

    public PhotonDatasetPreparer(Path cacheDirectory, Duration lockWaitTimeout, Duration pollInterval) {
        if (lockWaitTimeout.isNegative() || lockWaitTimeout.isZero()) {
            throw new IllegalArgumentException("Lock wait timeout must be positive.");
        }
        if (pollInterval.isNegative() || pollInterval.isZero()) {
            throw new IllegalArgumentException("Lock poll interval must be positive.");
        }
        this.cacheDirectory = cacheDirectory;
        this.lockWaitTimeout = lockWaitTimeout;
        this.pollInterval = pollInterval;
    }

    /**
     * Returns the prepared Photon data directory for {@code datasetId}.
     *
     * <p>The source must write a complete Photon data directory below the
     * supplied staging path. It is not allowed to modify a published dataset.
     * A source implementation can fetch from S3, while tests can use a local
     * fixture.</p>
     */
    public Path prepare(String datasetId, DatasetSource source) throws IOException, InterruptedException {
        validateDatasetId(datasetId);
        final var datasetsDirectory = cacheDirectory.resolve(DATASETS_DIRECTORY);
        final var datasetDirectory = datasetsDirectory.resolve(datasetId);
        final var dataDirectory = datasetDirectory.resolve(PHOTON_DATA_DIRECTORY);

        if (isComplete(datasetDirectory, dataDirectory, datasetId)) {
            return dataDirectory;
        }

        Files.createDirectories(datasetsDirectory);
        final var lockPath = cacheDirectory.resolve(LOCK_FILE);
        try (var channel = FileChannel.open(lockPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
             var lock = acquireLock(channel)) {
            if (lock == null) {
                throw new IOException("Timed out waiting for Photon dataset preparation lock.");
            }

            if (isComplete(datasetDirectory, dataDirectory, datasetId)) {
                return dataDirectory;
            }

            // A process can die after creating a dataset directory but before publishing
            // its completion marker. That directory is not usable and must not prevent a
            // later owner from publishing a fresh, complete dataset atomically.
            deleteTree(datasetDirectory);

            final var stagingDirectory = Files.createTempDirectory(datasetsDirectory, "." + datasetId + ".staging-");
            var published = false;
            try {
                source.materialize(stagingDirectory.resolve(PHOTON_DATA_DIRECTORY));
                final var stagedDataDirectory = stagingDirectory.resolve(PHOTON_DATA_DIRECTORY);
                if (!Files.isDirectory(stagedDataDirectory, LinkOption.NOFOLLOW_LINKS)) {
                    throw new IOException("Photon dataset source did not create photon_data.");
                }

                Files.writeString(
                        stagingDirectory.resolve(COMPLETION_MARKER),
                        datasetId,
                        StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE_NEW,
                        StandardOpenOption.WRITE);
                movePublished(stagingDirectory, datasetDirectory);
                published = true;
                return dataDirectory;
            } finally {
                if (!published) {
                    deleteTree(stagingDirectory);
                }
            }
        }
    }

    private FileLock acquireLock(FileChannel channel) throws IOException, InterruptedException {
        final long deadline = System.nanoTime() + lockWaitTimeout.toNanos();
        while (System.nanoTime() < deadline) {
            try {
                final var lock = channel.tryLock();
                if (lock != null) {
                    return lock;
                }
            } catch (OverlappingFileLockException ignored) {
                // Another caller in this JVM owns the same stable lock file.
            }
            Thread.sleep(Math.max(1, pollInterval.toMillis()));
        }
        return null;
    }

    private static boolean isComplete(Path datasetDirectory, Path dataDirectory, String datasetId) {
        final var marker = datasetDirectory.resolve(COMPLETION_MARKER);
        try {
            return Files.isDirectory(dataDirectory, LinkOption.NOFOLLOW_LINKS)
                    && Files.isRegularFile(marker, LinkOption.NOFOLLOW_LINKS)
                    && Files.readString(marker, StandardCharsets.UTF_8).equals(datasetId);
        } catch (IOException e) {
            return false;
        }
    }

    private static void movePublished(Path stagingDirectory, Path datasetDirectory) throws IOException {
        try {
            Files.move(stagingDirectory, datasetDirectory, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            throw new IOException("Photon dataset publication requires an atomic filesystem move.", e);
        }
    }

    private static void validateDatasetId(String datasetId) {
        if (datasetId.isBlank()
                || !datasetId.chars().allMatch(c -> Character.isLetterOrDigit(c) || c == '.' || c == '-' || c == '_')
                || datasetId.equals(".")
                || datasetId.equals("..")) {
            throw new IllegalArgumentException("Invalid Photon dataset id.");
        }
    }

    private static void deleteTree(Path root) throws IOException {
        if (!Files.exists(root, LinkOption.NOFOLLOW_LINKS)) {
            return;
        }
        try (var paths = Files.walk(root)) {
            for (var path : paths.sorted(Comparator.comparingInt(Path::getNameCount).reversed()).toList()) {
                try {
                    Files.deleteIfExists(path);
                } catch (DirectoryNotEmptyException e) {
                    throw new IOException("Could not clean incomplete Photon dataset staging directory.", e);
                }
            }
        }
    }

    @FunctionalInterface
    public interface DatasetSource {
        void materialize(Path stagingDataDirectory) throws IOException, InterruptedException;
    }
}
