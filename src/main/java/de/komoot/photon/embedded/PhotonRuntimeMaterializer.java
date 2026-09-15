package de.komoot.photon.embedded;

import org.jspecify.annotations.NullMarked;

import java.io.IOException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Set;

/**
 * Creates a private OpenSearch runtime tree over an immutable Photon dataset.
 *
 * <p>Node metadata and transient files are copied so each runtime owns its
 * node lock and writable state. Immutable Lucene index files are hard-linked
 * so the runtimes address the same file-backed pages. The reference dataset
 * must already be read-only; this class never changes its permissions.</p>
 */
@NullMarked
public final class PhotonRuntimeMaterializer {
    private PhotonRuntimeMaterializer() {
    }

    /**
     * Materializes {@code runtimeDataDirectory} from a prepared Photon data
     * directory. Both paths must be on the same filesystem when the dataset
     * contains Lucene index files.
     *
     * @param referenceDataDirectory immutable prepared Photon data directory
     * @param runtimeDataDirectory new private data directory for one runtime
     * @throws IOException if the source is incomplete, the target exists, a
     *                     shared file is writable, or a hard link cannot be created
     */
    public static void materialize(Path referenceDataDirectory, Path runtimeDataDirectory) throws IOException {
        if (!Files.isDirectory(referenceDataDirectory, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("Photon reference data directory does not exist: " + referenceDataDirectory);
        }
        if (Files.exists(runtimeDataDirectory, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("Photon runtime data directory already exists: " + runtimeDataDirectory);
        }
        var realReference = referenceDataDirectory.toRealPath();
        var realRuntime = resolveAgainstRealParent(runtimeDataDirectory);
        if (overlaps(realReference, realRuntime)) {
            throw new IOException("Photon reference and runtime data directories must be separate trees.");
        }

        var complete = false;
        try {
            Files.createDirectories(runtimeDataDirectory);
            if (overlaps(realReference, runtimeDataDirectory.toRealPath())) {
                throw new IOException("Photon reference and runtime data directories must be separate trees.");
            }
            try (var paths = Files.walk(referenceDataDirectory)) {
                for (var source : paths.sorted(Comparator.comparingInt(Path::getNameCount)).toList()) {
                    var target = runtimeDataDirectory.resolve(referenceDataDirectory.relativize(source));
                    copyEntry(referenceDataDirectory, source, target);
                }
            }
            complete = true;
        } finally {
            if (!complete) {
                deleteTree(runtimeDataDirectory);
            }
        }
    }

    private static Path resolveAgainstRealParent(Path path) throws IOException {
        var absolute = path.toAbsolutePath().normalize();
        var missing = new ArrayList<String>();
        var existing = absolute;
        while (!Files.exists(existing, LinkOption.NOFOLLOW_LINKS)) {
            var name = existing.getFileName();
            if (name == null) {
                throw new IOException("Photon runtime path has no existing parent: " + path);
            }
            missing.add(name.toString());
            existing = existing.getParent();
        }

        var resolved = existing.toRealPath();
        for (var i = missing.size() - 1; i >= 0; i--) {
            resolved = resolved.resolve(missing.get(i));
        }
        return resolved.normalize();
    }

    private static boolean overlaps(Path first, Path second) {
        return first.equals(second) || first.startsWith(second) || second.startsWith(first);
    }

    private static void copyEntry(Path referenceRoot, Path source, Path target) throws IOException {
        if (Files.isSymbolicLink(source)) {
            var link = Files.readSymbolicLink(source);
            var linkTarget = source.getParent().resolve(link).normalize();
            var realLinkTarget = resolveAgainstRealParent(linkTarget);
            if (!realLinkTarget.startsWith(referenceRoot.toRealPath())) {
                throw new IOException("Photon dataset symlink escapes the reference data directory: " + source);
            }
            if (link.isAbsolute()) {
                throw new IOException("Photon dataset must not contain absolute symlinks: " + source);
            }
            Files.createSymbolicLink(target, link);
        } else if (Files.isDirectory(source, LinkOption.NOFOLLOW_LINKS)) {
            Files.createDirectories(target);
        } else if (Files.isRegularFile(source, LinkOption.NOFOLLOW_LINKS)) {
            if (isImmutableLuceneFile(referenceRoot, source)) {
                requireReadOnly(source);
                try {
                    Files.createLink(target, source);
                } catch (FileSystemException e) {
                    throw new IOException(
                            "Photon reference and runtime data must be on the same filesystem for hard links",
                            e);
                }
            } else {
                Files.copy(source, target);
            }
        } else {
            throw new IOException("Unsupported Photon data entry: " + source);
        }
    }

    private static boolean isImmutableLuceneFile(Path referenceRoot, Path path) {
        var relative = referenceRoot.relativize(path);
        var insideIndex = false;
        for (var element : relative) {
            insideIndex |= element.toString().equals("index");
        }
        if (!insideIndex) {
            return false;
        }

        var name = path.getFileName().toString();
        return !name.endsWith(".lock") && !name.startsWith("pending_");
    }

    private static void requireReadOnly(Path path) throws IOException {
        try {
            var permissions = Files.getPosixFilePermissions(path, LinkOption.NOFOLLOW_LINKS);
            var writePermissions = Set.of(
                    PosixFilePermission.OWNER_WRITE,
                    PosixFilePermission.GROUP_WRITE,
                    PosixFilePermission.OTHERS_WRITE);
            if (permissions.stream().anyMatch(writePermissions::contains)) {
                throw new IOException("Shared Photon Lucene file is writable: " + path);
            }
        } catch (UnsupportedOperationException e) {
            throw new IOException("Cannot verify read-only permissions for shared Photon Lucene file: " + path, e);
        }
    }

    private static void deleteTree(Path root) throws IOException {
        if (!Files.exists(root, LinkOption.NOFOLLOW_LINKS)) {
            return;
        }
        try (var paths = Files.walk(root)) {
            for (var path : paths.sorted(Comparator.comparingInt(Path::getNameCount).reversed()).toList()) {
                Files.deleteIfExists(path);
            }
        }
    }
}
