package de.komoot.photon;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.komoot.photon.config.PhotonDBConfig;
import de.komoot.photon.opensearch.IncompleteSearchException;
import de.komoot.photon.opensearch.OpenSearchResult;
import de.komoot.photon.opensearch.PhotonIndex;
import org.jspecify.annotations.NullMarked;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchType;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch._types.query_dsl.Query;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.transport.client.Client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Owns one embedded Photon runtime for a single job.
 *
 * <p>The search path uses the OpenSearch transport client directly. It does
 * not make a request to Photon's HTTP API, and closing this runtime only
 * closes the node owned by this instance.</p>
 */
@NullMarked
public final class EmbeddedPhotonRuntime implements AutoCloseable {
    private final Server server;
    private final Client client;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private volatile boolean closed;

    private EmbeddedPhotonRuntime(Server server) {
        this.server = server;
        this.client = server.getEmbeddedClient();
    }

    /**
     * Opens an embedded runtime over a prepared, private Photon data tree.
     *
     * @param config configuration pointing at this runtime's data directory
     * @throws IOException if the Photon data tree cannot be opened
     */
    public static EmbeddedPhotonRuntime open(PhotonDBConfig config) throws IOException {
        return new EmbeddedPhotonRuntime(new Server(config, false, true));
    }

    /**
     * Opens an embedded runtime whose data directory is below {@code dataDirectory}.
     */
    public static EmbeddedPhotonRuntime open(Path dataDirectory, String clusterName) throws IOException {
        return open(new PhotonDBConfig(dataDirectory.toString(), clusterName, List.of()));
    }

    /**
     * Returns whether the local Photon index is available to this runtime.
     */
    public boolean isReady() {
        if (closed) {
            return false;
        }
        try {
            return client.admin().indices().prepareExists(PhotonIndex.NAME).get().isExists();
        } catch (RuntimeException e) {
            return false;
        }
    }

    /**
     * Executes an existing Photon query without using the HTTP transport.
     *
     * @throws IncompleteSearchException if the backend timed out or did not
     *                                   return every shard successfully
     */
    public SearchResult search(Query query, int limit, Duration timeout) {
        if (closed) {
            throw new IllegalStateException("Photon runtime has been closed.");
        }
        if (limit < 0) {
            throw new IllegalArgumentException("Search limit cannot be negative.");
        }
        if (timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("Search timeout must be positive.");
        }

        final SearchResponse response;
        try {
            response = client.prepareSearch(PhotonIndex.NAME)
                    .setSearchType(SearchType.QUERY_THEN_FETCH)
                    .setQuery(QueryBuilders.wrapperQuery(serialize(query)))
                    .setSize(limit)
                    .setTrackTotalHits(true)
                    .setTimeout(new TimeValue(timeout.toMillis(), TimeUnit.MILLISECONDS))
                    .get();
        } catch (RuntimeException | IOException e) {
            throw new RuntimeException("Error executing embedded Photon search", e);
        }

        requireComplete(response);

        final var hits = response.getHits().getHits();
        final var results = new ArrayList<OpenSearchResult>(hits.length);
        for (var hit : hits) {
            if (!hit.hasSource()) {
                throw new IncompleteSearchException("Photon hit did not contain a source.");
            }
            try {
                final var result = objectMapper.readValue(hit.getSourceAsString(), OpenSearchResult.class);
                result.setOpensearchScore((double) hit.getScore());
                results.add(result);
            } catch (IOException e) {
                throw new IncompleteSearchException("Photon hit could not be decoded.", e);
            }
        }

        return new SearchResult(response.getHits().getTotalHits().value(), List.copyOf(results));
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        server.shutdown();
    }

    private static void requireComplete(SearchResponse response) {
        if (response.isTimedOut()
                || response.getFailedShards() > 0
                || response.getShardFailures().length > 0
                || response.getSuccessfulShards() + response.getSkippedShards() != response.getTotalShards()) {
            throw new IncompleteSearchException();
        }
    }

    private static byte[] serialize(Query query)
            throws IOException {
        final var mapper = new JacksonJsonpMapper();
        final var output = new ByteArrayOutputStream();
        try (var generator = mapper.jsonProvider().createGenerator(output)) {
            mapper.serialize(query, generator);
        }
        return output.toByteArray();
    }

    public record SearchResult(long totalHits, List<OpenSearchResult> hits) {
    }
}
