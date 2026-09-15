package de.komoot.photon.opensearch;

import org.jspecify.annotations.NullMarked;
import org.opensearch.client.opensearch.core.SearchResponse;

/**
 * Validates the completeness metadata returned by OpenSearch searches.
 */
@NullMarked
public final class SearchResponseValidator {
    private SearchResponseValidator() {
    }

    public static <TDocument> SearchResponse<TDocument> requireComplete(SearchResponse<TDocument> response) {
        if (response.timedOut()) {
            throw new IncompleteSearchException();
        }

        final var shards = response.shards();
        if (shards == null
                || shards.failed() > 0
                || (shards.failures() != null && !shards.failures().isEmpty())
                || shards.successful() + (shards.skipped() == null ? 0 : shards.skipped()) != shards.total()) {
            throw new IncompleteSearchException();
        }

        return response;
    }
}
