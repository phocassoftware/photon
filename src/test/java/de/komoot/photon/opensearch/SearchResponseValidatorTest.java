package de.komoot.photon.opensearch;

import org.junit.jupiter.api.Test;
import org.opensearch.client.opensearch._types.ShardStatistics;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.opensearch.core.search.Hit;
import org.opensearch.client.opensearch.core.search.HitsMetadata;
import org.opensearch.client.opensearch.core.search.TotalHits;
import org.opensearch.client.opensearch.core.search.TotalHitsRelation;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SearchResponseValidatorTest {
    @Test
    void acceptsCompleteZeroHitResponse() {
        final var response = response(false, 1, 1, 0, 0, List.of());

        assertThat(SearchResponseValidator.requireComplete(response)).isSameAs(response);
    }

    @Test
    void acceptsCompleteResponseWithSkippedShards() {
        final var response = response(false, 2, 1, 0, 1, List.of());

        assertThat(SearchResponseValidator.requireComplete(response)).isSameAs(response);
    }

    @Test
    void rejectsTimedOutResponseWithNoHits() {
        assertIncomplete(response(true, 1, 1, 0, 0, List.of()));
    }

    @Test
    void rejectsTimedOutResponseWithHits() {
        assertIncomplete(response(true, 1, 1, 0, 0, List.of(hit("one"))));
    }

    @Test
    void rejectsResponseWithFailedShard() {
        assertIncomplete(response(false, 2, 1, 1, 0, List.of(hit("one"))));
    }

    @Test
    void rejectsResponseWithUnaccountedShard() {
        assertIncomplete(response(false, 2, 1, 0, 0, List.of()));
    }

    @Test
    void rejectsResponseWithTooManyAccountedShards() {
        assertIncomplete(response(false, 1, 2, 0, 0, List.of()));
    }

    private void assertIncomplete(SearchResponse<OpenSearchResult> response) {
        assertThatThrownBy(() -> SearchResponseValidator.requireComplete(response))
                .isInstanceOf(IncompleteSearchException.class)
                .hasMessage("Search backend returned an incomplete response.");
    }

    private SearchResponse<OpenSearchResult> response(boolean timedOut, int total, int successful,
                                                       int failed, int skipped,
                                                       List<Hit<OpenSearchResult>> hits) {
        return SearchResponse.searchResponseOf(builder -> builder
                .took(0)
                .timedOut(timedOut)
                .shards(ShardStatistics.of(shards -> shards
                        .total(total)
                        .successful(successful)
                        .failed(failed)
                        .skipped(skipped)))
                .hits(HitsMetadata.of(metadata -> metadata
                        .total(TotalHits.of(totalHits -> totalHits
                                .value(hits.size())
                                .relation(TotalHitsRelation.Eq)))
                        .hits(hits))));
    }

    private Hit<OpenSearchResult> hit(String id) {
        return Hit.of(hit -> hit.id(id));
    }
}
