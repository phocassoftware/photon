package de.komoot.photon;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.komoot.photon.config.PhotonDBConfig;
import de.komoot.photon.opensearch.IncompleteSearchException;
import de.komoot.photon.opensearch.DocFields;
import de.komoot.photon.opensearch.OpenSearchResult;
import de.komoot.photon.opensearch.PhotonIndex;
import de.komoot.photon.opensearch.SearchQueryBuilder;
import de.komoot.photon.query.StructuredSearchRequest;
import org.jspecify.annotations.Nullable;
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
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
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
    private static final float IMPORTANCE_FACTOR = 30.0f;

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
    SearchResult search(Query query, int limit, Duration timeout) {
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

    /**
     * Executes Photon's forward address search without using the HTTP API.
     *
     * <p>This keeps the query construction and strict-then-lenient fallback used by the normal
     * forward-search path, while returning only the fields needed by the geocoding consumer.</p>
     */
    public GeocodingSearchResult search(
            String query, List<String> countryCodes, int limit, Duration timeout) {
        if (query == null || query.isBlank()) {
            throw new IllegalArgumentException("Search query must not be blank.");
        }
        if (countryCodes == null) {
            throw new IllegalArgumentException("Country codes must not be null.");
        }
        if (limit <= 0) {
            throw new IllegalArgumentException("Search limit must be positive.");
        }

        final int candidateLimit = (int) Math.round(Math.max(6, limit) * 1.5);
        var strict = searchAddressQuery(query, countryCodes, false, candidateLimit, timeout);
        if (strict.totalHits() == 0) {
            strict = searchAddressQuery(query, countryCodes, true, candidateLimit, timeout);
        }

        return new GeocodingSearchResult(
                strict.totalHits(),
                strict.hits().stream()
                        .limit(limit)
                        .map(EmbeddedPhotonRuntime::toGeocodingHit)
                        .toList());
    }

    /**
     * Executes Photon's structured forward address search without using the HTTP API.
     *
     * <p>This follows the same field-specific query construction and strict-then-lenient fallback
     * as Photon's {@code /structured} endpoint.</p>
     */
    public GeocodingSearchResult searchStructured(
            StructuredSearchRequest request, int limit, Duration timeout) {
        if (request == null) {
            throw new IllegalArgumentException("Structured search request must not be null.");
        }
        if (limit <= 0) {
            throw new IllegalArgumentException("Search limit must be positive.");
        }

        final int candidateLimit = limit > 1 ? (int) Math.round(limit * 1.5) : 1;
        var results = searchStructuredAddressQuery(request, false, candidateLimit, timeout);
        if (results.totalHits() == 0) {
            results = searchStructuredAddressQuery(request, true, candidateLimit, timeout);

            if (results.totalHits() == 0 && request.hasStreet()) {
                final var street = request.getStreet();
                final var houseNumber = request.getHouseNumber();
                request.setStreet(null);
                request.setHouseNumber(null);
                try {
                    results = searchStructuredAddressQuery(request, true, candidateLimit, timeout);
                } finally {
                    request.setStreet(street);
                    request.setHouseNumber(houseNumber);
                }
            }
        }

        return new GeocodingSearchResult(
                results.totalHits(),
                results.hits().stream()
                        .limit(limit)
                        .map(EmbeddedPhotonRuntime::toGeocodingHit)
                        .toList());
    }

    private SearchResult searchAddressQuery(
            String query, List<String> countryCodes, boolean lenient, int limit, Duration timeout) {
        var queryBuilder = new SearchQueryBuilder(query, lenient, false);
        queryBuilder.addCountryCodeFilter(countryCodes);
        queryBuilder.addImportance(IMPORTANCE_FACTOR);
        return search(queryBuilder.build(), limit, timeout);
    }

    private SearchResult searchStructuredAddressQuery(
            StructuredSearchRequest request, boolean lenient, int limit, Duration timeout) {
        var queryBuilder = new SearchQueryBuilder(request, lenient);
        queryBuilder.addOsmTagFilter(request.getOsmTagFilters());
        queryBuilder.addLayerFilter(request.getLayerFilters());

        if (request.hasLocationBias()) {
            assert request.getLocationForBias() != null;
            queryBuilder.addLocationBias(
                    request.getLocationForBias(),
                    30.0f * (1.0f - request.getImportanceWeight()),
                    request.getRadiusForBias(),
                    request.getDecayRadiusForBias());
        }

        queryBuilder.includeCategories(request.getIncludeCategories());
        queryBuilder.excludeCategories(request.getExcludeCategories());
        queryBuilder.addBoundingBox(request.getBbox());
        return search(queryBuilder.build(), limit, timeout);
    }

    private static SearchHit toGeocodingHit(OpenSearchResult result) {
        var coordinates = result.getCoordinates();
        if (coordinates == OpenSearchResult.INVALID_COORDINATES) {
            throw new IncompleteSearchException("Photon result did not contain coordinates.");
        }
        return new SearchHit(
                coordinates[1],
                coordinates[0],
                normalizeCountryCode(asString(result.get(DocFields.COUNTRYCODE))),
                formattedAddress(result));
    }

    private static @Nullable String normalizeCountryCode(@Nullable String countryCode) {
        return countryCode == null ? null : countryCode.toUpperCase(Locale.ROOT);
    }

    private static @Nullable String formattedAddress(OpenSearchResult result) {
        var parts = new ArrayList<String>();
        addDistinct(parts, result.getLocalised(DocFields.NAME, "default"));

        var houseNumber = asString(result.get(DocFields.HOUSENUMBER));
        var street = result.getLocalised(DocFields.STREET, "default");
        addDistinct(parts, joinNonBlank(" ", houseNumber, street));

        addDistinct(parts, result.getLocalised(DocFields.LOCALITY, "default"));
        addDistinct(parts, result.getLocalised(DocFields.DISTRICT, "default"));
        addDistinct(parts, result.getLocalised(DocFields.CITY, "default"));
        addDistinct(parts, result.getLocalised(DocFields.COUNTY, "default"));
        addDistinct(parts, result.getLocalised(DocFields.STATE, "default"));
        addDistinct(parts, asString(result.get(DocFields.POSTCODE)));
        addDistinct(parts, result.getLocalised(DocFields.COUNTRY, "default"));

        return parts.isEmpty() ? null : String.join(", ", parts);
    }

    private static @Nullable String asString(@Nullable Object value) {
        if (!(value instanceof String string) || string.isBlank()) {
            return null;
        }
        return string.strip();
    }

    private static @Nullable String joinNonBlank(String delimiter, String... values) {
        var nonBlank = Arrays.stream(values)
                .filter(value -> value != null && !value.isBlank())
                .toList();
        return nonBlank.isEmpty() ? null : String.join(delimiter, nonBlank);
    }

    private static void addDistinct(List<String> parts, @Nullable String value) {
        if (value == null || value.isBlank()
                || parts.stream().anyMatch(existing -> existing.equalsIgnoreCase(value.strip()))) {
            return;
        }
        parts.add(value.strip());
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

    record SearchResult(long totalHits, List<OpenSearchResult> hits) {
    }

    public record GeocodingSearchResult(long totalHits, List<SearchHit> hits) {
        public GeocodingSearchResult {
            hits = List.copyOf(hits);
        }
    }

    public record SearchHit(
            double latitude,
            double longitude,
            @Nullable String countryCode,
            @Nullable String formattedAddress) {
    }
}
