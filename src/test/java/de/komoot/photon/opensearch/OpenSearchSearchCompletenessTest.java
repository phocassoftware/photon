package de.komoot.photon.opensearch;

import com.sun.net.httpserver.HttpServer;
import de.komoot.photon.query.ReverseRequest;
import de.komoot.photon.query.SimpleSearchRequest;
import de.komoot.photon.query.StructuredSearchRequest;
import org.apache.hc.core5.http.HttpHost;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OpenSearchSearchCompletenessTest {
    private static final String COMPLETE_ZERO_HITS = """
            {"took":1,"timed_out":false,"_shards":{"total":1,"successful":1,"skipped":0,"failed":0},"hits":{"total":{"value":0,"relation":"eq"},"max_score":null,"hits":[]}}
            """;
    private static final String TIMED_OUT_WITH_HITS = """
            {"took":1,"timed_out":true,"_shards":{"total":1,"successful":1,"skipped":0,"failed":0},"hits":{"total":{"value":1,"relation":"eq"},"max_score":1.0,"hits":[{"_index":"photon","_id":"1","_score":1.0,"_source":{}}]}}
            """;
    private static final String PARTIAL_WITHOUT_HITS = """
            {"took":1,"timed_out":false,"_shards":{"total":2,"successful":1,"skipped":0,"failed":0},"hits":{"total":{"value":0,"relation":"eq"},"max_score":null,"hits":[]}}
            """;
    private static final String PARTIAL_WITH_HITS = """
            {"took":1,"timed_out":false,"_shards":{"total":2,"successful":1,"skipped":0,"failed":0},"hits":{"total":{"value":1,"relation":"eq"},"max_score":1.0,"hits":[{"_index":"photon","_id":"1","_score":1.0,"_source":{}}]}}
            """;

    @Test
    void rejectsTimedOutForwardSearchBeforeFormattingOrFallback() throws Exception {
        try (var fixture = new SearchResponseFixture(List.of(TIMED_OUT_WITH_HITS))) {
            final var request = new SimpleSearchRequest();
            request.setQuery("address");

            assertIncomplete(() -> new OpenSearchSearchHandler(fixture.client(), 1).search(request).toList());
            assertThat(fixture.requestCount()).isEqualTo(1);
        }
    }

    @Test
    void rejectsEmptyPartialForwardSearch() throws Exception {
        try (var fixture = new SearchResponseFixture(List.of(PARTIAL_WITHOUT_HITS))) {
            final var request = new SimpleSearchRequest();
            request.setQuery("address");

            assertIncomplete(() -> new OpenSearchSearchHandler(fixture.client(), 1).search(request).toList());
        }
    }

    @Test
    void rejectsPartialStructuredFallbackWithHits() throws Exception {
        try (var fixture = new SearchResponseFixture(List.of(COMPLETE_ZERO_HITS, PARTIAL_WITH_HITS))) {
            final var request = new StructuredSearchRequest();
            request.setCity("Berlin");

            assertIncomplete(() -> new OpenSearchStructuredSearchHandler(fixture.client(), 1).search(request).toList());
            assertThat(fixture.requestCount()).isEqualTo(2);
        }
    }

    @Test
    void rejectsPartialStructuredStreetFallback() throws Exception {
        try (var fixture = new SearchResponseFixture(List.of(
                COMPLETE_ZERO_HITS, COMPLETE_ZERO_HITS, PARTIAL_WITHOUT_HITS))) {
            final var request = new StructuredSearchRequest();
            request.setCity("Berlin");
            request.setStreet("Main Street");
            request.setHouseNumber("1");

            assertIncomplete(() -> new OpenSearchStructuredSearchHandler(fixture.client(), 1).search(request).toList());
            assertThat(fixture.requestCount()).isEqualTo(3);
        }
    }

    @Test
    void rejectsPartialReverseSearch() throws Exception {
        try (var fixture = new SearchResponseFixture(List.of(PARTIAL_WITH_HITS))) {
            final var request = new ReverseRequest(new GeometryFactory().createPoint(new Coordinate(13.4, 52.5)));

            assertIncomplete(() -> new OpenSearchReverseHandler(fixture.client(), 1).search(request).toList());
        }
    }

    private void assertIncomplete(ThrowingCallable search) {
        assertThatThrownBy(search)
                .isInstanceOf(IncompleteSearchException.class)
                .hasMessage("Search backend returned an incomplete response.");
    }

    private static final class SearchResponseFixture implements AutoCloseable {
        private final HttpServer server;
        private final OpenSearchTransport transport;
        private final OpenSearchClient client;
        private final AtomicInteger requestCount = new AtomicInteger();
        private final List<String> responses;

        private SearchResponseFixture(List<String> responses) throws IOException {
            this.responses = responses;
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/", exchange -> {
                final int responseIndex = Math.min(requestCount.getAndIncrement(), this.responses.size() - 1);
                final byte[] body = this.responses.get(responseIndex).getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, body.length);
                try (OutputStream output = exchange.getResponseBody()) {
                    output.write(body);
                }
            });
            server.start();

            final var host = new HttpHost("http", "127.0.0.1", server.getAddress().getPort());
            transport = ApacheHttpClient5TransportBuilder.builder(host)
                    .setMapper(new JacksonJsonpMapper())
                    .build();
            client = new OpenSearchClient(transport);
        }

        private OpenSearchClient client() {
            return client;
        }

        private int requestCount() {
            return requestCount.get();
        }

        @Override
        public void close() throws IOException {
            transport.close();
            server.stop(0);
        }
    }
}
