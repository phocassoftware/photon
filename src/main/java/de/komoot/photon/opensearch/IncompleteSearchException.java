package de.komoot.photon.opensearch;

/**
 * Indicates that the search backend did not return a complete response.
 *
 * <p>This is a service failure rather than a valid no-match result. Callers
 * may retry the request once the backend is healthy again.</p>
 */
public final class IncompleteSearchException extends RuntimeException {
    public static final String CODE = "INCOMPLETE_SEARCH";

    public IncompleteSearchException() {
        super("Search backend returned an incomplete response.");
    }

    public IncompleteSearchException(String message) {
        super(message);
    }

    public IncompleteSearchException(String message, Throwable cause) {
        super(message, cause);
    }

    public int getHttpStatus() {
        return 503;
    }
}
