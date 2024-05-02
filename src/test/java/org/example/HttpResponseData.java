package org.example;

/**
 * A class representing an HTTP response with a status code and a response body.
 */
public class HttpResponseData {
    /**
     * The HTTP status code of the response.
     */
    private final int statusCode;
    /**
     * The response body of the HTTP response.
     */
    private final String responseBody;
    /**
     * Constructs an HttpResponseData object with the given status code and response body.
     *
     * @param statusCode the HTTP status code of the response
     * @param responseBody the response body of the HTTP response
     */
    public HttpResponseData(int statusCode, String responseBody) {
        this.statusCode = statusCode;
        this.responseBody = responseBody;
    }
    /**
     * Gets the HTTP status code of the response.
     *
     * @return the HTTP status code of the response
     */
    public int getStatusCode() {
        return statusCode;
    }
    /**
     * Gets the response body of the HTTP response.
     *
     * @return the response body of the HTTP response
     */
    public String getResponseBody() {
        return responseBody;
    }
}
