package org.example;

public class HttpResponseData {
    private final int statusCode;
    private final String responseBody;

    public HttpResponseData(int statusCode, String responseBody) {
        this.statusCode = statusCode;
        this.responseBody = responseBody;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public String getResponseBody() {
        return responseBody;
    }
}
