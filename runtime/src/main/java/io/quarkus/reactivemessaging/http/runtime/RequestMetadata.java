package io.quarkus.reactivemessaging.http.runtime;

import java.util.Map;

import io.vertx.core.MultiMap;

public class RequestMetadata {
    private final MultiMap queryParams;
    private final Map<String, String> pathParams;
    private final String invokedPath;

    public RequestMetadata(MultiMap queryParams,
            Map<String, String> pathParams,
            String invokedPath) {
        this.queryParams = queryParams;
        this.pathParams = pathParams;
        this.invokedPath = invokedPath;
    }

    public MultiMap getQueryParams() {
        return queryParams;
    }

    public Map<String, String> getPathParams() {
        return pathParams;
    }

    public String getInvokedPath() {
        return invokedPath;
    }
}
