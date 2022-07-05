package io.quarkus.reactivemessaging.http.runtime;

import io.vertx.core.Handler;
import io.vertx.ext.web.RoutingContext;

public class BidiWebSocketHandler implements Handler<RoutingContext> {

    private final BidiRegistry br;

    BidiWebSocketHandler(BidiRegistry br) {
        this.br = br;
    }

    @Override
    public void handle(RoutingContext event) {
        String key = event.currentRoute().getPath();
        BidiWebSocketNexus n = br.getNexusForKey(key);
        if (n != null) {
            n.handle(event);
        } else {
            event.response().setStatusCode(404).end();
        }
    }

}
