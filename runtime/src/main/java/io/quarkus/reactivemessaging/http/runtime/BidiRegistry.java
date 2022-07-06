package io.quarkus.reactivemessaging.http.runtime;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Singleton;

import io.quarkus.reactivemessaging.http.runtime.AuthenticatorProvider.Authenticator;
import io.quarkus.reactivemessaging.http.runtime.config.ReactiveHttpConfig;
import io.quarkus.reactivemessaging.http.runtime.config.WebSocketStreamConfig;
import io.quarkus.reactivemessaging.http.runtime.serializers.SerializerFactoryBase;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.subscription.BackPressureStrategy;

@Singleton
public class BidiRegistry {

    @Inject
    ReactiveHttpConfig config;

    @Inject
    SerializerFactoryBase serializerFactory;

    @Inject
    Authenticator auth;

    private final Map<String, BidiWebSocketNexus> knownNexuses = new HashMap<>();

    public BidiWebSocketNexus getNexusForKey(String key) {
        BidiWebSocketNexus result = knownNexuses.get(key);
        if (result == null) {
            throw new IllegalStateException("No incoming stream defined for path " + key);
        }
        return result;
    }

    public void addNexus(String key, BidiWebSocketNexus nexus) {
        knownNexuses.put(key, nexus);
    }

    @PostConstruct
    void init() {
        configs().forEach(this::addProcessor);
    }

    protected List<WebSocketStreamConfig> configs() {
        return config.getBidiWebSocketConfigs();
    }

    private void addProcessor(WebSocketStreamConfig streamConfig) {
        StrictQueueSizeGuard guard = new StrictQueueSizeGuard(streamConfig.bufferSize);

        BidiWebSocketNexus nexus = new BidiWebSocketNexus(guard, serializerFactory, auth);

        Multi<WebSocketMessage<?>> processor = Multi.createFrom()
                // emitter with an unbounded queue, we control the size ourselves, with the guard
                .<WebSocketMessage<?>> emitter(nexus::setEmitter, BackPressureStrategy.BUFFER)
                .onItem().invoke(guard::dequeue);
        nexus.setProcessor(processor);
        nexus.setPath(streamConfig.path);

        BidiWebSocketNexus previousNexus = knownNexuses.put(streamConfig.path, nexus);

        if (previousNexus != null) {
            throw new IllegalStateException("Duplicate incoming streams defined for " + streamConfig.path);
        }
    }
}
