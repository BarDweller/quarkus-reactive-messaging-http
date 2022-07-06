package io.quarkus.reactivemessaging.http.runtime;

import static io.smallrye.reactive.messaging.annotations.ConnectorAttribute.Direction.INCOMING;
import static io.smallrye.reactive.messaging.annotations.ConnectorAttribute.Direction.OUTGOING;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.eclipse.microprofile.reactive.messaging.spi.IncomingConnectorFactory;
import org.eclipse.microprofile.reactive.messaging.spi.OutgoingConnectorFactory;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;
import org.jboss.logging.Logger;

import io.smallrye.reactive.messaging.annotations.ConnectorAttribute;

/**
 * Quarkus-specific reactive messaging connector for Bidirectional WebSockets
 */
@ConnectorAttribute(name = "incoming-path", type = "string", direction = OUTGOING, description = "The path of the corresponding incoming websocket", mandatory = true)
@ConnectorAttribute(name = "serializer", type = "string", direction = OUTGOING, description = "Message serializer")

@ConnectorAttribute(name = "path", type = "string", direction = INCOMING, description = "The path of the endpoint", mandatory = true)
@ConnectorAttribute(name = "buffer-size", type = "string", direction = INCOMING, description = "Web socket endpoint buffers messages if a consumer is not able to keep up. This setting specifies the size of the buffer.", defaultValue = QuarkusHttpConnector.DEFAULT_SOURCE_BUFFER_STR)

@Connector(QuarkusBidiWebSocketConnector.NAME)
@ApplicationScoped
public class QuarkusBidiWebSocketConnector implements IncomingConnectorFactory, OutgoingConnectorFactory {
    private static final Logger log = Logger.getLogger(QuarkusHttpConnector.class);

    static final String DEFAULT_SOURCE_BUFFER_STR = "8";

    public static final Integer DEFAULT_SOURCE_BUFFER = Integer.valueOf(DEFAULT_SOURCE_BUFFER_STR);

    public static final String NAME = "quarkus-bidi-websocket";

    @Inject
    BidiRegistry registry;

    @Override
    public PublisherBuilder<WebSocketMessage<?>> getPublisherBuilder(Config configuration) {
        QuarkusBidiWebSocketConnectorIncomingConfiguration config = new QuarkusBidiWebSocketConnectorIncomingConfiguration(
                configuration);
        String path = config.getPath();
        BidiWebSocketNexus nexus = registry.getNexusForKey(path);

        return nexus.getPublisherBuilder();
    }

    @Override
    public SubscriberBuilder<? extends Message<?>, Void> getSubscriberBuilder(Config configuration) {
        QuarkusBidiWebSocketConnectorOutgoingConfiguration config = new QuarkusBidiWebSocketConnectorOutgoingConfiguration(
                configuration);

        String path = config.getIncomingPath();
        BidiWebSocketNexus nexus = registry.getNexusForKey(path);

        return nexus.getSubscriberBuilder();
    }
}
