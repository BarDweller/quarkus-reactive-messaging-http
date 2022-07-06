package io.quarkus.reactivemessaging.http.runtime;

import java.util.Optional;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Collectors;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;
import org.jboss.logging.Logger;

import io.quarkus.reactivemessaging.http.runtime.AuthenticatorProvider.Authenticator;
import io.quarkus.reactivemessaging.http.runtime.serializers.Serializer;
import io.quarkus.reactivemessaging.http.runtime.serializers.SerializerFactoryBase;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.MultiEmitter;
import io.smallrye.mutiny.vertx.AsyncResultUni;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.ext.web.RoutingContext;

public class BidiWebSocketNexus {

    private static final Logger log = Logger.getLogger(BidiWebSocketNexus.class);

    private final StrictQueueSizeGuard guard;
    private Multi<WebSocketMessage<?>> processor; // effectively final
    private MultiEmitter<? super WebSocketMessage<?>> emitter; // effectively final
    private String path;

    private final CopyOnWriteArraySet<ServerWebSocket> connections;

    private final SerializerFactoryBase serializerFactoryBase;
    private final Authenticator auth;

    public BidiWebSocketNexus(StrictQueueSizeGuard guard, SerializerFactoryBase serializerFactoryBase, Authenticator auth) {
        this.guard = guard;
        this.connections = new CopyOnWriteArraySet<>();
        this.serializerFactoryBase = serializerFactoryBase;
        this.auth = auth;
    }

    public void setProcessor(Multi<WebSocketMessage<?>> processor) {
        this.processor = processor;
    }

    public void setEmitter(MultiEmitter<? super WebSocketMessage<?>> emitter) {
        this.emitter = emitter;
    }

    public Multi<WebSocketMessage<?>> getProcessor() {
        return processor;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public void handle(RoutingContext event) {
        //event is an incoming websocket.. 
        // - accept it, and hook it's messages to the outbound channel
        if (auth != null) {
            if (!auth.allow(event)) {
                event.end();
                return;
            }
        }

        event.request().toWebSocket(
                webSocket -> {
                    if (webSocket.failed()) {
                        log.error("failed to connect web socket", webSocket.cause());
                    } else {
                        ServerWebSocket serverWebSocket = webSocket.result();

                        connections.add(serverWebSocket);

                        serverWebSocket.handler(
                                b -> {
                                    if (emitter == null) {
                                        onUnexpectedError(serverWebSocket, null,
                                                "No consumer subscribed for messages sent to " +
                                                        "Reactive Messaging WebSocket endpoint on path: " + path);
                                    } else if (guard.prepareToEmit()) {
                                        try {
                                            WebSocketMessage<Buffer> m = new WebSocketMessage<Buffer>(b,
                                                    new RequestMetadata(event.queryParams(), event.pathParams(),
                                                            event.normalizedPath()),
                                                    () -> {
                                                        /* success, nothing to do. */
                                                    },
                                                    error -> {
                                                        onUnexpectedError(serverWebSocket, error,
                                                                "Failed to process incoming web socket message.");
                                                    });

                                            emitter.emit(m);
                                        } catch (Exception error) {
                                            guard.dequeue();
                                            onUnexpectedError(serverWebSocket, error, "Emitting message failed");
                                        }
                                    } else {
                                        serverWebSocket.write(Buffer.buffer("BUFFER_OVERFLOW"));
                                    }
                                });
                    }
                });

    }

    private void onUnexpectedError(ServerWebSocket serverWebSocket, Throwable error, String message) {
        log.error(message, error);
        // TODO some error message for the client? exception mapper would be best...
        serverWebSocket.close((short) 3500, "Unexpected error while processing the message");
    }

    public PublisherBuilder<WebSocketMessage<?>> getPublisherBuilder() {
        return ReactiveStreams.fromPublisher(processor);
    }

    public SubscriberBuilder<? extends Message<?>, Void> getSubscriberBuilder() {
        //previously went to websocketsink (outbound websocket)
        //now needs to spray across all connected ServerWebSockets
        SubscriberBuilder<Message<?>, Void> subscriber = ReactiveStreams.<Message<?>> builder()
                .flatMapCompletionStage(m -> {
                    Uni<Void> send = send(m);

                    return send
                            .onItemOrFailure().transformToUni(
                                    (result, error) -> {
                                        if (error != null) {
                                            return Uni.createFrom().completionStage(m.nack(error).thenApply(x -> {
                                                log.debug("error responding", error);
                                                return m;
                                            }));
                                        }
                                        return Uni.createFrom().completionStage(m.ack().thenApply(x -> {
                                            log.debug("responded with success", error);
                                            return m;
                                        }));
                                    })
                            .subscribeAsCompletionStage();
                })
                .ignore();

        return subscriber;
    }

    private Uni<Void> send(Message<?> message) {
        Serializer<Object> serializer = serializerFactoryBase.getSerializer(null, message.getPayload());
        Buffer serialized = serializer.serialize(message.getPayload());

        return AsyncResultUni.toUni(
                handler -> {
                    //cleanup any dead sockets each time we send a message
                    connections.removeAll(connections.stream().filter(c -> c.isClosed()).collect(Collectors.toList()));
                    //send the packet to each sws
                    connections.stream()
                            //if we have request metdata present, we can filter sends by matching path
                            .filter(sws -> {
                                Optional<RequestMetadata> orm = message.getMetadata(RequestMetadata.class);
                                if (orm.isPresent()) {
                                    return sws.path().equals(orm.get().getInvokedPath());
                                } else {
                                    return true;
                                }
                            })
                            .forEach(sws -> {
                                System.out.println("sending to sws with path " + sws.path());
                                sws.writeTextMessage(serialized.toString(), writeResult -> {
                                    if (writeResult.succeeded()) {
                                        log.debug("success");
                                    } else {
                                        Throwable cause = writeResult.cause();
                                        log.debug("failure", cause);
                                    }
                                    handler.handle(writeResult);
                                });
                            });
                });
    }
}
