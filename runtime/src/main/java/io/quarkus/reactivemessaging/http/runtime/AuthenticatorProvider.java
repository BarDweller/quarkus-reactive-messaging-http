package io.quarkus.reactivemessaging.http.runtime;

import javax.enterprise.context.Dependent;
import javax.enterprise.inject.Produces;

import io.quarkus.arc.DefaultBean;
import io.vertx.ext.web.RoutingContext;

@Dependent
public class AuthenticatorProvider {
    public interface Authenticator {
        /**
         * Examine incoming request and approve/deny it for use.
         * 
         * @param event
         * @return true if allowed, false otherwise
         */
        public boolean allow(RoutingContext event);
    }

    public static final Authenticator ALLOW_ALL = new Authenticator() {
        public boolean allow(RoutingContext rc) {
            return true;
        }
    };

    @Produces
    @DefaultBean
    public Authenticator authenticator() {
        return ALLOW_ALL;
    }
}
