package com.kjetland.dropwizard.activemq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import io.dropwizard.ConfiguredBundle;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.jms.pool.PooledConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Message;

public class ActiveMQBundle implements ConfiguredBundle<ActiveMQConfigHolder>, Managed, ActiveMQSenderFactory {

    private final Logger log = LoggerFactory.getLogger(getClass());
    private ActiveMQConnectionFactory realConnectionFactory;
    private PooledConnectionFactory connectionFactory = null;
    private ObjectMapper objectMapper;
    private Environment environment;
    private long shutdownWaitInSeconds;
    private Optional<Integer> defaultTimeToLiveInSeconds;
    public static final ThreadLocal<String> correlationID = new ThreadLocal<>();

    public ActiveMQBundle() {

    }

    @Override
    public void run(ActiveMQConfigHolder configuration, Environment environment) throws Exception {
        this.environment = environment;
        final String brokerUrl = configuration.getActiveMQ().brokerUrl;
        final int configuredTTL = configuration.getActiveMQ().timeToLiveInSeconds;
        final Optional<String> username = Optional.fromNullable(configuration.getActiveMQ().brokerUsername);
        final Optional<String> password = Optional.fromNullable(configuration.getActiveMQ().brokerPassword);
        defaultTimeToLiveInSeconds = Optional.fromNullable(configuredTTL > 0 ? configuredTTL : null);

        log.info("Setting up activeMq with brokerUrl {}", brokerUrl);

        log.debug("All activeMQ config: " + configuration.getActiveMQ());

        realConnectionFactory = new ActiveMQConnectionFactory(brokerUrl);
        if (username.isPresent() && password.isPresent()) {
            realConnectionFactory.setUserName(username.get());
            realConnectionFactory.setPassword(password.get());
        }
        connectionFactory = new PooledConnectionFactory();
        connectionFactory.setConnectionFactory(realConnectionFactory);

        configurePool(configuration.getActiveMQ().pool);

        objectMapper = environment.getObjectMapper();

        environment.lifecycle().manage(this);

        // Must use realConnectionFactory instead of (pooled) connectionFactory for the healthCheck
        // Is needs its own connection since it is both sending and receiving.
        // If using pool, then it might block since no one is available..
        environment.healthChecks().register("ActiveMQ",
                new ActiveMQHealthCheck(
                        realConnectionFactory,
                        configuration.getActiveMQ().healthCheckMillisecondsToWait)
        );
        this.shutdownWaitInSeconds = configuration.getActiveMQ().shutdownWaitInSeconds;
    }

    private void configurePool(ActiveMQPoolConfig poolConfig) {
        if (poolConfig == null) {
            return;
        }

        if (poolConfig.maxConnections != null) {
            connectionFactory.setMaxConnections(poolConfig.maxConnections);
        }

        if (poolConfig.maximumActiveSessionPerConnection != null) {
            connectionFactory.setMaximumActiveSessionPerConnection(poolConfig.maximumActiveSessionPerConnection);
        }

        if (poolConfig.blockIfSessionPoolIsFull != null) {
            connectionFactory.setBlockIfSessionPoolIsFull(poolConfig.blockIfSessionPoolIsFull);
        }

        if (poolConfig.idleTimeoutMills != null) {
            connectionFactory.setIdleTimeout(poolConfig.idleTimeoutMills);
        }

        if (poolConfig.expiryTimeoutMills != null) {
            connectionFactory.setExpiryTimeout(poolConfig.expiryTimeoutMills);
        }

        if (poolConfig.createConnectionOnStartup != null) {
            connectionFactory.setCreateConnectionOnStartup(poolConfig.createConnectionOnStartup);
        }

        if (poolConfig.timeBetweenExpirationCheckMillis != null) {
            connectionFactory.setTimeBetweenExpirationCheckMillis(poolConfig.timeBetweenExpirationCheckMillis);
        }

    }

    @Override
    public void initialize(Bootstrap<?> bootstrap) {

    }

    @Override
    public void start() throws Exception {
        log.info("Starting activeMQ client");
        connectionFactory.start();
    }

    @Override
    public void stop() throws Exception {
        log.info("Stopping activeMQ client");
        connectionFactory.stop();
    }

    public ActiveMQSender createSender(String destination, boolean persistent) {
        return createSender(destination, persistent, defaultTimeToLiveInSeconds);
    }

    public ActiveMQSender createSender(String destination, boolean persistent, Optional<Integer> timeToLiveInSeconds) {
        return new ActiveMQSenderImpl(connectionFactory, objectMapper, destination, timeToLiveInSeconds, persistent);
    }

    // This must be used during run-phase
    public <T> void registerReceiver(String destination, ActiveMQReceiver<T> receiver, Class<? extends T> clazz,
                                     final boolean ackMessageOnException) {

        ActiveMQReceiverHandler<T> handler = new ActiveMQReceiverHandler<>(
                destination,
                connectionFactory,
                receiver,
                clazz,
                objectMapper,
                new ActiveMQExceptionHandler() {

                    @Override
                    public boolean onException(Message jmsMessage, String message, Exception exception) {
                        return onException(message, exception);
                    }

                    @Override
                    public boolean onException(String message, Exception exception) {
                        if(ackMessageOnException) {
                            log.error("Error processing received message - acknowledging it anyway", exception);
                            return true;
                        }else {
                            log.error("Error processing received message - NOT acknowledging it", exception);
                            return false;
                        }
                    }
                },
                shutdownWaitInSeconds
        );

        internalRegisterReceiver(destination, handler);
    }

    private <T> void internalRegisterReceiver(String destination, ActiveMQReceiverHandler<T> handler) {
        environment.lifecycle().manage(handler);
        environment.healthChecks().register("ActiveMQ receiver for " + destination, handler.getHealthCheck());
    }

    // This must be used during run-phase
    public <T> void registerReceiver(String destination, ActiveMQReceiver<T> receiver, Class<? extends T> clazz,
                                     ActiveMQBaseExceptionHandler exceptionHandler) {

        ActiveMQReceiverHandler<T> handler = new ActiveMQReceiverHandler<>(
                destination,
                connectionFactory,
                receiver,
                clazz,
                objectMapper,
                exceptionHandler,
                shutdownWaitInSeconds);

        internalRegisterReceiver(destination, handler);
    }

    // This must be used during run-phase
    public <T> void registerReceiver(String destination, ActiveMQReceiver<T> receiver, Class<? extends T> clazz,
                                     ActiveMQExceptionHandler exceptionHandler) {
        registerReceiver(destination, receiver, clazz, (ActiveMQBaseExceptionHandler) exceptionHandler);
    }
}
