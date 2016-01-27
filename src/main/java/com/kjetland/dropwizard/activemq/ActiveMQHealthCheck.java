package com.kjetland.dropwizard.activemq;

import com.codahale.metrics.health.HealthCheck;

import javax.jms.*;

public class ActiveMQHealthCheck extends HealthCheck {

    private ConnectionFactory connectionFactory;
    private long millisecondsToWait;

    public ActiveMQHealthCheck(ConnectionFactory connectionFactory, long millisecondsToWait) {

        this.connectionFactory = connectionFactory;
        this.millisecondsToWait = millisecondsToWait;
    }

    @Override
    protected Result check() throws Exception {

        final Connection connection = connectionFactory.createConnection();
        connection.start();
        try {
            final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            try {
                final TemporaryQueue tempQueue = session.createTemporaryQueue();

                try {
                    final MessageProducer producer = session.createProducer(tempQueue);
                    try {
                        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

                        final String messageText = "Test message-" + System.currentTimeMillis();

                        producer.send(tempQueue, session.createTextMessage(messageText));

                        final MessageConsumer consumer = session.createConsumer(tempQueue);

                        try {
                            // Wait for our testMessage
                            TextMessage receivedMessage = (TextMessage)consumer.receive(millisecondsToWait);

                            // Make sure we received the correct message
                            if (receivedMessage != null && messageText.equals(receivedMessage.getText())) {
                                return Result.healthy();
                            } else {
                                return Result.unhealthy("Did not receive testMessage via tempQueue in " +
                                        millisecondsToWait + " milliseconds");
                            }
                        } finally {
                            swallowException(new DoCleanup(){
                                @Override
                                public void doCleanup() throws Exception {
                                    consumer.close();
                                }
                            });
                        }
                    } finally {
                        swallowException(new DoCleanup(){
                            @Override
                            public void doCleanup() throws Exception {
                                producer.close();
                            }
                        } );
                    }
                } finally {
                    swallowException(new DoCleanup(){
                        @Override
                        public void doCleanup() throws Exception {
                            tempQueue.delete();
                        }
                    } );
                }
            } finally {
                swallowException(new DoCleanup(){
                    @Override
                    public void doCleanup() throws Exception {
                        session.close();
                    }
                });
            }
        } finally {
            swallowException(new DoCleanup(){
                @Override
                public void doCleanup() throws Exception {
                    connection.close();
                }
            });
        }
    }

    protected interface DoCleanup {
        void doCleanup() throws Exception;
    }

    protected void swallowException(DoCleanup doCleanup) {
        try {
            doCleanup.doCleanup();
        } catch(Exception e) {
            // do nothing about it
        }
    }
}
