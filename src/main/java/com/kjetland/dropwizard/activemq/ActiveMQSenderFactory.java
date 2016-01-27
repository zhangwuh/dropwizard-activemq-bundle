package com.kjetland.dropwizard.activemq;


import com.google.common.base.Optional;

public interface ActiveMQSenderFactory {
    ActiveMQSender createSender(String destination, boolean persistent);

    ActiveMQSender createSender(String destination, boolean persistent, Optional<Integer> timeToLiveInSeconds);
}
