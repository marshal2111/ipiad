package org.task;

import com.rabbitmq.client.Channel;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class Queue {
    private static final String USER = "guest";
    private static final String HOST = "172.17.0.2";
    private static final String PASS = "guest";
    private String QUEUE_NAME;
    private String EXCHANGE_NAME;
    private String ROUTING_KEY;
    Channel channel;
    private static Logger LOGGER = LogManager.getLogger();

    Queue(String routingKey, String exchangeName, String queueName) throws IOException {
        ROUTING_KEY = routingKey;
        EXCHANGE_NAME = exchangeName;
        QUEUE_NAME = queueName;

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOST);
        factory.setUsername(USER);
        factory.setPassword(PASS);
        factory.useNio();
        factory.setConnectionTimeout(50000);
        factory.setRequestedHeartbeat(100);

        Connection connection;
        try {
            connection = factory.newConnection();
        } catch (Exception e) {
            LOGGER.error("Failed to create connection: " + e);
            return;
        }
        Channel channel;
        try {
            channel = connection.createChannel();
        } catch (Exception e) {
            LOGGER.error("Failed to create channel: " + e);
            return;
        }

        try {
            channel.exchangeDeclare(EXCHANGE_NAME,"direct");
        } catch (Exception e) {
            LOGGER.error("Failed to declare exchange: " + e);
            return;
        }

        this.channel = channel;
    }

    public void BindQueue() {
        try {
            this.channel.queueBind(QUEUE_NAME,EXCHANGE_NAME,ROUTING_KEY);
        } catch (Exception e) {
            LOGGER.error("Failed to bind queue: " + e);
            // System.out.println(QUEUE_NAME + " " + EXCHANGE_NAME + " " + ROUTING_KEY);
            return;
        }
    }

    public void DeclareQueue() {
        try {
            this.channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        } catch (Exception e) {
            LOGGER.error("Failed to declare queue: " + e);
            return;
        }
    }

    public void Listen(DeliverCallback deliverCallback) {
        try {
            if (channel.isOpen()) {
                channel.basicConsume(QUEUE_NAME,false,deliverCallback, consumerTag-> { });
            } else {
                LOGGER.info("Input channel is closed");
            }
        } catch (Exception e) {
            LOGGER.error("Error during listening: " + e);
            return;
        }
    }

    public Channel getChannel() {
        return channel;
    }
}
