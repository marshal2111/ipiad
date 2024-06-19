package org.example;

import com.rabbitmq.client.Channel;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;

public class Queue {
    private static final String USER = "guest";
    private static final String HOST = "172.17.0.2";
    private static final String PASS = "guest";
    String QUEUE_NAME;
    String EXCHANGE_NAME;
    String ROUTING_KEY;
    Channel channel;

    Queue(String rKey, String exchangeName, String queueName) throws IOException {
        ROUTING_KEY = rKey;
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
            System.out.println("Queue()");
            System.out.println(e);
            return;
        }
        Channel channel;
        try {
            channel = connection.createChannel();
        } catch (Exception e) {
            System.out.println("Queue: Failed to create channel: " + e);
            return;
        }

        try {
            channel.exchangeDeclare(EXCHANGE_NAME,"direct");
        } catch (Exception e) {
            System.out.println("Queue: Failed to declare exchange: " + e);
            return;
        }

        this.channel = channel;
    }

    public void BindQueue() {
        try {
            this.channel.queueBind(QUEUE_NAME,EXCHANGE_NAME,ROUTING_KEY);
        } catch (Exception e) {
            System.out.println("Queue: Failed to bind queue: " + e);
            System.out.println(QUEUE_NAME + " " + EXCHANGE_NAME + " " + ROUTING_KEY);
            return;
        }
    }

    public void DeclareQueue() {
        try {
            this.channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        } catch (Exception e) {
            System.out.println("channel.queueDeclare");
            System.out.println(e);
            return;
        }
    }

    public void Listen(DeliverCallback deliverCallback) {
        try {
            if (channel.isOpen()) {
                channel.basicConsume(QUEUE_NAME,false,deliverCallback, consumerTag-> { });
            } else {
                System.out.println("closed channel input");
            }
        } catch (Exception e) {
            System.out.println("Error during listening: " + QUEUE_NAME);
            System.out.println(e);
            return;
        }
    }

    public Channel getChannel() {
        return channel;
    }
}
