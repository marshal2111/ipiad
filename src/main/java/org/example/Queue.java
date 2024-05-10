package org.example;

import com.rabbitmq.client.Channel;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;

public class Queue {
    private static final String USER = "guest";
    private static final String HOST = "127.0.0.1";
    private static final String PASS = "guest";
    String QUEUE_NAME;
    String EXCHANGE_NAME;
    String ROUTING_KEY;
    Channel channel;
    Queue(String rKey, String exchangeName, String queueName) throws IOException {
        ROUTING_KEY = rKey;
        EXCHANGE_NAME = exchangeName;
        QUEUE_NAME = queueName;
        // создание фабрики соединений
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOST);
        factory.setUsername(USER);
        factory.setPassword(PASS);

        // создание соединения
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
            System.out.println("connection.createChannel");
            System.out.println(e);
            return;
        }

        try {
            channel.exchangeDeclare(EXCHANGE_NAME,"direct");
        } catch (Exception e) {
            System.out.println(" channel.exchangeDeclare");
            System.out.println(e);
            return;
        }
        try {
            channel.queueBind(this.QUEUE_NAME,EXCHANGE_NAME,ROUTING_KEY);
        } catch (Exception e) {
            System.out.println("channel.queueBind");
            System.out.println(e);
            return;
        }
        this.channel = channel;

    }

    public void Listen(DeliverCallback deliverCallback) {
        try {
            if (channel.isOpen()) {
                channel.basicConsume(QUEUE_NAME,false,deliverCallback, consumerTag-> { });
            } else {
                System.out.println("closed channel input");
            }
        } catch (Exception e) {
            System.out.println("Listen(DeliverCallback deliverCallback)");
            System.out.println(e);
            return;
        }
    }

    public Channel getChannel() {
        return channel;
    }
}
