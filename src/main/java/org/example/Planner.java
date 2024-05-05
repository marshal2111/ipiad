package org.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import org.json.JSONArray;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

//TODO: single channel for single connection

public class Planner {
    private static final String USER = "guest";
    private static final String HOST = "127.0.0.1";
    private static final String PASS = "guest";
    Map<Integer, NewsHeader> newsStore = new HashMap<>();
    private String SEND_QUEUE_NAME = "planner_queue";
    private String RECIEVE_QUEUE_NAME = "crawler_queue";
    String SEND_ROUTING_KEY = "pl_to_cr";
    private String RECIEVE_ROUTING_KEY = "cr_to_pl";
    String RESP_ROUTING_KEY;
    Queue queue;

    Channel channel;
    ElasticWorker elasticWorker;
    private static final String EXCHANGE_NAME = "parser";

    Planner() throws InterruptedException, IOException {
        this.queue = new Queue(RECIEVE_ROUTING_KEY, EXCHANGE_NAME, RECIEVE_QUEUE_NAME);
        this.elasticWorker = new ElasticWorker();

        // создание фабрики соединений
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOST);
        factory.setUsername(USER);
        factory.setPassword(PASS);
//        NEW
        factory.useNio();
        factory.setConnectionTimeout(50000);
        factory.setRequestedHeartbeat(100);
//        NEW
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
            channel.exchangeDeclare(EXCHANGE_NAME, "direct");
        } catch (Exception e) {
            System.out.println("channel.exchangeDeclare");
            System.out.println(e);
            return;
        }

        try {
            channel.queueDeclare(SEND_QUEUE_NAME, false, false, false, null);
        } catch (Exception e) {
            System.out.println("channel.queueDeclare");
            System.out.println(e);
            return;
        }
        this.channel = channel;
    }

    public boolean getNewsLinks(String url) {
        try {
            // Получаем HTML-страницу с сайта
            Document document;
            try {
                document = Jsoup.connect(url).get();
            } catch (Exception e) {
                System.out.println("Error getting html code");
                System.out.println(e);
                return false;
            }
            int statusCode = document.connection().response().statusCode();
            switch (statusCode) {
                case 200:
                    break;
                case 404:
                    System.out.println("Error: page not found");
                    return false;
                case 500:
                    System.out.println("Internal server error");
                    return false;
                default:
                    System.out.printf("Incorrect status code: %d\n", statusCode);
                    return false;
            }

            Elements as = document.select("a.cell-list__item-link\\ color-font-hover-only");

            for (Element a : as) {
                Elements dates = a.select("div.cell-info__date");
                NewsHeader nif = new NewsHeader(a.attr("title"), dates.text(), a.attr("href"));
                newsStore.put(nif.ID, nif);
                nif.Store();
            }
            SendToProccess();
//            Listen();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public void Listen() throws IOException {
        queue.Listen(store);
    }

    public void SendMessage(String msg) {
        try {
            channel.basicPublish(EXCHANGE_NAME, SEND_ROUTING_KEY, null, msg.getBytes());
        } catch (Exception e) {
            System.out.println("channel.basicPublish");
            System.out.println(e);
        }
    }

    DeliverCallback store = (consumerTag, delivery) -> {
        String jsonString = new String(delivery.getBody(), StandardCharsets.UTF_8);
        ObjectMapper objectMapper = new ObjectMapper();
        NewsInfo ni;
        try {
            ni = objectMapper.readValue(jsonString, NewsInfo.class);
        } catch (JsonProcessingException e) {
            System.out.println("objectMapper.writeValueAsString(ni)");
            System.out.println(e);
            return;
        }

        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        byte[] messageDigest = md.digest((ni.header + ni.link).getBytes());
        BigInteger no = new BigInteger(1, messageDigest);
        String hashtext = no.toString(16);

        AtomicReference<Boolean> isStore = new AtomicReference<>(true);
        newsStore.forEach((key, value) -> {
            if ((Objects.equals(hashtext, value.hashMD5)) && (value.read)) {
                System.out.println("COPY");
                System.out.println(value.title);
                isStore.set(false);
            } else if (Objects.equals(hashtext, value.hashMD5)) {
                System.out.println("READ");
                value.read = true;
            }
        });
        if (isStore.get()) {
            System.out.println("STORED DATA");
            System.out.println(jsonString);
            elasticWorker.storeNewsInfo(ni);
            System.out.println("STORED DATA");
        }

    };

    private void SendToProccess() {
        List<String> urls = new ArrayList<>();
        this.newsStore.forEach((key, value) -> {
            urls.add(value.link);
        });
        JSONArray jsonArray = new JSONArray(urls);

        String json = jsonArray.toString();
        System.out.println(json);
        SendMessage(json);
    }
}
