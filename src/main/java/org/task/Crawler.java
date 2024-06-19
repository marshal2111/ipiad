package org.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.json.JSONArray;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Date;
import java.text.SimpleDateFormat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Crawler {
    private static final String USER = "guest";
    private static final String HOST = "172.17.0.2";
    private static final String PASS = "guest";

    private String RECIEVE_QUEUE_NAME = "scheduler_queue";
    private String SEND_QUEUE_NAME = "crawler_queue";
    private String SEND_ROUTING_KEY = "cr_to_pl";
    private String RECIEVE_ROUTING_KEY = "pl_to_cr";
    private String EXCHANGE_NAME = "parser";
    private int numThreads;
    Queue crawler_queue;
    Queue scheduler_queue;
    
    static {
        System.setProperty("log4j.configurationFile", "/home/farewelly/crawler/planner/src/main/java/org/task/log4j2.xml");
    }

    private static Logger LOGGER = LogManager.getLogger();

    Crawler(int numThreads) throws IOException {
        this.numThreads = numThreads;
        this.scheduler_queue = new Queue(RECIEVE_ROUTING_KEY, EXCHANGE_NAME, RECIEVE_QUEUE_NAME);
        this.crawler_queue = new Queue(SEND_ROUTING_KEY, EXCHANGE_NAME, SEND_QUEUE_NAME);
        crawler_queue.DeclareQueue();
    }

    public void SetNumThreads(int numThreads) {
        this.numThreads = numThreads;
    }

    public void Listen() {
        LOGGER.info("Start listening");
        scheduler_queue.BindQueue();
        scheduler_queue.Listen(start);
    }

    public void SendMessage(String msg) {
        try {
            crawler_queue.channel.basicPublish(EXCHANGE_NAME, SEND_ROUTING_KEY, null, msg.getBytes());
        } catch (Exception e) {
            LOGGER.info("channel.basicPublish");
            LOGGER.info(e);
        }
    }

    DeliverCallback start = (consumerTag, delivery) -> {

        LOGGER.info("Got message");
        String jsonString = new String(delivery.getBody(), StandardCharsets.UTF_8);
        JSONArray jsonArray = null;
        try {
            jsonArray = new JSONArray(jsonString);
        } catch (Exception e) {
            LOGGER.info("Incorrect input string");
            LOGGER.info(jsonString);
            return;
        }
        scheduler_queue.getChannel().basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        System.out.println("start");

        // Преобразуем JSONArray в ArrayList<String>
        ArrayList<String> urls = new ArrayList<>();
        for (int i = 0; i < jsonArray.length(); i++) {
            urls.add(jsonArray.getString(i));
        }
        LOGGER.info(urls);
        try {
            StartParse(urls);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        System.out.println("DONE");
    };

    public void StartParse(ArrayList<String> urls) throws InterruptedException {
        ExecutorService executor = null;
        try {
            executor = Executors.newFixedThreadPool(this.numThreads);
        } catch (Exception e) {
            LOGGER.info("Error: Executors.newFixedThreadPool()");
            LOGGER.info(e.getMessage());
            return;
        }

        for (String url : urls) {
            AtomicReference<NewsFull> news = new AtomicReference<>(new NewsFull());
            executor.submit(() -> {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                news.set(Parse(url));
                synchronized (Objects.requireNonNull(news)) {
                    this.Response(news.get());
                }
            });
        }
        executor.shutdown();
        while (!executor.isTerminated()) {
        }
    }

    private NewsFull Parse(String url) {
        NewsFull newsFull = new NewsFull();
        try {
            Document document = Jsoup.connect(url).get();
            int statusCode = document.connection().response().statusCode();
            switch (statusCode) {
                case 200:
                    break;
                case 404:
                    LOGGER.info("Error: page not found");
                    return null;
                case 500:
                    LOGGER.info("Internal server error");
                    return null;
                default:
                    LOGGER.info("Incorrect status code: %d\n", statusCode);
                    return null;    
            }

            Elements text_body = document.select("div.b_main");
            Elements text_piecies = text_body.select("p");
            
            StringBuilder sb = new StringBuilder();
            for (Element text: text_piecies) {
                sb.append(text.text()).append(" ");
            }
            
            Element date = document.selectFirst("[itemprop='datePublished']");
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
            String datetime = date.attr("datetime");

            try {
                newsFull.date = dateFormat.parse(datetime);
            } catch (Exception e) {
                LOGGER.info(e);
            }

            Element title = document.selectFirst(".headline");

            newsFull.link = url;
            newsFull.text = sb.toString();
            newsFull.header = title.text();
        } catch (IOException e) {
            LOGGER.info(e);
            return null;
        }
        return newsFull;
    }

    private void Response(NewsFull news) {
        ObjectMapper objectMapper = new ObjectMapper();
        String json;
        try {
            json = objectMapper.writeValueAsString(news);
        } catch (JsonProcessingException e) {
            LOGGER.info("objectMapper.writeValueAsString(ni)");
            LOGGER.info(e);
            return;
        }
        SendMessage(json);
    }


}
