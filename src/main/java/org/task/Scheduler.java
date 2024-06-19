package org.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import java.text.SimpleDateFormat;
import java.util.*;

//TODO: single channel for single connection

public class Scheduler {
    private static final String USER = "guest";
    private static final String HOST = "172.17.0.2";
    private static final String PASS = "guest";
    Map<Integer, NewsPreview> previewStore = new HashMap<>();
    private String SEND_QUEUE_NAME = "scheduler_queue";
    private String RECIEVE_QUEUE_NAME = "crawler_queue";
    String SEND_ROUTING_KEY = "pl_to_cr";
    private String RECIEVE_ROUTING_KEY = "cr_to_pl";
    String RESP_ROUTING_KEY;
    Queue crawler_queue;
    Queue scheduler_queue;

    ElasticSearchClient esClient;

    static {
        System.setProperty("log4j.configurationFile", "/home/farewelly/crawler/planner/src/main/java/org/task/log4j2.xml");
    }

    private static Logger LOGGER = LogManager.getLogger();
    private static final String EXCHANGE_NAME = "parser";

    Scheduler() throws IOException {
        this.crawler_queue = new Queue(RECIEVE_ROUTING_KEY, EXCHANGE_NAME, RECIEVE_QUEUE_NAME);
        this.scheduler_queue = new Queue(SEND_ROUTING_KEY, EXCHANGE_NAME, SEND_QUEUE_NAME);
        scheduler_queue.DeclareQueue();
        this.esClient = new ElasticSearchClient();
    }

    public void CloseES() throws IOException {
        if (esClient != null) {
            esClient.close();
        }
    }

    public boolean CollectRefs(String url) {
        try {
            Document document;
            try {
                document = Jsoup.connect(url).get();
            } catch (Exception e) {
                LOGGER.info("Error getting html code");
                LOGGER.info(e);
                return false;
            }
            int statusCode = document.connection().response().statusCode();
            switch (statusCode) {
                case 200:
                    break;
                case 404:
                    LOGGER.error("Error: page not found");
                    return false;
                case 500:
                    LOGGER.error("Internal server error");
                    return false;
                default:
                    LOGGER.error("Incorrect status code: %d\n", statusCode);
                    return false;
            }

            Elements a_with_refs = document.select("a[class~=(?i)b_ear]");

            for (Element a : a_with_refs) {
                Elements datetime = a.select("time.b_ear-time");
                Elements title = a.select("div.b_ear-title");
                String ref = a.attr("href");
                if(!ref.contains("https://www.gazeta.ru")) {
                    ref = "https://www.gazeta.ru" + ref;
                }
                NewsPreview preview = new NewsPreview(title.text(), datetime.attr("datetime"), ref);
                preview.hashMD5 = getMD5(ref);
                previewStore.put(preview.ID, preview);
                preview.Store();
            }

            SendRefsToCrawler();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public void Listen() {
        LOGGER.info("Start listening");
        crawler_queue.BindQueue();
        crawler_queue.Listen(store);
    }

    public void SendMessage(String msg) {
        try {
            scheduler_queue.channel.basicPublish(EXCHANGE_NAME, SEND_ROUTING_KEY, null, msg.getBytes());
            LOGGER.info("Message sent");
        } catch (Exception e) {
            LOGGER.error("channel.basicPublish");
            LOGGER.error(e);
        }
    }

    DeliverCallback store = (consumerTag, delivery) -> {
        String jsonString = new String(delivery.getBody(), StandardCharsets.UTF_8);
        ObjectMapper objectMapper = new ObjectMapper();

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
        objectMapper.setDateFormat(dateFormat);

        NewsFull newsFull;
        try {
            newsFull = objectMapper.readValue(jsonString, NewsFull.class);
        } catch (JsonProcessingException e) {
            LOGGER.error("objectMapper.writeValueAsString(ni)");
            LOGGER.error(e);
            return;
        }
        crawler_queue.getChannel().basicAck(delivery.getEnvelope().getDeliveryTag(), false);

        String hashtext = getMD5(newsFull.link);
        newsFull.setHash(hashtext);

        NewsFull already_stored = esClient.SearchNews(hashtext);

        //store if not found in ES
        if (already_stored == null) {
            LOGGER.info("======================================================");
            LOGGER.info("Article " + newsFull.header);
            esClient.StoreNews(newsFull);
        }

    };

    private void SendRefsToCrawler() {
        List<String> urls = new ArrayList<>();
        this.previewStore.forEach((key, value) -> {
            try {
                String hashtext = getMD5(value.link);
                NewsFull already_stored = esClient.SearchNews(hashtext);
                if (already_stored == null) {
                    LOGGER.debug("Новость еще не в базе " + value.link);
                    urls.add(value.link);
                }
            } catch (IOException e) {
                LOGGER.error(e);
            }

        });
        JSONArray jsonArray = new JSONArray(urls);

        String json = jsonArray.toString();
        LOGGER.info("Message: " + json);
        SendMessage(json);
    }

    private String getMD5(String ref) {
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        byte[] messageDigest = md.digest((ref).getBytes());
        BigInteger no = new BigInteger(1, messageDigest);
        String hashtext = no.toString(16);
        return hashtext;
    }

    // private String getMD5Header(NewsPreview news) {
    //     MessageDigest md = null;
    //     try {
    //         md = MessageDigest.getInstance("MD5");
    //     } catch (NoSuchAlgorithmException e) {
    //         throw new RuntimeException(e);
    //     }
    //     byte[] messageDigest = md.digest((news.title + news.link).getBytes());
    //     BigInteger no = new BigInteger(1, messageDigest);
    //     String hashtext = no.toString(16);
    //     return hashtext;
    // }
}
