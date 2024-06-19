package org.task;

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

public class Scheduler {
    private static final String USER = "guest";
    private static final String PASS = "guest";
    private static final String HOST = "172.17.0.2";
    private static final String SEND_QUEUE_NAME = "scheduler_queue";
    private static final String RECIEVE_QUEUE_NAME = "crawler_queue";
    private static final String SEND_ROUTING_KEY = "scheduler-to-crawler";
    private static final String RECIEVE_ROUTING_KEY = "crawler-to-scheduler";
    private static final String EXCHANGE_NAME = "parser";
    static {
        System.setProperty("log4j.configurationFile", "/home/farewelly/crawler/ipiad/src/main/java/org/task/log4j2.xml");
    }
    private static Logger LOGGER = LogManager.getLogger();

    Map<Integer, NewsPreview> previewStore = new HashMap<>();
    Queue crawler_queue;
    Queue scheduler_queue;
    ElasticSearchClient esClient;

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
                    LOGGER.error("Collect refs error: page not found " + url);
                    return false;
                case 500:
                    LOGGER.error("Collect refs error: internal server error " + url);
                    return false;
                default:
                    LOGGER.error("Collect refs error: incorrect status code: %d\n", statusCode);
                    return false;
            }

            Elements refs_to_news = document.select("a[class~=(?i)b_ear]");

            for (Element el : refs_to_news) {
                Elements datetime = el.select("time.b_ear-time");
                Elements title = el.select("div.b_ear-title");
                String ref = el.attr("href");
                if(!ref.contains("https://www.gazeta.ru")) {
                    ref = "https://www.gazeta.ru" + ref;
                }
                NewsPreview preview = new NewsPreview(title.text(), datetime.attr("datetime"), ref);
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

    DeliverCallback store = (consumerTag, delivery) -> {
        String jsonString = new String(delivery.getBody(), StandardCharsets.UTF_8);
        ObjectMapper objectMapper = new ObjectMapper();

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
        objectMapper.setDateFormat(dateFormat);

        NewsFull newsFull;
        try {
            newsFull = objectMapper.readValue(jsonString, NewsFull.class);
        } catch (JsonProcessingException e) {
            LOGGER.error("Error processing data from crawler: " + e);
            return;
        }
        // newsFull.print();
        crawler_queue.getChannel().basicAck(delivery.getEnvelope().getDeliveryTag(), false);

        // String hashtext = getMD5(newsFull.link);
        // newsFull.setHash(hashtext);

        NewsFull already_stored = esClient.SearchNews(newsFull.getHash());

        //store if not found in ES
        if (already_stored == null) {
            LOGGER.info("======================================================");
            LOGGER.info("Article " + newsFull.header);
            esClient.StoreNews(newsFull);
        }

    };

    public void SendMessage(String msg) {
        try {
            scheduler_queue.channel.basicPublish(EXCHANGE_NAME, SEND_ROUTING_KEY, null, msg.getBytes());
            LOGGER.info("Message sent");
        } catch (Exception e) {
            LOGGER.error("Error sending message to crawler: " + e);
            LOGGER.error(e);
        }
    }

    private void SendRefsToCrawler() {
        List<String> urls = new ArrayList<>();
        this.previewStore.forEach((key, value) -> {
            try {
                NewsFull already_stored = esClient.SearchNews(value.hashMD5);
                if (already_stored == null) {
                    LOGGER.info("Новость еще не в базе " + value.link);
                    urls.add(value.link);
                }
            } catch (IOException e) {
                LOGGER.error("Error searching news in ES: " + e);
            }

        });
        JSONArray jsonArray = new JSONArray(urls);

        String json = jsonArray.toString();
        LOGGER.info("Message: " + json);
        SendMessage(json);
    }
}
