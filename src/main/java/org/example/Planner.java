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

public class Planner {
    private static final String USER = "guest";
    private static final String HOST = "127.0.0.1";
    private static final String PASS = "guest";
    Map<Integer, NewsHeader> headerStore = new HashMap<>();
    private String SEND_QUEUE_NAME = "planner_queue";
    private String RECIEVE_QUEUE_NAME = "crawler_queue";
    String SEND_ROUTING_KEY = "pl_to_cr";
    private String RECIEVE_ROUTING_KEY = "cr_to_pl";
    String RESP_ROUTING_KEY;
    Queue queue;

    Channel channel;
    HighElasticClient esClient;

    static {
        System.setProperty("log4j.configurationFile", "C:\\Users\\senio\\IdeaProjects\\planner\\src\\main\\java\\org\\example\\log4j2.xml");
    }

    private static Logger LOGGER = LogManager.getLogger();
    private static final String EXCHANGE_NAME = "parser";

    Planner() throws IOException {
        this.queue = new Queue(RECIEVE_ROUTING_KEY, EXCHANGE_NAME, RECIEVE_QUEUE_NAME);
        this.esClient = new HighElasticClient();

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
            LOGGER.info("Queue()");
            LOGGER.info(e);
            return;
        }
        Channel channel;
        try {
            channel = connection.createChannel();
        } catch (Exception e) {
            LOGGER.info("connection.createChannel");
            LOGGER.info(e);
            return;
        }
        try {
            channel.exchangeDeclare(EXCHANGE_NAME, "direct");
        } catch (Exception e) {
            LOGGER.info("channel.exchangeDeclare");
            LOGGER.info(e);
            return;
        }

        try {
            channel.queueDeclare(SEND_QUEUE_NAME, false, false, false, null);
        } catch (Exception e) {
            LOGGER.info("channel.queueDeclare");
            LOGGER.info(e);
            return;
        }
        this.channel = channel;
    }
    public void close() throws IOException {
        if (esClient != null) {
            esClient.close();
        }
    }

    public boolean getNewsLinks(String url) {
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

            Elements as = document.select("a.cell-list__item-link\\ color-font-hover-only");

            for (Element a : as) {
                Elements dates = a.select("div.cell-info__date");
                NewsHeader nif = new NewsHeader(a.attr("title"), dates.text(), a.attr("href"));
                nif.hashMD5 = getMD5Header(nif);
                headerStore.put(nif.ID, nif);
                nif.Store();
            }
            SendToProccess();
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
            LOGGER.error("channel.basicPublish");
            LOGGER.error(e);
        }
    }

    DeliverCallback store = (consumerTag, delivery) -> {
        String jsonString = new String(delivery.getBody(), StandardCharsets.UTF_8);
        ObjectMapper objectMapper = new ObjectMapper();

        SimpleDateFormat dateFormat = new SimpleDateFormat("dd.MM.yyyy");
        objectMapper.setDateFormat(dateFormat);

        NewsInfo ni;
        try {
            ni = objectMapper.readValue(jsonString, NewsInfo.class);
        } catch (JsonProcessingException e) {
            LOGGER.error("objectMapper.writeValueAsString(ni)");
            LOGGER.error(e);
            return;
        }
        queue.getChannel().basicAck(delivery.getEnvelope().getDeliveryTag(), false);

        String hashtext = getMD5Info(ni);
        ni.setHash(hashtext);

        NewsInfo storedni = esClient.searchNewsInfo(hashtext);

//      Сохранение в базу
        if (storedni == null) {
            LOGGER.info("STORE" + ni.header);
            esClient.storeNewsInfo(ni);
//            ni.print();
        }

    };

//  Отправка на обработку в crawler
    private void SendToProccess() {
        List<String> urls = new ArrayList<>();
        this.headerStore.forEach((key, value) -> {
            try {
                String hashtext = getMD5Header(value);
                NewsInfo storedni = esClient.searchNewsInfo(hashtext);
//              Если записи в базе нет
                if(storedni == null) {
                    LOGGER.debug("Новость еще не в базе " + value.link);
                    urls.add(value.link);
                }
            } catch (IOException e) {
                LOGGER.error(e);
            }

        });
        JSONArray jsonArray = new JSONArray(urls);

        String json = jsonArray.toString();
        LOGGER.info(json);
        SendMessage(json);
    }

//  Вычисление хэша исходя из структуры данных о новости
    private String getMD5Info(NewsInfo ni) {
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        byte[] messageDigest = md.digest((ni.header + ni.link).getBytes());
        BigInteger no = new BigInteger(1, messageDigest);
        String hashtext = no.toString(16);
        return hashtext;
    }
    private String getMD5Header(NewsHeader ni) {
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        byte[] messageDigest = md.digest((ni.title + ni.link).getBytes());
        BigInteger no = new BigInteger(1, messageDigest);
        String hashtext = no.toString(16);
        return hashtext;
    }
}
