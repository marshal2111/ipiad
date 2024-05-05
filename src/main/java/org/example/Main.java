package org.example;


import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Main {
    public static void main(String[] args) throws Exception {
//        Server server = new Server();
//        server.start();






        String url = "https://ria.ru/";
        Planner planner = new Planner();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(() -> {
            try {
                planner.Listen();
            } catch (IOException e) {
                System.out.println("Ошибка запуска Listen RabbitMQ");
            }
        });
        planner.getNewsLinks(url);
        Thread.sleep(500000);
        executor.shutdownNow();




        ElasticWorker ew = new ElasticWorker();



//       WRITE
//        for (int i = 0; i < 15; i++) {
//            NewsInfo ni = new NewsInfo();
//            ni.link = "link" + i;
//            ni.date = "date" + i;
//            ni.header = "header" + i;
//            ni.text = "text" + i;
//            ni.hash = "hash" + i;
//            ew.storeNewsInfo(ni);
//        }

//        READ
        for (int i = 0; i < 15; i++) {
            ew.searchAgregations("header"+i, "link"+(i+1));
        }
//        System.exit(0);
    }

    private static void processNewsInfo(NewsInfo p) {
        p.print();
    }
}