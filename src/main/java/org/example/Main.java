package org.example;



import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Main {
    public static void main(String[] args) throws Exception {
//        Server server = new Server();
//        server.start();





//
//        String url = "https://ria.ru/";
//        Planner planner = new Planner();
//
//        ExecutorService executor = Executors.newSingleThreadExecutor();
//        executor.submit(() -> {
//            try {
//                planner.Listen();
//            } catch (IOException e) {
//                System.out.println("Ошибка запуска Listen RabbitMQ");
//            }
//        });
//        planner.getNewsLinks(url);
//        Thread.sleep(500000);
//        executor.shutdownNow();

        HighElasticClient esClient = new HighElasticClient();
        esClient.NewClient();

// MULTIGET
//        List<String> hashes = new ArrayList<String>();
//        hashes.add("934f804b04098365ca253f987e466115");
//        hashes.add("cada997bf273a797970c345cdc51aa01");
//        hashes.add("8a3bc6b4e7d9e4a9eccd6b303a675283");
//        List<NewsInfo> nis = esClient.multiGetNewsInfo(hashes);
//        for (NewsInfo ni: nis) {
//            ni.print();
//        }
// MULTIGET


        esClient.searchNewsInfoByDateRange("10.05.2024","10.05.2024");



    }

}