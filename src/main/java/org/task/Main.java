package org.task;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future; 
import java.util.concurrent.ExecutionException; 

public class Main {
    public static void main(String[] args) throws Exception {

        String url = "https://www.gazeta.ru/";
        Scheduler scheduler = new Scheduler();
        Crawler crawler = new Crawler(1);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        executor.submit(() -> {
            scheduler.CollectRefs(url);
            scheduler.Listen();
        });

        executor.submit(() -> {
            crawler.Listen();
        });

        executor.shutdown();



    // search by hash

       // ElasticSearchClient esClient = new ElasticSearchClient();
       // NewsFull news = esClient.SearchNews("63e9794ebe6652d15700e3f6dadf2dd3");
       // System.out.println("esClient.searchNewsInfo(\"63e9794ebe6652d15700e3f6dadf2dd3\");");
       // news.print();
       // System.out.println();


    // OR search

       // ElasticSearchClient esClient = new ElasticSearchClient();
       // NewsFull news = esClient.SearchNewsByHeaderOrLink("", "");
       // news.print();
       // System.out.println();


    // Aggrefation

        // ElasticSearchClient esClient = new ElasticSearchClient();
        // Map<String,Long> resp = esClient.AggregateNewsByDate();
        // System.out.println("esClient.searchNewsInfoSortByDate()");
        // System.out.println(resp);
        // System.out.println();

    // Multiget

       // ElasticSearchClient esClient = new ElasticSearchClient();
       // List<String> hashes = new ArrayList<String>();
       // hashes.add("7a1315fa0145fab310554568a77137d7");
       // hashes.add("3c27d993acbcc22bad119c2af33d607c");
       // hashes.add("371737d3546c6b8ba5cb07cd3a15f754");
       // List<NewsFull> news = esClient.MultiGetNewsInfo(hashes);
       // for (NewsFull n: news) {
       //     n.print();
       // }
       // System.out.println();

    // Search by datetime range

       // ElasticSearchClient esClient = new ElasticSearchClient();
       // Map<String,Long> resp = esClient.SearchNewsNewsByDatetimeRange("16.06.2024T00:00:00","18.06.2024T23:00:00");
       // System.out.println(resp);
       // System.out.println();

    // Text search

       // ElasticSearchClient esClient = new ElasticSearchClient();
       // List<NewsFull> resp = esClient.SearchNewsByText("Украина");
       // for (NewsFull n: resp) {
       //     n.print();
       // }
       // System.out.println();

    // Count by date

       // ElasticSearchClient esClient = new ElasticSearchClient();
       // long count = esClient.CountNewsByDate("18.06.2024");
       // System.out.println("Новостей по дате: " + count);
       // System.out.println();


    // Count logs

       // ElasticSearchClient esClient = new ElasticSearchClient();
       // c = esClient.countLogsByLevel("ERROR");;
       // System.out.println("Логов по level: " + c);
       // System.out.println();

    // Search records with links

       // ElasticSearchClient esClient = new ElasticSearchClient();
       // List<String> l = esClient.searchNewsWithLink(3);
       // System.out.println(l);
       // System.out.println();

    }

}