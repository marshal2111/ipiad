package org.example;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
    public static void main(String[] args) throws Exception {
        String url = "https://ria.ru/";
        Planner planner = new Planner();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(() -> {
            planner.getNewsLinks(url);
        });
        planner.Listen();

//        HighElasticClient esClient = new HighElasticClient();
//
//
//        NewsInfo ni = esClient.searchNewsInfo("934f804b04098365ca253f987e466115");
//        System.out.println("esClient.searchNewsInfo(\"934f804b04098365ca253f987e466115\");");
//        ni.print();
//        System.out.println();
//
//        ni = esClient.searchNewsInfoAnd("За участие в беспорядках в Тбилиси задержали россиянина", "https://ria.ru/20240510/tbilisi-1945138243.html");
//        System.out.println("esClient.searchNewsInfoAnd(\"За участие в беспорядках в Тбилиси задержали россиянина\", \"https://ria.ru/20240510/tbilisi-1945138243.html\");");
//        ni.print();
//        System.out.println();
//
//        ni = esClient.searchNewsInfoOr("Они давят. Украинский генерал рассказал о странном поведении США", "https://ria.ru/20240510/tbilisi-1945138243.html");
//        System.out.println("esClient.searchNewsInfoOr(\"Они давят. Украинский генерал рассказал о странном поведении США\", \"https://ria.ru/20240510/tbilisi-1945138243.html\");");
//        ni.print();
//        System.out.println();
//
//        Map<String,Long> m = esClient.searchNewsInfoSortByDate();
//        System.out.println("esClient.searchNewsInfoSortByDate()");
//        System.out.println(m);
//        System.out.println();
//
//        List<String> hashes = new ArrayList<String>();
//        hashes.add("934f804b04098365ca253f987e466115");
//        hashes.add("cada997bf273a797970c345cdc51aa01");
//        hashes.add("8a3bc6b4e7d9e4a9eccd6b303a675283");
//        List<NewsInfo> nis = esClient.multiGetNewsInfo(hashes);
//        System.out.println("esClient.multiGetNewsInfo(hashes)");
//        for (NewsInfo n: nis) {
//            n.print();
//        }
//        System.out.println();
//
//        System.out.println(" esClient.searchNewsInfoByDateRange(\"11.05.2024\",\"11.05.2024\");");
//        m = esClient.searchNewsInfoByDateRange("11.05.2024","11.05.2024");
//        System.out.println(m);
//        System.out.println();
//
//        System.out.println("esClient.searchNewsByText(\"МОСКВА\")");
//        Map<String,String> m2 = esClient.searchNewsByText("МОСКВА");
//        System.out.println(m2);
//        System.out.println();
//
//        System.out.println("esClient.countNewsByDate(\"10.05.2024\");");
//        long c = esClient.countNewsByDate("10.05.2024");
//        System.out.println("Новостей по дате: " + c);
//        System.out.println();
//
//        System.out.println("esClient.countLogsByLevel(\"ERROR\");");
//        c = esClient.countLogsByLevel("ERROR");;
//        System.out.println("Логов по level: " + c);
//        System.out.println();

//        System.out.println("esClient.searchNewsByHeaderAndText(\"Штурмуют российские позиции\",\"МОСКВА\",\"10.05.2024\")");
//        Map<String,String> m3 = esClient.searchNewsByHeaderAndText("Штурмуют российские позиции","МОСКВА","10.05.2024");
//        System.out.println(m3);
//        System.out.println();

    }

}