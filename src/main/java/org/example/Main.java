package org.example;



import java.io.IOException;
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

    }

}