package org.example;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.attribute.UserDefinedFileAttributeView;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Server {
    Planner planner;
    HttpServer server;
    Server() throws IOException, InterruptedException {
        this.planner = new Planner();
        this.server = HttpServer.create(new InetSocketAddress(8000), 0);
        server.createContext("/parse", new ParseHandler());
        server.createContext("/search", new SearchHandler());
        server.createContext("/stop", new StopHandler());
        server.setExecutor(null);
    }
    public void start() throws Exception {
        System.out.println("Server started on port 8000");
        server.start();
    }

    static class ParseHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendResponse(exchange, 405, "Method Not Allowed");
                return;
            }

            String requestBody = readRequestBody(exchange);
            String link = parseLink(requestBody);
            if (link == null) {
                sendResponse(exchange, 400, "Bad Request");
                return;
            }

            String responseBody = "hello " + link;
            sendResponse(exchange, 200, responseBody);
        }

        private String readRequestBody(HttpExchange exchange) throws IOException {
            byte[] requestBody = new byte[1024];
            int bytesRead = exchange.getRequestBody().read(requestBody);
            return new String(requestBody, 0, bytesRead, StandardCharsets.UTF_8);
        }

        private String parseLink(String requestBody) {
            try {
                JSONObject jsonObject = new JSONObject(requestBody);
                if (jsonObject.has("link")) {
                    String link = jsonObject.getString("link");
                    return link;
                }
            } catch (JSONException e) {
                System.out.println("Parsing error");
            }
            return null;
        }
    }

    static class SearchHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String responseBody = "world";
            sendResponse(exchange, 200, responseBody);
        }
    }

    static class StopHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String responseBody = "stop";
            sendResponse(exchange, 200, responseBody);
            System.exit(0);
        }
    }

    static void sendResponse(HttpExchange exchange, int statusCode, String responseBody) throws IOException {
        byte[] responseBytes = responseBody.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(statusCode, responseBytes.length);
        OutputStream outputStream = exchange.getResponseBody();
        outputStream.write(responseBytes);
        outputStream.close();
    }
}