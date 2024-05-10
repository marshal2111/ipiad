package org.example;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class NewsInfo {
    String hash;
    String text;
    String header;
    Date date;
    String link;

    public void print() {
        System.out.println();
        System.out.println("Header: " + header);
        System.out.println("Date: " + date);
        System.out.println("Link: " + link);
        System.out.println("Hash: " + hash);
        System.out.println();
    }

    public void printText() {
        System.out.println();
        System.out.println(this.text);
        System.out.println();
    }

    public Map<String, String> toMap() {
        Map<String, String> map = new HashMap<>();
        map.put("hash", hash);
        map.put("text", text);
        map.put("header", header);
        SimpleDateFormat dateFormat = new SimpleDateFormat("dd.MM.yyyy");
        String dateString = dateFormat.format(date);
        map.put("date", dateString);
        map.put("link", link);
        return map;
    }

    public void fromMap(Map<String, Object> map) {
        this.hash = map.get("hash").toString();
        this.text = map.get("text").toString();
        this.header = map.get("header").toString();
        // Преобразовать дату из формата "dd.MM.yyyy" в объект Date
        String dateString = map.get("date").toString();
        SimpleDateFormat dateFormat = new SimpleDateFormat("dd.MM.yyyy");
        try {
            this.date = dateFormat.parse(dateString);
        } catch (ParseException e) {
            throw new RuntimeException("Cannot parse date: " + dateString, e);
        }
        this.link = map.get("link").toString();
    }

    public String getText() {
        return text;
    }

    public String getHeader() {
        return header;
    }

    public java.util.Date getDate() {
        return date;
    }

    public String getLink() {
        return link;
    }

    public void setText(String text) {
        this.text = text;
    }

    public void setHeader(String header) {
        this.header = header;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public void setLink(String link) {
        this.link = link;
    }

    public String getHash() {
        return hash;
    }

    public void setHash(String hash) {
        this.hash = hash;
    }
}
