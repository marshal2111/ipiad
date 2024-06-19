package org.task;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class NewsFull {
    String hash;
    String text;
    String header;
    Date date;
    String link;

    NewsFull() {}

    NewsFull(String title, Date date, String link, String text) {
        this.header = title;
        this.date = date;
        this.link = link;
        this.hash = getMD5(link);
        this.text = text;
    }

    public void print() {
        System.out.println("Header: " + header);
        System.out.println("Date: " + date);
        System.out.println("Link: " + link);
        System.out.println("Hash: " + hash);
        System.out.println("Text: " + text);
    }

    public void printText() {
        System.out.println(this.text);
    }

    public Map<String, String> toMap() {
        Map<String, String> map = new HashMap<>();
        map.put("hash", hash);
        map.put("text", text);
        map.put("header", header);
        SimpleDateFormat dateFormat = new SimpleDateFormat("dd.MM.yyyy'T'HH:mm:ss");
        String dateString = dateFormat.format(date);
        map.put("date", dateString);
        map.put("link", link);
        return map;
    }

    public void fromMap(Map<String, Object> map) {
        this.hash = map.get("hash").toString();
        this.text = map.get("text").toString();
        this.header = map.get("header").toString();
        String dateString = map.get("date").toString();
        SimpleDateFormat dateFormat = new SimpleDateFormat("dd.MM.yyyy'T'HH:mm:ss");
        try {
            this.date = dateFormat.parse(dateString);
        } catch (ParseException e) {
            throw new RuntimeException("Cannot parse date: " + dateString, e);
        }
        this.link = map.get("link").toString();
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

    public String getHash() {
        return hash;
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

    public void setHash(String hash) {
        this.hash = hash;
    }
}
