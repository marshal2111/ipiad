package org.example;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

public class NewsHeader {
    String title;
    String date;
    String link;
    String hashMD5;
    Boolean read = false;
    int ID;

    NewsHeader(String title, String date, String link) {
        this.title = title;
        this.date = date;
        this.link = link;
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        byte[] messageDigest = md.digest((this.title + this.link).getBytes());
        BigInteger no = new BigInteger(1, messageDigest);
        this.hashMD5 = no.toString(16);
        this.ID = (int)UUID.randomUUID().getMostSignificantBits();
    }

    public void Store() {
        System.out.println("Новость");
        System.out.println(this.date);
        System.out.println(this.title);
        System.out.println(this.link);
        System.out.println(this.hashMD5);
    }
}
