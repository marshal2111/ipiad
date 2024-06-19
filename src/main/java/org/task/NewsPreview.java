package org.task;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

public class NewsPreview {
    String title;
    String date;
    String link;
    String hashMD5;
    Boolean read = false;
    int ID;

    NewsPreview(String title, String date, String link) {
        this.title = title;
        this.date = date;
        this.link = link;
        this.hashMD5 = getMD5(link);
        this.ID = (int)UUID.randomUUID().getMostSignificantBits();
    }

    public void Store() {
        System.out.println("========================================");
        System.out.println(this.date);
        System.out.println(this.title);
        System.out.println(this.link);
        System.out.println(this.hashMD5);
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
}
