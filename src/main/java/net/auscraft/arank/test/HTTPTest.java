package net.auscraft.arank.test;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.net.ssl.HttpsURLConnection;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.ByteBuffer;
import java.sql.*;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class HTTPTest
{
    private static HikariDataSource source;
    private static ExecutorService service;

    public static void main(String[] args)
    {
        service = Executors.newFixedThreadPool(50);
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:mariadb://172.17.0.2:3306");
        config.setUsername("root");
        config.setPassword("password1");
        config.setPoolName("arank-http-test");
        config.setDriverClassName("org.mariadb.jdbc.Driver");


        source = new HikariDataSource(config);

        Connection connection ;
        PreparedStatement statement = null;
        ResultSet set = null;

        List<UUID> messages = new LinkedList<>();
        int count = 0;

        try {
            connection = source.getConnection();
            statement =  connection.prepareStatement("SELECT * FROM survival12.ranks;");
            set = statement.executeQuery();

            while (set.next()) {
                messages.add(fromBytes(set.getBytes("uuid")));

                count++;
                System.out.println("Added uuid to list. UUID count: " + count);
            }


        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            close(config, set, statement);
        }

        List<String> names = new LinkedList<>();
        List<UUID> requestsQueries = new LinkedList<>();
        JsonParser parser = new JsonParser();
        AtomicInteger noContentResponses = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(messages.size());

        System.out.println("Size of messages: " + messages.size());
        long time = System.currentTimeMillis();

        for (UUID uuid : messages) {
            if (requestsQueries.contains(uuid)) {
                continue;
            }

            requestsQueries.add(uuid);

            service.submit(() -> {
                try {
                    URL url = new URL("https://sessionserver.mojang.com/session/minecraft/profile/" + uuid.toString().replace("-", ""));
                    HttpsURLConnection urlConnection = (HttpsURLConnection) url.openConnection();
                    urlConnection.setRequestMethod("GET");
                    urlConnection.setRequestProperty("User-Agent", "Mozilla/5.0");
                    urlConnection.setRequestProperty("Content-Type", "application/json");
                    int responseCode = urlConnection.getResponseCode(); //api.mojang.com/users/profiles/minecrafte();
                    if (responseCode == HttpsURLConnection.HTTP_OK) {
                        BufferedReader in = new BufferedReader(new InputStreamReader(urlConnection.getInputStream()));
                        StringBuilder response = new StringBuilder();

                        String input;

                        while ((input = in.readLine()) != null) {
                            response.append(input);
                        }

                        JsonObject json = parser.parse(response.toString()).getAsJsonObject();
                        names.add(json.get("name").getAsString());
                    } else {
                        if (responseCode != HttpsURLConnection.HTTP_NO_CONTENT) {
                            System.exit(1);
                        }

                        noContentResponses.incrementAndGet();
                        System.out.println(responseCode);
                    }

                    latch.countDown();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }

        long seconds = 0L;

        try {
            latch.await();
            seconds = (System.currentTimeMillis() - time);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println(names.size() + " collected. No content responses: " + noContentResponses + ". " + (messages.size() - names.size()) + " where either duplicate or not found. Took " + TimeUnit.MILLISECONDS.toSeconds(seconds) + " seconds.");
    }




    public static UUID fromBytes(byte[] array) {
        if (array.length != 16) {
            throw new IllegalArgumentException("Illegal byte array length: " + array.length);
        } else {
            ByteBuffer byteBuffer = ByteBuffer.wrap(array);
            long mostSignificant = byteBuffer.getLong();
            long leastSignificant = byteBuffer.getLong();
            return new UUID(mostSignificant, leastSignificant);
        }
    }

    public static void close(Object... os)
    {
        try {
            for (Object o : os) {
                if (o instanceof Connection) {
                    Connection c = (Connection) o;

                    if (!c.isClosed()) {
                        c.close();
                    }
                }

                if (o instanceof Statement) {
                    Statement s = (Statement) o;

                    if (!s.isClosed()) {
                        s.close();
                    }
                }

                if (o instanceof ResultSet) {
                    ResultSet s = (ResultSet) o;

                    if (!s.isClosed()) {
                        s.close();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
