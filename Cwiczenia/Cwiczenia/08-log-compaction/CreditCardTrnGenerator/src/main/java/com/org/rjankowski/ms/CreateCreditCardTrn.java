package com.org.rjankowski.ms;


import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;

public class CreateCreditCardTrn {

    private static final int quantity = 30000;
    private static final String url = "http://localhost:9021/credit-card-trn";

    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        for (int i = 0; i < quantity; i++) {
            String body = generateBody("Januszex- "+ i);
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(new URI(url))
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .header("Content-Type", "application/json")
                    .build();

            HttpResponse<String> send = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
            System.out.println("Record number: " + i);
        }
    }

    private static String generateBody(String lastName) throws IOException {
//        File file = new File(CreateCreditCardTrn.class
//                .getClassLoader()
//                .getResource("credit-card-trn-body.txt").getFile());
        String customerBody = """
                {
                    "clientId": "1",
                    "value": "10",
                    "city": "Krakow",
                    "companyName": "%s"
                }
                """;


        return String.format(customerBody, lastName);
    }

}
