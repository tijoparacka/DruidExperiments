package io.druid.example.scanquery;

import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Sample Code to send a http post request to scan query
 *
 */
public class ScanRequest
{

     public static void main(String[] args) throws Exception {

        ScanRequest obj = new ScanRequest();

            System.out.println("Send Http POST request");
            obj.sendPost("/Users/tijothomas/run/scan.json");

    }




    private void sendPost(String payloadLocation) throws Exception {

        String url = "http://localhost:8888/druid/v2/?pretty";

        List<String> lines = Collections.emptyList();
        lines  = Files.readAllLines(Paths.get(payloadLocation), StandardCharsets.UTF_8);
        Iterator<String> itr = lines.iterator();
        StringBuilder payload = new StringBuilder();
        while (itr.hasNext())
            payload.append(itr.next());

        CredentialsProvider provider = new BasicCredentialsProvider();
        provider.setCredentials(0
            AuthScope.ANY,
            new UsernamePasswordCredentials("tthomas", "abcd")
        );
        try (CloseableHttpClient client = HttpClientBuilder.create().
                    setDefaultCredentialsProvider(provider).build()) {

            HttpPost request = new HttpPost(url);
            request.setHeader("User-Agent", "Java client");
            request.setHeader("Content-Type","application/json");
            request.setHeader("Accept","application/json");

            request.setEntity(new StringEntity(payload.toString()));
            HttpResponse response = client.execute(request);
            BufferedReader bufReader = new BufferedReader(new InputStreamReader(
                response.getEntity().getContent()));
            String line ="";
            StringBuilder builder = new StringBuilder();
            while ((line = bufReader.readLine()) != null) {
                builder.append(line);
                builder.append(System.lineSeparator());
            }
            System.out.println(builder.toString());
        }
    }
}
