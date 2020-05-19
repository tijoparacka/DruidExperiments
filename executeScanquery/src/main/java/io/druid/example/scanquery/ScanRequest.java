package io.druid.example.scanquery;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

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

        String url = "http://localhost:8888/druid/v2/";

        List<String> lines = Collections.emptyList();
        lines  = Files.readAllLines(Paths.get(payloadLocation), StandardCharsets.UTF_8);
        Iterator<String> itr = lines.iterator();
        StringBuilder payload = new StringBuilder();
        while (itr.hasNext())
            payload.append(itr.next());

//        CredentialsProvider provider = new BasicCredentialsProvider();
//        provider.setCredentials(0
//            AuthScope.ANY,
//            new UsernamePasswordCredentials("tthomas", "abcd")
//        );
//        try (CloseableHttpClient client = HttpClientBuilder.create().
//                    setDefaultCredentialsProvider(provider).build()) {
        try (CloseableHttpClient client = HttpClientBuilder.create().build()){
            HttpPost request = new HttpPost(url);
            request.setHeader("User-Agent", "Java client");
            request.setHeader("Content-Type","application/json");
            request.setHeader("Accept","application/json");

            request.setEntity(new StringEntity(payload.toString()));
            HttpResponse response = client.execute(request);

            InputStreamReader reader = new InputStreamReader(response.getEntity().getContent());
            JsonFactory jf = new JsonFactory();
            JsonParser jp = jf.createParser(reader);
         //  JsonParser jp = jf.createParser(ObjectReadContext.empty(), new ByteArrayInputStream(DOC.getBytes("UTF-8")));
            JsonToken current;
            while (jp.nextToken() != JsonToken.END_OBJECT) {
                String fieldName = jp.getCurrentName();
                // move from field name to field value
                jp.nextToken();
                jp.nextToken();
                jp.nextToken();
                System.out.println("Segement id : "+ jp.getValueAsString());
                jp.nextToken() ;
                if (jp.getValueAsString().equalsIgnoreCase("columns")){
                    while (jp.nextToken() != JsonToken.END_ARRAY) {
                        System.out.println("Columns : "+ jp.getValueAsString());
                    }
                }
                jp.nextToken() ;
                if (jp.getValueAsString().equalsIgnoreCase("events")){
                    System.out.println("Events : ");
                    jp.nextToken();
                    while (jp.nextToken() != JsonToken.END_ARRAY) {
//                      TreeNode tree= jp.readValueAsTree();
                        while (jp.nextToken() != JsonToken.END_OBJECT) {
                            System.out.print( jp.getValueAsString() + "  " );
                        }
                        System.out.print( "\n");
                    }
                }

            }
        }
    }
}
