package com.example.elasticsearch.esaction;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class Test3 {

    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = null;
        try {
            RestClientBuilder builder = RestClient.builder(new HttpHost("172.16.3.198", 9200));
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("elastic", "detadatapoint"));
            builder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
            client = new RestHighLevelClient(builder);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }

    public static void add(BulkProcessor processor, String index) {
        for (int i = 0; i < 500000; i++) {
            IndexRequest request = new IndexRequest(index);
            Map<String, String> data = new HashMap<>();
            String uuid = UUID.randomUUID().toString();
            data.put("id", uuid);
            data.put("name", "name-" + uuid);
            data.put("sex", "F");
            data.put("field1", Math.random() + "");
            data.put("field2", Math.random() + "");
            data.put("field3", Math.random() + "");
            data.put("field4", Math.random() + "");
            data.put("field5", Math.random() + "");
            data.put("field6", Math.random() + "");
            data.put("field7", Math.random() + "");
            data.put("field8", Math.random() + "");
            data.put("field9", Math.random() + "");
            data.put("introduce", "不忘初衷，及时悔过，便永远不晚。也许，更多的时候，人生走出的是一条曲线，终点又回到起点，生命才是最圆满的吧。");
            request.source(data);
            processor.add(request);
        }
    }

}
