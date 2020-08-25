package com.example.elasticsearch;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class ElasticSearchBulkOut {

    public static void main(String[] args) {
        try {
            Settings settings = Settings.builder().put("cluster.name", "my-application").build();// cluster.name在elasticsearch.yml

            TransportClient client = new PreBuiltTransportClient(settings);
            client.addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"), 9300));

            QueryBuilder qb = QueryBuilders.matchAllQuery();
            // qb = QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("id", "8"));
            SearchResponse response =
                client.prepareSearch("blog").setTypes("article").setQuery(qb).execute().actionGet();
            SearchHits resultHits = response.getHits();

            File article = new File("bulk.txt");
            FileWriter fw = new FileWriter(article);
            BufferedWriter bfw = new BufferedWriter(fw);

            if (resultHits.getHits().length == 0) {
                System.out.println("查到0条数据!");

            } else {
                for (int i = 0; i < resultHits.getHits().length; i++) {
                    String jsonStr = resultHits.getHits()[i].getSourceAsString();
                    System.out.println(jsonStr);
                    bfw.write(jsonStr);
                    bfw.write("\n");
                }
            }
            bfw.close();
            fw.close();
            // 关闭client
            client.close();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
