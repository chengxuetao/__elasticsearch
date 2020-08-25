package com.example.elasticsearch;

import java.net.InetAddress;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class QueryBuilderTest {
    public static void main(String[] args) throws Exception {
        try {
            // 设置集群名称
            Settings settings = Settings.builder().put("cluster.name", "my-application").build();
            // 创建client
            TransportClient client = new PreBuiltTransportClient(settings);
            client.addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"), 9300));
            QueryBuilder qb2 = QueryBuilders.multiMatchQuery("基本", "title", "content");
            SearchResponse response =
                client.prepareSearch("blog").setTypes("article").setQuery(qb2).execute().actionGet();
            SearchHits hits = response.getHits();
            hits.forEach((item) -> {
                System.out.println(item.getSourceAsString());
            });
            // 关闭client
            client.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
