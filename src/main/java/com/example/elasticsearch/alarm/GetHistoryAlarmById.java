package com.example.elasticsearch.alarm;

import java.net.InetAddress;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class GetHistoryAlarmById {
    public static void main(String[] args) throws Exception {
        try {
            // 设置集群名称
            Settings settings = Settings.builder().put("cluster.name", "es-cluster").build();
            // 创建client
            TransportClient client = new PreBuiltTransportClient(settings);
            client.addTransportAddress(new TransportAddress(InetAddress.getByName("172.16.3.198"), 9300));

            BoolQueryBuilder queryBuilder =
                QueryBuilders.boolQuery().must(QueryBuilders.termQuery("id.raw", "5b7e7390-af75-4e5d-87d1-ca9a840e3f2c"));

            SearchRequestBuilder builder = client.prepareSearch("alarm-history*")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setQuery(queryBuilder).setSize(1);
            String result = null;
            SearchResponse response = builder.get();
            if (response != null && response.getHits() != null) {
                SearchHits hits = response.getHits();
                if (hits.getHits().length > 0) {
                    result = hits.getHits()[0].getSourceAsString();
                }
            }

            System.out.println(result);

            // 关闭client
            client.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
