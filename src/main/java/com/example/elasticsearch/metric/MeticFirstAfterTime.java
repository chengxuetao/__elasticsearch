package com.example.elasticsearch.metric;

import java.net.InetAddress;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class MeticFirstAfterTime {

    private static final String TASK_TIME_FIELD = "taskTime";

    private static final String METRIC_INDEX = "metrics-*";

    public static void main(String[] args) throws Exception {
        // 设置集群名称
        Settings settings = Settings.builder().put("cluster.name", "es-cluster").build();
        // 创建client
        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("172.16.3.198"), 9300));

        long startMs = 1L;
        long endMs = 1566204971000L;

        BoolQueryBuilder queryBuilder =
            QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery(TASK_TIME_FIELD).gte(startMs));
        TermQueryBuilder instBuilder = QueryBuilders.termQuery("instId", "fb5e36a2099dd3c95dbcb342d83a5daf");
        queryBuilder.must(instBuilder);
        TermQueryBuilder metricBuilder = QueryBuilders.termQuery("metric", "MEMRate");
        queryBuilder.must(metricBuilder);

        SearchRequestBuilder builder = client.prepareSearch(METRIC_INDEX).setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
            .setQuery(queryBuilder).addSort(TASK_TIME_FIELD, SortOrder.ASC).setSize(2);
        SearchResponse response = builder.get();
        System.out.println(response);
        if (response.getHits() != null) {
            SearchHit[] hits = response.getHits().getHits();
            if (hits != null && hits.length > 0) {
                for (SearchHit searchHit : hits) {
                    System.out.println(searchHit.getSourceAsString());
                }
            }
        }
        client.close();
    }
}
