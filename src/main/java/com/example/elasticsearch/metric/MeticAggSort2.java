package com.example.elasticsearch.metric;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

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

public class MeticAggSort2 {
    public static void main(String[] args) throws Exception {
        // 设置集群名称
        Settings settings = Settings.builder().put("cluster.name", "es-cluster").build();
        // 创建client
        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("172.16.3.198"), 9300));

        long end = new Date().getTime();
        long start = end - 55 * 60 * 60 * 1000;

        List<String> result = new ArrayList<>();
        BoolQueryBuilder queryBuilder =
            QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("taskTime").gte(start).lte(end));
        TermQueryBuilder metricBuilder = QueryBuilders.termQuery("metric", "CPURate");
        queryBuilder.must(metricBuilder);
        // TermsQueryBuilder instBuilder = QueryBuilders.termsQuery("instId", instIds);
        // queryBuilder.must(instBuilder);
        SearchRequestBuilder builder = client.prepareSearch("metrics-*").setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
            .setQuery(queryBuilder).addSort("taskTime", SortOrder.ASC).setSize(10000);
        SearchResponse response = builder.get();
        if (response.getHits() != null && response.getHits().getHits() != null) {
            SearchHit[] hits = response.getHits().getHits();
            for (SearchHit searchHit : hits) {
                result.add(searchHit.getSourceAsString());
            }
        }

        System.out.println(result);
        client.close();
    }
}
