package com.example.elasticsearch.business;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.PrefixQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

// 主题，业务体验列表
public class BusinessExponentTop {

    private static final String TASK_TIME_FIELD = "taskTime";

    private static final String CUSTOMERID = "100001/100001";

    public static void main(String[] args) throws Exception {

        long end = System.currentTimeMillis();
        long start = end - 24 * 60 * 60 * 1000;

        List<String> businessIds = new ArrayList<>();
        businessIds.add("business_67329d4e-f907-42b2-b56a-f449f05e488d");

        // 设置集群名称
        Settings settings = Settings.builder().put("cluster.name", "es-cluster").build();
        // 创建client
        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("172.16.3.249"), 9300));

        String agg = "agg";
        String avgAgg = "avgAgg";

        BoolQueryBuilder queryBuilder =
            QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery(TASK_TIME_FIELD).gte(end).lte(start));

        PrefixQueryBuilder customerQuery = QueryBuilders.prefixQuery("customerId", CUSTOMERID);
        queryBuilder.must(customerQuery);

        TermsQueryBuilder http = QueryBuilders.termsQuery("metric", "ExperiencedExponent");
        queryBuilder.must(http);

        TermsQueryBuilder businessQuery = QueryBuilders.termsQuery("instId", businessIds);
        queryBuilder.must(businessQuery);

        AggregationBuilder aggregation = AggregationBuilders.terms(agg).field("instId").size(1);
        AggregationBuilder subAggregation = AggregationBuilders.avg(avgAgg).field("val");
        aggregation.subAggregation(subAggregation);

        SearchRequestBuilder builder =
            client.prepareSearch("metrics-*").setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setQuery(queryBuilder)
                .setSize(1).setFrom(0).addAggregation(aggregation);

        SearchResponse response = builder.get();
        System.out.println("------business response ------:::" + response);
        client.close();
    }
}
