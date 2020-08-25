package com.example.elasticsearch.business;

import java.net.InetAddress;

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

// 体验详情，体验指数查询
public class BusinessExponentIds {

    private static final String TASK_TIME_FIELD = "taskTime";

    public static void main(String[] args) throws Exception {

        long end = System.currentTimeMillis();
        long start = end - 86400000 * 30;

        String[] ids = new String[] { "business_67329d4e-f907-42b2-b56a-f449f05e488d" };

        // 设置集群名称
        Settings settings = Settings.builder().put("cluster.name", "es-cluster").build();
        // 创建client
        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("172.16.3.249"), 9300));

        String agg = "agg";
        String avgAgg = "avgAgg";
        int page = 1;
        int pageSize = ids.length;

        BoolQueryBuilder queryBuilder =
            QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery(TASK_TIME_FIELD).gte(end).lte(start));

        PrefixQueryBuilder customerQuery = QueryBuilders.prefixQuery("customerId", "100001/100001");
        queryBuilder.must(customerQuery);

        TermsQueryBuilder http = QueryBuilders.termsQuery("metric", "ExperiencedExponent");
        queryBuilder.must(http);

        BoolQueryBuilder boolBuilder = QueryBuilders.boolQuery();
        for (String businessId : ids) {
            TermsQueryBuilder query = QueryBuilders.termsQuery("instId", businessId);
            boolBuilder.should(query);
        }
        queryBuilder.must(boolBuilder);
        AggregationBuilder aggregation = AggregationBuilders.terms(agg).field("instId").size(pageSize);
        AggregationBuilder subAggregation = AggregationBuilders.avg(avgAgg).field("val");
        aggregation.subAggregation(subAggregation);

        SearchRequestBuilder builder =
            client.prepareSearch("metrics-*").setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setQuery(queryBuilder)
                .setSize(pageSize).setFrom((page - 1) * pageSize).addAggregation(aggregation);

        SearchResponse response = builder.get();
        System.out.println("------business response ------:::" + response);
        client.close();
    }

}
