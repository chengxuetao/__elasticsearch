package com.example.elasticsearch;

import java.net.InetAddress;

import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class CloseIndex {

    private static final String TASK_TIME_FIELD = "taskTime";

    private static final String INDEX = "metrics-*";

    public static void main(String[] args) throws Exception {

        Settings settings = Settings.builder().put("cluster.name", "es-cluster").build();// cluster.name在elasticsearch.yml中配置
        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("172.16.3.198"), 9300));

        // client.admin().indices().prepareOpen("metrics-2019.06.03").execute().actionGet();
        // client.admin().indices().prepareClose("metrics-lasted").execute().actionGet();

        // long start = System.currentTimeMillis();
        // ImmutableOpenMap<String, IndexMetaData> indices =
        // client.admin().cluster().prepareState().execute().actionGet().getState().getMetaData().getIndices();
        //
        // indices.forEach((a) -> {
        // String key = a.key;
        // IndexMetaData value = a.value;
        // System.out.println(key + ": " + value.getState());
        // });
        //
        // System.out.println("Elapsed: " + (System.currentTimeMillis() - start));

        TermQueryBuilder instQuery = QueryBuilders.termQuery("instId", "6e39c2af40b88327cf15acb47505b8b6");
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        queryBuilder.must(instQuery);
        String[] inds = new String[0];
        String response = client.prepareSearch(inds).setSearchType(SearchType.QUERY_THEN_FETCH).setQuery(queryBuilder)
            .execute().actionGet().toString();
        System.out.println(response);
        client.close();
    }
}
