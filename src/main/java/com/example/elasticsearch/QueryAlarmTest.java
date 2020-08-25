package com.example.elasticsearch;

import java.net.InetAddress;

import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class QueryAlarmTest {
    public static void main(String[] args) throws Exception {
        try {
            // 设置集群名称
            Settings settings = Settings.builder().put("cluster.name", "es-cluster").build();
            // 创建client
            TransportClient client = new PreBuiltTransportClient(settings);
            client.addTransportAddress(new TransportAddress(InetAddress.getByName("172.16.3.198"), 9300));
            long startMs = 1540968650635L;
            long endMs = 1541573769861L;
            long start = System.currentTimeMillis();
            BoolQueryBuilder queryBuilder =
                QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("createdTime").gte(startMs).lte(endMs));

            // 172.16.3.158
            // CPU利用率重要预警
            // deta-ubuntu
            String condition = "deta-ubuntu";
            // QueryBuilder match1 =
            // QueryBuilders.constantScoreQuery(QueryBuilders.matchQuery("content", condition).minimumShouldMatch(
            // "100%"));
            // QueryStringQueryBuilder content = new QueryStringQueryBuilder("\"" + condition + "\"").field("content");
            // match1 = QueryBuilders.constantScoreQuery(content);
            // QueryBuilder match2 =
            // QueryBuilders.constantScoreQuery(QueryBuilders.matchQuery("instName", condition).minimumShouldMatch(
            // "100%"));
            // QueryBuilder match3 = QueryBuilders.constantScoreQuery(QueryBuilders.matchQuery("instIp", condition));
            // BoolQueryBuilder conditionBuilder =
            // QueryBuilders.boolQuery().should(match1).should(match2).should(match3);
            QueryStringQueryBuilder content = new QueryStringQueryBuilder("\"" + condition + "\"");
            QueryBuilder conditionBuilder = QueryBuilders.constantScoreQuery(content);
            queryBuilder.must(conditionBuilder);

            String response =
                client.prepareSearch("alarm-history-*").setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                    .setQuery(queryBuilder).setSize(20).setFrom(0).execute().actionGet().toString();
            System.out.println("search use time:" + (System.currentTimeMillis() - start));
            JSONObject jsonResponse = JSONObject.parseObject(response);
            jsonResponse = jsonResponse.getJSONObject("hits");
            JSONArray jsonArray = jsonResponse.getJSONArray("hits");
            System.out.println("Total size:" + jsonArray.size());
            if (jsonArray != null && jsonArray.size() > 0) {
                for (int i = 0; i < jsonArray.size(); i++) {
                    JSONObject jsonObject = jsonArray.getJSONObject(i);
                    System.out.println(jsonObject.getString("_source"));
                    System.out.println("=======================================================");
                }
            }
            // 关闭client
            client.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
