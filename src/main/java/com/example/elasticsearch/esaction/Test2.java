package com.example.elasticsearch.esaction;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

public class Test2 {

    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = null;
        try {
            RestClientBuilder builder = RestClient.builder(new HttpHost("172.16.5.161", 30200));
            builder.setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder.setConnectTimeout(6000000).setSocketTimeout(180000000));
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("elastic", "Deta2020"));
            builder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
            client = new RestHighLevelClient(builder);

            BulkProcessor.Listener listener = new BulkProcessor.Listener() {
                @Override
                public void beforeBulk(long executionId, BulkRequest request) {
                    int numberOfActions = request.numberOfActions();
                    System.out.println("Executing bulk [" + executionId + "] with " + numberOfActions + " requests.");
                }

                @Override
                public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                    System.err.println("Failed to execute bulk.");
                    failure.printStackTrace();
                }

                @Override
                public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                    if (response.hasFailures()) {
                        System.out.println("Bulk [" + executionId + "] executed with failures, response = "
                                + response.buildFailureMessage());
                    } else {
                        System.out.println("Bulk [" + executionId + "] completed in " + response.getTook().getMillis() + " milliseconds.");
                    }
                }
            };
            RestHighLevelClient finalClient = client;
            BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer =
                    (request, bulkListener) -> finalClient.bulkAsync(request, RequestOptions.DEFAULT, bulkListener);
            BulkProcessor processor = BulkProcessor.builder(consumer, listener).setBulkActions(500)
                    .setBulkSize(new ByteSizeValue(5L, ByteSizeUnit.MB)).setConcurrentRequests(0)
                    //.setFlushInterval(TimeValue.timeValueSeconds(1L))
                    .setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(1L), 3)).build();
            String index = "reindex_test_index";
            try {
                DeleteIndexRequest request = new DeleteIndexRequest(index);
                client.indices().delete(request, RequestOptions.DEFAULT);
            } catch (Exception e) {
            }

            long start = System.currentTimeMillis();
            // SearchRequest searchRequest = new SearchRequest(index + "_copy");
            // SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
            // searchBuilder.trackTotalHits(true);
            // searchRequest.source(searchBuilder);
            // SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
            // TotalHits totalHits = response.getHits().getTotalHits();
            // long value = totalHits.value;
            // System.out.println("total hits==================" + value);
            add(processor, index);
            long end = System.currentTimeMillis();
            System.out.println("索引数据耗时：" + (end - start) + " ms.");

            TimeUnit.SECONDS.sleep(5);

            // start = System.currentTimeMillis();
            // String destIndex = index + "_copy";
            // ReindexRequest request = new ReindexRequest();
            // request.setSourceIndices(index).setDestIndex(destIndex);
            // client.reindex(request, RequestOptions.DEFAULT);
            // end = System.currentTimeMillis();
            // System.out.println("reindex耗时：" + (end - start) + " ms.");
            //
            // start = System.currentTimeMillis();

            // SearchRequest request1 = new SearchRequest(index);
            // SearchSourceBuilder source = new SearchSourceBuilder().size(10000).trackTotalHits(true);
            // request1.source(source);
            // request1.scroll(TimeValue.timeValueMinutes(1));
            // SearchResponse response1 = client.search(request1, RequestOptions.DEFAULT);
            // while (true) {
            //     List<Object> dataList = new ArrayList<>();
            //     for (SearchHit hit : response1.getHits().getHits()) {
            //         Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            //         IndexRequest indexRequest = new IndexRequest(index);
            //         indexRequest.source(sourceAsMap);
            //         processor.add(indexRequest);
            //     }
            //     String scorllId = response1.getScrollId();
            //     if (scorllId != null && !scorllId.isEmpty()) {
            //         SearchScrollRequest scrollRequest = new SearchScrollRequest(scorllId);
            //         scrollRequest.scroll(TimeValue.timeValueMinutes(1));
            //         response1 = client.scroll(scrollRequest, RequestOptions.DEFAULT);
            //     } else {
            //         break;
            //     }
            //     if (response1.getHits().getHits().length == 0) {
            //         break;
            //     }
            // }
            //end = System.currentTimeMillis();
            //System.out.println("scroll查询写入耗时：" + (end - start) + " ms.");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }

    public static void add(BulkProcessor processor, String index) {
        for (int i = 1; i <= 500000; i++) {
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
            if (i % 1000 == 0) {
                System.err.println("Write count " + i);
            }
        }
    }

}
