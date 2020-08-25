package com.example.elasticsearch.search;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class BulkRequestTest {

    public static void main(String[] args) throws Exception {
        Settings settings = Settings.builder().put("cluster.name", "es-cluster").build();// cluster.name在elasticsearch.yml中配置
        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("172.16.5.181"), 9300));

        BulkRequestBuilder bulkRequest = client.prepareBulk();

        Map<String, Object> data = new HashMap<String, Object>();
        data.put("indexId", "10000000000000000000000000001");
        data.put("metric", "PingAvailStatus");
        data.put("customerId", "100001/100001");
        data.put("instId", "71f84d9979c59c6f91568c775beb7763");
        data.put("instName", "EG2000SE");
        data.put("taskTime", 1556420087702L);
        data.put("stringVal", "ok");
        data.put("rollupId", "100001/100001_71f84d9979c59c6f91568c775beb7763_PingAvailStatus");
        String id = data.get("indexId").toString();
        XContentBuilder object = XContentFactory.jsonBuilder().startObject();
        Set<Entry<String, Object>> entrySet = data.entrySet();
        for (Entry<String, Object> entry : entrySet) {
            object.field(entry.getKey(), entry.getValue());
        }
        object = object.endObject();
        IndexRequest indexRequest = new IndexRequest("metrics-lasted", "tsdb", id).source(object);
        UpdateRequest request = new UpdateRequest("metrics-lasted", "tsdb", id).doc(object).upsert(indexRequest);
        bulkRequest.add(request);
        if (bulkRequest.numberOfActions() > 0) {
            BulkResponse response = bulkRequest.execute().actionGet();
            if (response.hasFailures()) {
                System.out.println(response.buildFailureMessage());
            }
        }

        client.close();
    }

}
