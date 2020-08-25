package com.example.elasticsearch;

import java.net.InetAddress;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class UpdateRequestTest {
    public static void main(String[] args) throws Exception {
        try {
            long start = System.currentTimeMillis();
            // 设置集群名称
            Settings settings = Settings.builder().put("cluster.name", "my-application").build();
            // 创建client
            TransportClient client = new PreBuiltTransportClient(settings);
            client.addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"), 9300));
            long end = System.currentTimeMillis();
            System.out.println("create transport user:" + (end - start));
            update1(client);

            // 关闭client
            client.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void update1(TransportClient client) throws Exception {
        UpdateRequest uRequest = new UpdateRequest();
        uRequest.index("blog");
        uRequest.type("article");
        uRequest.id("lnNbZWUB-n9kq07YuSN-");
        uRequest.doc(XContentFactory.jsonBuilder().startObject().field("content", "Hibernate框架基础介绍及使用...").endObject());
        UpdateResponse updateResponse = client.update(uRequest).get();
        System.out.println(updateResponse.getResult());
    }

    private static void update2(TransportClient client) throws Exception {
        // 方法五：upsert 如果文档不存在则创建新的索引
        IndexRequest indexRequest =
            new IndexRequest("blog", "article", "10").source(XContentFactory.jsonBuilder().startObject()
                .field("id", "10").field("title", "Git安装10").field("content", "学习目标 git。。。10").endObject());

        UpdateRequest uRequest2 =
            new UpdateRequest("blog", "article", "10").doc(
                XContentFactory.jsonBuilder().startObject().field("title", "Git安装").field("content", "学习目标 git。。。")
                    .endObject()).upsert(indexRequest);
        UpdateResponse updateResponse = client.update(uRequest2).get();
        System.out.println(updateResponse.getResult());
        // UpdateRequest uRequest2 =
        // new UpdateRequest("blog", "article", "10").doc(
        // XContentFactory.jsonBuilder().startObject().field("title", "Git安装").field("content", "学习目标 git。。。")
        // .endObject()).upsert(indexRequest);
        // UpdateResponse updateResponse = client.update(uRequest2).get();
        // System.out.println(updateResponse.getResult());
    }

    private static void update3(TransportClient client) throws Exception {
        long start = System.currentTimeMillis();
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        for (int i = 0; i < 10000; i++) {
            String id = "" + i;
            String title = "title-" + i;
            String newTitle = "new-title-" + i;
            String content = "content-" + i;
            String newContent = "new-content-" + i;
            IndexRequest indexRequest =
                new IndexRequest("blog", "article", id).source(XContentFactory.jsonBuilder().startObject()
                    .field("id", id).field("title", title).field("content", content).endObject());
            UpdateRequest request =
                new UpdateRequest("blog", "article", id).doc(
                    XContentFactory.jsonBuilder().startObject().field("title", newTitle).field("content", newContent)
                        .endObject()).upsert(indexRequest);
            bulkRequest.add(request);
        }
        BulkResponse response = bulkRequest.execute().actionGet();
        long end = System.currentTimeMillis();
        System.out.println("execute bulk request use:" + (end - start));
        System.out.println("bulk request has failures:" + response.hasFailures());
    }
}
