package com.example.elasticsearch;

import java.net.InetAddress;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class DeleteTest {
    public static void main(String[] args) throws Exception {
        try {
            // 设置集群名称
            Settings settings = Settings.builder().put("cluster.name", "my-application").build();
            // 创建client
            TransportClient client = new PreBuiltTransportClient(settings);
            client.addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"), 9300));

            DeleteResponse response = client.prepareDelete("blog", "article", "10").execute().actionGet();
            System.out.println(response.getResult());
            // 关闭client
            client.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
