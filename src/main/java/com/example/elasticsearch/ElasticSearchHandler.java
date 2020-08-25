package com.example.elasticsearch;

import java.net.InetAddress;
import java.util.List;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class ElasticSearchHandler {
    public static void main(String[] args) {
        try {
            /* 创建客户端 */
            // client startup
            Settings esSettings = Settings.builder().put("cluster.name", "my-application") // 设置ES实例的名称
                .put("client.transport.sniff", true) // 自动嗅探整个集群的状态，把集群中其他ES节点的ip添加到本地的客户端列表中
                .build();

            TransportClient client = new PreBuiltTransportClient(esSettings);// 初始化client较老版本发生了变化，此方法有几个重载方法，初始化插件等。

            // 此步骤添加IP，至少一个，其实一个就够了，因为添加了自动嗅探配置
            client.addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"), 9300));

            List<byte[]> jsonData = DataFactory.getInitJsonData();

            for (int i = 0; i < jsonData.size(); i++) {
                IndexResponse response =
                    client.prepareIndex("blog", "article").setSource(jsonData.get(i), XContentType.JSON).get();
                System.out.println(response.getResult());
            }
            client.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
