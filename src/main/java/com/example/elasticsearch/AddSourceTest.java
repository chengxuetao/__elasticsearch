package com.example.elasticsearch;

import java.net.InetAddress;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class AddSourceTest {
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

            byte[] data = JsonUtil.model2Json(new Blog(9, "亿级电商ElasticSearch开发实践", "2016-06-19",
                " 本书介绍了ES的命令行操作方式，集群的概念，怎样用Java API来操作ES集群，并通过一个订单查询系统的电商实现来介绍ES在工程上的应用。"));

            IndexResponse response = client.prepareIndex("blog", "article").setSource(data, XContentType.JSON).get();

            System.out.println(response.getResult());

            client.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
