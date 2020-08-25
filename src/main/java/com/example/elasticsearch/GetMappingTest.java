package com.example.elasticsearch;

import java.net.InetAddress;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

public class GetMappingTest {
    public static String clusterName = "my-application";// 集群名称
    public static String serverIP = "127.0.0.1";// 服务器IP

    public static void main(String[] args) {
        System.out.println(getMapping("blog", "article"));
    }

    public static String getMapping(String indexname, String typename) {
        Settings settings = Settings.builder().put("cluster.name", clusterName).build();
        String mapping = "";
        try {
            TransportClient client = new PreBuiltTransportClient(settings);
            client.addTransportAddress(new TransportAddress(InetAddress.getByName(serverIP), 9300));

            ImmutableOpenMap<String, MappingMetaData> mappings =
                client.admin().cluster().prepareState().execute().actionGet().getState().getMetaData().getIndices()
                    .get(indexname).getMappings();
            for (ObjectObjectCursor<String, MappingMetaData> cursor : mappings) {
                System.out.println(cursor.key); // 索引下的每个type
                System.out.println(cursor.value.source().string()); // 每个type的mapping
            }

            // mapping = mappings.get(typename).source().toString();

            client.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return mapping;
    }

}
