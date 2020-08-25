package com.example.elasticsearch;

import java.net.InetAddress;

import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

public class IndexAdminTest {

    public static void main(String[] args) throws Exception {
        Settings settings = Settings.builder().put("cluster.name", "es-cluster").build();// cluster.name在elasticsearch.yml中配置
        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("172.16.3.198"), 9300));

        IndicesAdminClient indices = client.admin().indices();
        IndicesExistsResponse indicesExistsResponse = indices.prepareExists("metrics-lasted").get();
        System.out.println("index[metrics-lasted] exist:" + indicesExistsResponse.isExists());

        GetSettingsResponse getSettingsResponse = indices.prepareGetSettings("metrics-lasted").get();
        ImmutableOpenMap<String, Settings> indexToSettings = getSettingsResponse.getIndexToSettings();
        for (ObjectObjectCursor<String, Settings> cursor : indexToSettings) {
            String index = cursor.key;
            Settings setting = cursor.value;
            System.out.println(index + ":" + setting);
        }

        GetMappingsResponse getMappingsResponse = indices.prepareGetMappings("metrics-lasted").get();
        ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings =
            getMappingsResponse.getMappings();
        ImmutableOpenMap<String, MappingMetaData> immutableOpenMap = mappings.get("metrics-lasted");
        MappingMetaData mappingMetaData = immutableOpenMap.get("tsdb");
        System.out.println(mappingMetaData.getSourceAsMap());
        client.close();
    }

}
