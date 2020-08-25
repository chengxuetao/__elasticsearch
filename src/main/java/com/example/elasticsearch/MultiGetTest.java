package com.example.elasticsearch;

import java.net.InetAddress;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class MultiGetTest {

    public static void main(String[] args) {
        try {
            Settings settings = Settings.builder().put("cluster.name", "my-application").build();// cluster.name在elasticsearch.yml中配置

            TransportClient client = new PreBuiltTransportClient(settings);
            client.addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"), 9300));

            MultiGetResponse multiGetItemResponses =
                client.prepareMultiGet().add("blog", "article", "sHOeZWUB-n9kq07YDyO8")
                    .add("blog", "article", "sXOeZWUB-n9kq07YDyO8", "snOeZWUB-n9kq07YDyO8", "4")
                    .add("another", "article", "foo").get();

            for (MultiGetItemResponse itemResponse : multiGetItemResponses) {
                if (itemResponse.isFailed()) {
                    System.out.println(itemResponse.getIndex() + "-" + itemResponse.getType() + "-"
                        + itemResponse.getId() + ":" + itemResponse.getFailure().getMessage());
                } else {
                    GetResponse response = itemResponse.getResponse();
                    if (response.isExists()) {
                        String json = response.getSourceAsString();
                        System.out.println(json);
                    } else {
                        System.out.println(response.getIndex() + "-" + response.getType() + "-" + response.getId()
                            + " not exist.");
                    }
                }
            }
            client.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
