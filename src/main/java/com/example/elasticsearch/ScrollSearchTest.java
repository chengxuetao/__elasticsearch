package com.example.elasticsearch;

import java.net.InetAddress;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class ScrollSearchTest {

    public static void main(String[] args) throws Exception {
        try {
            Settings settings = Settings.builder().put("cluster.name", "es-cluster").build();
            TransportClient client = new PreBuiltTransportClient(settings);
            client.addTransportAddress(new TransportAddress(InetAddress.getByName("172.16.3.198"), 9300));
            long count = 0;
            SearchResponse scrollResp = client.prepareSearch("metrics-2019.06.11").addSort("taskTime", SortOrder.DESC)
                .setScroll(new TimeValue(60000)).setSize(10000).get();
            System.out.println("scrollId=" + scrollResp.getScrollId());
            do {
                SearchHits hits = scrollResp.getHits();
                count += hits.getHits().length;
                System.out.println("total count=" + hits.getTotalHits() + ", search count=" + count);
                // for (SearchHit hit : scrollResp.getHits().getHits()) {
                // Handle the hit...
                // }

                scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000))
                    .execute().actionGet();
                TimeUnit.SECONDS.sleep(1);
            } while (scrollResp.getHits().getHits().length != 0);

            // 关闭client
            client.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
