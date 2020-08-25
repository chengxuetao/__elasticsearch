package com.example.elasticsearch.business;

import java.net.InetAddress;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class CalIndexsTest {

    public static void main(String[] args) throws Exception {
        // 设置集群名称
        Settings settings = Settings.builder().put("cluster.name", "es-cluster").build();
        // 创建client
        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("172.16.3.198"), 9300));

        long startMs = 1543458013105L;
        long endMs = 1543458613105L;

        List<String> calIndexs = calIndexs(client.admin(), "metrics", startMs, endMs);
        System.out.println(calIndexs);

        client.close();
    }

    public static List<String> calIndexs(AdminClient adminClient, String indexPrefix, long start, long end) {

        DateFormat dateFormat = new SimpleDateFormat("yyyy.MM.dd");

        List<String> list = new ArrayList<>();
        Calendar dateFrom = Calendar.getInstance();
        dateFrom.setTimeInMillis(start);
        Calendar dateTo = Calendar.getInstance();
        dateTo.setTimeInMillis(end);
        // Date dateFrom = new Date(start);
        // Date dateTo = new Date(end);

        Calendar cal = null;
        while (dateFrom.before(dateTo) || dateFrom.get(Calendar.DATE) == dateTo.get(Calendar.DATE)) {
            cal = Calendar.getInstance();
            cal.setTime(dateFrom.getTime());
            String index = indexPrefix + "-" + dateFormat.format(cal.getTime());
            if (exists(adminClient, index)) {
                list.add(index);
            }
            cal.add(Calendar.DAY_OF_MONTH, 1);
            dateFrom = cal;
        }

        return list;
    }

    public static boolean exists(AdminClient adminClient, String indexName) {
        IndicesExistsRequest request = new IndicesExistsRequest(indexName);
        try {
            IndicesExistsResponse response = adminClient.indices().exists(request).get(10, TimeUnit.SECONDS);
            return response.isExists();
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            return false;
        }
    }

}
