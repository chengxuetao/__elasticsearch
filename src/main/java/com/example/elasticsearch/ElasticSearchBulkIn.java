package com.example.elasticsearch;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import com.alibaba.fastjson.JSONObject;

public class ElasticSearchBulkIn {

    static SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");

    public static void main1(String[] args) {
        try {
            Settings settings = Settings.builder().put("cluster.name", "es-cluster").build();// cluster.name在elasticsearch.yml中配置
            TransportClient client = new PreBuiltTransportClient(settings);
            client.addTransportAddress(new TransportAddress(InetAddress.getByName("172.16.3.198"), 9300));

            // InputStream is = new FileInputStream(new File("mapping.json"));
            // byte[] bytes = IOUtils.toByteArray(is);
            // IndicesAdminClient adminClient = client.admin().indices();
            // adminClient.putTemplate(new PutIndexTemplateRequest("jsgn-template").source(bytes, XContentType.JSON));

            BufferedReader reader =
                new BufferedReader(new InputStreamReader(new FileInputStream(new File("syslog2.txt")), "GBK"));
            String line = null;
            int i = 0;
            BulkRequestBuilder bulkRequest = client.prepareBulk();
            while ((line = reader.readLine()) != null) {
                i++;
                // if (i >= 10000) {
                // break;
                // }
                bulkRequest.add(client.prepareIndex("syslog-2018.11.01", "jsgn").setSource(
                    buildSource(line).toJSONString(), XContentType.JSON));
            }
            BulkResponse response = bulkRequest.execute().actionGet();
            if (response.hasFailures()) {
                System.out.println(response.buildFailureMessage());
            }

            reader.close();
            client.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(new File("syslog3.txt")));
            BufferedReader reader =
                new BufferedReader(new InputStreamReader(new FileInputStream(new File("syslog.txt")), "GBK"));
            String line = null;
            Map<String, JSONObject> map = new HashMap<>();
            while ((line = reader.readLine()) != null) {
                JSONObject source = buildSource(line);
                // String dname = source.getString("dname");
                // map.put(dname, source);
                String pkvalues = source.getString("pkvalues");
                if (pkvalues != null) {
                    String pk = pkvalues.replaceAll(";", "");
                    int pkcount = pkvalues.length() - pk.length();
                    if (pkcount > 100) {
                        // System.out.println(pkcount);
                        writer.write(line + "\r\n");
                    }
                }
            }
            // Set<Entry<String, JSONObject>> entrySet = map.entrySet();
            // for (Entry<String, JSONObject> entry : entrySet) {
            // System.out.println(entry.getKey() + "------" + entry.getValue().toJSONString());
            // }
            reader.close();
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static JSONObject buildSource(String line) throws Exception {
        JSONObject json = new JSONObject();
        String devid = "";
        Pattern p = Pattern.compile("devid=(.*?)\\s");
        Matcher matcher = p.matcher(line);
        if (matcher.find()) {
            devid = matcher.group(1);
        }
        json.put("devid", devid);
        String logtype = "";
        p = Pattern.compile("logtype=(.*?)\\s");
        matcher = p.matcher(line);
        if (matcher.find()) {
            logtype = matcher.group(1);
        }
        json.put("logtype", logtype);
        String mod = "";
        p = Pattern.compile("mod=\"(.*?)\"");
        matcher = p.matcher(line);
        if (matcher.find()) {
            mod = matcher.group(1);
        }
        json.put("mod", mod);
        String dname = "";
        p = Pattern.compile("dname=\"(.*?)\"");
        matcher = p.matcher(line);
        if (matcher.find()) {
            dname = matcher.group(1);
        }
        json.put("dname", dname);
        String pkvalues = "";
        p = Pattern.compile("pkvalues=\"(.*?)\"");
        matcher = p.matcher(line);
        if (matcher.find()) {
            pkvalues = matcher.group(1);
        }
        json.put("pkvalues", pkvalues);
        String datestr = "";
        p = Pattern.compile("date=\"(.*?)\"");
        matcher = p.matcher(line);
        if (matcher.find()) {
            datestr = matcher.group(1);
        } else {
            // System.out.println(line);
        }
        json.put("datestr", datestr);
        String date = "";
        p = Pattern.compile("date=\"(.*?)\"");
        matcher = p.matcher(line);
        if (matcher.find()) {
            date = matcher.group(1);
            Date parse = sdf.parse(date);
            date = parse.getTime() + "";
        }
        json.put("date", date);
        String appname = "";
        p = Pattern.compile("appname=\"(.*?)\"");
        matcher = p.matcher(line);
        if (matcher.find()) {
            appname = matcher.group(1);
        }
        json.put("appname", appname);
        String apporgnname = "";
        p = Pattern.compile("apporgnname=\"(.*?)\"");
        matcher = p.matcher(line);
        if (matcher.find()) {
            apporgnname = matcher.group(1);
        }
        json.put("apporgnname", apporgnname);
        String network = "";
        p = Pattern.compile("network=\"(.*?)\"");
        matcher = p.matcher(line);
        if (matcher.find()) {
            network = matcher.group(1);
        }
        json.put("network", network);
        String leveledid = "";
        p = Pattern.compile("leveledid=\"(.*?)\"");
        matcher = p.matcher(line);
        if (matcher.find()) {
            leveledid = matcher.group(1);
        }
        json.put("leveledid", leveledid);
        String alarmcode = "";
        p = Pattern.compile("alarmcode=\"(.*?)\"");
        matcher = p.matcher(line);
        if (matcher.find()) {
            alarmcode = matcher.group(1);
        }
        json.put("alarmcode", alarmcode);
        String message = "";
        p = Pattern.compile("message=\"(.*?)\"");
        matcher = p.matcher(line);
        if (matcher.find()) {
            message = matcher.group(1);
        }
        json.put("message", message);
        String apptype = "";
        p = Pattern.compile("apptype=\"(.*?)\"");
        matcher = p.matcher(line);
        if (matcher.find()) {
            apptype = matcher.group(1);
        }
        json.put("apptype", apptype);
        String appsourcename = "";
        p = Pattern.compile("appsourcename=\"(.*?)\"");
        matcher = p.matcher(line);
        if (matcher.find()) {
            appsourcename = matcher.group(1);
        }
        json.put("appsourcename", appsourcename);
        String bfiltered = "";
        p = Pattern.compile("bfiltered=\"(.*?)\"");
        matcher = p.matcher(line);
        if (matcher.find()) {
            bfiltered = matcher.group(1);
        }
        json.put("bfiltered", bfiltered);
        String filesize = "";
        p = Pattern.compile("filesize=\"(.*?)\"");
        matcher = p.matcher(line);
        if (matcher.find()) {
            filesize = matcher.group(1);
        }
        json.put("filesize", filesize);
        String filename = "";
        p = Pattern.compile("filename=\"(.*?)\"");
        matcher = p.matcher(line);
        if (matcher.find()) {
            filename = matcher.group(1);
        }
        json.put("filename", filename);
        String event = "";
        p = Pattern.compile("event=\"(.*?)\"");
        matcher = p.matcher(line);
        if (matcher.find()) {
            event = matcher.group(1);
        }
        json.put("event", event);
        String overpass = "";
        p = Pattern.compile("overpass=\"(.*?)\"");
        matcher = p.matcher(line);
        if (matcher.find()) {
            overpass = matcher.group(1);
        }
        json.put("overpass", overpass);
        String unpassreason = "";
        p = Pattern.compile("unpassreason=\"(.*?)\"");
        matcher = p.matcher(line);
        if (matcher.find()) {
            unpassreason = matcher.group(1);
        }
        json.put("unpassreason", unpassreason);
        String bytes = "";
        p = Pattern.compile("bytes=\"(.*?)\"");
        matcher = p.matcher(line);
        if (matcher.find()) {
            bytes = matcher.group(1);
        }
        json.put("bytes", bytes);
        return json;
    }
}
