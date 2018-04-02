package com.ainory.kafka.streams.entity;


import com.ainory.kafka.streams.util.JsonUtil;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by ainory on 2017. 3. 23..
 */
public class CollectdKafkaVO implements Serializable{

/*
[
  {
    "dsnames": [
      "value"
    ],
    "dstypes": [
      "gauge"
    ],
    "host": "afnas02",
    "interval": 60.0,
    "meta": {
      "network:received": true
    },
    "plugin": "df",
    "plugin_instance": "run-user-1000",
    "time": 1490174400.921,
    "type": "df_complex",
    "type_instance": "used",
    "values": [
      0
    ]
  }
]
 */

    private ArrayList<String> dsnames;
    private ArrayList<String> dstypes;
    private String host;
    private Double interval;
    private HashMap meta;
    private String plugin;
    private String plugin_instance;
    private String time;
    private String type;
    private String type_instance;
    private ArrayList values;

    public ArrayList<String> getDsnames() {
        return dsnames;
    }

    public void setDsnames(ArrayList<String> dsnames) {
        this.dsnames = dsnames;
    }

    public ArrayList<String> getDstypes() {
        return dstypes;
    }

    public void setDstypes(ArrayList<String> dstypes) {
        this.dstypes = dstypes;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Double getInterval() {
        return interval;
    }

    public void setInterval(Double interval) {
        this.interval = interval;
    }

    public HashMap getMeta() {
        return meta;
    }

    public void setMeta(HashMap meta) {
        this.meta = meta;
    }

    public String getPlugin() {
        return plugin;
    }

    public void setPlugin(String plugin) {
        this.plugin = plugin;
    }

    public String getPlugin_instance() {
        return plugin_instance;
    }

    public void setPlugin_instance(String plugin_instance) {
        this.plugin_instance = plugin_instance;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getType_instance() {
        return type_instance;
    }

    public void setType_instance(String type_instance) {
        this.type_instance = type_instance;
    }

    public ArrayList getValues() {
        return values;
    }

    public void setValues(ArrayList values) {
        this.values = values;
    }





    public static void main(String[] args) {


//        String json = "[{\"values\":[0],\"dstypes\":[\"gauge\"],\"dsnames\":[\"value\"],\"time\":1490174400.921,\"interval\":60.000,\"host\":\"afnas02\",\"plugin\":\"df\",\"plugin_instance\":\"run-user-1000\",\"type\":\"df_complex\",\"type_instance\":\"used\",\"meta\":{\"network:received\":true}}]";
        String json = "[{\"values\":[2.07,2.07,2.04],\"dstypes\":[\"gauge\",\"gauge\",\"gauge\"],\"dsnames\":[\"shortterm\",\"midterm\",\"longterm\"],\"time\":1489484423.230,\"interval\":60.000,\"host\":\"afnas-app\",\"plugin\":\"load\",\"plugin_instance\":\"\",\"type\":\"load\",\"type_instance\":\"\"}]";


        CollectdKafkaVO[] arrCollectdKafkaVO = (CollectdKafkaVO[]) JsonUtil.jsonStringToObject(json, CollectdKafkaVO[].class);

        System.out.println();



    }
}
