package com.example.elasticsearch;

import java.util.ArrayList;
import java.util.List;

public class DataFactory {
    public static DataFactory dataFactory = new DataFactory();

    private DataFactory() {
    }

    public DataFactory getInstance() {
        return dataFactory;
    }

    public static List<byte[]> getInitJsonData() throws Exception {
        List<byte[]> list = new ArrayList<byte[]>();
        byte[] data1 = JsonUtil.model2Json(new Blog(1, "git简介", "2016-06-19", "SVN与Git最主要的区别..."));
        byte[] data2 = JsonUtil.model2Json(new Blog(2, "Java中泛型的介绍与简单使用", "2016-06-19", "学习目标 掌握泛型的产生意义..."));
        byte[] data3 = JsonUtil.model2Json(new Blog(3, "SQL基本操作", "2016-06-19", "基本操作：CRUD ..."));
        byte[] data4 = JsonUtil.model2Json(new Blog(4, "Hibernate框架基础", "2016-06-19", "Hibernate框架基础..."));
        byte[] data5 = JsonUtil.model2Json(new Blog(5, "Shell基本知识", "2016-06-19", "Shell是什么..."));
        byte[] data6 = JsonUtil.model2Json(new Blog(6, "Git基本知识", "2016-06-19", "Git基本知识..."));
        byte[] data7 = JsonUtil.model2Json(new Blog(7, "C++基本知识", "2016-06-19", "C++基本知识..."));
        byte[] data8 = JsonUtil.model2Json(new Blog(8, "Mysql基本知识", "2016-06-19", "Mysql基本知识..."));
        list.add(data1);
        list.add(data2);
        list.add(data3);
        list.add(data4);
        list.add(data5);
        list.add(data6);
        list.add(data7);
        list.add(data8);
        return list;
    }

}
