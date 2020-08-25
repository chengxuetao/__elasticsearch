package com.example.elasticsearch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonUtil {
    // Java实体对象转json对象
    public static byte[] model2Json(Blog blog) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        byte[] json = objectMapper.writeValueAsBytes(blog);
        return json;
    }
}
