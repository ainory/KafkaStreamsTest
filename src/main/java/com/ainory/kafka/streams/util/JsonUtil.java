package com.ainory.kafka.streams.util;


import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Created by ainory on 2016. 9. 30..
 */
public class JsonUtil {

    /**
     * Object to Json String
     * @param obj
     * @return
     */
    public static String objectToJsonString(Object obj){

        ObjectMapper objectMapper = new ObjectMapper();

        try{
            return objectMapper.writeValueAsString(obj);
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Json String to Object
     * @param jsonString
     * @return
     */
    public static Object jsonStringToObject(String jsonString){

        try{
            return new ObjectMapper().readValue(jsonString, Object.class);
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Json String to Object
     * @param jsonString
     * @param aClass
     * @return
     */
    public static Object jsonStringToObject(String jsonString, Class aClass){

        try{
            return new ObjectMapper().readValue(jsonString, aClass);
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }
    }
}
