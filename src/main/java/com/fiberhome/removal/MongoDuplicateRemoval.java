package com.fiberhome.removal;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @description: mongodb sql去重
 * @author: ws
 * @time: 2020/6/30 11:13
 */
public class MongoDuplicateRemoval {
    private static final Logger logger = LoggerFactory.getLogger(MongoDuplicateRemoval.class);
    private static final String originPath = System.getProperty("user.dir") + File.separator  +  "sqlclear_new1.txt";
    private static final String goalPath = System.getProperty("user.dir") + File.separator  +  "sqlclear_new2.txt";
//    private static String originPath;
//    private static String goalPath;

    public static void main(String[] args) {
//        originPath = args[0];
//        goalPath = args[1];
        try {
            List<String> lines = FileUtils.readLines(new File(originPath), "UTF-8");
            //逐行读取sql
            for (String line : lines) {
                String lineData = line.replaceAll("\\\\", "");
                long num = 0L;  //规则外的数据原样保留

                JSONObject jsonObject = JSON.parseObject(lineData, Feature.OrderedField);

                if (lineData.equalsIgnoreCase("{}")){
                    num++;
                    logger.info("{}");
                    FileUtils.writeStringToFile(new File(goalPath), "{}\r\n", "UTF-8", true);
                }

                if (jsonObject.getJSONObject("query") != null) {
                    StringBuilder sb = new StringBuilder(2048);
                    sb.append("{\"query\":{");

                    //1.获取精确字段查询、返回查询、模糊查询三种情况
                    if (jsonObject.getJSONObject("query").getString("find") != null) {
                        //将find中数据存入findMap
                        Map<String, Object> findMap = new HashMap<>();
                        findMap.put("find", jsonObject.getJSONObject("query").getString("find"));
                        String findJson = mapToJson(findMap);
                        sb.append(findJson);
                    }

                    //将filter中数据存入filterMap(包括精确查询、范围查询、模糊查询三种情况)
                    if (jsonObject.getJSONObject("query").getString("filter") != null) {
                        int temp = 0;
                        Map<String, Object> filterMap = new HashMap<>();
                        String filterStr = jsonObject.getJSONObject("query").getString("filter");
//                        System.out.println("filterStr=" + filterStr);
                        JSONObject filterObject = JSONObject.parseObject(filterStr);
                        //判断是否含有模糊查询
                        if(! filterStr.contains("/")){
                            for (Map.Entry<String, Object> entry : filterObject.entrySet()) {
                                String value = entry.getValue().toString();
                                //如果entry中包含"范围查询"、"不等于",保留key，value为*
                                if (value.contains("$gt") || value.contains("$gte") || value.contains("$lt") || value.contains("$lte") || value.contains("$ne")) {
                                    temp ++;
                                    JSONObject jsonObject1 = JSON.parseObject(value);
                                    Map<String, Object> rangeMap = new HashMap<>();
                                    for (Map.Entry<String, Object> objectEntry : jsonObject1.entrySet()) {
                                        rangeMap.put(objectEntry.getKey(), "*");
                                    }
                                    String rangeStr = mapToJson(rangeMap);
                                    rangeStr = "{" + rangeStr + "}";      //符合json格式
                                    JSONObject jsonRangeStr = JSON.parseObject(rangeStr);   //由字符串转成jsonobject格式
                                    filterMap.put(entry.getKey(), jsonRangeStr);
                                }else {
                                    filterMap.put(entry.getKey(), "*");
                                }

                            }
                            if (temp ==0) {
                                String filterJson = mapToJson2("filter", filterMap);
                                sb.append(",").append(filterJson);
                            } else {
                                String filterJson = mapToJson2("filter", filterMap).replaceAll("\\\\", "");
                                sb.append(",").append(filterJson);
                            }
                        }else {     //包含模糊查询
                            Map<String, Object> filterRegexMap = new HashMap<>();
                            if (filterStr.contains("$regex")){
                                Map<String, Object> regexMap = new HashMap<>();
                                JSONObject jsonObject1 = JSON.parseObject(filterStr);
                                for (Map.Entry<String, Object> entry : jsonObject1.entrySet()) {
                                    JSONObject value = (JSONObject) entry.getValue();
                                    String regexValue = value.getString("$regex");
                                    String result = fuzzyProcess(regexValue);
                                    regexMap.put("$regex", result);

                                    String regexStr = mapToJson(regexMap);
                                    regexStr = "{" + regexStr + "}";
                                    JSONObject jsonObject2 = JSON.parseObject(regexStr);
                                    filterRegexMap.put(entry.getKey(), jsonObject2);
                                }

                            }else {
                                JSONObject jsonObject1 = JSON.parseObject(filterStr);
                                for (Map.Entry<String, Object> entry : jsonObject1.entrySet()) {
                                    String result = fuzzyProcess((String) entry.getValue());
                                    filterRegexMap.put(entry.getKey(), result);
                                }

                            }
                            String filterJson = mapToJson2("filter", filterRegexMap).replaceAll("\\\\", "");
                            sb.append(",").append(filterJson);
                        }

                    }

                    //将sort中数据存入sortMap
                    if (jsonObject.getJSONObject("query").getString("sort") != null) {
                        Map<String, Object> sortMap = new HashMap<>();
                        sortMap.put("sort", jsonObject.getJSONObject("query").getJSONObject("sort"));
                        String sortJson = mapToJson(sortMap); //去掉字符串中所有的反斜杠
                        sb.append(",").append(sortJson);
                    }

                    //2.insert插入
                    if (jsonObject.getJSONObject("query").getString("insert") != null) {
                        //将insert中数据存入insertMap
                        Map<String, Object> insertMap = new HashMap<>();
                        insertMap.put("insert", jsonObject.getJSONObject("query").getString("insert"));
                        String insertJson = mapToJson(insertMap);
                        sb.append(insertJson);
                    }
                    //将ordered中数据存入orderedMap
                    if (jsonObject.getJSONObject("query").getString("ordered") != null) {
                        Map<String, Object> orderedMap = new HashMap<>();
                        orderedMap.put("ordered", "*");
                        String orderedJson = mapToJson(orderedMap);
                        sb.append(",").append(orderedJson);
                    }
                    //将documentMap中数据存入documentMap
                    if (jsonObject.getJSONObject("query").getString("documents") != null) {
                        Map<String, Object> documentMap = new HashMap<>();
                        documentMap.put("documents", "[*]");
                        String documentJson = mapToJson(documentMap);
                        sb.append(",").append(documentJson);
                    }

                    //---处理getMore情况
                    if (jsonObject.getJSONObject("query").getString("getMore") != null) {
                        Map<String, Object> getMoreMap = new HashMap<>();
                        getMoreMap.put("getMore", "*");
                        String getMoreJson = mapToJson(getMoreMap);
                        sb.append(getMoreJson);
                    }

                    //--处理remove情况
                    if (jsonObject.getJSONObject("query").containsKey("remove")) {
                        String queryValue = jsonObject.getString("query");
                        JSONObject parseObject = JSON.parseObject(queryValue);
                        Map<String, Object> removeMap = new HashMap<>();
                        for (Map.Entry<String, Object> entry : parseObject.entrySet()) {
                            if(entry.getKey().equalsIgnoreCase("remove")) {
                                removeMap.put("remove", entry.getValue().toString());
                            }else {
                                removeMap.put(entry.getKey(), "*");
                            }
                        }
                        String removeJson = mapToJson( removeMap);
                        sb.append(removeJson);
                    }

                    sb.append("}}").append("\r\n");
                    num++;
//                    System.out.println(sb.toString());
                    logger.info(sb.toString());
                    FileUtils.writeStringToFile(new File(goalPath), sb.toString(), "UTF-8", true);
                }

                if(jsonObject.getJSONObject("command") != null) {
                    //3.count统计
                    if (jsonObject.getJSONObject("command").getString("count") != null) {
                        StringBuilder sb = new StringBuilder(2048);
                        sb.append("{\"command\":{");

                        //将count中数据存入countMap
                        Map<String, Object> countMap = new HashMap<>();
                        countMap.put("count", jsonObject.getJSONObject("command").getString("count"));
                        String countJson = mapToJson(countMap);
                        sb.append(countJson);

                        //将query中数据存入queryMap中
                        if (jsonObject.getJSONObject("command").getString("query") != null) {
                            int temp = 0;
                            Map<String, Object> queryMap = new HashMap<>();
                            String queryStr = jsonObject.getJSONObject("command").getString("query");
                            JSONObject queryObject = JSONObject.parseObject(queryStr);

                            //query中含有$or、$and情况处理
                            for (Map.Entry<String, Object> entry : queryObject.entrySet()) {
                                String key = entry.getKey();
                                //如果entry中包含"范围查询"、"不等于",保留key，value为*
                                if (key.contains("$or") || key.contains("$and")) {
                                    temp++;
                                    Map<String, Object> orMap = new HashMap<>();
                                    String value = entry.getValue().toString();
                                    JSONArray jsonArray = JSON.parseArray(value);
                                    //获取$or对应的value值
                                    Map<String, Object> orFieldMap = new HashMap<>();
                                    JSONObject jsonObject1 = JSON.parseObject(jsonArray.get(0).toString(), Feature.OrderedField);
                                    for (Map.Entry<String, Object> objectEntry : jsonObject1.entrySet()) {
                                        String key1 = objectEntry.getKey();
                                        orFieldMap.put(key1, "*");
                                    }
                                    orMap.put(entry.getKey(), orFieldMap);  //{"$or":{...}}
                                    String filterJson = mapToJson2("query", orMap).replaceAll("\\\\", "");
                                    sb.append(",").append(filterJson);

                                }else {
                                    queryMap.put(entry.getKey(), "*");
                                }

                            }
                            if (temp ==0) {
                                String filterJson = mapToJson2("query", queryMap).replaceAll("\\\\", "");
                                sb.append(",").append(filterJson);
                            }

                        }

                        sb.append("}}").append("\r\n");
                        num++;
//                        System.out.println(sb.toString());
                        logger.info(sb.toString());
                        FileUtils.writeStringToFile(new File(goalPath), sb.toString(), "UTF-8", true);

                    }

                    //4.聚合查询
                    if (jsonObject.getJSONObject("command").getString("aggregate") != null) {
                        StringBuilder sb = new StringBuilder(2048);
                        sb.append("{\"command\":{");

                        //将aggregate中数据存入aggregateMap
                        Map<String, Object> aggregateMap = new HashMap<String, Object>();
                        aggregateMap.put("aggregate", jsonObject.getJSONObject("command").getString("aggregate"));
                        String countJson = mapToJson(aggregateMap);
                        sb.append(countJson);

                        if (jsonObject.getJSONObject("command").getString("pipeline") != null) {
                            Map<String, JSONArray> pipelineMap = new HashMap<>();
                            JSONArray jsonArray = jsonObject.getJSONObject("command").getJSONArray("pipeline");
                            for (int i = 0; i < jsonArray.size(); i++) {
                                JSONObject temp = (JSONObject)jsonArray.get(i);
                                if (temp.containsKey("$match")) {
                                    Map<String, String> matchMap = new HashMap<>();
                                    JSONObject match = temp.getJSONObject("$match");
                                    for (Map.Entry<String, Object> entry : match.entrySet()) {
                                        matchMap.put(entry.getKey(), "*");
                                    }
                                    String matchJson = mapToJson("$match", matchMap);
                                    matchJson = "{" + matchJson + "}";      //符合json格式
                                    JSONObject newTemp = JSON.parseObject(matchJson);       //转成相应JsonObject格式
                                    Collections.replaceAll(jsonArray, temp, newTemp);
                                }
                            }
                            pipelineMap.put("pipeline", jsonArray);
                            String pipelineStr = mapToJson2(pipelineMap);
                            sb.append(",").append(pipelineStr);

                        }
                        sb.append("}}").append("\r\n");
                        num++;
//                        System.out.println(sb.toString().replaceAll("\\\\", ""));
                        logger.info(sb.toString());
                        FileUtils.writeStringToFile(new File(goalPath), sb.toString().replaceAll("\\\\", ""), "UTF-8", true);

                    }

                }

                //其他sql不作处理
                if (num == 0) {
//                    System.out.println(lineData+"\r\n");
                    logger.info(lineData);
                    FileUtils.writeStringToFile(new File(goalPath), lineData + "\r\n", "UTF-8", true);
                }

            }
            logger.info("ok");

        } catch (IOException e) {
            logger.error("文件读取失败!", e.getMessage());
        }

    }

    /**
     * 处理模糊查询value值
     * @param value
     */
    private static String fuzzyProcess(String value) {
        String regex = "[^/^.*$/]";
        String result = value.replaceAll(regex, "*").replaceAll("\\*[.*]", "");
        return result;
    }

    private static String mapToJson2(Map<String, JSONArray> pipelineMap) {
        String str = JSONObject.toJSONString(pipelineMap);
        return str.substring(1, str.length() - 1);
    }

    private static String mapToJson(Map<String, Object> map) {
        String str = JSONObject.toJSONString(map);
        return str.substring(1, str.length() - 1);
    }

    private static String mapToJson(String query, Map<String, String> map) {
        Map<String, Map<String, String>> resultMap = new HashMap<String, Map<String, String>>();
        resultMap.put(query, map);
        String str = JSONObject.toJSONString(resultMap);
        return str.substring(1, str.length() - 1);
    }

    private static String mapToJson2(String query, Map<String, Object> map) {
        Map<String, Map<String, Object>> resultMap = new HashMap<String, Map<String, Object>>();
        resultMap.put(query, map);
        String str = JSONObject.toJSONString(resultMap);
        return str.substring(1, str.length() - 1);
    }

}
