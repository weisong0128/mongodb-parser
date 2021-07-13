package com.fiberhome;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @description: mongodb去重文件字段解析
 * @author: ws
 * @time: 2020/7/7 10:46
 */
public class FieldParser2 {
    private static final Logger logger = LoggerFactory.getLogger(FieldParser2.class);
    private static final String originPath = System.getProperty("user.dir") + File.separator  +  "sqlclear_new2.txt";
    private static final String goalPath = System.getProperty("user.dir") + File.separator  +  "fieldparser.txt";
//    private static String originPath;
//    private static String goalPath;

    public static void main(String[] args) {
//        originPath = args[0];
//        goalPath = args[1];
        try {
            List<String> lines = FileUtils.readLines(new File(originPath), "UTF-8");
            for (String line : lines) {
                String lineData = line.replaceAll("\\\\", "");
                long num = 0L;  //规则外的数据使用默认规则
                StringBuilder filter = new StringBuilder();
                StringBuilder group = new StringBuilder();
                StringBuilder sort = new StringBuilder();

                JSONObject jsonObject = JSON.parseObject(lineData, Feature.OrderedField);

                //1.处理{}情况
                /*if (lineData.equalsIgnoreCase("{}")){
                    String lineResult = "\t\t";
//                    FileUtils.writeStringToFile(new File(goalPath), lineResult, "UTF-8", true);
                }*/

                //2.获取精确字段、返回查询、
                if (jsonObject.getJSONObject("query") != null) {
                    if (jsonObject.getJSONObject("query").getString("filter") != null && ! "{}".equals(jsonObject.getJSONObject("query").getString("filter"))) {
                        String filterValue = jsonObject.getJSONObject("query").getString("filter");
                        JSONObject jsonObject1 = JSON.parseObject(filterValue, Feature.OrderedField);
                        for (Map.Entry<String, Object> entry : jsonObject1.entrySet()) {
                            filter.append(entry.getKey() + ",");
                        }
                        filter.deleteCharAt(filter.length()-1);
                        filter.append("\t");
                    } else {        //无精确字段
                        filter.append("\t");
                    }

                }else if (jsonObject.getJSONObject("command") != null){        //处理精确字段查询含or情况
                    if(jsonObject.getJSONObject("command").getString("query") != null) {
                        String orValue = jsonObject.getJSONObject("command").getJSONObject("query").getString("$or");
                        if (orValue !=null){
                            JSONObject jsonObject1 = JSON.parseObject(orValue, Feature.OrderedField);
                            for (Map.Entry<String, Object> entry : jsonObject1.entrySet()) {
                                filter.append(entry.getKey() + ",");
                            }
                            filter.deleteCharAt(filter.length() - 1);
                            filter.append("\t");
                        }else {
                            filter.append("\t");
                        }

                    }else {
                        filter.append("\t");
                    }
                }else {
                    filter.append("\t");
                }

                //3.获取group字段
                if (jsonObject.getJSONObject("command") != null) {
                    if(jsonObject.getJSONObject("command").getString("pipeline") != null) {
                        JSONArray jsonArray = jsonObject.getJSONObject("command").getJSONArray("pipeline");
                        for (int i = 0; i < jsonArray.size(); i++) {
                            JSONObject jsonObject1 = (JSONObject) jsonArray.get(i);
                            if (jsonObject1.containsKey("$group")) {
                                JSONObject groupObject = jsonObject1.getJSONObject("$group");
                                for (Map.Entry<String, Object> entry : groupObject.entrySet()) {
                                    if (! "_id".equals(entry.getKey()) && ! "count".equals(entry.getKey())) {
                                        group.append(entry.getKey() + ",");
                                    }
                                    group.deleteCharAt(group.length()-1);
                                    group.append("\t");
                                }
                            }
                        }

                    }else {
                        group.append("\t");
                    }

                } else {
                    group.append("\t");
                }

                //4.获取sort字段
                if (jsonObject.getJSONObject("query") != null && ! "{}".equals(jsonObject.getJSONObject("query").getString("filter"))) {
                    if (jsonObject.getJSONObject("query").getString("sort") != null) {
                        JSONObject jsonObject1 = jsonObject.getJSONObject("query").getJSONObject("sort");
                        for (Map.Entry<String, Object> entry : jsonObject1.entrySet()) {
                            sort.append(entry.getKey() + ",");
                        }
                        sort.deleteCharAt(sort.length()-1);
                    }
                }else if(jsonObject.getJSONObject("command") !=null) {
                    if (jsonObject.getJSONObject("command").getString("pipeline") != null) {
                        JSONArray sortArray = jsonObject.getJSONObject("command").getJSONArray("pipeline");
                        for (int i = 0; i < sortArray.size(); i++) {
                            JSONObject jsonObject1 = (JSONObject) sortArray.get(i);
                            if(jsonObject1.containsKey("$sort")){
                                JSONObject jsonObject2 = jsonObject1.getJSONObject("$sort");
                                for (Map.Entry<String, Object> entry : jsonObject2.entrySet()) {
                                    sort.append(entry.getKey() + ",");
                                }
                                sort.deleteCharAt(sort.length()-1);
                            }
                        }
                    }

                }
//                System.out.println("filter=" + filter);
//                System.out.println("group=" + group);
//                System.out.println("sort=" + sort);

                String lineResult = filter.append(group).append(sort).append("\r\n").toString();

                FileUtils.writeStringToFile(new File(goalPath), lineResult, "UTF-8", true);

            }
            logger.info("ok");

        } catch (IOException e) {
            logger.error("文件读取失败!", e.getMessage());
        }


    }

}
