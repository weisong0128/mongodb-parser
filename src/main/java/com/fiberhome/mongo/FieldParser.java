package com.fiberhome.mongo;

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
public class FieldParser {
    private static final Logger logger = LoggerFactory.getLogger(FieldParser.class);
//    private static final String originPath = System.getProperty("user.dir") + File.separator  +  "removal_result.txt";
//    private static final String goalPath = System.getProperty("user.dir") + File.separator  +  "field_result.txt";
    private static String originPath;
    private static String goalPath;

    public static void main(String[] args) {
        originPath = args[0];
        goalPath = args[1];
        try {
            List<String> lines = FileUtils.readLines(new File(originPath), "UTF-8");
            for (String line : lines) {
                String lineData = line.replaceAll("\\\\", "");
                long num = 0L;  //规则外的数据使用默认规则
                //精确字段
                StringBuilder filter = new StringBuilder();
                //分组字段
                StringBuilder group = new StringBuilder();
                //排序字段
                StringBuilder sort = new StringBuilder();
                //模糊字段
                StringBuilder regex = new StringBuilder();
                //范围字段
                StringBuilder scope = new StringBuilder();
                //统计字段
                StringBuilder count = new StringBuilder();
                //关联字段
                StringBuilder relation = new StringBuilder();
                //展示字段
                StringBuilder showField = new StringBuilder();

                String[] splitData = lineData.split("\t");
                //对sql列进行去重处理
                String sql = splitData[13];

                JSONObject jsonObject = JSON.parseObject(sql, Feature.OrderedField);

                //1.获取精确字段、返回查询
                if (jsonObject.getJSONObject("query") != null) {
                    if (jsonObject.getJSONObject("query").getString("filter") != null && ! "{}".equals(jsonObject.getJSONObject("query").getString("filter"))) {
                        String filterValue = jsonObject.getJSONObject("query").getString("filter");
                        JSONObject jsonObject1 = JSON.parseObject(filterValue, Feature.OrderedField);
                        //排除模糊查询情况
                        if (! filterValue.contains("/")) {
                            //排查范围查询情况
                            if (filterValue.contains("$gt") || filterValue.contains("$gte") || filterValue.contains("$lt") || filterValue.contains("$lte")) {
                                filter.append("\t");
                            } else {
                                for (Map.Entry<String, Object> entry : jsonObject1.entrySet()) {
                                    filter.append(entry.getKey()).append(",");
                                }
                                filter.deleteCharAt(filter.length()-1);
                                filter.append("\t");
                            }
                        } else {
                            filter.append("\t");
                        }
                    } else {        //无精确字段
                        filter.append("\t");
                    }
                } else if (jsonObject.getJSONObject("command") != null){       //处理精确字段查询含or情况
                    //如果不包含count的key，算做精确查询
                    if (jsonObject.getJSONObject("command").getString("count") == null) {
                        if (jsonObject.getJSONObject("command").getString("query") != null) {
                            String orValue = jsonObject.getJSONObject("command").getJSONObject("query").getString("$or");
                            if (orValue !=null){
                                JSONObject jsonObject1 = JSON.parseObject(orValue, Feature.OrderedField);
                                for (Map.Entry<String, Object> entry : jsonObject1.entrySet()) {
                                    filter.append(entry.getKey()).append(",");
                                }
                                filter.deleteCharAt(filter.length() - 1);
                                filter.append("\t");
                            } else {
                                filter.append("\t");
                            }

                        } else if (jsonObject.getJSONObject("command").getString("pipeline") != null) {
                            JSONArray jsonArray = jsonObject.getJSONObject("command").getJSONArray("pipeline");
                            for (int i = 0; i < jsonArray.size(); i++) {
                                JSONObject temp = (JSONObject)jsonArray.get(i);
                                if (temp.containsKey("$match")) {
                                    JSONObject match = temp.getJSONObject("$match");
                                    for (Map.Entry<String, Object> entry : match.entrySet()) {
                                        filter.append(entry.getKey()).append(",");
                                    }
                                }
                            }
                            if (filter.toString().contains(",")){
                                filter.deleteCharAt(filter.length() - 1);
                                filter.append("\t");
                            }
                            // 处理关联sql情况，filter字段按\t分隔
                            if (jsonObject.getJSONObject("command").getString("pipeline").contains("$lookup")) {
                                filter.append("\t");
                            }

                        } else {
                            filter.append("\t");
                        }
                    } else {
                        filter.append("\t");
                    }

                } else {
                    filter.append("\t");
                }

                //2.获取group字段
                if (jsonObject.getJSONObject("command") != null) {
                    if (jsonObject.getJSONObject("command").getString("pipeline") != null) {
                        JSONArray jsonArray = jsonObject.getJSONObject("command").getJSONArray("pipeline");
                        for (int i = 0; i < jsonArray.size(); i++) {
                            JSONObject jsonObject1 = (JSONObject) jsonArray.get(i);
                            if (jsonObject1.containsKey("$group")) {
                                JSONObject groupObject = jsonObject1.getJSONObject("$group");
                                for (Map.Entry<String, Object> entry : groupObject.entrySet()) {
                                    if (! "_id".equals(entry.getKey()) && ! "count".equals(entry.getKey())) {
                                        group.append(entry.getKey()).append(",");
                                    }
                                    group.deleteCharAt(group.length()-1);
                                    group.append("\t");
                                }
                            }
                        }

                        // 处理关联sql情况，group字段按\t分隔
                        if (jsonObject.getJSONObject("command").getString("pipeline").contains("$lookup")) {
                            group.append("\t");
                        }

                        //处理展示字段sql情况，group字段按\t分隔
                        if (jsonObject.getJSONObject("command").getString("pipeline").contains("$project")) {
                            group.append("\t");
                        }

                    } else {
                        group.append("\t");
                    }

                } else {
                    group.append("\t");
                }

                //3.获取sort字段
                if (jsonObject.getJSONObject("query") != null && ! "{}".equals(jsonObject.getJSONObject("query").getString("filter"))) {
                    if (jsonObject.getJSONObject("query").getString("sort") != null) {
                        JSONObject jsonObject1 = jsonObject.getJSONObject("query").getJSONObject("sort");
                        for (Map.Entry<String, Object> entry : jsonObject1.entrySet()) {
                            sort.append(entry.getKey()).append(",");
                        }
                        sort.deleteCharAt(sort.length()-1);
                        sort.append("\t");
                    } else {
                        sort.append("\t");
                    }
                } else if (jsonObject.getJSONObject("command") !=null) {
                    if (jsonObject.getJSONObject("command").getString("pipeline") != null) {
                        JSONArray sortArray = jsonObject.getJSONObject("command").getJSONArray("pipeline");
                        for (int i = 0; i < sortArray.size(); i++) {
                            JSONObject jsonObject1 = (JSONObject) sortArray.get(i);
                            if(jsonObject1.containsKey("$sort")){
                                JSONObject jsonObject2 = jsonObject1.getJSONObject("$sort");
                                for (Map.Entry<String, Object> entry : jsonObject2.entrySet()) {
                                    sort.append(entry.getKey()).append(",");
                                }
                                sort.deleteCharAt(sort.length()-1);
                                sort.append("\t");
                            }
                        }

                        // 处理关联sql情况，sort字段按\t分隔
                        if (jsonObject.getJSONObject("command").getString("pipeline").contains("$lookup")) {
                            sort.append("\t");
                        }
                    } else {
                        sort.append("\t");
                    }

                } else {
                    sort.append("\t");
                }

                //4.获取regex字段
                if (jsonObject.getJSONObject("query") != null) {
                    if (jsonObject.getJSONObject("query").getString("filter") != null) {
                        String filterStr = jsonObject.getJSONObject("query").getString("filter");
                        JSONObject filterObject = JSONObject.parseObject(filterStr);
                        if (filterStr.contains("/")) {
                            for (Map.Entry<String, Object> entry : filterObject.entrySet()) {
                                regex.append(entry.getKey()).append(",");
                            }
                            regex.deleteCharAt(regex.length()-1);
                            regex.append("\t");

                        } else {
                            regex.append("\t");
                        }
                    } else {
                        regex.append("\t");
                    }
                } else {
                    regex.append("\t");
                }

                //5.获取范围字段
                if (jsonObject.getJSONObject("query") != null) {
                    if (jsonObject.getJSONObject("query").getString("filter") != null) {
                        String filterStr = jsonObject.getJSONObject("query").getString("filter");
                        JSONObject filterObject = JSONObject.parseObject(filterStr);
                        for (Map.Entry<String, Object> entry : filterObject.entrySet()) {
                            String value = entry.getValue().toString();
                            if (value.contains("$gt") || value.contains("$gte") || value.contains("$lt") || value.contains("$lte")) {
                                scope.append(entry.getKey()).append(",");
                            }
                        }
                        if(scope.toString().contains(",")) {
                            scope.deleteCharAt(scope.length()-1);
                            scope.append("\t");
                        } else {
                            scope.append("\t");
                        }
                    } else {
                        scope.append("\t");
                    }
                } else {
                    scope.append("\t");
                }

                //6.统计字段
                if(jsonObject.getJSONObject("command") != null) {
                    if (jsonObject.getJSONObject("command").getString("count") != null) {
                        if (jsonObject.getJSONObject("command").getString("query") != null && ! "{}".equals(jsonObject.getJSONObject("command").getString("query"))) {
                            String queryStr = jsonObject.getJSONObject("command").getString("query");
                            JSONObject queryObject = JSONObject.parseObject(queryStr);

                            for (Map.Entry<String, Object> entry : queryObject.entrySet()) {
                                String key = entry.getKey();
                                if (key.contains("$or") || key.contains("$and")) {
                                    String value = entry.getValue().toString();
                                    JSONObject jsonObject1 = JSON.parseObject(value);
                                    for (Map.Entry<String, Object> objectEntry : jsonObject1.entrySet()) {
                                        count.append(objectEntry.getKey()).append(",");
                                    }
                                } else {
                                    count.append(key).append(",");
                                }
                            }
                            count.deleteCharAt(count.length() - 1);
                            count.append("\t");
                        } else {
                            count.append("\t");
                        }

                    } else {
                        count.append("\t");
                    }
                } else {
                    count.append("\t");
                }

                //7.获取关联relation字段
                if(jsonObject.getJSONObject("command") != null) {
                    if (jsonObject.getJSONObject("command").getString("pipeline") != null) {
                        if (jsonObject.getJSONObject("command").getString("pipeline").contains("$lookup")) {
                            JSONArray relationArray = jsonObject.getJSONObject("command").getJSONArray("pipeline");
                            for (int i = 0; i < relationArray.size(); i++) {
                                JSONObject jsonObject1 = (JSONObject) relationArray.get(i);
                                if(jsonObject1.containsKey("$lookup")){
                                    JSONObject jsonObject2 = jsonObject1.getJSONObject("$lookup");
                                    String localField = jsonObject2.getString("localField");
                                    String foreignField = jsonObject2.getString("foreignField");
                                    if ( ! localField.isEmpty() && ! foreignField.isEmpty()) {
                                        relation.append(localField).append(",").append(foreignField);
                                    }
                                }
                            }
                            relation.append("\t");
                        } else {
                            relation.append("\t");
                        }
                    } else {
                        relation.append("\t");
                    }

                } else {
                    relation.append("\t");
                }

                //8.获取展示字段showField
                if (jsonObject.getJSONObject("query") != null) {
                    if (jsonObject.getJSONObject("query").getString("projection") != null) {
                        String projectionStr = jsonObject.getJSONObject("query").getString("projection");
                        JSONObject projectionObject = JSONObject.parseObject(projectionStr);
                        for (Map.Entry<String, Object> entry : projectionObject.entrySet()) {
                            showField.append(entry.getKey()).append(",");
                        }
                        showField.deleteCharAt(showField.length()-1);
                    }
                } else if(jsonObject.getJSONObject("command") != null) {
                    if (jsonObject.getJSONObject("command").getString("pipeline") != null) {
                        if (jsonObject.getJSONObject("command").getString("pipeline").contains("$project")) {
                            JSONArray showArray = jsonObject.getJSONObject("command").getJSONArray("pipeline");
                            for (int i = 0; i < showArray.size(); i++) {
                                JSONObject jsonObject1 = (JSONObject) showArray.get(i);
                                if (jsonObject1.containsKey("$project")) {
                                    JSONObject projectValue = jsonObject1.getJSONObject("$project");
                                    for (Map.Entry<String, Object> entry : projectValue.entrySet()) {
                                        showField.append(entry.getKey()).append(",");
                                    }
                                    showField.deleteCharAt(showField.length()-1);
                                }

                            }
                        }
                    }
                }

//                System.out.println("filter=" + filter);
//                System.out.println("group=" + group);
//                System.out.println("sort=" + sort);
//                System.out.println("regex=" + regex);
//                System.out.println("scope=" + scope);
//                System.out.println("count=" + count);
//                System.out.println("relation=" + relation);
//                System.out.println("showField=" + showField);
//                System.out.println("===========================");

                String lineResult = filter.append(group).append(sort).append(regex).append(scope).append(count).append(relation).append(showField).append("\r\n").toString();

                FileUtils.writeStringToFile(new File(goalPath), lineData + "\t" + lineResult, "UTF-8", true);
            }
            logger.info("ok");

        } catch (IOException e) {
            logger.error("文件读取失败!", e.getMessage());
        }


    }

}
