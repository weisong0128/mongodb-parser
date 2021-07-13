package com.fiberhome.mongo;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * @description: 将原始mongo日志进行解析
 * @author: ws
 * @time: 2020/7/20 10:04
 */
public class LogParser {
    private static final Logger logger = LoggerFactory.getLogger(LogParser.class);
//    private static final String originPath = System.getProperty("user.dir") + File.separator  +  "good.txt";
//    private static final String goalPath = System.getProperty("user.dir") + File.separator  +  "log_result.txt";
    private static String originPath;
    private static String goalPath;


    public static void main(String[] args) {
        originPath = args[0];
        goalPath = args[1];

        try {
            List<String> lines = FileUtils.readLines(new File(originPath), "UTF-8");

            for (String line : lines) {
                String lineData = line.replaceAll("\\\\", "");
                long num = 0L;  //规则外的数据原样保留

                JSONObject jsonObject = JSON.parseObject(lineData, Feature.OrderedField);

                StringBuilder sb = new StringBuilder(2048);
                //需要解析的key
                String op = jsonObject.getString("op");
                String ns = jsonObject.getString("ns");
                String docsExamined = jsonObject.getString("docsExamined");
                String nreturned = jsonObject.getString("nreturned");
                String responseLength = jsonObject.getString("responseLength");
                String millis = jsonObject.getString("millis");
                String planSummary = jsonObject.getString("planSummary");
                String ts = jsonObject.getString("ts");
                String client = jsonObject.getString("client");
                String appName = jsonObject.getString("appName");
                String user = jsonObject.getString("user");
                String sql = jsonObject.getString("query"); //sql的key为query或command

                if (op != null) {
                    sb.append(op).append("\t");
                } else {
                    sb.append("\t");
                }

                if (ns != null) {
                    String databaseName = ns.substring(0, ns.indexOf('.'));
                    sb.append(databaseName).append("\t");
                } else {
                    sb.append("\t");
                }

                if (ns != null) {
                    String tableName = ns.substring(ns.indexOf('.') + 1 , ns.length());
                    sb.append(tableName).append("\t");
                } else {
                    sb.append("\t");
                }

                if (docsExamined != null) {
                    sb.append(docsExamined).append("\t");
                } else {
                    sb.append("\t");
                }

                if (nreturned != null) {
                    sb.append(nreturned).append("\t");
                } else {
                    sb.append("\t");
                }

                if (responseLength != null) {
                    sb.append(responseLength).append("\t");
                } else {
                    sb.append("\t");
                }

                if (millis != null) {
                    sb.append(millis).append("\t");
                } else {
                    sb.append("\t");
                }

                if(planSummary != null) {
                    sb.append(planSummary).append("\t");
                } else {
                    sb.append("\t");
                }

                if (ts != null) {
                    JSONObject jsonObject1 = JSON.parseObject(ts);
                    String date = jsonObject1.getString("$date");
                    if (date != null) {
                        sb.append(date).append("\t");
                    }else {
                        sb.append("\t");
                    }
                } else {
                    sb.append("\t");
                }

                if (client != null) {
                    sb.append(client).append("\t");
                } else {
                    sb.append("\t");
                }

                if (appName != null) {
                    sb.append(appName).append("\t");
                } else {
                    sb.append("\t");
                }

                if (user != null) {
                    sb.append(user.substring(0, user.indexOf('@'))).append("\t");
                } else {
                    sb.append("\t");
                }

                //获取sql，key为query或command
                if (sql != null) {
                    sb.append("{\"query\":").append(sql).append("}");
                } else if (jsonObject.getString("command") != null) {
                    sb.append("{\"command\":").append(jsonObject.getString("command")).append("}");
                } else {
                    sb.append("{}");
                }

//                System.out.println("sb=" + sb);
                FileUtils.writeStringToFile(new File(goalPath), sb.toString() + "\r\n", "UTF-8", true);
            }
            logger.info("ok");

        } catch (IOException e) {
            logger.error("文件读取失败!", e.getMessage());
        }

    }

}
