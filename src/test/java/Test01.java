import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import com.fiberhome.removal.MongoDuplicateRemoval;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * @description: 描述
 * @author: ws
 * @time: 2020/7/1 8:53
 */
public class Test01 {
    private static final String originPath = System.getProperty("user.dir") + File.separator  +  "sqlclear_new1.txt";

    public static void main(String[] args) {
        try {
            List<String> lines = FileUtils.readLines(new File(originPath), "UTF-8");
            for (String line : lines) {
                JSONObject jsonObject = JSON.parseObject(line, Feature.OrderedField);
                //将line序列化为Json字符串
                String s = JSON.toJSONString(line);
                String s1 = s.replace("\\", "");    //去掉斜杠
                String s2 = s1.substring(1, s1.length() - 1);       //去掉首尾引号
//                System.out.println("s2=" + s2);

                System.out.println("jsonObject=" + jsonObject);
                String op = jsonObject.getString("op");
                System.out.println("op=" + op);
                String ns = jsonObject.getString("ns");
                System.out.println("ns=" + ns);
                String query = jsonObject.getString("query");
                System.out.println("query=" + query);
                String cursorid = jsonObject.getString("cursorid");
                System.out.println("cursorid=" + cursorid);
                String keysExamined = jsonObject.getString("keysExamined");
                System.out.println("keysExamined=" + keysExamined);
                String ts = jsonObject.getString("ts");
                System.out.println("ts=" + ts);
                String client = jsonObject.getString("client");
                System.out.println("client=" + client);
                String user = jsonObject.getString("user");
                System.out.println("user=" + user);
                String planSummary = jsonObject.getString("planSummary");
                System.out.println("planSummary=" + planSummary);

            }

//            String aa = "{\"_id\":\"5ee883ae5041ee25abdb213b\"}";
//            JSONObject jsonObject = JSON.parseObject(aa);
//            System.out.println(jsonObject);
//            for (Map.Entry<String, Object> entry : jsonObject.entrySet()) {
//                String key = entry.getKey();
//            }
//            if(jsonObject.containsValue("5ee883ae5041ee25abdb213b")) {
//                System.out.println(jsonObject.getString("_id"));
//            }

//            String bb = "/^张/";
//            String regex = "[^/^.*$/]";
//            String str = bb.replaceAll(regex,"*").replaceAll("\\*[.*]", "");
//            System.out.println("str=" + str);



        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
