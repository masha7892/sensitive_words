package com.userportrait.func;

import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.JdbcUtils;
import com.userportrait.domain.DimBaseCategory;
import com.userportrait.domain.DimCategoryCompare;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
/**
 *  设备打分模型
 */
public class MapDeviceAndSearchMarkModelFunc extends RichMapFunction<JSONObject,JSONObject> {

    //设备权重系数
    private final double deviceRate;
    //搜索权重系数
    private final double searchRate;
    //存储jdbc查询结果的map
    private final Map<String, DimBaseCategory> categoryMap;
    private List<DimCategoryCompare> dimCategoryCompares;
    private Connection connection;

    //构造方法
    //参数为三级分类的汇总表的实体类list,设备权重系数,搜索权重系数
    public MapDeviceAndSearchMarkModelFunc(List<DimBaseCategory> dimBaseCategories, double deviceRate, double searchRate) {
        this.deviceRate = deviceRate;
        this.searchRate = searchRate;
        this.categoryMap = new HashMap<>();
        // 将 DimBaseCategory 对象存储到 Map中  加快查询,
        //map的键值对为: 3级类名b3name和实体类,实体类有id,b3name,b2name,b1name
        for (DimBaseCategory category : dimBaseCategories) {
            categoryMap.put(category.getB3name(), category);
        }
    }

    //open方法
    //使用jdbc读取category_compare_dic返回实体类list
    @Override
    public void open(Configuration parameters) throws Exception {
        connection = JdbcUtils.getMySQLConnection(
                ConfigUtils.getString("mysql.url"),
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"));
        String sql = "select id, category_name, search_category from e_commerce.category_compare_dic;";
        dimCategoryCompares = JdbcUtils.queryList2(connection, sql, DimCategoryCompare.class, true);
        super.open(parameters);
    }



    //map方法
    @Override
    public JSONObject map(JSONObject jsonObject) throws Exception {
        //获取os字段,使用split切分为数组
        String os = jsonObject.getString("os");
        String[] labels = os.split(",");
        //提取第一个操作系统标签，放回 jsonObject
        // 因为os中如果有iso,Android两个系统,iso系统放第一个
        // 如果只有Android,第一个为Android
        String judge_os = labels[0];
        jsonObject.put("judge_os", judge_os);


        if (judge_os.equals("iOS")) {
            jsonObject.put("device_18_24", round(0.7 * deviceRate));
            jsonObject.put("device_25_29", round(0.6 * deviceRate));
            jsonObject.put("device_30_34", round(0.5 * deviceRate));
            jsonObject.put("device_35_39", round(0.4 * deviceRate));
            jsonObject.put("device_40_49", round(0.3 * deviceRate));
            jsonObject.put("device_50",    round(0.2 * deviceRate));
        } else if (judge_os.equals("Android")) {
            jsonObject.put("device_18_24", round(0.8 * deviceRate));
            jsonObject.put("device_25_29", round(0.7 * deviceRate));
            jsonObject.put("device_30_34", round(0.6 * deviceRate));
            jsonObject.put("device_35_39", round(0.5 * deviceRate));
            jsonObject.put("device_40_49", round(0.4 * deviceRate));
            jsonObject.put("device_50",    round(0.3 * deviceRate));
        }

        // 取出搜索词
        String searchItem = jsonObject.getString("search_item");
        //搜索词不为空
        if (searchItem != null && !searchItem.isEmpty()) {
            //根据搜索词获取到一级品类,并且将一级品类写会到jsonObject
            DimBaseCategory category = categoryMap.get(searchItem);
            if (category != null) {
                jsonObject.put("b1_category", category.getB1name());
            }
        }

        //获取一级品类
        String b1Category = jsonObject.getString("b1_category");
        //一级品类不为null
        if (b1Category != null && !b1Category.isEmpty()){
            //遍历category_compare_dic表的结果
            for (DimCategoryCompare dimCategoryCompare : dimCategoryCompares) {
                if (b1Category.equals(dimCategoryCompare.getCategoryName())){
                    //将搜索类别写入json
                    jsonObject.put("searchCategory",dimCategoryCompare.getSearchCategory());
                    //如果成功写入了,在这里结束代码
                    break;
                }
            }
        }

        //如果上一步没有成功写入searchCategory,则将searchCategory设置为unknown
        String searchCategory = jsonObject.getString("searchCategory");
        if (searchCategory == null) {
            searchCategory = "unknown";
        }
        //根据searchCategory的值,设置搜索权重
        switch (searchCategory) {
            case "时尚与潮流":
                jsonObject.put("search_18_24", round(0.9 * searchRate));
                jsonObject.put("search_25_29", round(0.7 * searchRate));
                jsonObject.put("search_30_34", round(0.5 * searchRate));
                jsonObject.put("search_35_39", round(0.3 * searchRate));
                jsonObject.put("search_40_49", round(0.2 * searchRate));
                jsonObject.put("search_50", round(0.1    * searchRate));
                break;
            case "性价比":
                jsonObject.put("search_18_24", round(0.2 * searchRate));
                jsonObject.put("search_25_29", round(0.4 * searchRate));
                jsonObject.put("search_30_34", round(0.6 * searchRate));
                jsonObject.put("search_35_39", round(0.7 * searchRate));
                jsonObject.put("search_40_49", round(0.8 * searchRate));
                jsonObject.put("search_50", round(0.8    * searchRate));
                break;
            case "健康与养生":
            case "家庭与育儿":
                jsonObject.put("search_18_24", round(0.1 * searchRate));
                jsonObject.put("search_25_29", round(0.2 * searchRate));
                jsonObject.put("search_30_34", round(0.4 * searchRate));
                jsonObject.put("search_35_39", round(0.6 * searchRate));
                jsonObject.put("search_40_49", round(0.8 * searchRate));
                jsonObject.put("search_50", round(0.7    * searchRate));
                break;
            case "科技与数码":
                jsonObject.put("search_18_24", round(0.8 * searchRate));
                jsonObject.put("search_25_29", round(0.6 * searchRate));
                jsonObject.put("search_30_34", round(0.4 * searchRate));
                jsonObject.put("search_35_39", round(0.3 * searchRate));
                jsonObject.put("search_40_49", round(0.2 * searchRate));
                jsonObject.put("search_50", round(0.1    * searchRate));
                break;
            case "学习与发展":
                jsonObject.put("search_18_24", round(0.4 * searchRate));
                jsonObject.put("search_25_29", round(0.5 * searchRate));
                jsonObject.put("search_30_34", round(0.6 * searchRate));
                jsonObject.put("search_35_39", round(0.7 * searchRate));
                jsonObject.put("search_40_49", round(0.8 * searchRate));
                jsonObject.put("search_50", round(0.7    * searchRate));
                break;
            default:
                jsonObject.put("search_18_24", 0);
                jsonObject.put("search_25_29", 0);
                jsonObject.put("search_30_34", 0);
                jsonObject.put("search_35_39", 0);
                jsonObject.put("search_40_49", 0);
                jsonObject.put("search_50", 0);
        }


        return jsonObject;

    }


    //将 double 转为 BigDecimal
    private static double round(double value) {
        return BigDecimal.valueOf(value)
                .setScale(3, RoundingMode.HALF_UP)
                .doubleValue();
    }


    // 关闭连接
    @Override
    public void close() throws Exception {
        super.close();
        connection.close();
    }
}
