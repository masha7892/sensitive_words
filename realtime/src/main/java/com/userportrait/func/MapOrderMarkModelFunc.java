package com.userportrait.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.math.RoundingMode;
//进行品牌,品类,价格区间,时间区间进行评分
public class MapOrderMarkModelFunc extends ProcessFunction<JSONObject, JSONObject> {

    private static final double tmRate = 0.2;
    private static final double categoryRate = 0.3;
    private static final double priceRate = 0.15;
    private static final double timeRate = 0.1;

    @Override
    public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
        JSONObject max_result = jsonObject.getJSONObject("max_result");
        jsonObject.put("tm_name",max_result.getString("tm_name"));
        jsonObject.put("order_category",max_result.getString("order_category"));
        jsonObject.put("order_price",max_result.getString("order_price"));
        jsonObject.put("order_time",max_result.getString("order_time"));
        jsonObject.remove("max_result");

        JSONObject result = new JSONObject();
        result.putAll(jsonObject);


        //根据tm_name的值,设置tm_18_24,tm_25_29,tm_30_34,tm_35_39,tm_40_49,tm_50以上
        String tm_name = result.getString("tm_name");
        if (tm_name == null) {
            tm_name = "unknown";
        }
        switch (tm_name) {
            case "索芙特":
            case "小米":
            case "Redmi":
            case "CAREMiLLE":
            case "TCL":
            case "联想":
            case "金沙河":
                result.put("tm_18_24",round(0.9 * tmRate));
                result.put("tm_25_29",round(0.7 * tmRate));
                result.put("tm_30_34",round(0.5 * tmRate));
                result.put("tm_35_39",round(0.3 * tmRate));
                result.put("tm_40_49",round(0.2 * tmRate));
                result.put("tm_50以上",round(0.1    * tmRate));
                break;
            case "苹果":
            case "长粒香":
            case "香奈儿":
                result.put("tm_18_24",round(0.1 * tmRate));
                result.put("tm_25_29",round(0.3 * tmRate));
                result.put("tm_30_34",round(0.5 * tmRate));
                result.put("tm_35_39",round(0.7 * tmRate));
                result.put("tm_40_49",round(0.8 * tmRate));
                result.put("tm_50以上",round(0.9 * tmRate));
                break;
            default:
                result.put("tm_18_24",0);
                result.put("tm_25_29",0);
                result.put("tm_30_34",0);
                result.put("tm_35_39",0);
                result.put("tm_40_49",0);
                result.put("tm_50以上",0);
        }


        //根据order_category的值,设置category_phone,category_tv,category_computer,category_other
        String order_category = result.getString("order_category");
        if (order_category == null) {
            order_category = "unknown";
        }
        switch (order_category) {
            case "潮流服饰":
                result.put("category_18_24",round(0.9 * categoryRate));
                result.put("category_25_29",round(0.8 * categoryRate));
                result.put("category_30_34",round(0.6 * categoryRate));
                result.put("category_35_39",round(0.4 * categoryRate));
                result.put("category_40_49",round(0.2 * categoryRate));
                result.put("category_50以上",round(0.1    * categoryRate));
                break;
            case "家具用品":
                result.put("category_18_24",round(0.2 * categoryRate));
                result.put("category_25_29",round(0.4 * categoryRate));
                result.put("category_30_34",round(0.6 * categoryRate));
                result.put("category_35_39",round(0.8 * categoryRate));
                result.put("category_40_49",round(0.9 * categoryRate));
                result.put("category_50以上",round(0.7 * categoryRate));
                break;
            case "健康食品":
                result.put("category_18_24",round(0.1 * categoryRate));
                result.put("category_25_29",round(0.2 * categoryRate));
                result.put("category_30_34",round(0.4 * categoryRate));
                result.put("category_35_39",round(0.6 * categoryRate));
                result.put("category_40_49",round(0.8 * categoryRate));
                result.put("category_50以上",round(0.9 * categoryRate));
                break;
            default:
                result.put("category_18_24",0);
                result.put("category_25_29",0);
                result.put("category_30_34",0);
                result.put("category_35_39",0);
                result.put("category_40_49",0);
                result.put("category_50以上",0);
        }

        //根据order_price的值
        String order_price = result.getString("order_price");
        if (order_price == null) {
            order_price = "unknown";
        }
        switch (order_price) {
            case "低价商品":
                result.put("price_18_24",round(0.8 * priceRate));
                result.put("price_25_29",round(0.6 * priceRate));
                result.put("price_30_34",round(0.4 * priceRate));
                result.put("price_35_39",round(0.3 * priceRate));
                result.put("price_40_49",round(0.2 * priceRate));
                result.put("price_50以上",round(0.1    * priceRate));
                break;
            case "中价商品":
                result.put("price_18_24",round(0.2 * priceRate));
                result.put("price_25_29",round(0.4 * priceRate));
                result.put("price_30_34",round(0.6 * priceRate));
                result.put("price_35_39",round(0.7 * priceRate));
                result.put("price_40_49",round(0.8 * priceRate));
                result.put("price_50以上",round(0.7 * priceRate));
                break;
            case "高价商品":
                result.put("price_18_24",round(0.1 * priceRate));
                result.put("price_25_29",round(0.2 * priceRate));
                result.put("price_30_34",round(0.3 * priceRate));
                result.put("price_35_39",round(0.4 * priceRate));
                result.put("price_40_49",round(0.5 * priceRate));
                result.put("price_50以上",round(0.6 * priceRate));
                break;
            default:
                result.put("price_18_24",0);
                result.put("price_25_29",0);
                result.put("price_30_34",0);
                result.put("price_35_39",0);
                result.put("price_40_49",0);
                result.put("price_50以上",0);
        }

        //根据order_time的值
        String order_time = result.getString("order_time");
        if (order_time == null) {
            order_time = "unknown";
        }
        switch (order_time) {
            case "凌晨":
                result.put("time_18_24",round(0.2 * timeRate));
                result.put("time_25_29",round(0.1 * timeRate));
                result.put("time_30_34",round(0.1 * timeRate));
                result.put("time_35_39",round(0.1 * timeRate));
                result.put("time_40_49",round(0.1 * timeRate));
                result.put("time_50以上",round(0.1    * timeRate));
                break;
            case "早晨":
                result.put("time_18_24",round(0.1 * timeRate));
                result.put("time_25_29",round(0.1 * timeRate));
                result.put("time_30_34",round(0.1 * timeRate));
                result.put("time_35_39",round(0.1 * timeRate));
                result.put("time_40_49",round(0.2 * timeRate));
                result.put("time_50以上",round(0.3 * timeRate));
                break;
            case "上午":
                result.put("time_18_24",round(0.2 * timeRate));
                result.put("time_25_29",round(0.2 * timeRate));
                result.put("time_30_34",round(0.2 * timeRate));
                result.put("time_35_39",round(0.2 * timeRate));
                result.put("time_40_49",round(0.3 * timeRate));
                result.put("time_50以上",round(0.4 * timeRate));
                break;
            case "中午":
                result.put("time_18_24",round(0.4 * timeRate));
                result.put("time_25_29",round(0.4 * timeRate));
                result.put("time_30_34",round(0.4 * timeRate));
                result.put("time_35_39",round(0.4 * timeRate));
                result.put("time_40_49",round(0.4 * timeRate));
                result.put("time_50以上",round(0.3 * timeRate));
                break;
            case "下午":
                result.put("time_18_24",round(0.4 * timeRate));
                result.put("time_25_29",round(0.5 * timeRate));
                result.put("time_30_34",round(0.5 * timeRate));
                result.put("time_35_39",round(0.5 * timeRate));
                result.put("time_40_49",round(0.5 * timeRate));
                result.put("time_50以上",round(0.4 * timeRate));
                break;
            case "晚上":
                result.put("time_18_24",round(0.8 * timeRate));
                result.put("time_25_29",round(0.7 * timeRate));
                result.put("time_30_34",round(0.6 * timeRate));
                result.put("time_35_39",round(0.5 * timeRate));
                result.put("time_40_49",round(0.4 * timeRate));
                result.put("time_50以上",round(0.3 * timeRate));
                break;
            case "夜间":
                result.put("time_18_24",round(0.9 * timeRate));
                result.put("time_25_29",round(0.7 * timeRate));
                result.put("time_30_34",round(0.5 * timeRate));
                result.put("time_35_39",round(0.3 * timeRate));
                result.put("time_40_49",round(0.2 * timeRate));
                result.put("time_50以上",round(0.1 * timeRate));
                break;
            default:
                result.put("time_18_24",0);
                result.put("time_25_29",0);
                result.put("time_30_34",0);
                result.put("time_35_39",0);
                result.put("time_40_49",0);
                result.put("time_50以上",0);
        }

        collector.collect(result);

    }

    private static double round(double value) {
        return BigDecimal.valueOf(value)
                .setScale(3, RoundingMode.HALF_UP)
                .doubleValue();
    }

}
