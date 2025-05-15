package com.userportrait.func;

import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.JdbcUtils;
import com.userportrait.domain.DimCategoryCompare;
import com.userportrait.domain.DimOrderCompare;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.util.List;

//根据sku_id,使用jdbc直接读取mysql,获取到品类和品牌
//根据order_price,order_time,获取到订单价格区间和时间区间
public class OrderValueConvertSetFunc extends KeyedProcessFunction<String, JSONObject, JSONObject> {
    private Connection connection;
    private List<DimOrderCompare> dimOrderCompares;
    @Override
    public void open(Configuration parameters) throws Exception {
        connection = JdbcUtils.getMySQLConnection(
                ConfigUtils.getString("mysql.url"),
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"));
        String sql = "select\n" +
                "    sku_info.id as sku_id,\n" +
                "    base_trademark.tm_name,\n" +
                "    sku_info.price as sku_price,\n" +
                "    category_compare_dic2.order_category\n" +
                "from sku_info left join spu_info on sku_info.spu_id = spu_info.id\n" +
                "left join base_category3 on sku_info.category3_id = base_category3.id\n" +
                "left join base_category2 on base_category3.category2_id = base_category2.id\n" +
                "left join base_category1 on base_category2.category1_id = base_category1.id\n" +
                "left join category_compare_dic2 on base_category1.id = category_compare_dic2.id\n" +
                "left join base_trademark on sku_info.tm_id = base_trademark.id";
        dimOrderCompares = JdbcUtils.queryList2(connection,sql, DimOrderCompare.class);
    }

    @Override
    public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
        JSONObject result = new JSONObject();
        result.put("ts",jsonObject.getLong("ts_ms"));
        for (DimOrderCompare dimOrderCompare : dimOrderCompares) {
            if(dimOrderCompare.getSku_id().equals(jsonObject.getInteger("sku_id"))){
                result.put("user_id",jsonObject.getInteger("user_id"));
                result.put("order_category",dimOrderCompare.getOrder_category());
                result.put("tm_name",dimOrderCompare.getTm_name());
                if (Integer.valueOf(dimOrderCompare.getSku_price())<500){
                    result.put("order_price","低价商品");
                } else if (Integer.valueOf(dimOrderCompare.getSku_price()) >= 500 && Integer.valueOf(dimOrderCompare.getSku_price()) <= 1000) {
                    result.put("order_price","中价商品");
                }else {
                    result.put("order_price","高价商品");
                }

                //时间区间
                if (Integer.valueOf(jsonObject.getString("order_time"))>=0 && Integer.valueOf(jsonObject.getString("order_time"))<6){
                    result.put("order_time","凌晨");
                } else if (Integer.valueOf(jsonObject.getString("order_time"))>=6 && Integer.valueOf(jsonObject.getString("order_time"))<9) {
                    result.put("order_time","早晨");
                }else if (Integer.valueOf(jsonObject.getString("order_time"))>=9 && Integer.valueOf(jsonObject.getString("order_time"))<12) {
                    result.put("order_time","上午");
                }else if (Integer.valueOf(jsonObject.getString("order_time"))>=12 && Integer.valueOf(jsonObject.getString("order_time"))<14) {
                    result.put("order_time","中午");
                }else if (Integer.valueOf(jsonObject.getString("order_time"))>=14 && Integer.valueOf(jsonObject.getString("order_time"))<18) {
                    result.put("order_time","下午");
                }else if (Integer.valueOf(jsonObject.getString("order_time"))>=18 && Integer.valueOf(jsonObject.getString("order_time"))<22) {
                    result.put("order_time","晚上");
                }else {
                    result.put("order_time","夜间");
                }
            }
        }
        collector.collect(result);
    }
}
