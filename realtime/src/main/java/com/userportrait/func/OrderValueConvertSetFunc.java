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

//将订单数据转为用户id,其余key为set类型,set中为价格区间,时间区间,品牌名,类目名称,都使用用户比例最大的
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
        for (DimOrderCompare dimOrderCompare : dimOrderCompares) {
            System.out.println(dimOrderCompare);
        }
    }

    public static void main(String[] args) {
         Connection connection;
         List<DimOrderCompare> dimOrderCompares;
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
}
