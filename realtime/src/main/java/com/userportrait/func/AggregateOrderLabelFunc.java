package com.userportrait.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

//进行一个汇总的操作,设置一个mapState,键为价格区间,品牌,品类,时间区间,值为map,map的键为具体分类和出现次数


public class AggregateOrderLabelFunc extends KeyedProcessFunction<String, JSONObject, JSONObject> {

    MapState<String, Map<String, Integer>> mapState;
    //保存上次已经输出的 maxResult
    private ValueState<JSONObject> lastMaxState;


    @Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor<String, Map<String, Integer>> descriptor = new MapStateDescriptor<>("mapState", TypeInformation.of(String.class), TypeInformation.of(new TypeHint<Map<String, Integer>>() {}));

        mapState = getRuntimeContext().getMapState(descriptor);

        // ValueState 描述器，用来存上次的 maxResult
        ValueStateDescriptor<JSONObject> lastDesc =
                new ValueStateDescriptor<>("lastMaxState",
                        TypeInformation.of(JSONObject.class));
        lastMaxState = getRuntimeContext().getState(lastDesc);
    }

    @Override
    public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
        String tm_name = jsonObject.getString("tm_name");
        String order_category = jsonObject.getString("order_category");
        String order_price = jsonObject.getString("order_price");
        String order_time = jsonObject.getString("order_time");

        Map<String, Integer> tmMap = mapState.get("tm_name");
        if (tmMap == null) {
            tmMap = new HashMap<>();
        }
        tmMap.put(tm_name, tmMap.getOrDefault(tm_name, 0) + 1);
        mapState.put("tm_name", tmMap);

        // 品类
        Map<String, Integer> categoryMap = mapState.get("order_category");
        if (categoryMap == null) {
            categoryMap = new HashMap<>();
        }
        categoryMap.put(order_category, categoryMap.getOrDefault(order_category, 0) + 1);
        mapState.put("order_category", categoryMap);

        // 价格
        Map<String, Integer> priceMap = mapState.get("order_price");
        if (priceMap == null) {
            priceMap = new HashMap<>();
        }
        priceMap.put(order_price, priceMap.getOrDefault(order_price, 0) + 1);
        mapState.put("order_price", priceMap);

        // 时间区间
        Map<String, Integer> timeMap = mapState.get("order_time");
        if (timeMap == null) {
            timeMap = new HashMap<>();
        }
        timeMap.put(order_time, timeMap.getOrDefault(order_time, 0) + 1);
        mapState.put("order_time", timeMap);

        // 汇总状态
        Map<String, Map<String, Integer>> resultMap = new HashMap<>();
        for (Map.Entry<String, Map<String, Integer>> entry : mapState.entries()) {
            resultMap.put(entry.getKey(), entry.getValue());
        }

        // —— 计算每个分类下的最大键 ——
        JSONObject maxResult = new JSONObject();
        for (Map.Entry<String, Map<String, Integer>> categoryEntry : resultMap.entrySet()) {
            String category = categoryEntry.getKey();    // "tm_name" / "order_category" / "order_price"
            Map<String, Integer> values = categoryEntry.getValue(); //获取键下的Map<String, Integer>

            // 找到出现次数最大的那个 key
            String maxKey = values.entrySet().stream()
                    .max(Map.Entry.comparingByValue())
                    .map(Map.Entry::getKey)
                    .orElse(null);

            // 只存 key
            maxResult.put(category, maxKey);
        }

        //去重,只有当和上次不一样时才输出 ——
        JSONObject lastMax = lastMaxState.value();
        if (lastMax == null || !lastMax.equals(maxResult)) {
            // 构造最终输出
            JSONObject out = new JSONObject();
            out.put("user_id", context.getCurrentKey());
            out.put("max_result", maxResult);
            out.put("ts", jsonObject.getLongValue("ts"));
            collector.collect(out);
            // 更新状态
            lastMaxState.update(maxResult);
        }
    }
}
