package com.userportrait.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class AggregateUserDataProcessFunction extends KeyedProcessFunction<String, JSONObject, JSONObject> {
    private transient ValueState<Long> pvState;
    private transient MapState<String, Set<String>> fieldsState;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化PV状态
        pvState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("pv-state", Long.class)
        );

        // 初始化字段集合状态（使用TypeHint保留泛型信息）
        MapStateDescriptor<String, Set<String>> fieldsDescriptor =
                new MapStateDescriptor<>(
                        "fields-state",
                        Types.STRING,
                        TypeInformation.of(new TypeHint<Set<String>>() {})
                );

        fieldsState = getRuntimeContext().getMapState(fieldsDescriptor);
    }

    @Override
    public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
        // 更新PV,当pv没有值时,则更新为1,否则+1
        Long pv = pvState.value() == null ? 1L : pvState.value() + 1;
        pvState.update(pv);
//{"uid":"183","search_item":"小米","deviceInfo":{"ar":"20","uid":"183","os":"Android","ch":"xiaomi","md":"vivo x90","vc":"v2.1.134","ba":"vivo"},"ts":1746757084000}
        // 提取设备信息和搜索词
        JSONObject deviceInfo = jsonObject.getJSONObject("deviceInfo");
        String os = deviceInfo.getString("os");
        String ch = deviceInfo.getString("ch");
        String md = deviceInfo.getString("md");
        String ba = deviceInfo.getString("ba");
        String searchItem = jsonObject.containsKey("search_item") ? jsonObject.getString("search_item") : null;

        // 更新字段集合
        updateField("os", os);
        updateField("ch", ch);
        updateField("md", md);
        updateField("ba", ba);
        if (searchItem != null) {
            updateField("search_item", searchItem);
        }

        // 构建输出JSON
        JSONObject output = new JSONObject();
        output.put("uid", jsonObject.getString("uid"));
        output.put("pv", pv);
        output.put("os", String.join(",", getField("os")));
        output.put("ch", String.join(",", getField("ch")));
        output.put("md", String.join(",", getField("md")));
        output.put("ba", String.join(",", getField("ba")));
        output.put("search_item", String.join(",", getField("search_item")));
        output.put("ts", jsonObject.getLong("ts"));
        collector.collect(output);
    }

    // 辅助方法：更新字段集合,如果状态取不到set,则创建一个空的set,如果取到set,则将参数添加到set中,最后将set更新到状态中
    private void updateField(String field, String value) throws Exception {
        Set<String> set = fieldsState.get(field) == null ? new HashSet<>() : fieldsState.get(field);
        set.add(value);
        fieldsState.put(field, set);
    }

    // 辅助方法：获取状态中的set
    private Set<String> getField(String field) throws Exception {
        return fieldsState.get(field) == null ? Collections.emptySet() : fieldsState.get(field);
    }
}
