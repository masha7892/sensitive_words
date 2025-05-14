package com.userportrait.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;


/**
 * 对log数据进行去重,json内容完全相同的,通过hashset状态去重
 */
public class ProcessFilterRepeatTsData extends KeyedProcessFunction<String, JSONObject, JSONObject> {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessFilterRepeatTsData.class);
    private ValueState<HashSet<String>> processedDataState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<HashSet<String>> descriptor = new ValueStateDescriptor<>(
                "processedDataState",
                /**由于 Java 在编译后会“擦除”泛型信息（type erasure），
                 * 对于像 HashSet<String> 这种带泛型参数的复杂类型，
                 * Flink 无法仅通过 Class 对象来推断，
                 * 所以需要借助 TypeHint<T> 这个辅助类来“捕获”泛型信息，
                 * 进而生成正确的 TypeInformation<HashSet<String>>，
                 * 以便 Flink 在序列化、状态管理、键控分区等场景下能够正确处理。*/
                TypeInformation.of(new TypeHint<HashSet<String>>() {
                })
        );
        processedDataState = getRuntimeContext().getState(descriptor);
        super.open(parameters);
    }

    @Override
    public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
        //获取状态值,如果为null,赋值一个新的HashSet
        HashSet<String> processedData = processedDataState.value();
        if (processedData == null) {
            processedData = new HashSet<>();
        }

        //如果状态中不包含当前数据，则添加到状态中，并输出到下游,确保下游只接收到“首次出现”的数据
        String dataStr = jsonObject.toJSONString();
        if (!processedData.contains(dataStr)) {
            processedData.add(dataStr);
            processedDataState.update(processedData);
            collector.collect(jsonObject);
        }else {
            //当程序运行到这里时,会输出一条info级别的日志
            LOG.info("数据重复,过滤掉,data:{}",dataStr);
        }


    }
}
