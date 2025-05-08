package com.realtime.util;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaOffsetUtils {
    //keypoint 重置消费者组偏移量
    public static void KafkaOffsetReset(String botstrap,int partition,String groupId,String topic,Long newOffset){
        Properties prop = new Properties();
        prop.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,botstrap);
        try(AdminClient adminClient = AdminClient.create(prop)){
            //封装topic和分区
            TopicPartition tp = new TopicPartition(topic, partition);
            //封装偏移量
            OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(newOffset);
            //定义HashMap
            HashMap<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            //topic和分区,偏移量都存入hashmap
            offsets.put(tp,offsetAndMetadata);
            //向 Kafka 提交偏移量修改请求。
            AlterConsumerGroupOffsetsResult result = adminClient.alterConsumerGroupOffsets(groupId, offsets);
            //等待所有修改操作完成，确保偏移量已成功更新。
            result.all().get();
            System.out.printf("消费者组 '%s' 在 Topic '%s' 分区 %d 的偏移量已重置到 %d%n",
                    groupId, topic, partition, newOffset);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
