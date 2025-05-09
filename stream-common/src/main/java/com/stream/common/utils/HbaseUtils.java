package com.stream.common.utils;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class HbaseUtils {
    //声明一个私有成员变量 connection，用于存储与 HBase 的连接。
    private Connection connection;
    //声明一个静态常量 LOG，用于记录日志。
    private static final Logger LOG = LoggerFactory.getLogger(HbaseUtils.class.getName());

    //keypoint 构造函数
    public HbaseUtils(String zookeeper_quorum) throws Exception {
        //创建一个 HBase 配置对象
        Configuration entries = HBaseConfiguration.create();
        //设置 ZooKeeper 地址
        entries.set(HConstants.ZOOKEEPER_QUORUM,zookeeper_quorum);
        // RPC 超时时间、扫描器超时时间、内存存储刷新大小等
        entries.set(HConstants.HBASE_RPC_TIMEOUT_KEY,"1800000");
        entries.set(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD,"1800000");
        entries.set(HConstants.HREGION_MEMSTORE_FLUSH_SIZE,"128M");
        entries.set("hbase.incremental.wal","true");
        entries.set(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD,"3600000");
        //使用配置对象创建与 HBase 的连接
        this.connection = ConnectionFactory.createConnection(entries);
    }

    //获取连接
    public Connection getConnection(){
        return connection;
    }

    //向hbase写入数据
    public static void put(String rowKey, JSONObject value, BufferedMutator mutator)throws IOException{
        //创建一个 Put 对象，指定行键。
        Put put = new Put(Bytes.toBytes(rowKey));
        //遍历 JSONObject 中的键值对，将其添加到 Put 对象中。
        for (Map.Entry<String, Object> entry : value.entrySet()) {
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes(entry.getKey()),Bytes.toBytes(String.valueOf(entry.getValue())));
        }
        //将 Put 对象添加到 BufferedMutator 中
        mutator.mutate(put);
    }

    //hbase建表
    public boolean createTable(String nameSpace,String tableName,String... columnFamily) throws Exception{
        //如果表存在,返回true结束代码
        boolean b = tableIsExists(tableName);
        if (b) {
            return true;
        }
        Admin admin = connection.getAdmin();
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(nameSpace,tableName));
        if (columnFamily.length > 0) {
            for (String s : columnFamily) {
                ColumnFamilyDescriptor build = ColumnFamilyDescriptorBuilder.newBuilder(s.getBytes()).setCompressionType(Compression.Algorithm.SNAPPY).build();
                System.err.println("构建表列族：" + s);
                tableDescriptorBuilder.setColumnFamily(build);
            }
        } else {
            ColumnFamilyDescriptor build = ColumnFamilyDescriptorBuilder.newBuilder("info".getBytes()).setCompressionType(Compression.Algorithm.SNAPPY).build();
            System.err.println("构建表列族：info");
            tableDescriptorBuilder.setColumnFamily(build);
        }
        TableDescriptor build = tableDescriptorBuilder
                .build();
        admin.createTable(build);
        admin.close();
        LOG.info("Create Table {}",tableName);
        return tableIsExists(tableName);

    }


    //查看Hbase表是否存在
    public boolean tableIsExists(String tableName) throws Exception{
        Thread.sleep(1000);
        Admin admin = connection.getAdmin();
        boolean b = admin.tableExists(TableName.valueOf(tableName));
        admin.close();
        System.err.println("表"+tableName+(b?"存在":"不存在"));
        return b;
    }
}
