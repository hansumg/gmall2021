package com.atguigu.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.gmall.realtime.app.function.DimSinkFunction;
import com.atguigu.gmall.realtime.app.function.TableProcessFunction;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.utils.CustomDeserializer;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class BaseDBApp {

    public static void main(String[] args) throws Exception {

        //TODO 获取执行环境
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //TODO 读取kafka ods_base_db 业务数据
        String sourceTopic = "ods_base_db";
        String groupId = "base_db_app";
        DataStreamSource<String> baseDBDS =
                env.addSource(MyKafkaUtil.getConsumer(sourceTopic, groupId));

        //TODO 将数据转换为JSON对象并对数据进行过滤
        SingleOutputStreamOperator<JSONObject> jsonDS = baseDBDS.flatMap(new FlatMapFunction<String
                , JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                JSONObject baseDBJson = JSON.parseObject(value);
                String type = baseDBJson.getString("type");
                if (!"delete".equals(type)) {
                    out.collect(baseDBJson);
                }
            }
        });

        //TODO 使用flinkCDC消费配置表

        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .startupOptions(StartupOptions.initial())
                .deserializer(new CustomDeserializer())
                .databaseList("gmall2021_realtime")
                .build();

        DataStreamSource<String> tableProcessDS = env.addSource(sourceFunction);

        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>(
                "tableProcess", String.class,
                TableProcess.class);
        BroadcastStream<String> tableProcessBroadCastDS = tableProcessDS.broadcast(mapStateDescriptor);

        //TODO 连接主流和广播流
        OutputTag<JSONObject> outputTag = new OutputTag<JSONObject>("hbase") {
        };
        BroadcastConnectedStream<JSONObject, String> connect =
                jsonDS.connect(tableProcessBroadCastDS);

        //TODO 分流处理数据
        SingleOutputStreamOperator<JSONObject> processDS =
                connect.process(new TableProcessFunction(mapStateDescriptor, outputTag));

        //TODO 提取kafka流和Hbase流
        DataStream<JSONObject> hbaseDS = processDS.getSideOutput(outputTag);

        //TODO 将kafka数据写入kafka主题，Hbase数据写入Phoenix表
        processDS.print("kafka>>>>>>");
        hbaseDS.print("Hbase>>>>>>");

        processDS.addSink(MyKafkaUtil.getProducer(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject element, @Nullable Long timestamp) {
            return    new ProducerRecord<byte[], byte[]>(
                       element.getString("sinkTable"),
                       element.getString("after").getBytes()
               );
            }
        }));

        hbaseDS.addSink(new DimSinkFunction());
        //TODO 执行任务
        env.execute();


    }
}
