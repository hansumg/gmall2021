package com.atguigu.gmall.realtime.app.ods;


import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.gmall.realtime.utils.CustomDeserializer;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDCWithDB {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall2021")
                .startupOptions(StartupOptions.latest())
                .deserializer(new CustomDeserializer())
                .build();

        DataStreamSource<String> sqlDS = env.addSource(sourceFunction);

        sqlDS.print("ods_base_db");

        //TODO 将数据写入kafka
        String sinkTopic = "ods_base_db";
        sqlDS.addSink(MyKafkaUtil.getProducer(sinkTopic));
        env.execute("FlinkCDC");
    }

}
