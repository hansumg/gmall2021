package com.atguigu.gmall.realtime.app.dwd;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class BaseLogApp {

    public static void main(String[] args) throws Exception {

        //TODO 获取执行环境
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 读取kafka 日志原始数据
        String sourceTopic = "ods_base_log";
        String groupId = "base_log_app";
        DataStreamSource<String> odsBaseLogDS =
                env.addSource(MyKafkaUtil.getConsumer(sourceTopic, groupId));

        //TODO 将数据转换为JSON对象并对数据进行简单过滤
        OutputTag<String> dirtyData = new OutputTag<String>("dirtyData") {
        };
        SingleOutputStreamOperator<JSONObject> JSONDS =
                odsBaseLogDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    ctx.output(dirtyData, value);
                }

            }
        });
        //TODO 打印脏数据
        JSONDS.getSideOutput(dirtyData).print("Dirty>>>>>>");

        //TODO 新老用户校验
        KeyedStream<JSONObject, String> keyedStream = JSONDS.keyBy(data -> data.getJSONObject(
                "common").getString("mid"));
        SingleOutputStreamOperator<JSONObject> flatDS =
                keyedStream.flatMap(new RichFlatMapFunction<JSONObject, JSONObject>() {
            private ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>(
                        "isNew", String.class));
            }

            @Override
            public void flatMap(JSONObject value, Collector<JSONObject> out) throws Exception {
                String isNew = value.getJSONObject("common").getString("is_new");
                if ("1".equals(isNew)) {
                    String state = valueState.value();
                    if (state == null) {
                        valueState.update("1");
                    } else {
                        value.getJSONObject("common").put("is_new", "0");
                    }
                }
                out.collect(value);
            }
        });

        //TODO 分流 侧输出流 页面主流 启动：侧输出流 曝光：侧输出流
        OutputTag<String> startLog = new OutputTag<String>("start") {
        };
        OutputTag<String> disPlayLog = new OutputTag<String>("disPlayLog") {
        };
        SingleOutputStreamOperator<String> resultDS =
                flatDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {
                JSONObject start = value.getJSONObject("start");
                if (start != null && start.size() > 0) {
                    ctx.output(startLog, value.toJSONString());
                }
                JSONObject page = value.getJSONObject("page");
                if (page != null && page.size() > 0) {
                    out.collect(value.toJSONString());

                    JSONArray displays = value.getJSONArray("displays");

                    if (displays != null && displays.size() > 0) {
                        String pageId = value.getJSONObject("page").getString("page_id");
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject jsonObject = displays.getJSONObject(i);
                            jsonObject.put("page_id", pageId);
                            ctx.output(disPlayLog,jsonObject.toJSONString());
                        }
                    }
                }

            }
        });

        //TODO 打印流
        resultDS.getSideOutput(startLog).print("start>>>>>>");
        resultDS.getSideOutput(disPlayLog).print("display>>>>>>");
        resultDS.print("page>>>>>>");

        //TODO 写入kafka对应主题
        String sinkTopicStart = "dwd_start_log";
        String sinkTopicPage = "dwd_page_log";
        String sinkTopicDisplayLog = "dwd_display_log";

        resultDS.getSideOutput(startLog).addSink(MyKafkaUtil.getProducer(sinkTopicStart));
        resultDS.getSideOutput(disPlayLog).addSink(MyKafkaUtil.getProducer(sinkTopicDisplayLog));
        resultDS.addSink(MyKafkaUtil.getProducer(sinkTopicPage));

        env.execute();
    }
}
