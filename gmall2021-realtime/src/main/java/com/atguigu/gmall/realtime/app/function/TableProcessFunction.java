package com.atguigu.gmall.realtime.app.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.common.GmallConfig;
import com.atguigu.gmall.realtime.bean.TableProcess;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject,String,JSONObject>{

    private MapStateDescriptor<String,TableProcess> mapStateDescriptor;
    private OutputTag<JSONObject> outputTag;
    private Connection connection;
    private PreparedStatement preparedStatement;

    public TableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor,
                                OutputTag<JSONObject> outputTag) {
        this.mapStateDescriptor = mapStateDescriptor;
        this.outputTag = outputTag;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {


        //TODO 将广播数据转换为JAVABEAN
        JSONObject jsonObject = JSON.parseObject(value);
        String after = jsonObject.getString("after");
        TableProcess tableProcess = JSON.parseObject(after, TableProcess.class);

        //TODO 检查Hbase表是否存在并建表
        String sinkType = tableProcess.getSinkType();
        if (TableProcess.SINK_TYPE_HBASE.equals(sinkType)) {
            checkTable(tableProcess.getSinkTable(),tableProcess.getSinkColumns(),
                    tableProcess.getSinkExtend(),tableProcess.getSinkPk());
        }

        //TODO 写入状态广播出去
        BroadcastState<String, TableProcess> broadcastState =
                ctx.getBroadcastState(mapStateDescriptor);
        String key = tableProcess.getSourceTable() + "-" + tableProcess.getOperateType();
        broadcastState.put(key,tableProcess);

    }

    private void checkTable(String sinkTable,String sinkColumns,String sinkExtend,String sinkPK) {

        try {
            if (sinkExtend == null) {
                sinkExtend = "";
            }
            if (sinkPK == null) {
                sinkPK = "id";
            }
            StringBuffer sqlStr = new StringBuffer().append("create table if not"
                    + " exists ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("( ");
            String[] fields = sinkColumns.split(",");
            for (int i = 0; i < fields.length; i++) {

                String field = fields[i];
                sqlStr.append(field);

                //TODO 判断是否为主键
                if (sinkPK.equals(field)) {
                    sqlStr.append(" varchar primary key");
                } else {
                    sqlStr.append(" varchar ");
                }
                if (i < fields.length - 1) {
                    sqlStr.append(",");
                }
            }
            sqlStr.append(" ) ").append(sinkExtend);
            //TODO 打印建表语句
            System.out.println(sqlStr);

            //TODO 预编译SQL
            preparedStatement = connection.prepareStatement(sqlStr.toString());
            preparedStatement.execute();
        } catch (SQLException e) {
            throw new RuntimeException("Phoenix表" + sinkTable + "建表失败");
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
        }

    }

    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {

        //TODO 获取状态数据
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String key = value.getString("tableName") + "-" + value.getString("type");
        TableProcess tableProcess = broadcastState.get(key);

        //TODO 过滤字段
        if (tableProcess != null) {
            JSONObject after = value.getJSONObject("after");
            filterColumns(after, tableProcess);
            //TODO 分流
            value.put("sinkTable", tableProcess.getSinkTable());
            String sinkType = tableProcess.getSinkType();

            if (TableProcess.SINK_TYPE_KAFKA.equals(sinkType)) {
                out.collect(value);
            } else if (TableProcess.SINK_TYPE_HBASE.equals(sinkType)) {
                ctx.output(outputTag, value);
            }
        } else {
            System.out.println("该组合" + key + "不存在");
        }

    }

    private void filterColumns(JSONObject after, TableProcess tableProcess) {
        String sinkColumns = tableProcess.getSinkColumns();
        String[] finalFields = sinkColumns.split(",");
        List<String> columns = Arrays.asList(finalFields);

        Iterator<Map.Entry<String, Object>> iterator = after.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Object> next = iterator.next();
            String key = next.getKey();
            if (!columns.contains(key))
                iterator.remove();
        }
    }

    }


