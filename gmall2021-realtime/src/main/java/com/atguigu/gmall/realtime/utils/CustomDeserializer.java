package com.atguigu.gmall.realtime.utils;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

public class CustomDeserializer implements DebeziumDeserializationSchema<String> {
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {

        JSONObject sqlJSONOBJ = new JSONObject();
        String topic = sourceRecord.topic();
        String[] split = topic.split("\\.");
        String dataBase = split[1];
        String tableName = split[2];
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String type = operation.toString().toLowerCase();
        if ("create".equals(type)) {
            type = "insert";
        }

        Struct value = (Struct)sourceRecord.value();
        Struct before = value.getStruct("before");
        JSONObject beforeJSON = new JSONObject();
        if (before != null) {
            Schema schema = before.schema();
            List<Field> fields = schema.fields();
            for (Field field : fields) {
                String name = field.name();
                Object fieldValue = before.get(name);
                beforeJSON.put(name, fieldValue);
            }
        }

        Struct after = value.getStruct("after");
        JSONObject afterData = new JSONObject();
        if (after != null) {
            Schema schema = after.schema();
            List<Field> fields = schema.fields();
            for (Field field : fields) {
                String name = field.name();
                Object afterValue = after.get(name);
                afterData.put(name, afterValue);
            }
        }

        sqlJSONOBJ.put("database",dataBase);
        sqlJSONOBJ.put("tableName",tableName);
        sqlJSONOBJ.put("before",beforeJSON);
        sqlJSONOBJ.put("after",afterData);
        sqlJSONOBJ.put("type",type);

        collector.collect(sqlJSONOBJ.toJSONString());


    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
