package com.atguigu.gmall.realtime.app.function;


import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.common.GmallConfig;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

public class DimSinkFunction extends RichSinkFunction<JSONObject> {

    private Connection connection;
    private PreparedStatement preparedStatement;
    @Override
    public void open(Configuration parameters) throws Exception {

        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        connection.setAutoCommit(true);
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {

        try {
            String sinkTable = value.getString("sinkTable");
            JSONObject data = value.getJSONObject("after");
            String upsertSql = genUpsertSql(sinkTable, data);
            System.out.println(upsertSql);
            preparedStatement = connection.prepareStatement(upsertSql);
            preparedStatement.executeUpdate();
        } catch (SQLException throwables) {
                throwables.printStackTrace();
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();;
            }
        }


    }

    private String genUpsertSql(String sinkTable, JSONObject data) {

        Set<String> keys = data.keySet();
        Collection<Object> values = data.values();

        String sqlStr =
                "upsert into " + GmallConfig.HBASE_SCHEMA + "." + sinkTable + "(" + StringUtils.join(keys
                , ",") + " ) " + "values('" + StringUtils.join(values, "','") + "')";
        return sqlStr;
    }
}
