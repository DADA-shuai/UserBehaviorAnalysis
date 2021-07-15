package com.puhuilink.hotItems_analysis;

import com.puhuilink.beans.UserBehavior;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;


/**
 * @author ：yjj
 * @date ：Created in 2021/7/8 14:07
 * @description：
 * @modified By：
 * @version: $
 */
public class HotItemsWithTableApi {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build());

        DataStreamSource<String> dataStream = env.readTextFile("C:\\Users\\无敌大大帅逼\\IdeaProjects\\UserBehaviorAnalysis\\HotItemsAnalysls\\src\\main\\resources\\UserBehavior.csv");

        SingleOutputStreamOperator<UserBehavior> userBehaviorStream = dataStream.map(line -> {
            String[] split = line.split(",");
            return new UserBehavior(new Long(split[0]), new Long(split[1]), new Integer(split[2]), split[3], new Long(split[4]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior userBehavior) {
                return userBehavior.getTimestamp() * 1000L;
            }
        });

        Table dataTable = tableEnv.fromDataStream(userBehaviorStream, "itemId,behavior,timestamp.rowtime as ts");

        Table windowAggTable = dataTable.filter("behavior === 'pv'")
                .window(Slide.over("1.hours").every("5.minutes").on("ts").as("w"))
                .groupBy("itemId,w")
                .select("itemId,w.end as windowEnd,itemId.count as cnt");

        DataStream<Row> windowAggData = tableEnv.toAppendStream(windowAggTable, Row.class);
        tableEnv.createTemporaryView("windowAggTable",windowAggData,"itemId,windowEnd,cnt");

        Table result = tableEnv.sqlQuery("select * from (select *,ROW_NUMBER() OVER(partition by windowEnd order by cnt desc) as row_num from windowAggTable) t where t.row_num <=5");

        tableEnv.toRetractStream(result,Row.class).print();

        env.execute();

    }
}
