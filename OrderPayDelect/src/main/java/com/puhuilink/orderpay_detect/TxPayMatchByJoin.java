package com.puhuilink.orderpay_detect;

import com.puhuilink.orderpay_detect.beans.OrderEvent;
import com.puhuilink.orderpay_detect.beans.ReceiptEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


/**
 * @author ：yjj
 * @date ：Created in 2021/7/14 18:05
 * @description：
 * @modified By：
 * @version: $
 */
public class TxPayMatchByJoin {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        SingleOutputStreamOperator<OrderEvent> OrderStream = env.readTextFile("C:\\Users\\无敌大大帅逼\\IdeaProjects\\UserBehaviorAnalysis\\OrderPayDelect\\src\\main\\resources\\OrderLog.csv")
                .map(data -> {
                    String[] split = data.split(",");
                    return new OrderEvent(Long.parseLong(split[0]), split[1], split[2], Long.parseLong(split[3]));
                }).assignTimestampsAndWatermarks(
                        new AscendingTimestampExtractor<OrderEvent>() {
                            @Override
                            public long extractAscendingTimestamp(OrderEvent element) {
                                return element.getTimestamp() * 1000;
                            }
                        }
                );

        SingleOutputStreamOperator<OrderEvent> payStream = OrderStream.filter(data -> "pay".equals(data.getEventType()) && !"".equals(data.getTxId()));


        SingleOutputStreamOperator<ReceiptEvent> ReceiptStream = env.readTextFile("C:\\Users\\无敌大大帅逼\\IdeaProjects\\UserBehaviorAnalysis\\OrderPayDelect\\src\\main\\resources\\ReceiptLog.csv")
                .map(data -> {
                    String[] split = data.split(",");
                    return new ReceiptEvent(split[0], split[1], Long.parseLong(split[2]));
                }).assignTimestampsAndWatermarks(
                        new AscendingTimestampExtractor<ReceiptEvent>() {
                            @Override
                            public long extractAscendingTimestamp(ReceiptEvent element) {
                                return element.getTimestamp() * 1000;
                            }
                        }
                );


        SingleOutputStreamOperator<Tuple2<OrderEvent, ReceiptEvent>> process = payStream
                .keyBy(OrderEvent::getTxId)
                .intervalJoin(ReceiptStream.keyBy(ReceiptEvent::getTxId))
                .between(Time.seconds(-3), Time.seconds(5))
                .process(new TxPayMatchByJoinResult());

        process.print();

        env.execute();

    }

    private static class TxPayMatchByJoinResult extends ProcessJoinFunction<OrderEvent,ReceiptEvent,Tuple2<OrderEvent,ReceiptEvent>> {
        @Override
        public void processElement(OrderEvent left, ReceiptEvent right, Context ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
            out.collect(new Tuple2<>(left,right));
        }
    }
}
