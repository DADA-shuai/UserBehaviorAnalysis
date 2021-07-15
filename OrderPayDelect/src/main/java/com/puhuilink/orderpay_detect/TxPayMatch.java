package com.puhuilink.orderpay_detect;

import com.puhuilink.orderpay_detect.beans.OrderEvent;
import com.puhuilink.orderpay_detect.beans.OrderResult;
import com.puhuilink.orderpay_detect.beans.ReceiptEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;


/**
 * @author ：yjj
 * @date ：Created in 2021/7/14 18:05
 * @description：
 * @modified By：
 * @version: $
 */
public class TxPayMatch {

    private final static OutputTag<OrderEvent> unmatchedPays = new OutputTag<OrderEvent>("unmatchedPays"){};
    private final static OutputTag<ReceiptEvent> unmatchedReceipts = new OutputTag<ReceiptEvent>("unmatchedReceipts"){};


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


        SingleOutputStreamOperator<Tuple2<OrderEvent, ReceiptEvent>> process = payStream.keyBy(OrderEvent::getTxId)
                .connect(ReceiptStream.keyBy(ReceiptEvent::getTxId))
                .process(new TxPayMatchDetect());

        process.print("success");
        process.getSideOutput(unmatchedPays).print("unmatchedPays");
        process.getSideOutput(unmatchedReceipts).print("unmatchedReceipts");

        env.execute();

    }

    private static class TxPayMatchDetect extends CoProcessFunction<OrderEvent,ReceiptEvent, Tuple2<OrderEvent,ReceiptEvent>> {
        //定义状态，保存当前已经到来的订单支付事件和到账时间
        ValueState<OrderEvent> payState;
        ValueState<ReceiptEvent> receiptState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            payState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("pay-state",OrderEvent.class));
            receiptState = getRuntimeContext().getState(new ValueStateDescriptor<ReceiptEvent>("receipt-state",ReceiptEvent.class));
        }

        @Override
        public void processElement1(OrderEvent value, Context ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
            ReceiptEvent value1 = receiptState.value();
            if (value1 != null ){
                out.collect(new Tuple2<>(value,value1));
                payState.clear();
                receiptState.clear();
            } else {
                ctx.timerService().registerEventTimeTimer((value.getTimestamp()+5) * 1000L);
                payState.update(value);
            }
        }

        @Override
        public void processElement2(ReceiptEvent value, Context ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
            OrderEvent pay = payState.value();
            if (pay != null ){
                out.collect(new Tuple2<>(pay,value));
                payState.clear();
                receiptState.clear();
            } else {
                ctx.timerService().registerEventTimeTimer((value.getTimestamp()+5) * 1000L);
                receiptState.update(value);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            if (payState.value() != null ){
                ctx.output(unmatchedPays,payState.value());
            }
            if (receiptState.value() != null ){
                ctx.output(unmatchedReceipts,receiptState.value());
            }
            payState.clear();
            receiptState.clear();
        }
    }
}
