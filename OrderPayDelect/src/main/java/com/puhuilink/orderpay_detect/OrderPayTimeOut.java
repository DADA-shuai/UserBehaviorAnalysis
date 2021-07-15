package com.puhuilink.orderpay_detect;

import com.puhuilink.orderpay_detect.beans.OrderEvent;
import com.puhuilink.orderpay_detect.beans.OrderResult;
import org.apache.commons.math3.distribution.AbstractMultivariateRealDistribution;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
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
public class OrderPayTimeOut {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        SingleOutputStreamOperator<OrderEvent> dataStream = env.readTextFile("C:\\Users\\无敌大大帅逼\\IdeaProjects\\UserBehaviorAnalysis\\OrderPayDelect\\src\\main\\resources\\OrderLog.csv")
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

        Pattern<OrderEvent, OrderEvent> orderPayPattern = Pattern.<OrderEvent>begin("create").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent orderEvent) throws Exception {
                return "create".equals(orderEvent.getEventType());
            }
        }).followedBy("pay").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent orderEvent) throws Exception {
                return "pay".equals(orderEvent.getEventType());
            }
        }).within(Time.minutes(15));

        //2.定义侧输出流标签，用来表示超时事件
        OutputTag<OrderResult> orderTimeoutputTag = new OutputTag<OrderResult>("order-timeout"){};

        //3.将pattern应用到数据流上，得到pattern stream
        PatternStream<OrderEvent> pattern = CEP.pattern(dataStream.keyBy(OrderEvent::getOrderId), orderPayPattern);

        //4.调用select方法，实现对匹配复杂事件和超时复杂事件的提取和处理
        SingleOutputStreamOperator<OrderResult> select = pattern.select(orderTimeoutputTag, new OrderTimeoutSelect(), new OrderPaySelect());

        select.print();

        select.getSideOutput(orderTimeoutputTag).print();

        env.execute();

    }

    private static class OrderTimeoutSelect implements PatternTimeoutFunction<OrderEvent,OrderResult> {
        @Override
        public OrderResult timeout(Map<String, List<OrderEvent>> map, long l) throws Exception {
            Long timeoutOrder = map.get("create").iterator().next().getOrderId();
            return new OrderResult(timeoutOrder,"timeout " + l);
        }
    }

    private static class OrderPaySelect implements PatternSelectFunction<OrderEvent,OrderResult> {
        @Override
        public OrderResult select(Map<String, List<OrderEvent>> map) throws Exception {
            Long timeoutOrder = map.get("pay").iterator().next().getOrderId();
            return new OrderResult(timeoutOrder," pay ");
        }
    }
}
