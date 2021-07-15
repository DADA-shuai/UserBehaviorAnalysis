package com.puhuilink.loginfall_delect;

import com.puhuilink.loginfall_delect.beans.LoginEvent;
import com.puhuilink.loginfall_delect.beans.LoginFailWarning;
import org.apache.commons.math3.distribution.AbstractMultivariateRealDistribution;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.operators.SimpleOutputFormatOperatorFactory;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

/**
 * @author ：yjj
 * @date ：Created in 2021/7/14 15:41
 * @description：
 * @modified By：
 * @version: $
 */
public class LoginFailWithCep {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        SingleOutputStreamOperator<LoginEvent> dataStream = env.readTextFile("C:\\Users\\无敌大大帅逼\\IdeaProjects\\UserBehaviorAnalysis\\LoginFallDetect\\src\\main\\resources\\LoginLog.csv")
                .map(data -> {
                    String[] split = data.split(",");
                    return new LoginEvent(Long.parseLong(split[0]), split[1], split[2], Long.parseLong(split[3]));
                }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LoginEvent>(Time.seconds(3)) {
                    @Override
                    public long extractTimestamp(LoginEvent element) {
                        return element.getTimestamp();
                    }
                });

        //1.定义匹配模式
//        Pattern<LoginEvent, LoginEvent> loginEventPattern = Pattern.<LoginEvent>begin("first fail").where(new SimpleCondition<LoginEvent>() {
//            @Override
//            public boolean filter(LoginEvent loginEvent) throws Exception {
//                return "fail".equals(loginEvent.getLoginState());
//            }
//        }).next("second fail").where(new SimpleCondition<LoginEvent>() {
//            @Override
//            public boolean filter(LoginEvent loginEvent) throws Exception {
//                return "fail".equals(loginEvent.getLoginState());
//            }
//        }).within(Time.seconds(2));

        Pattern<LoginEvent, LoginEvent> loginEventPattern1 = Pattern.<LoginEvent>begin("first fail").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent loginEvent) throws Exception {
                return "fail".equals(loginEvent.getLoginState());
            }
        }).times(3).consecutive().within(Time.seconds(5));

        //2.将匹配模式应用到数据流上，得到一个pattern stream
        PatternStream<LoginEvent> pattern = CEP.pattern(dataStream.keyBy(LoginEvent::getUserId), loginEventPattern1);

        //3/检出符合匹配雕件的复杂事件，进行转换处理，得到报警信息
        SingleOutputStreamOperator<LoginFailWarning> process = pattern.select(new LoginFailMatchDetectWarning());

        process.print();

        env.execute();

    }

    private static class LoginFailMatchDetectWarning implements PatternSelectFunction<LoginEvent,LoginFailWarning> {
        @Override
        public LoginFailWarning select(Map<String, List<LoginEvent>> map) throws Exception {
            LoginEvent first_fail = map.get("first fail").get(0);
            LoginEvent last_fail = map.get("first fail").get(map.get("first fail").size()-1);
            return new LoginFailWarning(first_fail.getUserId(),first_fail.getTimestamp(),last_fail.getTimestamp(),"twos timers");
        }
    }
}
