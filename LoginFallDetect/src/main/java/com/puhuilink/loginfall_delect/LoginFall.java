package com.puhuilink.loginfall_delect;

import com.puhuilink.loginfall_delect.beans.LoginEvent;
import com.puhuilink.loginfall_delect.beans.LoginFailWarning;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author ：yjj
 * @date ：Created in 2021/7/14 10:56
 * @description：
 * @modified By：
 * @version: $
 */
public class LoginFall {
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

        // 自定义处理函数
        SingleOutputStreamOperator<LoginFailWarning> process = dataStream.keyBy(LoginEvent::getUserId)
                .process(new LoginFailDetectWarning0(2));

        process.print();

        env.execute();

    }

    private static class LoginFailDetectWarning extends KeyedProcessFunction<Long, LoginEvent, LoginFailWarning> {
        //最大连续登录失败次数
        private Integer maxFailTimes;

        public LoginFailDetectWarning(Integer maxFailTimes) {
            this.maxFailTimes = maxFailTimes;
        }

        private ListState<LoginEvent> loginFailEventListState ;

        private ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            loginFailEventListState = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("login-fail", LoginEvent.class));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-state",Long.class));
        }

        @Override
        public void processElement(LoginEvent value, Context ctx, Collector<LoginFailWarning> out) throws Exception {
            if ("fail".equals(value.getLoginState())) {
                loginFailEventListState.add(value);
                if ( timerTsState.value() == null ) {
                    Long ts = (value.getTimestamp() + 2) * 1000;
                    ctx.timerService().registerEventTimeTimer(ts);
                    timerTsState.update(ts);
                }
            } else {
                if ( timerTsState.value() != null ) {
                    ctx.timerService().deleteEventTimeTimer(timerTsState.value());
                }
                loginFailEventListState.clear();
                timerTsState.clear();
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<LoginFailWarning> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            ArrayList<LoginEvent> loginEvents = Lists.newArrayList(loginFailEventListState.get());
            int failTimes = loginEvents.size();
            if (failTimes >= maxFailTimes){
                out.collect(new LoginFailWarning(ctx.getCurrentKey(), loginEvents.get(0).getTimestamp(),loginEvents.get(failTimes-1).getTimestamp(),"login fail in 2s for " + failTimes + " times"));
            }

            loginFailEventListState.clear();
            timerTsState.clear();
        }
    }

    private static class LoginFailDetectWarning0 extends KeyedProcessFunction<Long, LoginEvent, LoginFailWarning> {
        //最大连续登录失败次数
        private Integer maxFailTimes;

        public LoginFailDetectWarning0(Integer maxFailTimes) {
            this.maxFailTimes = maxFailTimes;
        }

        private ListState<LoginEvent> loginFailEventListState;


        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            loginFailEventListState = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("login-fail", LoginEvent.class));
        }

        @Override
        public void processElement(LoginEvent value, Context ctx, Collector<LoginFailWarning> out) throws Exception {
            if ("fail".equals(value.getLoginState())){
                Iterator<LoginEvent> iterator = loginFailEventListState.get().iterator();
                if (iterator.hasNext()){
                    LoginEvent next = iterator.next();
                    if (value.getTimestamp() - next.getTimestamp() <=2) {
                        out.collect(new LoginFailWarning(ctx.getCurrentKey(),next.getTimestamp(),value.getTimestamp(),"two timers"));
                    }
                    loginFailEventListState.clear();
                    loginFailEventListState.add(value);
                } else {
                    loginFailEventListState.add(value);
                }
            } else {
                loginFailEventListState.clear();
            }
        }
    }
}
