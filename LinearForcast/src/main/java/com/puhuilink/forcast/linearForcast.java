package com.puhuilink.forcast;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.ml.common.LabeledVector;
import org.apache.flink.ml.math.DenseVector;
import org.apache.flink.ml.regression.MultipleLinearRegression;

/**
 * @author ：yjj
 * @date ：Created in 2021/7/9 11:41
 * @description：
 * @modified By：
 * @version: $
 */
public class linearForcast {
    public static void main(String[] args) {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataSource<String> stringDataSource = env.readTextFile("C:\\Users\\无敌大大帅逼\\IdeaProjects\\UserBehaviorAnalysis\\LinearForcast\\src\\main\\resources\\data.csv");

        DataSet<LabeledVector> map = stringDataSource.map(data -> {
            String[] split = data.split(",");
            double v = Double.parseDouble(split[1]);
            double v1 = Double.parseDouble(split[0]);
            return new LabeledVector(v, new DenseVector(new double[]{v1}));
        });

        MultipleLinearRegression mlr = new MultipleLinearRegression().setIterations(10).setStepsize(0.5).setConvergenceThreshold(0.001);




    }
}
