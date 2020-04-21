/*
 * Copyright 2018 data Artisans GmbH, 2019 Ververica GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.flinktraining.exercises.datastream_java.windows;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * The "Hourly Tips" exercise of the Flink training
 * (http://training.ververica.com).
 * <p>
 * The task of the exercise is to first calculate the total tips collected by each driver, hour by hour, and
 * then from that stream, find the highest tip total in each hour.
 * <p>
 * Parameters:
 * -input path-to-input-file
 */
public class HourlyTipsExerciseV2 extends ExerciseBase {

    public static void main(String[] args) throws Exception {

        // read parameters
        ParameterTool params = ParameterTool.fromArgs(args);
        final String input = params.get("input", ExerciseBase.pathToFareData);

        final int maxEventDelay = 60;       // events are out of order by max 60 seconds
        final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(ExerciseBase.parallelism);

        // start the data generator
        DataStream<TaxiFare> fares = env
                .addSource(fareSourceOrTest(new TaxiFareSource(input, maxEventDelay, servingSpeedFactor)));

        SingleOutputStreamOperator<Tuple3<Long, Long, Float>> hourlyTipsByDriver = fares
                .map(fare -> new Tuple2<Long, Float>(fare.driverId, fare.tip))
                .returns(Types.TUPLE(Types.LONG, Types.FLOAT))
                .keyBy(x -> x.f0)
                .timeWindow(Time.hours(1))
                .reduce(new Reducer(), new WrapWithWindowInfo());

        SingleOutputStreamOperator<Tuple3<Long, Long, Float>> hourlyMax = hourlyTipsByDriver
                .timeWindowAll(Time.hours(1))
                .maxBy(2);

        printOrTest(hourlyMax);

        // execute the transformation pipeline
        env.execute("Hourly Tips (java)");
    }

    public static class WrapWithWindowInfo extends ProcessWindowFunction<Tuple2<Long, Float>, Tuple3<Long, Long, Float>, Long, TimeWindow> {
        @Override
        public void process(Long key, Context
                context, Iterable<Tuple2<Long, Float>> elements, Collector<Tuple3<Long, Long, Float>> out) {
            Float sumOfTips = elements.iterator().next().f1;
            out.collect(new Tuple3(context.window().getEnd(), key, sumOfTips));
        }
    }

    public static class Reducer implements ReduceFunction<Tuple2<Long, Float>> {
        @Override
        public Tuple2<Long, Float> reduce(Tuple2<Long, Float> driverTip1, Tuple2<Long, Float> driverTip2) throws Exception {
            return new Tuple2<Long, Float>(driverTip1.f0, driverTip1.f1 + driverTip2.f1);
        }
    }
}
