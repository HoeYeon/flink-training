/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.training.exercises.hourlytips;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.aggregation.AggregationFunction;
import org.apache.flink.api.java.aggregation.MaxAggregationFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.training.exercises.common.datatypes.TaxiFare;
import org.apache.flink.training.exercises.common.sources.TaxiFareGenerator;
import org.apache.flink.training.exercises.common.utils.MissingSolutionException;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * The Hourly Tips exercise from the Flink training.
 *
 * <p>The task of the exercise is to first calculate the total tips collected by each driver, hour
 * by hour, and then from that stream, find the highest tip total in each hour.
 */
public class HourlyTipsExercise {

    private final SourceFunction<TaxiFare> source;
    private final SinkFunction<Tuple3<Long, Long, Float>> sink;

    /** Creates a job using the source and sink provided. */
    public HourlyTipsExercise(
            SourceFunction<TaxiFare> source, SinkFunction<Tuple3<Long, Long, Float>> sink) {

        this.source = source;
        this.sink = sink;
    }

    /**
     * Main method.
     *
     * @throws Exception which occurs during job execution.
     */
    public static void main(String[] args) throws Exception {

        HourlyTipsExercise job =
                new HourlyTipsExercise(new TaxiFareGenerator(), new PrintSinkFunction<>());

        job.execute();
    }

    /**
     * Create and execute the hourly tips pipeline.
     *
     * @return {JobExecutionResult}
     * @throws Exception which occurs during job execution.
     */
    public JobExecutionResult execute() throws Exception {

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // start the data generator
        DataStream<TaxiFare> fares = env.addSource(source);

        WatermarkStrategy<TaxiFare> strategy = WatermarkStrategy
                .<TaxiFare>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                .withTimestampAssigner((event, timestamp) -> event.getEventTimeMillis());

        DataStream<TaxiFare> faresWithWatermark = fares.assignTimestampsAndWatermarks(strategy);



        faresWithWatermark.keyBy(fare -> fare.driverId)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .process(new MyProcessWindowFunction())
                .windowAll(TumblingEventTimeWindows.of(Time.hours(1)))
                .reduce(new ReduceFunction<Tuple3<Long, Long, Float>>() {
                    public Tuple3<Long, Long, Float> reduce(Tuple3<Long, Long, Float> v1, Tuple3<Long, Long, Float> v2) {
                        if(v1.f2 > v2.f2){
                            return v1;
                        }
                        return v2;
                    }
                })
                .addSink(sink)
        ;






        // the results should be sent to the sink that was passed in
        // (otherwise the tests won't work)
        // you can end the pipeline with something like this:

        // DataStream<Tuple3<Long, Long, Float>> hourlyMax = ...
        // hourlyMax.addSink(sink);

        // execute the pipeline and return the result
        return env.execute("Hourly Tips");
    }
//
//    public static class TipAggregate
//            implements AggregateFunction<TaxiFare,Tuple3<Long, Long,Float>,Tuple3<Long, Long,Float>> {
//        @Override
//        public Tuple3<Long, Long,Float> createAccumulator() {
//            return new Tuple3<>(0L,0L,0f);
//        }
//
//        @Override
//        public Tuple3<Long, Long,Float> add(TaxiFare value, Tuple3<Long, Long,Float> accumulator) {
//            return new Tuple3<>(Math.max(value.getEventTimeMillis(),accumulator.f0),value.driverId,value.tip + accumulator.f2);
//        }
//
//        @Override
//        public Tuple3<Long, Long,Float> getResult(Tuple3<Long, Long,Float> accumulator) {
//            return accumulator;
//        }
//
//        @Override
//        public Tuple3<Long, Long,Float> merge(Tuple3<Long, Long,Float> a, Tuple3<Long, Long,Float> b) {
//            return new Tuple3<>(Math.max(a.f0,b.f0),a.f1,a.f2 + b.f2);
//        }
//
//    }

    public class MyProcessWindowFunction
            extends ProcessWindowFunction<TaxiFare, Tuple3<Long, Long,Float>, Long, TimeWindow> {

        @Override
        public void process(Long driverId,
                            ProcessWindowFunction<TaxiFare, Tuple3<Long, Long, Float>, Long, TimeWindow>.Context context,
                            Iterable<TaxiFare> iterable,
                            Collector<Tuple3<Long, Long, Float>> collector) throws Exception {
            float tips = 0f;
            for(TaxiFare fare: iterable){
                tips += fare.tip;
            }
            collector.collect(new Tuple3<>(context.window().getEnd(),driverId,tips));

        }
    }

}
