package org.ergemp.rdd.streaming.joins;

public class JoinExamples {
    public static void main(String[] args) {

        /*
        * Stream-stream joins
        *
            JavaPairDStream<String, String> stream1 = ...
            JavaPairDStream<String, String> stream2 = ...
            JavaPairDStream<String, Tuple2<String, String>> joinedStream = stream1.join(stream2);
        *
        *
            JavaPairDStream<String, String> windowedStream1 = stream1.window(Durations.seconds(20));
            JavaPairDStream<String, String> windowedStream2 = stream2.window(Durations.minutes(1));
            JavaPairDStream<String, Tuple2<String, String>> joinedStream = windowedStream1.join(windowedStream2);
        *
        *
        * Stream-Dataset joins
        *
            JavaPairRDD<String, String> dataset = ...
            JavaPairDStream<String, String> windowedStream = stream.window(Durations.seconds(20));
            JavaPairDStream<String, String> joinedStream = windowedStream.transform(rdd -> rdd.join(dataset));
        * */

    }
}
