package org.ergemp.sql.structuredStreaming;

public class JoinStreamStreamInnerExample {
    public static void main(String[] args) {

        /*
        Inner joins on any kind of columns along with any kind of join conditions are supported.
        However, as the stream runs, the size of streaming state will keep growing indefinitely
        as all past input must be saved as any new input can match with any input from the past.

        To avoid unbounded state, you have to define additional join conditions
        such that indefinitely old inputs cannot match with future inputs and therefore can be cleared from the state.
        In other words, you will have to do the following additional steps in the join.

        - Define watermark delays on both inputs such that the engine knows how delayed the input can be (similar to streaming aggregations)

        - Define a constraint on event-time across the two inputs such that the engine can figure out when old rows of one input is not going to be required
            - Time range join conditions (e.g. ...JOIN ON leftTime BETWEEN rightTime AND rightTime + INTERVAL 1 HOUR),
            - Join on event-time windows (e.g. ...JOIN ON leftTimeWindow = rightTimeWindow).

        */

        /*
        Watermark delays: Say, the impressions and the corresponding clicks can be late/out-of-order
        in event-time by at most 2 and 3 hours, respectively.

        Event-time range condition: Say, a click can occur within a time range of 0 seconds to 1 hour
        after the corresponding impression.
        */

        /*
        Dataset<Row> impressions = spark.readStream(). ...
        Dataset<Row> clicks = spark.readStream(). ...

        // Apply watermarks on event-time columns
        Dataset<Row> impressionsWithWatermark = impressions.withWatermark("impressionTime", "2 hours");
        Dataset<Row> clicksWithWatermark = clicks.withWatermark("clickTime", "3 hours");

        // Join with event-time constraints
        impressionsWithWatermark.join(
                clicksWithWatermark,
                expr(
                        "clickAdId = impressionAdId AND " +
                                "clickTime >= impressionTime AND " +
                                "clickTime <= impressionTime + interval 1 hour ")
        );
        */

    }
}
