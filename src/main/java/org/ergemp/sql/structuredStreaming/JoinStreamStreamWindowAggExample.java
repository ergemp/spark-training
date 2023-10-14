package org.ergemp.sql.structuredStreaming;

public class JoinStreamStreamWindowAggExample {
    public static void main(String[] args) {

        /*
        Dataset<Row> clicksWindow = clicksWithWatermark
                .groupBy(functions.window(clicksWithWatermark.col("clickTime"), "1 hour"))
                .count();

        Dataset<Row> impressionsWindow = impressionsWithWatermark
                .groupBy(functions.window(impressionsWithWatermark.col("impressionTime"), "1 hour"))
                .count();

        clicksWindow.join(impressionsWindow, "window", "inner");
        */

        //
        //
        //

        /*
        Dataset<Row> joined = impressionsWithWatermark.join(
                clicksWithWatermark,
                expr(
                        "clickAdId = impressionAdId AND " +
                        "clickTime >= impressionTime AND " +
                        "clickTime <= impressionTime + interval 1 hour "),
                        "leftOuter"  // can be "inner", "leftOuter", "rightOuter", "fullOuter", "leftSemi"
        );

        joined
                .groupBy(joined.col("clickAdId"), functions.window(joined.col("clickTime"), "1 hour"))
                .count();
        */

    }
}
