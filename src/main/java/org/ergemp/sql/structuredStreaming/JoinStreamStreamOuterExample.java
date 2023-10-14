package org.ergemp.sql.structuredStreaming;

public class JoinStreamStreamOuterExample {
    public static void main(String[] args) {

        /*
        While the watermark + event-time constraints is optional for inner joins,
        for outer joins they must be specified. This is because for generating the NULL results in outer join,
        the engine must know when an input row is not going to match with anything in future.

        Hence, the watermark + event-time constraints must be specified for generating correct results.
        Therefore, a query with outer-join will look quite like the ad-monetization example earlier,
        except that there will be an additional parameter specifying it to be an outer-join.
        */

        /*
        impressionsWithWatermark.join(
                clicksWithWatermark,
                expr(
                        "clickAdId = impressionAdId AND " +
                        "clickTime >= impressionTime AND " +
                        "clickTime <= impressionTime + interval 1 hour "),
                        "leftOuter"  // can be "inner", "leftOuter", "rightOuter", "fullOuter", "leftSemi"
        );
        */

    }
}
