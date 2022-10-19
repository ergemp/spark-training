package org.ergemp.rdd.streaming.sinks;

public class SaveAsFunctionsExample {
    public static void main(String[] args) {

        /*
        * print()
        *
        * Prints the first ten elements of every batch of data in a DStream
        * on the driver node running the streaming application.
        * This is useful for development and debugging.
        *
        * saveAsTextFiles(prefix, [suffix])
        *
        * Save this DStream's contents as text files.
        * The file name at each batch interval is generated based on prefix and suffix:
        * "prefix-TIME_IN_MS[.suffix]".
        *
        * saveAsObjectFiles(prefix, [suffix])
        *
        * Save this DStream's contents as SequenceFiles of serialized Java objects.
        * The file name at each batch interval is generated based on prefix and suffix: "prefix-TIME_IN_MS[.suffix]".
        *
        * saveAsHadoopFiles(prefix, [suffix])
        *
        * Save this DStream's contents as Hadoop files.
        * The file name at each batch interval is generated based on prefix and suffix: "prefix-TIME_IN_MS[.suffix]".
        * */

    }
}
