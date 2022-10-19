package org.ergemp.rdd.streaming.actions;

public class SaveAsHadoopFiles {
}

/*
saveAsHadoopFiles(prefix, [suffix])

Save this DStream's contents as Hadoop files.
The file name at each batch interval is generated based on prefix and suffix: "prefix-TIME_IN_MS[.suffix]".
Python API This is not available in the Python API.
*/