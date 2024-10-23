package org.ergemp.sql.sources;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class CreateDfFromRdd {
    public static void main(String[] args) {

        Logger log = Logger.getRootLogger();
        log.setLevel(Level.ERROR);

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkSession spark = SparkSession
                .builder()
                .appName("CreateDfFromRdd")
                .master("local")
                .getOrCreate();

        StructType schema = new StructType()
                .add("id", "long")
                .add("name", "string")
                .add("keywords","string")
                .add("duration", "string")
                ;

        JavaRDD<Row> items = null;

        items = spark
                .read()
                .schema(schema)
                .option("mode", "DROPMALFORMED")
                .json("data/sample.json")
                .toJavaRDD();

        items.take(10).forEach(item -> {
            System.out.println(item);
        });

        JavaRDD<RowObject> mRDD = items.map(new Function<Row, RowObject>() {

            @Override
            public RowObject call(Row row) throws Exception {
                if (row != null) {

                    RowObject rowObject = new RowObject();
                    rowObject.id = row.getLong(0);
                    rowObject.name = row.getString(1);
                    rowObject.keywords = row.getString(2);
                    rowObject.duration = row.getString(3);
                    return rowObject;

                    //return RowFactory.create(row.getLong(0), row.getString(1), row.getString(2), row.getString(3) );
                }
                else {
                    return null;
                }
            }
        });

        mRDD.take(10).forEach(item -> {
            System.out.println(item);
        });

        Dataset<Row> mFrame = spark.sqlContext().createDataFrame(mRDD, RowObject.class);
        mFrame.show();
    }

    public static class RowObject implements java.io.Serializable{
        public Long id;
        public  String name;
        public String keywords;
        public String duration;

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getKeywords() {
            return keywords;
        }

        public void setKeywords(String keywords) {
            this.keywords = keywords;
        }

        public String getDuration() {
            return duration;
        }

        public void setDuration(String duration) {
            this.duration = duration;
        }

        @Override
        public String toString() {
            return "RowObject{" +
                    "id=" + id +
                    ", name='" + name + '\'' +
                    ", keywords='" + keywords + '\'' +
                    ", duration='" + duration + '\'' +
                    '}';
        }
    }
}
