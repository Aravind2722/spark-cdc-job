package com.salescode.spark;

import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.*;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;

public class EmployeeCdcToIcebergJob {

    public static void main(String[] args) throws Exception {

        SparkSession spark = SparkSession.builder()
                .appName("Employee CDC to Iceberg")
                .master("local[*]")
                .config(
                        "spark.sql.extensions",
                        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
                )
                .getOrCreate();

        // Iceberg REST catalog + MinIO
        spark.conf().set("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog");
        spark.conf().set("spark.sql.catalog.demo.catalog-impl", "org.apache.iceberg.rest.RESTCatalog");
        spark.conf().set("spark.sql.catalog.demo.uri", "http://iceberg-rest:8181");
        spark.conf().set("spark.sql.catalog.demo.warehouse", "s3://warehouse/");
        spark.conf().set("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        spark.conf().set("spark.sql.catalog.demo.s3.endpoint", "http://minio:9000");
        spark.conf().set("spark.sql.catalog.demo.s3.path-style-access", "true");
        spark.conf().set("spark.sql.catalog.demo.s3.access-key-id", "minioadmin");
        spark.conf().set("spark.sql.catalog.demo.s3.secret-access-key", "minioadmin");

        spark.conf().set("spark.sql.defaultCatalog", "demo");

        // namespace + table
        spark.sql("CREATE NAMESPACE IF NOT EXISTS default");

        spark.sql(
                "CREATE TABLE IF NOT EXISTS default.employee (\n" +
                        "  id          INT,\n" +
                        "  name        STRING,\n" +
                        "  department  STRING,\n" +
                        "  op          STRING\n" +
                        ")\n" +
                        "USING iceberg\n" +
                        "PARTITIONED BY (department)"
        );

        // Debezium schema
        StructType afterSchema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("name", DataTypes.StringType, true, Metadata.empty()),
                new StructField("department", DataTypes.StringType, true, Metadata.empty())
        });

        StructType payloadSchema = new StructType(new StructField[]{
                new StructField("after", afterSchema, true, Metadata.empty()),
                new StructField("op", DataTypes.StringType, true, Metadata.empty())
        });

        StructType rootSchema = new StructType(new StructField[]{
                new StructField("payload", payloadSchema, true, Metadata.empty())
        });

        Dataset<Row> kafkaStream = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "kafka:29092")
                .option("subscribe", "mysql.quarkus_demo.Employee")
                .option("startingOffsets", "earliest")
                .load();

        Dataset<Row> parsed = kafkaStream
                .selectExpr("CAST(value AS STRING) AS json")
                .select(from_json(col("json"), rootSchema).alias("data"))
                .select(
                        col("data.payload.after.id").alias("id"),
                        col("data.payload.after.name").alias("name"),
                        col("data.payload.after.department").alias("department"),
                        col("data.payload.op").alias("op")
                )
                .where("id IS NOT NULL");

        StreamingQuery query = parsed.writeStream()
                .option("checkpointLocation", "/tmp/spark-checkpoints/employee-cdc")
                .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (batch, batchId) -> {

                    System.out.println("======= BATCH ID: " + batchId + " =======");
                    batch.show(false);

                    batch.writeTo("default.employee").append();

                })
                .start();

        query.awaitTermination();
    }
}
