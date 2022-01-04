package com.test;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.*;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.orc.impl.ColumnStatisticsImpl;
import org.apache.orc.impl.ReaderImpl;
import org.apache.orc.impl.RecordReaderImpl;
import org.apache.orc.mapreduce.OrcMapreduceRecordReader;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.execution.datasources.orc.OrcColumnarBatchReader;
import org.apache.spark.sql.execution.datasources.parquet.VectorizedColumnReader;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.sources.Or;
import scala.collection.Seq;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Predicate;


public class SparkOp {
    public SparkOp() throws Exception {
    }


    public void queryExecutor(long orgId){
//        OrcProto.IntegerStatistics colStats = null;
//        for(OrcProto.StripeStatistics o : tail){
//            colStats= o.getColStatsList().get(1).getIntStatistics();
//            if(orgId>colStats.getMinimum() && orgId<colStats.getMaximum()){
//
//            }
//        }
    }
    public static void main(String[] args) throws IOException, InterruptedException {
//        SparkConf sparkConf = new SparkConf().setAppName("Print Elements of RDD").setMaster("local[4]").set("spark.executor.memory","2g");
//        JavaSparkContext sc = new JavaSparkContext(sparkConf);
//
//        List<Integer> collection = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
//        JavaRDD<Integer> rdd = sc.parallelize(collection,1);
//
//        rdd.foreach(new VoidFunction<Integer>(){
//            public void call(Integer number) {
//                System.out.println(number);
//            }});

        SparkSession spark = SparkSession.builder().appName("java.ReadFromHdfs").master("local").getOrCreate();
        spark.sparkContext().hadoopConfiguration().set("ipc.maximum.data.length", "1342177282");
        Dataset<Row> df = spark.read().format("orc").load("hdfs://172.20.29.0:9000/crm/172_20_2_208/db126odb/CrmEntityAuditLog/CrmEntityAuditLog");
        df.createOrReplaceTempView("crmentityauditlog");
        long startTime = System.currentTimeMillis();
        Dataset<Row> df2 = spark.sql("select count(recordid) from crmentityauditlog where auditlogid>415280000000000000 and auditlogid<415289999999999999");
        df2.show();
        //df2.coalesce(1).write().orc("output/orc");
        long endTime = System.currentTimeMillis();
        spark.close();

//        long orgId = 47744;
//        Configuration conf = new Configuration();
//        Reader reader = OrcFile.createReader(new Path("output/orc/part-00000-d201a70e-ec01-42b9-8600-094aaeb3a02c-c000.snappy.orc"), OrcFile.readerOptions(conf));
//        TypeDescription schema = reader.getSchema();
//        List<String> fieldNames = schema.getFieldNames();
//        List<TypeDescription> columnTypes = schema.getChildren();
//        int size = fieldNames.size();
//
//        System.out.println(fieldNames);
//        System.out.println(columnTypes);
//
//        VectorizedRowBatch batch = schema.createRowBatch();
//        LongColumnVector auditLog = (LongColumnVector) batch.cols[1];
//        List<OrcProto.StripeStatistics> tail = reader.getOrcProtoStripeStatistics();
//        for(OrcProto.StripeStatistics o : tail){
//            System.out.println(o);
//            break;
//        }
//
//        RecordReader rows= reader.rows();
//
//        LongColumnVector recordId = (LongColumnVector) batch.cols[1];
//        while (rows.nextBatch(batch)) {
//            System.out.println(batch.getDataColumnCount());
//            break;
//        }

//        int count=0;
//        VectorizedRowBatch batch = reader.getSchema().createRowBatch();
//        RecordReader row = reader.rows();
//        reader.getStripes().get(1);
//        while (row.nextBatch(batch)){
//            count++;
//        }
//        System.out.println(count);

    }
}


//System.out.println(tail);
//List<org.apache.orc.StripeInformation> rows = reader.getStripes();
//        RecordReaderImpl row = (RecordReaderImpl) reader.rows();
//        System.out.println(row.readRowIndex(1,new boolean[]{},new boolean[]{}));
//        VectorizedRowBatch batch = reader.getSchema().createRowBatch();
//        System.out.println(reader.getSchema());
//        while (rows.nextBatch(batch)){
//            System.out.println(Arrays.toString(batch.cols));
//            break;
//        }