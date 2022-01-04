package com.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.orc.*;
import org.apache.orc.impl.OrcIndex;
import org.apache.orc.impl.RecordReaderImpl;

import java.io.IOException;
import java.util.*;
import java.util.function.BiFunction;

public class OrcFileReader {
    private static final long orgDiv = 10000000000000L;
    private static final long orgId = 41528;
    public static long batch = 1, last_batch=0, first_batch=0;
    public static List<Long> result = new ArrayList<>();
    public static List<Integer> columns = new ArrayList<>();

    public static void main(String[] args) throws IOException {
        long startTime = System.nanoTime();
        List<String> reqFields = new ArrayList<>();
        reqFields.add("AUDITLOGID");
        reqFields.add("RECORDID");
        reqFields.add("RECORDNAME");
        List<Map<String, Object>> rows = read(new Configuration(), "output/orc/part-00000-d201a70e-ec01-42b9-8600-094aaeb3a02c-c000.snappy.orc", reqFields);
        for(Map<String, Object> row : rows){
            System.out.println(row);
            break;
        }
        long endTime = System.nanoTime();
        System.out.println("Function execution time : " + (endTime - startTime) / 1000000000.0);
        System.out.println("Total number of records returned : " + rows.size());
    }

    public static void getRowIndex(RecordReaderImpl records, int stripeIndex) throws IOException {
        OrcIndex indexes = records.readRowIndex(stripeIndex, null, null);
        OrcProto.RowIndex orgIndex = indexes.getRowGroupIndex()[1];
        List<OrcProto.RowIndexEntry> rowIndexEntries = orgIndex.getEntryList();
        long max,min;
        for(OrcProto.RowIndexEntry rowIndexEntry: rowIndexEntries){
            OrcProto.IntegerStatistics stats = rowIndexEntry.getStatistics().getIntStatistics();
            min = stats.getMinimum()/orgDiv;
            max = stats.getMaximum()/orgDiv;
            if(min<=orgId && orgId<=max)
                result.add(batch);
            batch++;
        }
    }

    public static List<Map<String, Object>> read(Configuration configuration, String path, List<String> reqColumns)
            throws IOException {
        List<Map<String, Object>> rows = new LinkedList<>();
        Reader reader = OrcFile.createReader(new Path(path), OrcFile.readerOptions(configuration));
        TypeDescription schema = reader.getSchema();
        List<String> fieldNames = schema.getFieldNames();
        List<TypeDescription> columnTypes = schema.getChildren();

        // Get column vectors for the given schema
        int size = fieldNames.size();
        BiFunction<ColumnVector, Integer, Object>[] mappers = new BiFunction[size];
        for (int i = 0; i < size; i++) {
            TypeDescription type = columnTypes.get(i);
            mappers[i] = createColumnReader(type);
        }

        for(String col : reqColumns){
            columns.add(fieldNames.indexOf(col));
        }

        try (RecordReaderImpl records = (RecordReaderImpl) reader.rows(reader.options())) {
            for(int iter=0; iter<reader.getStripes().size(); iter++)
                getRowIndex(records, iter);

            first_batch = result.get(0);
            last_batch = result.get(result.size()-1);

            VectorizedRowBatch batch = reader.getSchema().createRowBatch(10000);
            long current_batch = 0;

            while ((++current_batch)<=last_batch && records.nextBatch(batch)) {
                if(current_batch < first_batch || !result.contains(current_batch))
                    continue;
                for (int row = 0; row < batch.size; row++) {
                    if ((long) mappers[1].apply(batch.cols[1], row) / orgDiv != orgId)
                        continue;
                    Map<String, Object> map = new HashMap<>();
                    for (Integer col : columns) {
                        ColumnVector columnVector = batch.cols[col];
                        if (columnVector.isNull[row]) {
                            map.put(fieldNames.get(col), null);
                        } else {
                            Object value = mappers[col].apply(columnVector, row);
                            map.put(fieldNames.get(col), value);
                        }
                    }
                    rows.add(map);
                }
            }
        }
        return rows;
    }

    public static BiFunction<ColumnVector, Integer, Object> createColumnReader(TypeDescription description) {
        String type = description.getCategory().getName();
        BiFunction<ColumnVector, Integer, Object> mapper;
        if ("tinyint".equals(type)) {
            mapper = (columnVector, row) -> (byte) ((LongColumnVector) columnVector).vector[row];
        } else if ("smallint".equals(type)) {
            mapper = (columnVector, row) -> (short) ((LongColumnVector) columnVector).vector[row];
        } else if ("int".equals(type) || "date".equals(type)) {
            mapper = (columnVector, row) -> (int) ((LongColumnVector) columnVector).vector[row];
        } else if ("bigint".equals(type)) {
            mapper = (columnVector, row) -> ((LongColumnVector) columnVector).vector[row];
        } else if ("boolean".equals(type)) {
            mapper = (columnVector, row) -> ((LongColumnVector) columnVector).vector[row] == 1;
        } else if ("float".equals(type)) {
            mapper = (columnVector, row) -> (float) ((DoubleColumnVector) columnVector).vector[row];
        } else if ("double".equals(type)) {
            mapper = (columnVector, row) -> ((DoubleColumnVector) columnVector).vector[row];
        } else if ("decimal".equals(type)) {
            mapper = (columnVector, row) -> ((DecimalColumnVector) columnVector).vector[row].getHiveDecimal().bigDecimalValue();
        } else if ("string".equals(type) || type.startsWith("varchar")) {
            mapper = (columnVector, row) -> ((BytesColumnVector) columnVector).toString(row);
        } else if ("char".equals(type)) {
            mapper = (columnVector, row) -> ((BytesColumnVector) columnVector).toString(row).charAt(0);
        } else if ("timestamp".equals(type)) {
            mapper = (columnVector, row) -> ((TimestampColumnVector) columnVector).getTimestampAsLong(row);
        } else {
            throw new RuntimeException("Unsupported type " + type);
        }
        return mapper;
    }
}


//old code without stats
//        try (RecordReaderImpl records = (RecordReaderImpl) reader.rows(reader.options())) {
//            // Read rows in batch for better performance.
//            VectorizedRowBatch batch = reader.getSchema().createRowBatch(10000);
//            while (records.nextBatch(batch)) {
//                for (int row = 0; row < batch.size; row++) {
//                    if ((long) mappers[1].apply(batch.cols[1], row) / orgDiv != orgId)
//                        continue;
//                    Map<String, Object> map = new HashMap<>();
//                    for (int col = 0; col < batch.numCols; col++) {
//                        ColumnVector columnVector = batch.cols[col];
//                        if (columnVector.isNull[row]) {
//                            map.put(fieldNames.get(col), null);
//                        } else {
//                            Object value = mappers[col].apply(columnVector, row);
//                            map.put(fieldNames.get(col), value);
//                        }
//                    }
//                    rows.add(map);
//                }
//            }
//        }