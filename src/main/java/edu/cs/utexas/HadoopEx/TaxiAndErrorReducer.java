package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TaxiAndErrorReducer extends Reducer<Text, IntPairWritable, Text, IntPairWritable> {

    public void reduce(Text key, Iterable<IntPairWritable> values, Context context) throws IOException, InterruptedException {
        int totalRecords = 0;
        int totalErrors = 0;

        for (IntPairWritable value : values) {
            totalRecords += value.getInt1();
            totalErrors += value.getInt2();
        }

        context.write(key, new IntPairWritable(totalRecords, totalErrors));
    }
}
