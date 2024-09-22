package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;
import java.util.PriorityQueue;
import java.util.Collections;

public class TopFiveErrorReducer extends Reducer<Text, IntPairWritable, Text, FloatWritable> {
    
    private PriorityQueue<TaxiAndErrorCount> pq;

    @Override
    protected void setup(Context context) {
        pq = new PriorityQueue<>(5, (a, b) -> Float.compare(a.getErrorFraction(), b.getErrorFraction()));
    }

    @Override
    public void reduce(Text key, Iterable<IntPairWritable> values, Context context) throws IOException, InterruptedException {
        int totalRecords = 0;
        int totalErrors = 0;

        for (IntPairWritable value : values) {
            totalRecords += value.getInt1();
            totalErrors += value.getInt2();
        }
        
        TaxiAndErrorCount taxi = new TaxiAndErrorCount(new Text(key), new IntWritable(totalRecords), new IntWritable(totalErrors));
        pq.add(taxi);

        if (pq.size() > 5) {
            pq.poll();
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        while (!pq.isEmpty()) {
            TaxiAndErrorCount taxiAndError = pq.poll();
            context.write(new Text(taxiAndError.getTaxiId()), new FloatWritable(taxiAndError.getErrorFraction()));
        }
    }
}
