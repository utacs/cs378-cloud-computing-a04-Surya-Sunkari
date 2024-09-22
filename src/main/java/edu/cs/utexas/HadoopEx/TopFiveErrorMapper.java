package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.PriorityQueue;

public class TopFiveErrorMapper extends Mapper<Text, Text, Text, IntPairWritable> {
    private PriorityQueue<TaxiAndErrorCount> pq;

    @Override
    protected void setup(Context context) {
        pq = new PriorityQueue<>(5, (a, b) -> Float.compare(a.getErrorFraction(), b.getErrorFraction()));
    }

    @Override
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        
        String[] tokens = value.toString().split(" ");
        
        System.out.println("current top 5: " + pq.toString());
    
        try {
            int totalRecords = Integer.parseInt(tokens[0]);
            int gpsErrors = Integer.parseInt(tokens[1]);
            
            TaxiAndErrorCount taxi = new TaxiAndErrorCount(new Text(key), new IntWritable(totalRecords), new IntWritable(gpsErrors));
            pq.add(taxi);

            if (pq.size() > 5) {
                pq.poll();
            }
        } catch(Exception e) {
            System.out.println("error top 5 mapping line: " + value.toString());
            e.printStackTrace();
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        while (!pq.isEmpty()) {
            TaxiAndErrorCount taxiAndError = pq.poll();
            System.out.println("local top 5: " + taxiAndError.getTaxiId().toString() + " " + taxiAndError.getErrorFraction());
            context.write(new Text(taxiAndError.getTaxiId()), new IntPairWritable(taxiAndError.getTotalRecords(), taxiAndError.getGPSErrors()));
        }
    }
}
