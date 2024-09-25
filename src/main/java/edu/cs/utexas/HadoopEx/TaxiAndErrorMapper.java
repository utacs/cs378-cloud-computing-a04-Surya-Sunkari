package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TaxiAndErrorMapper extends Mapper<Object, Text, Text, IntPairWritable> {

    private final IntWritable one = new IntWritable(1);
    private final IntWritable error = new IntWritable(1);
    private final IntWritable noError = new IntWritable(0);

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");

        if (Utils.validLine(fields)) {
            try {
                boolean[] gpsErrors = Utils.countGPSErrors(fields);
                boolean isGPSError = gpsErrors[0] || gpsErrors[1];

                String taxiId = fields[0];

                // int writable in the form [count (always 1), error (1 if gps error, 0 otherwise)]
                context.write(
                    new Text(taxiId),
                    new IntPairWritable(
                        one.get(), 
                        isGPSError ? error.get() : noError.get()
                    )
                );
                
            } catch (Exception e) {
                // invalid input
                System.out.println("error parsing line: " + value.toString());
                e.printStackTrace();
            }
        }

    }
}
