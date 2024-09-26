package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EarningsPerMinuteMapper extends Mapper<Object, Text, Text, IntDoublePairWritable> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        
        if (Utils.validLine(fields)) {
            try {
                String driverId = fields[1];  // Driver ID
                double earnings = Double.parseDouble(fields[16]);  // Total earnings for the trip
                int timeInSeconds = Integer.parseInt(fields[4]);  // Trip time in seconds converted to minutes

                context.write(new Text(driverId), new IntDoublePairWritable(timeInSeconds, earnings));
            } catch (Exception e) {
                // invalid input
                System.out.println("Error parsing line: " + value.toString());
                e.printStackTrace();
            }
        }
    }
}
