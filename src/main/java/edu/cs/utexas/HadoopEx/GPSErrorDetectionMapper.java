
package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class GPSErrorDetectionMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

	private final IntWritable one = new IntWritable(1);
	private IntWritable hourWritable = new IntWritable();

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
        
        String[] fields = value.toString().split(",");
        
        
        if (Utils.validLine(fields)) {
            try {
                // write if error gps position
                boolean[] gpsErrors = Utils.countGPSErrors(fields);
                if(gpsErrors[0]) {
                    String pickupDateTime = fields[2];
                    String[] pickupDateTimeSplit = pickupDateTime.split(" ");
                    String[] pickupTimeSplit = pickupDateTimeSplit[1].split(":");
                    int hour = Integer.parseInt(pickupTimeSplit[0]) + 1; // one index
                    hourWritable.set(hour);
                    context.write(hourWritable, one);
                }
                if(gpsErrors[1]) {
                    String dropoffDateTime = fields[3];
                    String[] dropoffDateTimeSplit = dropoffDateTime.split(" ");
                    String[] dropoffTimeSplit = dropoffDateTimeSplit[1].split(":");
                    int hour = Integer.parseInt(dropoffTimeSplit[0]) + 1;
                    hourWritable.set(hour);
                    context.write(hourWritable, one);
                }
            } catch(Exception e) {
                // invalid input
                System.out.println("error parsing line: " + value.toString());
                e.printStackTrace();
            }
        }
	}
}