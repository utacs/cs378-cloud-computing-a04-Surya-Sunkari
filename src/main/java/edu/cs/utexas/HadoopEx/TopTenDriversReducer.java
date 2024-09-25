package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.PriorityQueue;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopTenDriversReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    
    private PriorityQueue<DriverEarnings> topDrivers;

    @Override
    protected void setup(Context context) {
        topDrivers = new PriorityQueue<>(10);
    }

    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        for (DoubleWritable value : values) {
            topDrivers.add(new DriverEarnings(new Text(key), new DoubleWritable(value.get()))); // TODO: maybe change to just value instead of value.get()

            if (topDrivers.size() > 10) {
                topDrivers.poll();
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        while (!topDrivers.isEmpty()) {
            DriverEarnings driver = topDrivers.poll();
            context.write(driver.getDriverId(),driver.getEarningsPerMinute());
        }
    }
}
