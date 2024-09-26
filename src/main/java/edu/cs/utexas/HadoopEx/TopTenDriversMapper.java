package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.PriorityQueue;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopTenDriversMapper extends Mapper<Text, Text, Text, DoubleWritable> {
    
    private PriorityQueue<DriverEarnings> topDrivers;

    @Override
    protected void setup(Context context) {
        topDrivers = new PriorityQueue<>(10);
    }

    @Override
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        try {
            double earnings = Double.parseDouble(value.toString().trim());

            topDrivers.add(new DriverEarnings(new Text(key), new DoubleWritable(earnings))); 

            if (topDrivers.size() > 10) {
                topDrivers.poll();
            }
        } catch(Exception e) {
            System.out.println("error top 10 mapping line: " + value.toString());
            e.printStackTrace();
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        while (!topDrivers.isEmpty()) {
            DriverEarnings driver = topDrivers.poll();
            context.write(driver.getDriverId(), driver.getEarningsPerMinute());
        }
    }
}
