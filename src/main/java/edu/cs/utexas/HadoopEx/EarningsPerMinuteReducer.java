package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class EarningsPerMinuteReducer extends Reducer<Text, IntDoublePairWritable, Text, DoubleWritable> {

    public void reduce(Text key, Iterable<IntDoublePairWritable> values, Context context) throws IOException, InterruptedException {
        int totalSeconds = 0;
        double totalEarnings = 0;

        for (IntDoublePairWritable value : values) {
            totalSeconds += value.getInt1();
            totalEarnings += value.getDouble2();
        }

        if (totalSeconds > 0) {
            double earningsPerMinute = totalEarnings / ((double)totalSeconds / 60.0);
            context.write(key, new DoubleWritable(earningsPerMinute));
        }
    }
}
