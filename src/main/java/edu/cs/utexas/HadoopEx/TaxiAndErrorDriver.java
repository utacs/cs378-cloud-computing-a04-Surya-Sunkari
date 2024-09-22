package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TaxiAndErrorDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TaxiAndErrorDriver(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // count GPS errors and total taxi records
        Job job1 = Job.getInstance(conf, "TaxiGPSErrorCount");
        job1.setJarByClass(TaxiAndErrorDriver.class);
        job1.setMapperClass(TaxiAndErrorMapper.class);
        job1.setReducerClass(TaxiAndErrorReducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntPairWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntPairWritable.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        if (!job1.waitForCompletion(true)) {
            return 1;
        }

        // find top 5 taxis by error rate
        Job job2 = Job.getInstance(conf, "Top5TaxisByErrorRate");
        job2.setJarByClass(TaxiAndErrorDriver.class);
        job2.setMapperClass(TopFiveErrorMapper.class);
        job2.setReducerClass(TopFiveErrorReducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntPairWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(FloatWritable.class);

        job2.setInputFormatClass(KeyValueTextInputFormat.class);

        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        return job2.waitForCompletion(true) ? 0 : 1;
    }
}
