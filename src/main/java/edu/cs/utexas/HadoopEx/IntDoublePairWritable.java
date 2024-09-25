package edu.cs.utexas.HadoopEx;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class IntDoublePairWritable implements Writable {
    private IntWritable value1;
    private DoubleWritable value2;

    public IntDoublePairWritable() {
        value1 = new IntWritable();
        value2 = new DoubleWritable();
    }

    public IntDoublePairWritable(int value1, double value2) {
        this.value1 = new IntWritable(value1);
        this.value2 = new DoubleWritable(value2);
    }

    public IntDoublePairWritable(IntWritable value1, DoubleWritable value2) {
        this.value1 = value1;
        this.value2 = value2;
    }

    public int getInt1() {
        return value1.get();
    }

    public double getDouble2() {
        return value2.get();
    }

    @Override
    public String toString() {
        return value1.toString() + " " + value2.toString();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        value1.readFields(in);
        value2.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        value1.write(out);
        value2.write(out);
    }
}
