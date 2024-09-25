package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

public class DriverEarnings implements Comparable<DriverEarnings> {

    private final Text driverId;
    private final DoubleWritable earningsPerMinute;

    public DriverEarnings(Text driverId, DoubleWritable earningsPerMinute) {
        this.driverId = driverId;
        this.earningsPerMinute = earningsPerMinute;
    }

    public Text getDriverId() {
        return driverId;
    }

    public DoubleWritable getEarningsPerMinute() {
        return earningsPerMinute;
    }

    @Override
    public int compareTo(DriverEarnings other) {
        return Double.compare(this.earningsPerMinute.get(), other.earningsPerMinute.get());
    }

    @Override
    public String toString() {
        return  "(" + this.driverId.toString()+ ", " + this.earningsPerMinute.toString() + ")";
    }
}
