package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class TaxiAndErrorCount implements Comparable<TaxiAndErrorCount> {

    private final Text taxiId;
    private final IntWritable totalRecords;
    private final IntWritable gpsErrors;

    public TaxiAndErrorCount(Text taxiId, IntWritable totalRecords, IntWritable gpsErrors) {
        this.taxiId = taxiId;
        this.totalRecords = totalRecords;
        this.gpsErrors = gpsErrors;
    }

    public Text getTaxiId() {
        return taxiId;
    }

    public IntWritable getTotalRecords() {
        return totalRecords;
    }

    public IntWritable getGPSErrors() {
        return gpsErrors;
    }

    public float getErrorFraction() {
        return (float) gpsErrors.get() / totalRecords.get();
    }

    @Override
    public int compareTo(TaxiAndErrorCount other) {
        return Float.compare(this.getErrorFraction(), other.getErrorFraction());
    }

    @Override
    public String toString() {
        return  "(" + taxiId.toString()+ ", " + getErrorFraction() + ")";
    }
}
