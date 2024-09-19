package edu.cs.utexas.HadoopEx;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class DelayTupleWritable implements Writable {
    private int totalDelay;
    private int flightCount;

    // Constructor with parameters
    public DelayTupleWritable(int delay, int count) {
        this.totalDelay = delay;
        this.flightCount = count;
    }

    // Default constructor (no arguments)
    public DelayTupleWritable() {
        this.totalDelay = 0;
        this.flightCount = 0;
    }

    public void set(int delay, int count) {
        this.totalDelay = delay;
        this.flightCount = count;
    }

    public int getTotalDelay() {
        return totalDelay;
    }

    public int getFlightCount() {
        return flightCount;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(totalDelay);
        out.writeInt(flightCount);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        totalDelay = in.readInt();
        flightCount = in.readInt();
    }

    @Override
    public String toString() {
        return totalDelay + "\t" + flightCount;
    }
}
