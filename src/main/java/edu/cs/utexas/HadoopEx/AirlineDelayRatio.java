package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.Text;

public class AirlineDelayRatio implements Comparable<AirlineDelayRatio> {
    private Text airline;
    private double delayRatio;

    public AirlineDelayRatio(Text airline, double delayRatio) {
        this.airline = airline;
        this.delayRatio = delayRatio;
    }

    public Text getAirline() {
        return airline;
    }

    public double getDelayRatio() {
        return delayRatio;
    }

    @Override
    public int compareTo(AirlineDelayRatio other) {
        return Double.compare(this.delayRatio, other.delayRatio);
    }
}
