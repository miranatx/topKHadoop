package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AirportMapper extends Mapper<Object, Text, Text, DelayTupleWritable> {

    private DelayTupleWritable outTuple = new DelayTupleWritable(); // Writable to hold delay and flight count
    private final static IntWritable count = new IntWritable(1);
    private Text airport = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            String airline = fields[4];  // AIRLINE column
            String originAirport = fields[7];  // ORIGIN_AIRPORT column
            String departureDelayStr = fields[11];  // DEPARTURE_DELAY column

            context.write(new Text("AIRPORT:" + originAirport), new DelayTupleWritable(0, 1));

            int departureDelay = 0;
            if (!departureDelayStr.equals("")) {
                departureDelay = Integer.parseInt(departureDelayStr);  
            }
            outTuple.set(departureDelay, 1);  
            context.write(new Text("AIRLINE:" + airline), outTuple);  
    }
}