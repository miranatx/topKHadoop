package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AirportMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable count = new IntWritable(1);
    private Text airport = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        // Extract ORIGIN_AIRPORT (column 8)
        String originAirport = fields[7];
        airport.set(originAirport);
        // Emit (ORIGIN_AIRPORT, 1) to count each flight
        context.write(airport, count);
    }
}