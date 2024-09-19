package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.PriorityQueue;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AirportReducer extends Reducer<Text, DelayTupleWritable, Text, Text> {

    // Priority queues for top 3 results
    private PriorityQueue<WordAndCount> topAirports = new PriorityQueue<>(3); // For Task 1 (Top airports)
    private PriorityQueue<AirlineDelayRatio> topAirlines = new PriorityQueue<>(3); // For Task 2 (Top airlines by delay ratio)

    public void reduce(Text key, Iterable<DelayTupleWritable> values, Context context) throws IOException, InterruptedException {
        String keyType = key.toString().split(":")[0]; 
        String keyValue = key.toString().split(":")[1]; 

        if (keyType.equals("AIRPORT")) {
            // Task 1: Sum up the number of flights for each airport
            int flightCount = 0;
            for (DelayTupleWritable val : values) {
                flightCount += val.getFlightCount();
            }

            // Use PriorityQueue to keep top-3 airports
            topAirports.add(new WordAndCount(new Text(keyValue), new IntWritable(flightCount)));
            if (topAirports.size() > 3) {
                topAirports.poll();
            }

        } else if (keyType.equals("AIRLINE")) {
            // Task 2: Calculate delay ratio for each airline
            int totalDelay = 0;
            int totalFlights = 0;

            for (DelayTupleWritable val : values) {
                totalDelay += val.getTotalDelay();
                totalFlights += val.getFlightCount();
            }

            double delayRatio = totalFlights == 0 ? 0 : (double) totalDelay / totalFlights;

            // Use PriorityQueue to keep top-3 airlines by delay ratio
            topAirlines.add(new AirlineDelayRatio(new Text(keyValue), delayRatio));
            if (topAirlines.size() > 3) {
                topAirlines.poll();
            }
        }
    }

    public void cleanup(Context context) throws IOException, InterruptedException {
        // Task 1: Output the top-3 airports
        context.write(new Text("Top 3 Airports:"), null);
        while (!topAirports.isEmpty()) {
            WordAndCount airport = topAirports.poll();
            context.write(airport.getWord(), new Text("Flight Count: " + airport.getCount().toString()));
        }

        // Task 2: Output the top-3 airlines by delay ratio
        context.write(new Text("Top 3 Airlines by Delay Ratio:"), null);
        while (!topAirlines.isEmpty()) {
            AirlineDelayRatio airline = topAirlines.poll();
            context.write(airline.getAirline(), new Text("Delay Ratio: " + airline.getDelayRatio()));
        }
    }
}