package bdtc.lab1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class HW1Reducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long sum = 0;

        /*
         * Reducer sums up all units received from the mapper and gives the total number of clicks on the area
         */

        for (LongWritable val:values) {
            sum += val.get();
        }

        context.write(key, new LongWritable(sum));
    }
}
