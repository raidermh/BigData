package bdtc.lab1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Клас HW1Reducer подсчитывает количество кликов после стадии МАП
 * и выдает обее кол-во кликов по заданной области.
 *
 * @author Mikhail Khrychev
 * @version  1.0.1
 * @since 20.03.2021
 */

public class HW1Reducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long sum = 0;

        for (LongWritable val:values) {
            sum += val.get();
        }

        context.write(key, new LongWritable(sum));
    }
}
