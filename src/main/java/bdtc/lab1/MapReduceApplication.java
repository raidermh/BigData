package bdtc.lab1;

import lombok.extern.log4j.Log4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.net.URI;

/**
 * Клас MapReduceApplication - точка входа для запуска приложения.
 * В нем находятся базовые харакетеристики джобы для запуска приложения.
 *
 * @author Mikhail Khrychev
 * @version  1.0.1
 * @since 20.03.2021
 */

@Log4j
public class MapReduceApplication {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String []otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();

        if (otherArgs.length!=2) {
            System.err.println("Error: Give only two paths for <input> <output>");
            System.exit(1);
        }

        Job job = Job.getInstance(conf,"Use Distributed Cache and Create Sequence File");

        job.setJarByClass(MapReduceApplication.class);
        job.setMapperClass(HW1Mapper.class);
        job.setReducerClass(HW1Reducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        try {

            /**
             * Задается путь к файлу справочника в HDFS
             */
            job.addCacheFile(new URI("hdfs://localhost:9000/cached_Dict/dictPlace.txt"));


        } catch (Exception e) {
            System.out.println("File Not Added");
            System.exit(1);
        }

        /**
         * Формат вых. файла = SequenceFile
         */
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        log.info("=====================JOB STARTED=====================");
        job.waitForCompletion(true);
        log.info("=====================JOB ENDED=====================");
    }
}
