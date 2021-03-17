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
            //the complete URI(Uniform Resource Identifier) file path in Hdfs
            job.addCacheFile(new URI("hdfs://localhost:9000/cached_Dict/dictPlace.txt"));

        } catch (Exception e) {
            System.out.println("File Not Added");
            System.exit(1);
        }

        // Устанавливаем формат выходного файла = SequenceFile
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        log.info("=====================JOB STARTED=====================");
        job.waitForCompletion(true);
        log.info("=====================JOB ENDED=====================");
    }
}
