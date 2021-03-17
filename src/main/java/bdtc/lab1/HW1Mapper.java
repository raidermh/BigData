package bdtc.lab1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;


public class HW1Mapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    ArrayList<String> dictPlace = null;

    public void setup(Context context) throws IOException,InterruptedException {
        dictPlace = new ArrayList<String>();
        URI[]cacheFiles = context.getCacheFiles();
        if (cacheFiles != null && cacheFiles.length > 0) {
            try {
                String line = "";
				/*  Create a FileSystem object and pass configuration object in it.
					FileSystem is an abstract base class for a fairly generic filesystem.
					All user code that may potentially use the Hadoop Distributed File System
					         should be written to use a FileSystem object.
				*/
                FileSystem fs = FileSystem.get(context.getConfiguration());
                Path getFilePath = new Path(cacheFiles[0].toString());

				/*
				We open the file using FileSystem object, convert the input byte stream to
				character streams using InputStreamReader
				and wrap it in BufferedReader to make it more efficient
				*/
                BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(getFilePath)));

                while ((line = reader.readLine()) != null) {
                    String [] words = line.split(" ");

                    for (int i = 0; i < words.length; i++) {
                        dictPlace.add(words[i]); //add the words to ArrayList
                    }
                }
            } catch (Exception e) {
                System.out.println("Unable to read the file");
                System.exit(1);
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 	{
        String [] words = value.toString().split(" ");

        for (int i = 0; i < words.length; i++) {
            //removing all special symbols and converting it to lowerCase
            String temp = words[i].replaceAll("[?,'()]", "").toLowerCase();
            //if not present in ArrayList we write
            if (dictPlace.contains(temp)) {
                context.write(new Text(temp), new LongWritable(1));
            }
        }
    }
}
