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
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class HW1Mapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    List<String> dictPlace = null;

    public void setup(Context context) throws IOException {
        dictPlace = new ArrayList<>();
        URI[]cacheFiles = context.getCacheFiles();
        if (cacheFiles != null && cacheFiles.length > 0) {
            try {
                String line;
				/*  Create a FileSystem object and pass configuration object in it.
					All code that may potentially use the Hadoop Distributed File System
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
            String temp = words[i];

            Pattern patternX = Pattern.compile("X=(\\w+)");
            Pattern patternY = Pattern.compile("Y=(\\w+)");

            Matcher matcherX = patternX.matcher(temp);
            Matcher matcherY = patternY.matcher(temp);

            String resultStrX = null;
            String resultStrY = null;

            while (matcherX.find()) {
                String resultX = matcherX.group();
                resultStrX = resultX.substring(resultX.lastIndexOf("=") + 1);
            }

            while (matcherY.find()) {
                String resultY = matcherY.group();
                resultStrY = resultY.substring(resultY.lastIndexOf("=") + 1);
            }

            String [] dictWords = dictPlace.toArray(new String[dictPlace.size()]);

            for (int j = 0; j < dictWords.length; j++) {
                String dictTemp = dictWords[j];

                Pattern patternP = Pattern.compile("P=(\\w+)");
                Pattern patternMinX = Pattern.compile("minX=(\\w+)");
                Pattern patternMaxX = Pattern.compile("maxX=(\\w+)");
                Pattern patternMinY = Pattern.compile("minY=(\\w+)");
                Pattern patternMaxY = Pattern.compile("maxY=(\\w+)");

                Matcher matcherP = patternP.matcher(dictTemp);
                Matcher matcherMinX = patternMinX.matcher(dictTemp);
                Matcher matcherMaxX = patternMaxX.matcher(dictTemp);
                Matcher matcherMinY = patternMinY.matcher(dictTemp);
                Matcher matcherMaxY = patternMaxY.matcher(dictTemp);

                String resultStrP = null;
                String resultStrMinX = null;
                String resultStrMaxX = null;
                String resultStrMinY = null;
                String resultStrMaxY = null;

                while (matcherP.find()) {
                    String resultP = matcherP.group();
                    resultStrP = resultP.substring(resultP.lastIndexOf("=") + 1);
                }

                while (matcherMinX.find()) {
                    String resultMinX = matcherMinX.group();
                    resultStrMinX = resultMinX.substring(resultMinX.lastIndexOf("=") + 1);
                }

                while (matcherMaxX.find()) {
                    String resultMaxX = matcherMaxX.group();
                    resultStrMaxX = resultMaxX.substring(resultMaxX.lastIndexOf("=") + 1);
                }

                while (matcherMinY.find()) {
                    String resultMinY = matcherMinY.group();
                    resultStrMinY = resultMinY.substring(resultMinY.lastIndexOf("=") + 1);
                }

                while (matcherMaxY.find()) {
                    String resultMaxY = matcherMaxY.group();
                    resultStrMaxY = resultMaxY.substring(resultMaxY.lastIndexOf("=") + 1);
                }

                int resultIntX = 0;
                int resultIntY = 0;
                int resultIntMinX = 0;
                int resultIntMaxX = 0;
                int resultIntMinY = 0;
                int resultIntMaxY = 0;


                if (resultStrX != null) {
                    resultIntX = Integer.parseInt(resultStrX.trim());
                }
                if (resultStrY != null) {
                    resultIntY = Integer.parseInt(resultStrY.trim());
                }
                if (resultStrMinX != null) {
                    resultIntMinX = Integer.parseInt(resultStrMinX.trim());
                }
                if (resultStrMaxX != null) {
                    resultIntMaxX = Integer.parseInt(resultStrMaxX.trim());
                }
                if (resultStrMinY != null) {
                    resultIntMinY = Integer.parseInt(resultStrMinY.trim());
                }
                if (resultStrMaxY != null) {
                    resultIntMaxY = Integer.parseInt(resultStrMaxY.trim());
                }

                if (resultIntX > resultIntMinX && resultIntX <= resultIntMaxX && resultIntY > resultIntMinY && resultIntY <= resultIntMaxY) {
                    context.write(new Text(resultStrP), new LongWritable(1));
                }
            }
        }
    }
}
