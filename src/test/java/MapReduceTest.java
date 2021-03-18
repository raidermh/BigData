import bdtc.lab1.HW1Mapper;
import bdtc.lab1.HW1Reducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class MapReduceTest {

    private MapDriver<LongWritable, Text, Text, LongWritable> mapDriver;
    private ReduceDriver<Text, LongWritable, Text, LongWritable> reduceDriver;
    private MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable> mapReduceDriver;


    private final String testreducer = "footer";
    private final String testmapper = "09-04-2020 12:46:25 36 486 admin footer HIGH";



    @Before
    public void setUp() {
        HW1Mapper mapper = new HW1Mapper();
        HW1Reducer reducer = new HW1Reducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }


    @Test
    public void testMapper() throws IOException {
        mapDriver
                .withInput(new LongWritable(1), new Text(testmapper))
                .withOutput(new Text(testreducer), new LongWritable(1))
                .runTest();
    }


    @Test
    public void testReducer() throws IOException {
        List<LongWritable> values = new ArrayList<LongWritable>();
        values.add(new LongWritable(1));
        values.add(new LongWritable(1));
        reduceDriver
                .withInput(new Text(testreducer), values)
                .withOutput(new Text(testreducer), new LongWritable(2))
                .runTest();
    }


    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver
                .withInput(new LongWritable(), new Text(testreducer))
                .withInput(new LongWritable(), new Text(testreducer))
                .withOutput(new Text(testreducer), new LongWritable(2))
                .runTest();
    }

}
