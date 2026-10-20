package com.example.bigdata;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class ParkVisits extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new ParkVisits(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "ParkVisits");
        job.setJarByClass(this.getClass());
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(ParkVisitsMapper.class);
        job.setCombinerClass(ParkVisitsReducer.class);
        job.setReducerClass(ParkVisitsReducer.class);

        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class ParkVisitsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final Text date = new Text();
        private final IntWritable entries = new IntWritable();

        public void map(LongWritable offset, Text lineText, Context context) {
            try {
                if (offset.get() == 0) return;

                String line = lineText.toString();
                String[] values = line.split(",");

                String datetimeString = values[3];
                String dateString = datetimeString.split("T")[0];

                date.set(dateString);
                entries.set(1);

                context.write(date, entries);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class ParkVisitsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable resultValue = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            int entries = 0;

            for (IntWritable val : values) {
                entries += val.get();
            }

            resultValue.set(entries);

            context.write(key, resultValue);
        }
    }
}