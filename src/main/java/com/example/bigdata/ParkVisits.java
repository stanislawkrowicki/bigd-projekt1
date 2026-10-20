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
        job.setCombinerClass(ParkVisitsCombiner.class);
        job.setReducerClass(ParkVisitsReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SumCount.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class ParkVisitsMapper extends Mapper<LongWritable, Text, Text, SumCount> {

        private final Text key = new Text();
        private final SumCount sumCount = new SumCount();
        private final IntWritable one = new IntWritable(1);

        public void map(LongWritable offset, Text lineText, Context context) {
            try {
                if (offset.get() == 0) return;

                String line = lineText.toString();
                String[] values = line.split(",");

                String parkId = values[1];
                String datetimeString = values[3];
                String dateString = datetimeString.split("T")[0];

                double groupSize = Double.parseDouble(values[5]);

                key.set(parkId + "\t" + dateString);
                sumCount.set(new DoubleWritable(groupSize), one);

                context.write(key, sumCount);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class ParkVisitsReducer extends Reducer<Text, SumCount, Text, Text> {
        private final Text result = new Text();

        @Override
        public void reduce(Text key, Iterable<SumCount> values,
                           Context context) throws IOException, InterruptedException {
            double sumOfPeople = 0;
            int entries = 0;

            for (SumCount val : values) {
                entries += val.getCount().get();
                sumOfPeople += val.getSum().get();
            }

            double averageGroupSize = sumOfPeople / entries;
            result.set(entries + "\t" + String.format("%.2f", averageGroupSize));

            context.write(key, result);
        }
    }

    public static class ParkVisitsCombiner extends Reducer<Text, SumCount, Text, SumCount> {
        private final SumCount aggregate = new SumCount();

        @Override
        public void reduce(Text key, Iterable<SumCount> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;

            for (SumCount val : values) {
                sum += val.getSum().get();
                count += val.getCount().get();
            }

            aggregate.set(new DoubleWritable(sum), new IntWritable(count));
            context.write(key, aggregate);
        }
    }
}