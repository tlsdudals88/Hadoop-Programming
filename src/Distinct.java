/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
//package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Distinct {

    public static class FirstMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            System.out.println("map input: key=" + key + ", value=" + value);

            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class SecondMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private  Text word = new Text();
        private Text word1 = new Text("same_key");

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            System.out.println("map input: key=" + key + ", value=" + value);

            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word1, one);
            }
        }
    }

    public static class CombineReducer
            extends Reducer<Text,IntWritable,NullWritable,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

            System.out.print("reduce input: key=" + key + ", values=");

            int sum = 0;
            for (IntWritable val : values) {
                System.out.print(val + " ");
                sum += val.get();
            }

            System.out.println();

            result.set(sum);
            context.write(NullWritable.get(), result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }

        Path tempDir =
                new Path("tempDir" +
                        Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));



        ////////////////////////////// Word Count ////////////////////////////////
        Job wordCountJob = Job.getInstance(conf, "Word Count!!");
        wordCountJob.setJarByClass(Distinct.class);
        wordCountJob.setMapperClass(FirstMapper.class);
        // job.setCombinerClass(IntSumReducer.class);
        // I don't want to keep same format between mapper and reducer
        wordCountJob.setReducerClass(CombineReducer.class);

        // Map output
        wordCountJob.setMapOutputKeyClass(Text.class);
        wordCountJob.setMapOutputValueClass(IntWritable.class);

        // Reduce output
        wordCountJob.setOutputKeyClass(NullWritable.class);
        wordCountJob.setOutputValueClass(IntWritable.class);

        // Set FileFormat for Input and Output
        FileInputFormat.setInputPaths(wordCountJob, new Path(args[0])); // Input
        FileOutputFormat.setOutputPath(wordCountJob, tempDir); // tempDir_23489234

        wordCountJob.waitForCompletion(true);

        ////////////////////////////// The number of distinct value ////////////////////////////////
        Job distinctJob = Job.getInstance(conf, "Distinct Value!!");
        distinctJob.setJarByClass(Distinct.class);
        distinctJob.setMapperClass(SecondMapper.class);
        // job.setCombinerClass(IntSumReducer.class);
        // I don't want to keep same format between mapper and reducer
        distinctJob.setReducerClass(CombineReducer.class);

        // Map output
        distinctJob.setMapOutputKeyClass(Text.class);
        distinctJob.setMapOutputValueClass(IntWritable.class);

        // Reduce output
        distinctJob.setOutputKeyClass(NullWritable.class);
        distinctJob.setOutputValueClass(IntWritable.class);

        // Set FileFormat for Input and Output
        FileInputFormat.setInputPaths(distinctJob, tempDir);
        FileOutputFormat.setOutputPath(distinctJob, new Path(args[1]));

        distinctJob.waitForCompletion(true);

        /*

            for (int i = 0; i < otherArgs.length - 1; ++i) {
                FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
            }
            FileOutputFormat.setOutputPath(job,
                    new Path(otherArgs[otherArgs.length - 1]));

        */

        // delete tempDir
        FileSystem.get(conf).delete(tempDir, true);

        System.exit(distinctJob.waitForCompletion(true) ? 0 : 1);

    }
}
