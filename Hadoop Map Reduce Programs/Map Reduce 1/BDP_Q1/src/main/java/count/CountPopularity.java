package count;

import Q1.PopularMovieCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class CountPopularity {

    public static class CountMovieMapper extends

            Mapper<LongWritable, Text, Text, Text> {

        @Override


        protected

        void

        map(LongWritable key, Text value, Context context)

                throws IOException, InterruptedException

        {
            String[] movieData = value.toString().split(",");

            // No header row to skip, but still handle invalid records
            try {
                int voteCount = Integer.parseInt(movieData[4]); // Correct column index for voteCount

                if (voteCount > 10000) {
                    //System.out.println("Mapper emitting: " + key + ", " + value); // Uncomment for logging
                    context.write(new Text("popular_movies"), new Text("1"));
                }
            } catch (NumberFormatException e) {
                // Handle invalid data (e.g., log a warning)
                System.err.println("Invalid numerical data encountered in record: " + value.toString());
            }
        }
    }

    public static class MovieReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (Text value : values) {
                count += Integer.parseInt(value.toString());
            }
            context.write(new Text("Movies with vote count > 10000:"), new Text(String.valueOf(count)));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: CountPopularity <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Popular Movie Count");
        job.setJarByClass(CountPopularity.class);
        job.setMapperClass(CountPopularity.CountMovieMapper.class);
        job.setCombinerClass(CountPopularity.MovieReducer.class); // Optional for efficiency
        job.setReducerClass(CountPopularity.MovieReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

