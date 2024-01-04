package Q1;

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

public class PopularMovieCount {

    public static class MovieMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] movieData = value.toString().split(",");

            // Skip header and invalid records
            if (!movieData[0].equals("id") && !movieData[1].equals("#VALUE!") && !movieData[4].equals("#VALUE!") && !movieData[5].equals("#VALUE!")) {
                try {
                    double popularity = Double.parseDouble(movieData[1]);
                    double voteAverage = Double.parseDouble(movieData[4]);
                    double voteCount = Double.parseDouble(movieData[5]);

                    if (popularity > 500.0 && voteAverage > 8.0 && voteCount > 10000.0) {
                        context.write(new Text("popular_movies"), new IntWritable(1));
                    }
                } catch (NumberFormatException e) {
                    // Handle invalid data (e.g., log a warning)
                    System.err.println("Invalid numerical data encountered in record: " + value.toString());
                }
            }
        }
    }

    public static class MovieReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable value : values) {
                count += value.get();
            }
            context.write(key, new IntWritable(count));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: PopularMovieCount <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Popular Movie Count");
        job.setJarByClass(PopularMovieCount.class);
        job.setMapperClass(MovieMapper.class);
        job.setCombinerClass(MovieReducer.class); // Optional for efficiency
        job.setReducerClass(MovieReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job,

                new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
