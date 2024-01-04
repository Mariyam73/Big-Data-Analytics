package popularity;

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

public class PopularMovies {

    public static class MovieMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] movieData = value.toString().split(",");

            // Skip header and invalid records
            if (!movieData[0].equals("id") && !movieData[1].equals("#VALUE!") && !movieData[4].equals("#VALUE!") && !movieData[5].equals("#VALUE!")) {
                try {
                    double popularity = Double.parseDouble(movieData[1]);

                    if (popularity > 500.0) {
                        context.write(new Text("popular_movies"), new Text("1")); // Emit "1" as a Text value
                    }
                } catch (NumberFormatException e) {
                    // Handle invalid data (e.g., log a warning)
                    System.err.println("Invalid numerical data encountered in record: " + value.toString());
                }
            }
        }
    }

    public static class MovieReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (Text value : values) {
                count += Integer.parseInt(value.toString()); // Parse Text values as integers
            }
            context.write(new Text("Movies with popularity greater than 500:"), new Text(String.valueOf(count)));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: PopularMovies <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Popular Movie Count");
        job.setJarByClass(PopularMovies.class);
        job.setMapperClass(MovieMapper.class);
        // job.setCombinerClass(MovieReducer.class); // Commented out to avoid potential issues
        job.setReducerClass(MovieReducer.class);

        // Set output key and value classes to Text
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
