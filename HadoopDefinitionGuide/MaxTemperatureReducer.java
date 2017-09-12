import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.io.InterruptedIOException;

public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        //super.reduce(key, values, context);

        int maxTemperature = Integer.MIN_VALUE;

        for(IntWritable airTemperature : values) {
            maxTemperature = Math.max(maxTemperature, airTemperature.get());
        }

        context.write(key, new IntWritable(maxTemperature));
    }
}
