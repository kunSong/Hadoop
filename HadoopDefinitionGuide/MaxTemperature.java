import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by sWX469012 on 2017/9/11.
 */
public class MyAppDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        MyAppDriver driver = new MyAppDriver();
        ToolRunner.printGenericCommandUsage(System.out);
        ToolRunner.run(driver, args);
    }


    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        conf.setLong(FileInputFormat.SPLIT_MAXSIZE, 15360);

        FileSystem fs = FileSystem.get(conf);
        Path outDir = new Path("/user/output");
        Path tmp = new Path("/tmp");
        if(fs.exists(outDir)){
            fs.delete(outDir, true);
        }
        if(fs.exists(tmp)){
            fs.delete(tmp, true);
        }

        Job job = Job.getInstance();

        job.setJarByClass(MyApp.class);
        job.setJobName("Max Temperature");

        job.setNumReduceTasks(2);

        job.setCombinerClass(MyReducer.class);
        job.setPartitionerClass(HashPartitioner.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.out.println(job.waitForCompletion(true));
        return 0;
    }
}
