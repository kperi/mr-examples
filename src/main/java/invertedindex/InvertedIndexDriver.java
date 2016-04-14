package invertedindex;

/**
 * Created by kostas on 08/04/16.
 */

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import joins.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * Created by kostas on 07/04/16.
 */
public class InvertedIndexDriver {

    public static void main(String[] args) throws Exception {

        Configuration config = new Configuration();
        String inputDir = args[0];
        String outputDir = args[1];

        Job job = Job.getInstance(config, "InvertedIndexDriver");
        job.setJarByClass(InvertedIndexDriver.class);



        TextInputFormat.addInputPath(job, new Path(inputDir));
        TextInputFormat.setInputDirRecursive(job, true);

        FileOutputFormat.setOutputPath(job, new Path( outputDir ));

        job.setMapperClass(InvertedIndexMapper.class);
        job.setReducerClass(InvertedIndexReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}

