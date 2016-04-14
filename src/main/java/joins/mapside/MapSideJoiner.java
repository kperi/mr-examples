package joins.mapside;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import joins.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by kostas on 07/04/16.
 */
public class MapSideJoiner {

    public static void main(String[] args) throws Exception {
        Splitter splitter = Splitter.on('/');
        StringBuilder filePaths = new StringBuilder();

        Configuration config = new Configuration();
        config.set("keyIndex", "0");
        config.set("separator", ",");


        for(int i = 0; i< args.length - 1; i++) {
            String fileName = Iterables.getLast(splitter.split(args[i]));
            config.set(fileName, Integer.toString(i+1));
            filePaths.append(args[i]).append(",");
        }

        filePaths.setLength(filePaths.length() - 1);
        Job job = Job.getInstance(config, "ReduceSideJoin");
        job.setJarByClass(ReduceSideJoin.class);

        FileInputFormat.addInputPaths(job, filePaths.toString());
        FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1]));

        job.setMapperClass(JoiningMapper.class);
        job.setReducerClass(JoiningReducer.class);
        job.setPartitionerClass(TaggedJoiningPartitioner.class);
        job.setGroupingComparatorClass(TaggedJoiningGroupingComparator.class);
        job.setOutputKeyClass(TaggedKey.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}

