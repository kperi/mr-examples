
package invertedindex;

/**
 * Created by kostas on 08/04/16.
 */

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.HashSet;


public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //Path filePath = ((FileSplit) context.getInputSplit()).getPath();
        String filePathString = ((FileSplit) context.getInputSplit()).getPath().toString();

        String [] tokens = value.toString().toLowerCase().split("\\s+");

        HashSet<String> tokenSet = new HashSet<String>();


        for(String token: tokens) {
            tokenSet.add(token);
        }


        for(String token:tokenSet)
            context.write(new Text(token), new Text(filePathString));

    }

}