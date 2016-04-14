package invertedindex;

/**
 * Created by kostas on 08/04/16.
 */

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import joins.TaggedKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;


public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {

    private Text joinedText = new Text();
    private StringBuilder builder = new StringBuilder();


    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        HashSet<String> strings = new HashSet<String>();
        for( Text val: values){
            strings.add( val.toString() );
        }

        StringBuilder sb = new StringBuilder();
        for(String val:strings){
            sb.append(val.toString()).append(";");
        }



        context.write(key, new Text(sb.toString()));

    }
}