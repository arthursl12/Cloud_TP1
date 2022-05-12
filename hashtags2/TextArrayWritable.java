package hashtags;
import java.util.Arrays;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;


/* The TextArrayWritable class allows sending a list of MapReduce Text
 * objects from a Mapper to a Reducer:
 *     ArrayList<String> hashtags = new ArrayList<String>();
 *     // fill in hashtags
 *     String[] hashtagsArray = new String[hashtags.size()];
 *     hashtagsArray = hashtags.toArray(hashtagsArray);
 *     TextArrayWritable hashtagsW = new TextArrayWritable(hashtagsArray);
 *     // can then send hashtagsW from Map to Reduce
 */
public class TextArrayWritable extends ArrayWritable {
    public TextArrayWritable() {
        super(Text.class);
    }

    public TextArrayWritable(String[] values){
        super(Text.class);
        Text[] convertedValues = new Text[values.length];
        for(int i = 0; i < values.length; i++) {
            convertedValues[i] = new Text(values[i]);
        }
        set(convertedValues);
    }

    @Override
    public Text[] get() {
        return (Text[]) super.get();
    }

    @Override
    public String toString() {
        Text[] values = get();
        String big_str = "";
        for(int i = 0; i < values.length; i++) {
            big_str += values[i].toString() + ", ";
        }
        return big_str;
    }
}
