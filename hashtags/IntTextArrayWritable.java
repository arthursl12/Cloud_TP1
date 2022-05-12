package hashtags;
import java.util.Arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
public class IntTextArrayWritable extends ArrayWritable {
    private IntWritable counter = new IntWritable();

    public IntTextArrayWritable() {
        super(Text.class);
    }

    public IntTextArrayWritable(int value, String[] values) {
        super(Text.class);
        Text[] convertedValues = new Text[values.length];
        for(int i = 0; i < values.length; i++) {
            convertedValues[i] = new Text(values[i]);
        }
        set(convertedValues);
        counter.set(value);
    }

    @Override
    public Text[] get() {
        return (Text[]) super.get();
    }

    public int get_counter() {
        return counter.get();
    }

    @Override
    public String toString() {
        Text[] values = get();
        int counter = get_counter();

        String big_str = "";
        big_str += Integer.toString(counter) + ",";
        for(int i = 0; i < values.length; i++) {
            big_str += values[i].toString();
            if (i != values.length-1){
                big_str += " ";
            }
        }
        return big_str;
    }
}
