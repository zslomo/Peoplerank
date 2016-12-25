package peoplerank;

import source.BaseDriver;
import source.HadoopUtil;
import source.JobInitModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by bitnan on 16/12/18.
 * 对pr值进行重计算,每个pr都除以pr总值
 */
public class FinallyResult {

    public static class FinallyResultMapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable
            , Text, Text, Text> {

        Text k = new Text("finally");

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(k, value);
        }
    }

    public static class FinallyResultReducer extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text> {
        Text k = new Text();
        Text v = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder printStr = new StringBuilder();
            float totalPr = 0f;
            List<String> list = new ArrayList<String>();
            for (Text value : values) {
                String valueStr = value.toString();
                list.add(valueStr);

                String[] strArr = HadoopUtil.SPARATOR.split(valueStr);
                totalPr += Float.parseFloat(strArr[1]);

                printStr.append(",").append(valueStr);
            }

            for (String s : list) {
                String[] strArr = HadoopUtil.SPARATOR.split(s);
                k.set(strArr[0]);
                v.set(String.valueOf(Float.parseFloat(strArr[1]) / totalPr));
                context.write(k, v);
            }
        }
    }

    public static void run() throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();
        String inPath = HadoopUtil.HDFS + "/people_input/peoplerank.csv";
        String outPath = HadoopUtil.HDFS + "/people_output/finally-result";
        JobInitModel job = new JobInitModel(new String[]{inPath}, outPath, conf, null, "FinallyResult", FinallyResult.class
                , null, FinallyResultMapper.class, Text.class, Text.class, null, null
                , FinallyResultReducer.class, Text.class, Text.class);
        BaseDriver.initJob(new JobInitModel[]{job});
    }
}
