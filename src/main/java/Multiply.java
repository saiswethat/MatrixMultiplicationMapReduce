import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

public class Multiply {

    public static class Elem implements Writable {
        private short tag;
        private int index;
        private double value;

        public Elem() {
        }

        public Elem(short tag, int index, double value) {
            this.tag = tag;
            this.index = index;
            this.value = value;
        }

        public void set(short tag, int index, double value) {
            this.tag = tag;
            this.index = index;
            this.value = value;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeShort(tag);
            out.writeInt(index);
            out.writeDouble(value);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            tag = in.readShort();
            index = in.readInt();
            value = in.readDouble();
        }
    }

    public static class Pair implements WritableComparable<Pair> {
        private int i;
        private int j;

        public Pair() {
        }

        public Pair(int i, int j) {
            this.i = i;
            this.j = j;
        }

        public void set(int i, int j) {
            this.i = i;
            this.j = j;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(i);
            out.writeInt(j);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            i = in.readInt();
            j = in.readInt();
        }

        @Override
        public int compareTo(Pair o) {
            int cmp = Integer.compare(i, o.i);
            if (cmp != 0) {
                return cmp;
            }
            return Integer.compare(j, o.j);
        }

        @Override
        public String toString() {
            return i+","+j+",";
        }
    }

    public static class FirstMatrixMapper extends Mapper<LongWritable, Text, IntWritable, Elem> {
        private final IntWritable key = new IntWritable();
        private final Elem value = new Elem();

        @Override
        public void map(LongWritable offset, Text line, Context context)
                throws IOException, InterruptedException {
            String[] tokens = line.toString().split(",");
            int i = Integer.parseInt(tokens[0]);
            int j = Integer.parseInt(tokens[1]);
            double v = Double.parseDouble(tokens[2]);

            key.set(j);
            value.set((short) 0, i, v);

            context.write(key, value);
        }
    }

    public static class SecondMatrixMapper extends Mapper<LongWritable, Text, IntWritable, Elem> {
        private final IntWritable key = new IntWritable();
        private final Elem value = new Elem();

        @Override
        protected void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
            String[] tokens = line.toString().split(",");
            int i = Integer.parseInt(tokens[0]);
            int j = Integer.parseInt(tokens[1]);
            double v = Double.parseDouble(tokens[2]);

            key.set(i);
            value.set((short) 1, j, v);

            context.write(key, value);
        }
    }

    public static class MultiplyReducer extends Reducer<IntWritable, Elem, Pair, DoubleWritable> {
        private final Pair pair = new Pair();
        private final DoubleWritable multiplicationResult = new DoubleWritable();

        @Override
        public void reduce(IntWritable key, Iterable<Elem> values, Context context)
                throws IOException, InterruptedException {

            List<Elem> matrixM = new ArrayList<>();
            List<Elem> matrixN = new ArrayList<>();

            for (Elem val : values) {
                if (val.tag == 0) {
                    matrixM.add(new Elem(val.tag, val.index, val.value));
                } else {

                    matrixN.add(new Elem(val.tag, val.index, val.value));

                }
            }

            for (Elem mValue : matrixM) {
                for (Elem nValue : matrixN) {
                    pair.set(mValue.index, nValue.index);
                    multiplicationResult.set(mValue.value * nValue.value);
		    context.write(pair, multiplicationResult);
                }
            }
        }
    }

    public static class AdditionMapper extends Mapper<Text, Text, Pair, DoubleWritable> {
        @Override
        public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            Pair pair = new Pair();
            String[] index = key.toString().split(",");
            pair.set(Integer.parseInt(index[0]), Integer.parseInt(index[1]));
            double val = Double.parseDouble(value.toString());
            context.write(pair, new DoubleWritable(val));
        }
    }

    public static class AdditionReducer extends Reducer<Pair, DoubleWritable, Pair, DoubleWritable> {
        private final DoubleWritable additionResult = new DoubleWritable();

        @Override
        public void reduce(Pair key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0.0;
            for (DoubleWritable value : values) {
                sum = sum + value.get();
            }
            additionResult.set(sum);
            context.write(key, additionResult);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        Job multiplicationJob = Job.getInstance(config, "First Map Reduce Job: Multiplication");
        multiplicationJob.setJarByClass(Multiply.class);
        multiplicationJob.setReducerClass(MultiplyReducer.class);
        multiplicationJob.setOutputKeyClass(Pair.class);
        multiplicationJob.setOutputValueClass(DoubleWritable.class);
        multiplicationJob.setMapOutputKeyClass(IntWritable.class);
        multiplicationJob.setMapOutputValueClass(Elem.class);

        MultipleInputs.addInputPath(multiplicationJob, new Path(args[0]), TextInputFormat.class,
                FirstMatrixMapper.class);
        MultipleInputs.addInputPath(multiplicationJob, new Path(args[1]), TextInputFormat.class,
                SecondMatrixMapper.class);

        FileOutputFormat.setOutputPath(multiplicationJob, new Path(args[2]));

        if (!multiplicationJob.waitForCompletion(true)) {
            System.exit(1);
        }

        Job additionJob = Job.getInstance(config, "Second Map Reduce Job: Addition");
        additionJob.setJarByClass(Multiply.class);
        additionJob.setReducerClass(AdditionReducer.class);
        additionJob.setOutputKeyClass(Pair.class);
        additionJob.setOutputValueClass(DoubleWritable.class);
        additionJob.setMapperClass(AdditionMapper.class);
        additionJob.setMapOutputKeyClass(Pair.class);
        additionJob.setMapOutputValueClass(DoubleWritable.class);
        additionJob.setInputFormatClass(KeyValueTextInputFormat.class);

        FileInputFormat.addInputPath(additionJob, new Path(args[2]));
        FileOutputFormat.setOutputPath(additionJob, new Path(args[3]));

        if (!additionJob.waitForCompletion(true)) {
            System.exit(1);
        }

        System.exit(0);
    }
}
