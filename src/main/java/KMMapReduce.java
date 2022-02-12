import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.Map;

public class KMMapReduce {

    private static double distanceBetweenTwo(double x1, double y1, double x2, double y2)
    {
        double xDist = x2 - x1;
        double yDist = y2 - y1;
        return Math.sqrt(xDist*xDist + yDist*yDist);
    }

    public static class KMapper extends Mapper<Object, Text, Text, Text>
    {
        private final Text centroid = new Text();
        private final Text point = new Text();
        private ArrayList<double[]> seeds;
        private BufferedReader brReader;
        private TreeMap<Double, double[]> distMap;

        enum COUNTERS {
            FILE_EXISTS, FILE_NOT_FOUND, SOME_OTHER_ERROR
        }

        @Override
        protected void setup(Context context) throws IOException
        {
            seeds = new ArrayList<>();
            distMap = new TreeMap<>(); // Map of format <distance, tuple>

            Path[] filePath = context.getFileClassPaths();

            for (Path eachPath : filePath) {
                if (eachPath.getName().trim().equals("seed-points.csv")) {
                    context.getCounter(COUNTERS.FILE_EXISTS).increment(1);
                }
                loadMap(eachPath.getName(), context);
            }
        }

        private void loadMap(String filePath, Context context) throws IOException
        {
            String lineIn;

            try {
                brReader = new BufferedReader(new FileReader(filePath));

                // Read each line, split and load to HashMap
                while ((lineIn = brReader.readLine()) != null) {

                    String[] line = lineIn.split(",");

                    double x = Double.parseDouble(line[0].trim());
                    double y = Double.parseDouble(line[1].trim());

                    seeds.add(new double[]{x,y});
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                context.getCounter(COUNTERS.FILE_NOT_FOUND).increment(1);
            } catch (IOException e) {
                context.getCounter(COUNTERS.SOME_OTHER_ERROR).increment(1);
                e.printStackTrace();
            } finally {
                if (brReader != null) {
                    brReader.close();
                }
            }
        }

        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException
        {
            String[] lineIn = value.toString().split(",");

            double x1 = Double.parseDouble(lineIn[0]);
            double y1 = Double.parseDouble(lineIn[1]);

            double distance;

            for (double[] tuple : seeds)
            {
                distance = distanceBetweenTwo(x1, y1, tuple[0], tuple[1]);
                distMap.put(distance, tuple);
            }

            double shortest = 10000; // set to 10k to ensure reassignment

            for (Map.Entry<Double, double[]> entry : distMap.entrySet())
            {
                if(entry.getKey() < shortest) shortest = entry.getKey();
            }

            String cent = distMap.get(shortest)[0]+","+distMap.get(shortest)[1]; // create centroid string (comma separated)
            centroid.set(cent); // set centroid string to be written out

            String pnt = x1+","+y1; // create point string (comma separated)
            point.set(pnt); // set point string to be written out

            context.write(centroid, point);
            distMap.clear(); // clear out previous distance values to avoid storing 3,000,000 <K,V> pairs in map
        }
    }

    public static class KReducer extends Reducer<Text, Text, Text, Text>
    {
        private final Text newCentroid = new Text();
        private double xSum = 0;
        private double ySum = 0;
        private double count = 0;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for(Text point: values)
            {
                String[] pointIn = point.toString().split(",");

                double pointX = Double.parseDouble(pointIn[0]);
                double pointY = Double.parseDouble(pointIn[1]);

                xSum += pointX; // sum of X values
                ySum += pointY; // sum of Y values

                count++; // count of associated points
            }
            double xAvg = xSum / count; // new centroid x value
            double yAvg = ySum / count; // new centroid y value

            String newCentroidStr = xAvg+","+yAvg;
            newCentroid.set(newCentroidStr);

            //TODO
            // If final iteration or convergence has been reached, then return with associated point
            // Else just return new centroids
            context.write(newCentroid, new Text(""));
        }
    }
    public void debug(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        /* JOB 1 */
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length < 3) {
            System.err.println("Error: please provide 2 paths");
            System.exit(2);
        }

        Job job1 = Job.getInstance(conf, "K Means");
        job1.setJarByClass(KMMapReduce.class);

        job1.setMapperClass(KMMapReduce.KMapper.class);
        job1.setReducerClass(KMMapReduce.KReducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        job1.addFileToClassPath(new Path(args[2]));

        NLineInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));


        System.exit(job1.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) {
        // ...
    }
}
