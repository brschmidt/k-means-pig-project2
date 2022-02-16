import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.*;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.Map;

public class OptimizedKMeans {

    // Threshold for determining convergence
    protected static final int CONVERGENCE_CRITERIA = 1000;

    // Calculates euclidean distance between two x,y points
    private static double distanceBetweenTwo(double x1, double y1, double x2, double y2) {
        double xDist = x2 - x1;
        double yDist = y2 - y1;
        return Math.sqrt(xDist * xDist + yDist * yDist);
    }

    // Returns true if avg distance between two sets of centroids is <=CONVERGENCE_CRITERIA, otherwise returns false
    private static boolean checkConvergence(int i, String filePath, String seedsFilePath) throws IOException {

        ArrayList<double[]> lastCentroidList = new ArrayList<>();
        ArrayList<double[]> currCentroidList = new ArrayList<>();

        double avgDiff;
        double diff = 0;

        if (i <= 1) {
            String currCentroids = filePath + (i) + "/part-r-00000";

            BufferedReader last = new BufferedReader(new FileReader(seedsFilePath));
            BufferedReader current = new BufferedReader(new FileReader(currCentroids));
            String line;

            while ((line = last.readLine()) != null) {
                String[] temp = line.split(",");
                double tempX = Double.parseDouble(temp[0]);
                String[] bool = temp[1].split("\t"); // separate boolean from y-value
                double tempY = Double.parseDouble(bool[0]);
                lastCentroidList.add(new double[]{tempX, tempY});
            }
            last.close();

            while ((line = current.readLine()) != null) {
                String[] temp = line.split(",");
                double tempX = Double.parseDouble(temp[0]);
                String[] bool = temp[1].split("\t"); // separate boolean from y-value
                double tempY = Double.parseDouble(bool[0]);
                currCentroidList.add(new double[]{tempX, tempY});
            }
            current.close();

            int size = currCentroidList.size();

            for (int j = 0; j < size; j++) {
                double[] tup1 = lastCentroidList.get(j);
                double[] tup2 = currCentroidList.get(j);
                diff += Math.abs(distanceBetweenTwo(tup1[0], tup1[1], tup2[0], tup2[1]));
            }
            avgDiff = diff / size;

            return avgDiff <=CONVERGENCE_CRITERIA;
        } else {
            String lastCentroids = filePath + (i - 1) + "/part-r-00000";
            String currCentroids = filePath + (i) + "/part-r-00000";

            BufferedReader last = new BufferedReader(new FileReader(lastCentroids));
            BufferedReader current = new BufferedReader(new FileReader(currCentroids));

            String line;
            while ((line = last.readLine()) != null) {
                String[] temp = line.split(",");
                double tempX = Double.parseDouble(temp[0]);
                String[] bool = temp[1].split("\t"); // separate boolean from y-value
                double tempY = Double.parseDouble(bool[0]);
                lastCentroidList.add(new double[]{tempX, tempY});
            }
            last.close();

            while ((line = current.readLine()) != null) {
                String[] temp = line.split(",");
                double tempX = Double.parseDouble(temp[0]);
                String[] bool = temp[1].split("\t"); // separate boolean from y-value
                double tempY = Double.parseDouble(bool[0]);
                currCentroidList.add(new double[]{tempX, tempY});
            }
            current.close();

            int size = lastCentroidList.size();

            for (int j = 0; j < size; j++) {
                double[] tup1 = lastCentroidList.get(j);
                double[] tup2 = currCentroidList.get(j);
                diff += Math.abs(distanceBetweenTwo(tup1[0], tup1[1], tup2[0], tup2[1]));
            }
            avgDiff = diff / size;

            return avgDiff <=CONVERGENCE_CRITERIA;
        }
    }

    // Mapper for data points to centroids
    public static class KMapper extends Mapper<Object, Text, Text, Text> {
        private final Text centroid = new Text();
        private final Text point = new Text();
        private ArrayList<double[]> centroids;
        private BufferedReader brReader;
        private TreeMap<Double, double[]> distMap;

        enum COUNTERS {
            FILE_EXISTS, FILE_NOT_FOUND, SOME_OTHER_ERROR
        }

        @Override
        protected void setup(Context context) throws IOException {
            centroids = new ArrayList<>();
            distMap = new TreeMap<>(); // Map of format <distance, tuple>

            Path[] filePath = context.getFileClassPaths();

            for (Path eachPath : filePath) {
                if (!eachPath.getName().isEmpty()) {
                    context.getCounter(COUNTERS.FILE_EXISTS).increment(1);
                }
                loadMap(eachPath.getName(), context);
            }
        }

        private void loadMap(String filePath, Context context) throws IOException {
            String lineIn;

            try {
                brReader = new BufferedReader(new FileReader(filePath));

                // Read each line, split and load to HashMap
                while ((lineIn = brReader.readLine()) != null) {

                    String[] line = lineIn.split(",");
                    String[] line2 = line[1].split("\t"); // separate boolean from y-value

                    double x = Double.parseDouble(line[0].trim());
                    double y = Double.parseDouble(line2[0].trim());

                    centroids.add(new double[]{x, y});
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
                throws IOException, InterruptedException {
            String[] lineIn = value.toString().split(",");

            double x1 = Double.parseDouble(lineIn[0]);
            double y1 = Double.parseDouble(lineIn[1]);

            double distance;

            for (double[] tuple : centroids) {
                distance = distanceBetweenTwo(x1, y1, tuple[0], tuple[1]);
                distMap.put(distance, tuple);
            }

            double shortest = 10000; // set to 10k to ensure reassignment

            for (Map.Entry<Double, double[]> entry : distMap.entrySet()) {
                if (entry.getKey() < shortest) shortest = entry.getKey();
            }

            String cent = distMap.get(shortest)[0] + "," + distMap.get(shortest)[1]; // create centroid string (comma separated)
            centroid.set(cent); // set centroid string to be written out

            String pnt = x1 + "," + y1; // create point string (comma separated)
            point.set(pnt); // set point string to be written out

            context.write(centroid, point);
            distMap.clear(); // clear out previous distance values to avoid storing 3,000,000 <K,V> pairs in map
        }
    }

    // Combiner for iterations of K Means
    public static class CentroidsCombiner extends Reducer<Text, Text, Text, Text> {
        private double xSum = 0;
        private double ySum = 0;
        private double count = 0;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for (Text point : values) {
                String[] pointIn = point.toString().split(",");

                double pointX = Double.parseDouble(pointIn[0]);
                double pointY = Double.parseDouble(pointIn[1]);

                xSum += pointX; // sum of X values
                ySum += pointY; // sum of Y values

                count++; // count of associated points
            }
            context.write(key, new Text(xSum+","+ySum+","+count));

        }
    }

    // Single Reducer for iteration of K Means
    public static class SingleReducer extends Reducer<Text, Text, Text, Text> {
        private final Text newCentroid = new Text();
        private final BooleanWritable converged = new BooleanWritable();
        private double xSum = 0;
        private double ySum = 0;
        private double count = 0;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            String[] keyIn = key.toString().split(",");
            double keyX, keyY;
            keyX = Double.parseDouble(keyIn[0]);
            keyY = Double.parseDouble(keyIn[1]);

            for (Text point : values) {
                String[] aggValues = point.toString().split(",");

                if(aggValues.length > 2) // If Combiner output
                {
                    xSum = Double.parseDouble(aggValues[0]);
                    ySum = Double.parseDouble(aggValues[1]);
                    count = Double.parseDouble(aggValues[2]);
                    break;
                }
                else // Else Mapper output
                {
                    double pointX = Double.parseDouble(aggValues[0]);
                    double pointY = Double.parseDouble(aggValues[1]);

                    xSum += pointX; // sum of X values
                    ySum += pointY; // sum of Y values

                    count++; // count of associated point
                }
            }

            double xAvg = xSum / count; // new centroid x value
            double yAvg = ySum / count; // new centroid y value

            converged.set(distanceBetweenTwo(keyX, keyY, xAvg, yAvg) <=CONVERGENCE_CRITERIA);

            String newCentroidStr = xAvg + "," + yAvg;
            newCentroid.set(newCentroidStr);

            context.write(newCentroid, new Text(converged.toString()));
            }
        }

//    // unused function
//    public static void writeCentroids(int iteration, String fileIn) throws IOException {
//        // args[2]+iteration+"/part-r-0000
//        File toRead = new File(fileIn + iteration + "/part-r-00000");
//        BufferedReader br = new BufferedReader(new FileReader(toRead));
//
//        String fileName = String.format("centroid-iteration%d.csv", iteration);
////        String fileName = "new-centroids.csv";
//        FileWriter myWriter = new FileWriter(fileName);
//
//        String line;
//
//        while ((line = br.readLine()) != null) {
//            myWriter.write(line + "\n");
//        }
//
//        br.close();
//        myWriter.close();
//    }

    public void debug(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        boolean done = false;
        boolean converged = false;

        if (otherArgs.length < 3) {
            System.err.println("Error: please provide 3 paths");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "K Means");
        job.setJarByClass(OptimizedKMeans.class);

        int R = Integer.parseInt(args[0]);

        for (int i = 0; i < R; i++) // loop through R times
        {
            done = false;
            job.setMapperClass(OptimizedKMeans.KMapper.class);

            job.setReducerClass(OptimizedKMeans.SingleReducer.class);
            job.setCombinerClass(OptimizedKMeans.CentroidsCombiner.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            if (i != 0) {
                job.addFileToClassPath(new Path(args[2] + (i) + "/part-r-00000"));
            } else
                job.addFileToClassPath(new Path(args[3]));

            NLineInputFormat.addInputPath(job, new Path(args[1]));
            FileOutputFormat.setOutputPath(job, new Path(args[2] + (i + 1)));

            while (!done)
                done = job.waitForCompletion(true);

            if (converged)
                break;

            // Configuration for next job
            conf = new Configuration();
            job = Job.getInstance(conf, "K Means Iterator" + i);

            // Must be later than first iteration to check convergence, otherwise FileNotFound Error occurs
            if (i > 0)
                converged = checkConvergence(i, args[2], args[3]);
        }
        System.exit(done ? 0 : 1);
    }

    public static void main(String[] args) {

        //TODO
        // Else just return new centroids - (diff reducer for final iterations/ convergence?)
        // args[0] = R (iterations)
        // args[1] = input filepath for first iteration
        // args[2] = output path for first iteration/ input path for subsequent iterations
        // args[3] = path to seed-points data file

        // ...
    }
}
