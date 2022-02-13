import org.junit.Test;

import java.io.IOException;

public class KMMapReduceTest {

    @Test
    public void debug() {
        String[] input = new String[4];

        /* TODO change to local file paths in your own system */
        // R argument
        input[0] = String.valueOf(5);
        // input data for 1st run
         input[1] = "file:///Users/bailey/DS4433_home/IdeaProjects/k-means-pig-project2/data-points.csv";
        // output location for 1st run
         input[2] = "/Users/bailey/DS4433_home/IdeaProjects/k-means-pig-project2/output/iteration";
         // K seeds input
         input[3] = "file:///Users/bailey/DS4433_home/IdeaProjects/k-means-pig-project2/seed-points.csv";

        KMMapReduce test = new KMMapReduce();
        try {
            test.debug(input);
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}