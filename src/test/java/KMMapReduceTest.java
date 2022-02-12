import org.junit.Test;

import java.io.IOException;

public class KMMapReduceTest {

    @Test
    public void debug() {
        String[] input = new String[3];

        /* TODO change to local file paths in your own system */
        // input data for 1st run
         input[0] = "file:///Users/bailey/DS4433_home/IdeaProjects/k-means-pig-project2/data-points.csv";
        // output location for 1st run
         input[1] = "file:///Users/bailey/DS4433_home/IdeaProjects/k-means-pig-project2/output/KMM_out";
         // K seeds input
         input[2] = "file:///Users/bailey/DS4433_home/IdeaProjects/k-means-pig-project2/seed-points.csv";

        KMMapReduce test = new KMMapReduce();
        try {
            test.debug(input);
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}