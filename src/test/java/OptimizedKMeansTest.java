import org.junit.Test;

import java.io.IOException;

public class OptimizedKMeansTest {

    @Test
    public void debug() {
        String[] input = new String[5];

        /* TODO change to local file paths in your own system */

        // R argument
        input[0] = String.valueOf(3);
        // input data for 1st run
        input[1] = "/Users/bailey/DS4433_home/IdeaProjects/k-means-pig-project2/data-clusters.csv";
        // output location for 1st run
        input[2] = "/Users/bailey/DS4433_home/IdeaProjects/k-means-pig-project2/output/optimized-test/iteration";
        // K seeds input
        input[3] = "/Users/bailey/DS4433_home/IdeaProjects/k-means-pig-project2/seed-points.csv";

        // 0: return only cluster centers along with an indication if convergence has been reached;
        // 1: return the final clustered data points along with their cluster centers.
        input[4] = String.valueOf(0);
//        input[4] = String.valueOf(1);

        OptimizedKMeans test = new OptimizedKMeans();
        try {
            test.debug(input);
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}