import org.junit.Test;

import java.io.IOException;

public class ConvergenceKMeansTest {

    @Test
    public void debug() {
        String[] input = new String[4];

        /* TODO change to local file paths in your own system */

        // R argument
        input[0] = String.valueOf(20);
        // input data for 1st run
        input[1] = "/Users/bailey/DS4433_home/IdeaProjects/k-means-pig-project2/data-points.csv";
        // output location for 1st run
        input[2] = "/Users/bailey/DS4433_home/IdeaProjects/k-means-pig-project2/output/convergence-test/iteration";
        // K seeds input
        input[3] = "/Users/bailey/DS4433_home/IdeaProjects/k-means-pig-project2/seed-points.csv";

        ConvergenceKMeans test = new ConvergenceKMeans();
        try {
            test.debug(input);
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}