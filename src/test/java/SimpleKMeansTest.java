import org.junit.Test;

import java.io.IOException;

public class SimpleKMeansTest {

    @Test
    public void debugOne() {
        String[] input = new String[4];

        /* TODO change to local file paths in your own system */

        // R argument
        input[0] = String.valueOf(1);
        // input data for 1st run
        input[1] = "/Users/bailey/DS4433_home/IdeaProjects/k-means-pig-project2/data-clusters.csv";
        // output location for 1st run
        input[2] = "/Users/bailey/DS4433_home/IdeaProjects/k-means-pig-project2/output/simple-test1/iteration";
        // K seeds input
        input[3] = "/Users/bailey/DS4433_home/IdeaProjects/k-means-pig-project2/seed-points.csv";

        SimpleKMeans test = new SimpleKMeans();

        // 1 iteration
        try {
            test.debug(input);
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

    @ Test
    public void debugTen(){
        String[] input = new String[4];

        /* TODO change to local file paths in your own system */

        // R argument
        input[0] = String.valueOf(10);
        // input data for 1st run
        input[1] = "/Users/bailey/DS4433_home/IdeaProjects/k-means-pig-project2/data-clusters.csv";
        // output location for 1st run
        input[2] = "/Users/bailey/DS4433_home/IdeaProjects/k-means-pig-project2/output/simple-test2/iteration";
        // K seeds input
        input[3] = "/Users/bailey/DS4433_home/IdeaProjects/k-means-pig-project2/seed-points.csv";

        SimpleKMeans test = new SimpleKMeans();

        // 10 iterations
        try {
            test.debug(input);
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}