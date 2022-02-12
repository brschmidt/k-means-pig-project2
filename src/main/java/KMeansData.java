import java.io.FileWriter;   // Import the FileWriter class
import java.io.IOException;  // Import the IOException class to handle errors

public class KMeansData {

    private static int generateInt()
    {
        return (int) (10000 * Math.random());
    }

    protected static void writeData() throws IOException
    {
        FileWriter myWriter = new FileWriter("data-points.csv");

        for (int i = 0; i < 1000000; i++) // 1,000,000 data points
        {
            int x = generateInt();
            int y = generateInt();

            String tupleString = String.format("%d,%d"+"\n", x, y);
            myWriter.write(tupleString);

        }
        myWriter.close();
        System.out.println("Successfully wrote to the file: " + System.getProperty("user.dir") +"/data-points.csv");
    }

    public static void writeSeeds(int K) throws IOException
    {
        FileWriter myWriter = new FileWriter("seed-points.csv");

        for (int i =0; i < K; i++)
        {
            int x = generateInt();
            int y = generateInt();

            String tupleString = String.format("%d,%d"+"\n", x, y);
            myWriter.write(tupleString);
        }
        myWriter.close();
        System.out.println("Successfully wrote to the file: " + System.getProperty("user.dir") +"/seed-points.csv");
    }

    public static void main(String[] args)
    {
        // Write data points into local csv
        try {
            writeData();
        } catch (IOException e) {
            System.out.println("An Error Occurred.");
            e.printStackTrace();
        }
        // Write seed points into local csv
        try {
            writeSeeds(3);
        } catch (IOException e) {
            System.out.println("An Error Occurred.");
            e.printStackTrace();
        }
    }
}


