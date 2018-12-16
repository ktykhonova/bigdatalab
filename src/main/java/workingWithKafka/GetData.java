package workingWithKafka;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class GetData {

    public static ArrayList<String> getDataFromFiles() throws IOException {

        ArrayList<String> userVisits = new ArrayList<String>();
        for (int i = 0; i < 10; i++) {
            File file = new File("C:\\Users\\Lenovo Flex\\Desktop\\BigDataLab\\part-0000" + i);
            BufferedReader br = new BufferedReader(new FileReader(file));

            String st;
            while ((st = br.readLine()) != null)
                userVisits.add(st);
        }

        return userVisits;
    }
}
