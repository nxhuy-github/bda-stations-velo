import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class CSVReader {

    public static List<List<Object>> readCSV(String pathToFile) {

        BufferedReader br = null;
        List<List<Object>> values = new ArrayList<>();
        String line = "";
        String fileContent = "";

        try {

            br = new BufferedReader(new FileReader(pathToFile));
            while ((line = br.readLine()) != null) {
                String[] vals = line.split(";");
                values.add(Arrays.<Object>asList(vals));
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }


        return values;
    }

    public static List<String> getColumns() {
        List<String> columns = new ArrayList<String>();
        columns.add("id");
        columns.add("time_stamp");
        columns.add("hour");
        columns.add("dayofweek");
        columns.add("availablebikestands");
        columns.add("availablebikes");

        return columns;
    }

}
