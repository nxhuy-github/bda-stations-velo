import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.io.Resources;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.storm.shade.org.apache.http.HttpHost;
import org.apache.storm.shade.org.apache.http.HttpResponse;
import org.apache.storm.shade.org.apache.http.client.HttpClient;
import org.apache.storm.shade.org.apache.http.client.methods.HttpGet;
import org.apache.storm.shade.org.apache.http.conn.params.ConnRoutePNames;
import org.apache.storm.shade.org.apache.http.impl.client.DefaultHttpClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * ce producteur va envoyer les message sur topic 0 vers un consommateur storm
 */
public class Producer {

    private static final String VELOV_TABLE = "velov";
    private static final String DATA_VELOV_API_TABLE = "data_velov_api";
    private static final String key =  "d525e43ff58fff3ec1dd3c1b6a61f738fecce5c8";
    private static final String[] API_DATA_KEYS = {"number","available_bike_stands","available_bikes","last_update"};
    private static final int DELAY = 60 * 10 * 1000; // Seconds X Minutes X Milliseconds

    public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {
        // set up the producer
        KafkaProducer<String, String> producer;
        try (InputStream props = Resources.getResource("producer.props").openStream()) {
            Properties properties = new Properties();
            properties.load(props);
            producer = new KafkaProducer<>(properties);
        }

        // init Hive
        IOHiveStockage hiveStockage = new IOHiveStockage();
        hiveStockage.initConnection();

        if (!hiveStockage.isExist(VELOV_TABLE)){
            // get Data from Csv
            List<List<Object>>  csv = CSVReader.readCSV(args[1]);
            // Creating Table
            hiveStockage.createTable(VELOV_TABLE,CSVReader.getColumns());
            // Insert data into table
            hiveStockage.insertTable(VELOV_TABLE,csv);
        }

        try {


            // Creating Table for data collected from the API
            hiveStockage.createTable(DATA_VELOV_API_TABLE, Arrays.asList(API_DATA_KEYS));

            HttpClient client = new DefaultHttpClient();
            HttpGet request;
            HttpResponse response;
            HttpHost proxy_http = new HttpHost("proxy.univ-lyon1.fr", 3128, "http");
            HttpHost proxy_https = new HttpHost("proxy.univ-lyon1.fr", 3128, "http");

            try {
                while (true){

                    request = new HttpGet("https://api.jcdecaux.com/vls/v1/stations?contract=lyon&apiKey="+key);
                    client.getParams().setParameter(ConnRoutePNames.DEFAULT_PROXY,proxy_http);
                    client.getParams().setParameter(ConnRoutePNames.DEFAULT_PROXY,proxy_https);
                    response = client.execute(request);

                    // ici c'est la stockage de la reponse http dans un buffer
                    BufferedReader br;

                    br = new BufferedReader(new InputStreamReader(response
                            .getEntity().getContent()));

                    String line = br.readLine();

                    ObjectMapper mapper = new ObjectMapper();
                    JsonNode actualObj = mapper.readTree(line);
                    List<List<Object>> rows = new ArrayList<>();
                    List<Object> row;
                    JsonNode station;
                    for ( int i=0;i< actualObj.size();++i) {
                        row = new ArrayList<>();
                        station = actualObj.get(i);

                        for (String key : API_DATA_KEYS){
                            row.add(station.get(key));
                        }

                        System.out.println(station.toString());

                        rows.add(row);
                        producer.send(new ProducerRecord<String, String>("group-six-topic-reader",station.toString()));
                        producer.flush();
                    }
                    // Insert data into table
                    hiveStockage.insertTable(DATA_VELOV_API_TABLE,rows);
                    Thread.sleep(DELAY);
                }

            } catch (IOException | UnsupportedOperationException e) {
                e.printStackTrace();
            }

        } catch (Throwable throwable) {
            System.out.printf("%s", throwable.getStackTrace());
        } finally {
            hiveStockage.close();
            producer.close();
        }

    }
}
