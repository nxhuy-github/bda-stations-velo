package operator;

import org.apache.log4j.Logger;
import org.apache.storm.tuple.Tuple;
import topology.NoBicycleTopology;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonReader;
import java.io.StringReader;

public class Computation {
    private String data;
    private static final Logger LOG = Logger.getLogger(Computation.class);

    public Computation(String data) {
        this.data = data;
    }

    public String getInfos() {
        JsonReader json = Json.createReader(new StringReader(data));
        JsonObject readingObj = json.readObject();
        JsonObjectBuilder js = Json.createObjectBuilder();
        js.add("number",readingObj.getInt("number"));
        js.add("bike_stands",readingObj.getInt("bike_stands"));
        js.add("available_bike_stands",readingObj.getInt("available_bike_stands"));
        js.add("available_bikes",readingObj.getInt("available_bikes"));
        js.add("status",readingObj.getString("status"));
        json.close();
        return js.build().toString();
    }

    // c'set la fonction qui lance une alerte dans le cas d'une station sans velo
    public static String noAvailableBicycleAlert(Tuple tuple){
        //LOG.info(tuple.getString(4));
        String alert = null;
        String entree = tuple.getString(4);
        JsonReader json = Json.createReader(new StringReader(entree));
        JsonObject readingObj = json.readObject();
        JsonObjectBuilder js = Json.createObjectBuilder();
        if(readingObj.getInt("available_bikes") == 0) {
            js.add("id",readingObj.getInt("number"));
            js.add("status","No available bicycle");
            alert = js.build().toString();
        }
        json.close();
        return alert;
    }

    public static String noAvailableBikeStandAlert(Tuple tuple) {
        String alert = null;
        String entree = tuple.getString(4);
        JsonReader json = Json.createReader(new StringReader(entree));
        JsonObject readingObj = json.readObject();
        JsonObjectBuilder js = Json.createObjectBuilder();
        if(readingObj.getInt("available_bike_stands") == 0) {
            js.add("id",readingObj.getInt("number"));
            js.add("status","No available bike stand");
            alert = js.build().toString();
        }
        json.close();
        return alert;
    }
}
