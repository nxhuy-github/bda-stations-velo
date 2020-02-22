package operator;

import org.apache.log4j.Logger;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import topology.NoBicycleTopology;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;

public class Computation1 {
    private static final Logger LOG = Logger.getLogger(Computation1.class);

    public ArrayList<String> FivePlaceLast3Hours(TupleWindow tupleWindow) {

        // ici en remplis une map par cle(c'est la station) pour eviter les parcours a chaque fois
        ArrayList<String> stations_inf_5 = new ArrayList<>();
        HashMap<Integer, ArrayList<Integer>> carnet = new HashMap<>();

        int station;

        for (Tuple t : tupleWindow.get()) {
            station = t.getIntegerByField("number");
            if(carnet.containsKey(station)){
                carnet.get(station).add(t.getIntegerByField("available_bikes"));
            }else{
                carnet.put(station,new ArrayList<Integer>());
                carnet.get(station).add(t.getIntegerByField("available_bikes"));
            }
        }

        for(Integer cle : carnet.keySet()){
            Boolean truth = Boolean.TRUE;
            for (Integer ab:carnet.get(cle)){
                if(ab > 5){
                    truth = Boolean.FALSE;
                    break;
                }
            }
            if(truth){
                stations_inf_5.add(cle.toString());
            }
        }
        return  stations_inf_5;
    }
}