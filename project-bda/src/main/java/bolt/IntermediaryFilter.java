package bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import java.io.StringReader;
import java.util.Map;
import java.util.Random;

public class IntermediaryFilter implements IRichBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        JsonReader json = Json.createReader(new StringReader(tuple.getString(4)));
        JsonObject readingObj = json.readObject();
        Values values = new Values();
        values.add(readingObj.getInt("number"));
        values.add(readingObj.getInt("bike_stands"));
        values.add(readingObj.getInt("available_bike_stands"));
        values.add(readingObj.getInt("available_bikes"));
        Long idMsg = new Random().nextLong();
        values.add(idMsg);
        collector.emit(tuple, values);
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("number", "bike_stands", "available_bike_stands", "available_bikes", "idMsg"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}

