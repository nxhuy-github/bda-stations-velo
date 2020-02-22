package bolt;
import java.util.ArrayList;
import java.util.Map;
import java.util.Random;

import operator.Computation1;
import org.apache.log4j.Logger;
import org.apache.storm.state.KeyValueState;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseStatefulWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import javax.json.Json;
import javax.json.JsonObjectBuilder;

/**
 *
 * @author abdenacer
 */
public class ExitFivePlace3LastHoursBolt implements IRichBolt {
    private static final Logger LOG = Logger.getLogger(FivePlaceLast3HoursBolt.class);

    private static final long serialVersionUID = 1L;
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void execute(Tuple tuple) {
        ArrayList<String> res = (ArrayList<String>) tuple.getValueByField("object");
        JsonObjectBuilder builder;
        for (String s:res) {
            builder = Json.createObjectBuilder();
            builder.add("id", s).add("status", "Less than 5 bicycles in the last 3 hours");
            collector.emit(tuple, new Values(builder.build().toString()));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("alerte"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}



