package bolt;
import java.util.Map;
import java.util.Random;
import org.apache.log4j.Logger;
import operator.Computation1;
import org.apache.storm.state.KeyValueState;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseStatefulWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

public class FivePlaceLast3HoursBolt extends BaseStatefulWindowedBolt<KeyValueState<String, Integer>> {
    private static final long serialVersionUID = 4262379330788107343L;
    private static final Logger LOG = Logger.getLogger(FivePlaceLast3HoursBolt.class);
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void initState(KeyValueState<String, Integer> stringIntegerKeyValueState) {
    }

    @Override
    public void execute(TupleWindow tupleWindow) {
        Computation1 cmp1 = new Computation1();
        Values val = new Values();
        Long idMsg = new Random().nextLong();
        val.add(cmp1.FivePlaceLast3Hours(tupleWindow));
        val.add(idMsg);
        collector.emit(val);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields( "object","idMsg"));
    }

}
