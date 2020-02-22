import operator.Computation;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.log4j.Logger;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

public class ConsumeMessageBolt extends BaseBasicBolt {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = Logger.getLogger(ConsumeMessageBolt.class);

    @Override
    // ici il faut faire les traitement de tous
    public void execute(Tuple input, BasicOutputCollector collector) {
        Computation cmp = new Computation(input.getString(0));
        LOG.info(cmp.getInfos());

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("message"));
    }
}