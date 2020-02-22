package bolt;

import com.google.common.io.Resources;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ExitNoBicycleBolt extends KafkaBolt {
    public ExitNoBicycleBolt() {
        super();

        try {
            InputStream props = Resources.getResource("producer.props").openStream();
            Properties properties = new Properties();
            properties.load(props);
            this.withTopicSelector(new DefaultTopicSelector("AlertTopicGroupSix"))
                    .withProducerProperties(properties)
                    .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper("key", "alert"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}