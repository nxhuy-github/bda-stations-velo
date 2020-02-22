package topology;

import bolt.ExitFivePlace3LastHoursBolt;
import bolt.FivePlaceLast3HoursBolt;
import bolt.IntermediaryFilter;
import com.google.common.io.Resources;
import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.*;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class FivePlaceLast3HoursTopo {
    private static final Logger LOG = Logger.getLogger(NoBicycleTopology.class);

    public static void main(String[] args) throws IOException, InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        // si le nombre de parametre est inferieur a 4 on doit sortir avec status 1
        // entrez avec une ligne de commande spout configuration as (zookeeper host,topic,brokers,
        // la commande a utiliser est java -jar projet-bda... localhost:2181 topic0 /brokers storm-consumer
        int nbExecutors = 1;
        //final BrokerHosts zkrHosts = new ZkHosts("cl-ter-manager-jt3m3gbgctz6-qdrjqxrsgate-nlx635i3m5wq:2181");
        final String kafkaTopic = "group-six-topic-reader";
        final String broker = "cluster-node-cztqntk4f5x6-clogovtafhcq-ikmht4fosq6e.novalocal:9092";
        //final String zkRoot = "/cluster-node-cztqntk4f5x6-clogovtafhcq-ikmht4fosq6e.novalocal:9092";
        //final String clientId = "storm-consumer";;
        //Builder<String, String> builder = KafkaSpoutConfig.builder(broker,kafkaTopic);
        KafkaSpoutConfig kafkaSpoutConf = KafkaSpoutConfig.builder(broker,kafkaTopic).setGroupId("group-six").build();

        org.apache.storm.kafka.spout.KafkaSpout kafkaConf = new org.apache.storm.kafka.spout.KafkaSpout<>(kafkaSpoutConf);
        // Create topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-spout", kafkaConf);
        builder.setBolt("IntermediaryFilter", new IntermediaryFilter(),nbExecutors).globalGrouping("kafka-spout");
        builder.setBolt(
                "FivePlaLast3H",
                new FivePlaceLast3HoursBolt()
                        .withTumblingWindow(new BaseWindowedBolt.Duration(10, TimeUnit.SECONDS))
                        .withMessageIdField("idMsg"), nbExecutors
        ).globalGrouping("IntermediaryFilter");


        builder.setBolt("ExitToKafka",new ExitFivePlace3LastHoursBolt(),nbExecutors).globalGrouping("FivePlaLast3H");




        // ici c'est les properties de producer storm
        InputStream props = Resources.getResource("producer.props").openStream();
        Properties properties = new Properties();
        properties.load(props);
        // kafka bolt
        KafkaBolt less5 = new KafkaBolt()
                .withTopicSelector(new DefaultTopicSelector("AlertTopicGroupSix"))
                .withProducerProperties(properties)
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper("id", "alerte"));
        builder.setBolt("less5", less5 , nbExecutors).globalGrouping("ExitToKafka");


        // submit the topolgy to local cluster
        Config config = new Config();
        config.setMessageTimeoutSecs(40);

        // En local
        //final LocalCluster localCluster = new LocalCluster();
        //localCluster.submitTopology("FivePlcaLas3Hours", config, builder.createTopology());

        // In the VM
        StormSubmitter.submitTopology("NoBicycleTopology", config, builder.createTopology());
    }
}
