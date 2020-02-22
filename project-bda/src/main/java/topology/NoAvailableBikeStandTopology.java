package topology;

import bolt.ExitNoAvailableBikeStandBolt;
import bolt.NoAvailableBikeStandBolt;
import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.*;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

import java.util.HashMap;

public class NoAvailableBikeStandTopology {
    private static final Logger LOG = Logger.getLogger(NoBicycleTopology.class);

    // arg 1: <zk-hosts> ip:port
    // arg 2: <kafka-topic> topic
    // arg 3: <zk-path> /brokers
    // arg 4: <clientid> storm-consumer

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {


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
        builder.setBolt("NoAvailableBikeStandBolt", new NoAvailableBikeStandBolt(), nbExecutors).globalGrouping("kafka-spout");
        builder.setBolt("ExitNoAvailableBikeStandBolt", new ExitNoAvailableBikeStandBolt(), nbExecutors).globalGrouping("NoAvailableBikeStandBolt");

        // submit the topolgy to local cluster
        // En local
        /*final LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("NoAvailableBikeStandTopology",new HashMap() , builder.createTopology());*/


        // In the VM
        Config config = new Config();
        StormSubmitter.submitTopology("NoAvailableBikeStandTopology", config, builder.createTopology());

    }
}



