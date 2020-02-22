package topology;
import bolt.ExitNoBicycleBolt;
import bolt.NoBicycleBolt;
import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
;import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;

;import java.util.HashMap;

import static org.apache.storm.kafka.spout.KafkaSpoutConfig.*;

public class NoBicycleTopology {
    private static final Logger LOG = Logger.getLogger(NoBicycleTopology.class);
    // arg 1: <zk-hosts> ip:port
    // arg 2: <kafka-topic> topic
    // arg 3: <zk-path> /brokers
    // arg 4: <clientid> storm-consumer

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {

      /*  if(args.length < 4) {
            LOG.fatal("Incorrect number of arguments. Required arguments: <zk-hosts> <kafka-topic> <zk-path> <clientid>");
            System.exit(1);
        }
        */
        int nbExecutors = 1;
        //final BrokerHosts zkrHosts = new ZkHosts("cl-ter-manager-jt3m3gbgctz6-qdrjqxrsgate-nlx635i3m5wq:2181");
         final String kafkaTopic = "group-six-topic-reader";
         final String broker = "cluster-node-cztqntk4f5x6-clogovtafhcq-ikmht4fosq6e.novalocal:9092";
        //final String zkRoot = "/cluster-node-cztqntk4f5x6-clogovtafhcq-ikmht4fosq6e.novalocal:9092";
        //final String clientId = "storm-consumer";;
        //Builder<String, String> builder = KafkaSpoutConfig.builder(broker,kafkaTopic);
        KafkaSpoutConfig kafkaSpoutConf = KafkaSpoutConfig.builder(broker,kafkaTopic).setGroupId("group-six").build();

        KafkaSpout kafkaConf = new KafkaSpout<>(kafkaSpoutConf);
        //kafkaSpoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());

        // Create topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-spout", kafkaConf);
        builder.setBolt("NoBicycleBolt", new NoBicycleBolt(), nbExecutors).shuffleGrouping("kafka-spout");
        builder.setBolt("ExitNoBicycleBolt", new ExitNoBicycleBolt(), nbExecutors).shuffleGrouping("NoBicycleBolt");

        // Submit the topolgy to local cluster
        // En local
        //final LocalCluster localCluster = new LocalCluster();
        //localCluster.submitTopology("NoBicycleTopology",new HashMap() , builder.createTopology());


        // In the VM
        Config config = new Config();
        config.setDebug(true);
        //config.put(config.TOPOLOGY_MAX_SPOUT_PENDING,1);
        //config.put(Config.NIMBUS_HOST,"192.168.76.144");
        //config.put(Config.NIMBUS_THRIFT_PORT,6627);
        StormSubmitter.submitTopology("NoBicycleTopology", config, builder.createTopology());
    }

}

