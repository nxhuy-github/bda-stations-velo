
import java.util.HashMap;

import org.apache.log4j.Logger;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

/**
 * @author Abdenacer KERAGHEL
 */
public class kafkaStormInteg{

    private static final Logger LOG = Logger.getLogger(kafkaStormInteg.class);

    public static void main(String[] args) {
        // si le nombre de parametre est inferieur a 4 on doit sortir avec status 1
        if(args.length < 4) {
            LOG.fatal("Nombre d'arguments incorrecte. args recommandés arguments: <zk-hosts> <kafka-topic> <zk-path> <clientid>");
            System.exit(1);
        }

        // entrez avec une ligne de commande spout configuration as (zookeeper host,topic,brokers,
        // la commande a utiliser est java -jar projet-bda... localhost:2181 topic0 /brokers storm-consumer
        final BrokerHosts zkrHosts = new ZkHosts(args[0]);
        final String kafkaTopic = args[1];
        final String zkRoot = args[2];
        final String clientId = args[3];
        final SpoutConfig kafkaConf = new SpoutConfig(zkrHosts, kafkaTopic, zkRoot, clientId);
        kafkaConf.scheme = new SchemeAsMultiScheme(new StringScheme());

        // Build topology to consume message from kafka and print them on console
        final TopologyBuilder topologyBuilder = new TopologyBuilder();
        // ici on cree un spout en utilisant kafka configs et on l'ajoute a la topologie
        topologyBuilder.setSpout("kafka-spout", new KafkaSpout(kafkaConf), 1);
        //c'est le bolt pour router les message vers la sortie (consommation)
        topologyBuilder.setBolt("consume-messages", new ConsumeMessageBolt()).globalGrouping("kafka-spout");

        // subbmiter la topologie vers le serveur blocal
        final LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("kafka-topology", new HashMap<>(), topologyBuilder.createTopology());
    }
}