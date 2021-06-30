package OrinToHBase;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

import java.util.Map;

public class StormTopology  {
    private static BrokerHosts brokerHosts;
    private static SpoutConfig spoutConfig;
    private static TopologyBuilder builder;
    private static String TOPOLOGY_NAME;
    Map<String, String> map;

    public static void main(String args[]) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        brokerHosts = new ZkHosts("172.31.17.200:2181,172.31.17.26:2181,172.31.17.29:2181");
        spoutConfig = new SpoutConfig(brokerHosts, "test", "", "kafkaspout");
        spoutConfig.scheme = new SchemeAsMultiScheme(new MessageScheme());
        builder = new TopologyBuilder();
        builder.setSpout("spout", new KafkaSpout(spoutConfig));
        builder.setBolt("bolt", new HbaseBolt()).shuffleGrouping("spout");
        TOPOLOGY_NAME = "TOPOLOGY_Hbase";

        Config config = new Config();
        config.setNumWorkers(4);
        config.put(config.TOPOLOGY_MAX_SPOUT_PENDING, 1000);
        config.setDebug(false);

        if (args.length == 0) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
        //    Utils.sleep(30000);
            cluster.killTopology(TOPOLOGY_NAME);
            cluster.shutdown();

        } else {
            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
        }
    }

}
