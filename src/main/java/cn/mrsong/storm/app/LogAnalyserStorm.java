package cn.mrsong.storm.app;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

//Create main class LogAnalyserStorm submit topology.
public class LogAnalyserStorm {
 public static void main(String[] args) throws Exception{
    //Create Config instance for cluster configuration
    Config config = new Config();
    config.setDebug(true);
    
//    Map<String, String> map = new HashMap<String,String>();
//    
//    map.put("storm.zookeeper.servers", "itcast04");
//    config.setEnvironment(map);
    
		
    //
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("call-log-reader-spout", new FakeCallLogReaderSpout());

    builder.setBolt("call-log-creator-bolt", new CallLogCreatorBolt())
       .shuffleGrouping("call-log-reader-spout");

    builder.setBolt("call-log-counter-bolt", new CallLogCounterBolt())
       .fieldsGrouping("call-log-creator-bolt", new Fields("call"));
			
//    LocalCluster cluster = new LocalCluster();
//    cluster.submitTopology("LogAnalyserStorm", config, builder.createTopology());
//    Thread.sleep(10000);
//	cluster.shutdown();
    //Stop the topology
		
    
    StormSubmitter.submitTopology("LogAnalyserStorm", config, builder.createTopology());
 }
}