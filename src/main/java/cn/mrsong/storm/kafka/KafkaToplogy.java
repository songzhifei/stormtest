package cn.mrsong.storm.kafka;

import java.util.ArrayList;
import java.util.List;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;


public class KafkaToplogy {
	public static void main(String[] args) throws Exception, InvalidTopologyException, AuthorizationException {
	  ZkHosts zkHosts = new ZkHosts("itcast02:2181");

	  SpoutConfig spoutConfig = new SpoutConfig(zkHosts, "test2","","id7");
	  
      List<String> zkServers = new ArrayList<String>() ;
      zkServers.add("itcast03");
      spoutConfig.zkServers = zkServers;
      spoutConfig.zkPort = 2181;
      spoutConfig.socketTimeoutMs = 60 * 1000 ;
      
	  spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
	  TopologyBuilder builder = new TopologyBuilder();
	  builder.setSpout("KafkaSpout", new KafkaSpout(spoutConfig),1);
	  builder.setBolt("SentenceBolt", new SentenceBolt(),1).globalGrouping("KafkaSpout");
	  builder.setBolt("PrinterBolt", new PrinterBolt(),1).globalGrouping("SentenceBolt");
	  
	  
	  
	  Config config = new Config();
	  config.setDebug(true);
	  LocalCluster cluster = new LocalCluster();
	  cluster.submitTopology("KafkaTopolgy", config, builder.createTopology());
	  StormSubmitter.submitTopology("KafkaTopolgy", config, builder.createTopology());
	  try{
		  System.out.println("waiting to consume from kafka");
		  Thread.sleep(60000);
	  }catch (Exception e) {
		// TODO: handle exception
		  System.out.println("Thread interrupted exception : "+e);
	  }
	  cluster.killTopology("KafkaTopolgy");
	  cluster.shutdown();
	}
}
