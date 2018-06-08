package cn.mrsong.storm.trident;

import java.util.HashMap;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.testing.FeederBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import com.google.common.collect.ImmutableList;

public class TridentTopologyApp {

	public static void main(String[] args) throws Exception {
		
		Config config = new Config();
		//config.setNumWorkers(3);
		//config.setDebug(true);
		
	    Map<String, String> map = new HashMap<String,String>();
	    
	    map.put("storm.zookeeper.servers", "itcast02");
	    config.setEnvironment(map);
	    
	    TridentTopology top = new TridentTopology();
	    
	    FeederBatchSpout testSpout = new FeederBatchSpout(ImmutableList.of("a","b","c","d"));
	    
	    Stream newStream = top.newStream("sput", testSpout);
	    
	    newStream = newStream.shuffle()
	    		.each(new Fields("a","b"), new CheckEvenSumFilter())
	    		.each(new Fields("a","b"), new SumFunction(),new Fields("sum"))
	    		.each(new Fields("a","b","c","d","sum"),new AvgFunction(),new Fields("avg"))
	    		.project(new Fields("a","b","sum","avg"))//只保留设置的字段，其他字段数据被抛弃
	    		.parallelismHint(2);
	    
//	    newStream.shuffle().each(new Fields("a","b"), new SumFunction(),new Fields("sum"));
	    
	    LocalCluster cluster = new LocalCluster();
	    
	    cluster.submitTopology("TridentTopologyApp", config, top.build());
	    
	    testSpout.feed(ImmutableList.of(new Values(1,1,3,4)));
	    testSpout.feed(ImmutableList.of(new Values(2,1,3,4)));
	    testSpout.feed(ImmutableList.of(new Values(3,1,3,4)));
	    testSpout.feed(ImmutableList.of(new Values(4,1,3,4)));
	    testSpout.feed(ImmutableList.of(new Values(5,1,3,4)));
	    
	    Thread.sleep(10000);
	    
	    cluster.shutdown();

	}

}
