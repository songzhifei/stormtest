package cn.mrsong.storm.tridenttest;

import java.util.HashMap;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.tuple.Fields;

public class TridentHelloWorldTopology {
	public static void main(String[] args) throws Exception {
		Config conf = new Config();
		Map<String, String> map = new HashMap<String, String>();
		map.put("storm.zookeeper.servers", "itcast02");

		conf.setEnvironment(map);
		conf.setMaxSpoutPending(20);
		
		if (args.length == 0) {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("Count", conf, buildTopology());
		} else {
			conf.setNumWorkers(3);
			StormSubmitter.submitTopology(args[0], conf, buildTopology());
		}
	}

	public static StormTopology buildTopology() {
		FakeTweetSpout spout = new FakeTweetSpout(10);
		TridentTopology topology = new TridentTopology();
		topology.newStream("faketweetspout", spout).shuffle()
				.each(new Fields("text", "Country"), new TweetFilter())
				.groupBy(new Fields("Country"))
				.aggregate(new Fields("Country"), new Count(), new Fields("count"))
				.each(new Fields("count"), new Print()).parallelismHint(2);
		return topology.build();
	}
}
