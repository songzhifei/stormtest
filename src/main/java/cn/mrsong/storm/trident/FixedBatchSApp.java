package cn.mrsong.storm.trident;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class FixedBatchSApp {
	public static void main(String[] args) {

		// @SuppressWarnings("unchecked")
		FixedBatchSpout fixedBatchSpout = new FixedBatchSpout(new Fields("a", "b"), 3, new Values(1, 2),
				new Values(2, 2), new Values(3, 2), new Values(4, 2), new Values(5, 2));

		Config config = new Config();
		Map<String, String> map = new HashMap<String, String>();

		map.put("storm.zookeeper.servers", "itcast02");

		config.setEnvironment(map);
		config.setDebug(false);

		fixedBatchSpout.setCycle(true);

		TridentTopology top = new TridentTopology();

		Stream newStream = top.newStream("sput", fixedBatchSpout);
		newStream.shuffle().each(new Fields("a", "b"), new CheckEvenSumFilter())
				.partitionAggregate(new Fields("a"), new Count(), new Fields("count")).parallelismHint(2);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("FixedBatchSApp", config, top.build());
	}
}
