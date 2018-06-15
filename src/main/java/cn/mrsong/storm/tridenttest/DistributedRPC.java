package cn.mrsong.storm.tridenttest;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.StormSubmitter;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.FilterNull;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.DRPCClient;
import org.apache.storm.utils.Utils;

public class DistributedRPC {

	public static void main(String[] args) throws Exception {
		Config conf = new Config();
		LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("Count");
		builder.addBolt(new ExclaimBolt(),3);
//		Map<String, String> map = new HashMap<String, String>();
//		map.put("storm.zookeeper.servers", "itcast02");
//
//		conf.setEnvironment(map);
//		conf.setDebug(true);
//		conf.setMaxSpoutPending(20);
		Map config = Utils.readDefaultConfig();
		LocalDRPC drpc = new LocalDRPC();
		if (args.length == 0) {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("CountryCount", conf, buildTopology(drpc));
			//cluster.submitTopology("CountryCount", conf, builder.createLocalTopology(drpc));

			//System.out.println("Results for 'hello'"+drpc.execute("Count", "1,2"));
			//System.out.println("Results for 'hello'"+drpc.execute("Count", "Japan,India,Europe"));
//			for(int i=0;i<100;i++) {
//				System.out.println("Results for 'hello'"+drpc.execute("Count", "Japan,India,Europe"));
//
//			}
		    //Thread.sleep(20000);
			cluster.shutdown();
			drpc.shutdown();
		} else {
			conf.setNumWorkers(3);
			StormSubmitter.submitTopology(args[0], config, builder.createRemoteTopology());
			Thread.sleep(2000);
			DRPCClient client = new DRPCClient(config, "itcast03", 3772);
			System.out.println("Results for 'hello'"+client.execute("Count", "3,4"));
		}
	}

	public static StormTopology buildTopology(LocalDRPC drpc) {
		FakeTweetSpout spout = new FakeTweetSpout(10);
		TridentTopology topology = new TridentTopology();
		TridentState countryCount = topology.newStream("spout1", spout).shuffle()
				.each(new Fields("text", "Country"), new TweetFilter()).groupBy(new Fields("Country"))
				.persistentAggregate(new MemoryMapState.Factory(), new Fields("Country"), new Count(),
						new Fields("count"))
				.parallelismHint(2);
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
		}
		topology.newDRPCStream("Count", drpc).each(new Fields("args"), new Split(), new Fields("Country"))
				.stateQuery(countryCount, new Fields("Country"), new MapGet(), new Fields("count"))
				.each(new Fields("count"), new FilterNull());
		return topology.build();
	}
}
