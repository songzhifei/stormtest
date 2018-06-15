package cn.mrsong.storm.tridenttest;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.StormSubmitter;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.utils.DRPCClient;

public class RemoteRPC {

	public static void main(String[] args) throws Exception, InvalidTopologyException, AuthorizationException {
		Config conf = new Config();
		LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("Count");
		builder.addBolt(new ExclaimBolt(),3);
		conf.setDebug(false);
		conf.put("storm.thrift.transport", "org.apache.storm.security.auth.SimpleTransportPlugin");
		conf.put(Config.STORM_NIMBUS_RETRY_TIMES, 3);
		conf.put(Config.STORM_NIMBUS_RETRY_INTERVAL, 10);
		conf.put(Config.STORM_NIMBUS_RETRY_INTERVAL_CEILING, 20);
		conf.put(Config.DRPC_MAX_BUFFER_SIZE, 1048576);
		LocalDRPC drpc = new LocalDRPC();
		if (args.length == 0) {
			LocalCluster cluster = new LocalCluster();
			//cluster.submitTopology("CountryCount", conf, buildTopology(drpc));
			//cluster.submitTopology("CountryCount", conf, builder.createLocalTopology(drpc));

			//System.out.println("Results for 'hello'"+drpc.execute("Count", "1,2"));
			//System.out.println("Results for 'hello'"+drpc.execute("Count", "Japan,India,Europe"));
			for(int i=0;i<100;i++) {
				System.out.println("Results for 'hello'"+drpc.execute("Count", "Japan,India,Europe"));

			}
		    //Thread.sleep(20000);
			cluster.shutdown();
			drpc.shutdown();
		} else {
			conf.setNumWorkers(3);
			StormSubmitter.submitTopology(args[0], conf, builder.createRemoteTopology());
			Thread.sleep(2000);
			DRPCClient client = new DRPCClient(conf, "itcast03", 3772);
			System.out.println("Results for 'hello'"+client.execute("Count", "3,4"));
		}

	}

}
