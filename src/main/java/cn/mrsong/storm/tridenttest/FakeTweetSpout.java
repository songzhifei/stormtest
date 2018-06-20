package cn.mrsong.storm.tridenttest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.IBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class FakeTweetSpout implements IBatchSpout {
	private static final long serialVersionUID = 10L;
	private int batchSize;
	private HashMap<Long, List<List<Object>>> batchesMap = new HashMap<Long, List<List<Object>>>();

	public FakeTweetSpout(int batchSize) {
		this.batchSize = batchSize;
	}

	private static final Map<Integer, String> TWEET_MAP = new HashMap<Integer, String>();
	static {
		TWEET_MAP.put(0, " Adidas #FIFA World Cup Chant Challenge ");
		TWEET_MAP.put(1, "#FIFA worldcup");
		TWEET_MAP.put(2, "#FIFA worldcup");
		TWEET_MAP.put(3, " The Great Gatsby is such a good #movie ");
		TWEET_MAP.put(4, "#Movie top 10");
	}
	private static final Map<Integer, String> COUNTRY_MAP = new HashMap<Integer, String>();
	static {
		COUNTRY_MAP.put(0, "United State");
		COUNTRY_MAP.put(1, "Japan");
		COUNTRY_MAP.put(2, "India");
		COUNTRY_MAP.put(3, "China");
		COUNTRY_MAP.put(4, "Brazil");
	}

	private List<Object> recordGenerator() {
		final Random rand = new Random();
		int randomNumber = rand.nextInt(5);
		int randomNumber2 = rand.nextInt(5);
		return new Values(TWEET_MAP.get(randomNumber), COUNTRY_MAP.get(randomNumber2));
	}


	public void ack(long batchId) {
		this.batchesMap.remove(batchId);
	}


	public void close() {
		/*
		 * This method is used to destroy or close all the connection opened in
		 * open method.
		 */
	}


	public void emitBatch(long batchId, TridentCollector collector) {
		List<List<Object>> batches = this.batchesMap.get(batchId);
		if (batches == null) {
			batches = new ArrayList<List<Object>>();
			for (int i = 0; i < this.batchSize; i++) {
				batches.add(this.recordGenerator());
			}
			this.batchesMap.put(batchId, batches);
		}
		for (List<Object> list : batches) {
			collector.emit(list);
		}
	}


	public Map getComponentConfiguration() {
		/*
		 * This method is use to set the spout configuration like defining the
		 * parallelism, etc.
		 */
		return null;
	}


	public Fields getOutputFields() {
		return new Fields("text", "Country");
	}


	public void open(Map arg0, TopologyContext arg1) {
		/*
		 * This method is used to initialize the variable, open the connection
		 * with external source, etc.
		 */
	}
}