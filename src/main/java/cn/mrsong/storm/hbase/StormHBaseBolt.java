package cn.mrsong.storm.hbase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

public class StormHBaseBolt extends BaseBasicBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private HBaseOperations hbaseOperations;
	private String tableName;
	private List<String> columnFamilies;
	private List<String> zookeeperIPs;
	private int zkPort;

	public StormHBaseBolt(String tableName, List<String> columnFamilies, List<String> zookeeperIPs, int zkPort) {
		this.tableName = tableName;
		this.columnFamilies = columnFamilies;
		this.zookeeperIPs = zookeeperIPs;
		this.zkPort = zkPort;
	}

	public void execute(Tuple input, BasicOutputCollector collector) {
		Map<String, Map<String, Object>> record = new HashMap<String, Map<String, Object>>();
		Map<String, Object> personalMap = new HashMap<String, Object>();
		personalMap.put("firstName", input.getValueByField("firstName"));
		personalMap.put("lastName", input.getValueByField("lastName"));
		Map<String, Object> companyMap = new HashMap<String, Object>();
		companyMap.put("companyName", input.getValueByField("companyName"));
		record.put("personal", personalMap);
		record.put("company", companyMap);
		// call the inset methodof HBaseOperations class to insert record into
		// HBase
		hbaseOperations.insert(record, UUID.randomUUID().toString());
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		// create the instance of HBaseOperations class
		hbaseOperations = new HBaseOperations(tableName, columnFamilies, zookeeperIPs, zkPort);
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
	}

}
