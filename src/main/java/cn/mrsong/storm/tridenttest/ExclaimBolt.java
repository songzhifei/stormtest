package cn.mrsong.storm.tridenttest;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class ExclaimBolt implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private OutputCollector collector;
	

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
	}


	public void execute(Tuple input) {
		String string = input.getString(1);
		String[] strings =  string.split(",");
		Integer a = Integer.parseInt(strings[0]);
		Integer b = Integer.parseInt(strings[1]);
		
		collector.emit(new Values(input.getValue(0),a+b));
	}


	public void cleanup() {
		
	}


	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id","result"));
		
	}


	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
