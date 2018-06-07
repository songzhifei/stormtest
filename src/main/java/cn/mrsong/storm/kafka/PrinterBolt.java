package cn.mrsong.storm.kafka;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import cn.mrsong.storm.myutil.MyUtilNew;

public class PrinterBolt extends BaseBasicBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6298972509459082859L;


	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		String sentence = input.getString(0);
		//MyUtilNew.OutLog2NC(this, sentence);
		System.out.println("Received Sentence:"+sentence);
	}


	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		
	}

}
