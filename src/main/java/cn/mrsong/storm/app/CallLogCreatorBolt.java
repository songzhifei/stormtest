package cn.mrsong.storm.app;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import cn.mrsong.storm.myutil.MyUtil;

//Create a class CallLogCreatorBolt which implement IRichBolt interface
public class CallLogCreatorBolt implements IRichBolt {
 //Create instance for OutputCollector which collects and emits tuples to produce output
 private OutputCollector collector;
 public CallLogCreatorBolt() {
	 MyUtil.OutLog2NC(this, "new CallLogCreatorBolt()");
 }


 public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
	 MyUtil.OutLog2NC(this, "prepare()");
    this.collector = collector;
 }


 public void execute(Tuple tuple) {
    String from = tuple.getString(0);
    String to = tuple.getString(1);
    Integer duration = tuple.getInteger(2);
    MyUtil.OutLog2NC(this, "execute():"+from+" - "+to+" - "+duration);
    collector.emit(new Values(from + " - " + to, duration));
 }


 public void cleanup() {}


 public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("call", "duration"));
 }
	

 public Map<String, Object> getComponentConfiguration() {
    return null;
 }
}
