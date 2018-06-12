package cn.mrsong.storm.trident;

import java.io.Serializable;
import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;
import cn.mrsong.storm.myutil.MyUtilNew;

public class SumAsAggregator extends BaseAggregator<SumAsAggregator.State> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	static class State implements Serializable{

		private static final long serialVersionUID = 1L;
		long count = 0;
	}

	
	public void aggregate(State state, TridentTuple tuple, TridentCollector collector) {
		MyUtilNew.OutLog2NC(this, "aggregate("+state.count+")");
		state.count = tuple.getInteger(0)+state.count;
		
	}

	
	public void complete(State state, TridentCollector collector) {
		MyUtilNew.OutLog2NC(this, "complete("+state.count+")");
		collector.emit(new Values(state.count));
	}

	
	public State init(Object batchId, TridentCollector collector) {
		MyUtilNew.OutLog2NC(this, "init()");
		return new State();
	}
}
