package cn.mrsong.storm.trident;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class SumFunction extends BaseFunction {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		int number1 = tuple.getInteger(0);
		int number2 = tuple.getInteger(1);
		int sum = number1 +number2;
		System.out.println("SumFunction:"+sum);
		collector.emit(new Values(sum));
	}
}
