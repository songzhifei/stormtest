package cn.mrsong.storm.trident;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class AvgFunction extends BaseFunction {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;


	public void execute(TridentTuple tuple, TridentCollector collector) {
		int a = tuple.getInteger(0);
		int b = tuple.getInteger(1);
		int c = tuple.getInteger(2);
		int d = tuple.getInteger(3);
		int e = tuple.getInteger(4);
		int sum = a+b+c+d+e;
		double avg = (double)sum /5;
		System.out.println(a+","+b+","+c+","+d+","+e);
		System.out.println("AvgFunction:"+avg);
		collector.emit(new Values(avg));
		
	}

}
