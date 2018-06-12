package cn.mrsong.storm.trident;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class PrinterFunction extends BaseFunction {


	private static final long serialVersionUID = 1L;


	public void execute(TridentTuple tuple, TridentCollector collector) {
		
		long number1 = tuple.getLong(0);
		
		System.out.println("==========>avg:"+number1);
		
		collector.emit(new Values(number1));
	}

}
