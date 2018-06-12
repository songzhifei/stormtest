package cn.mrsong.storm.tridenttest;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

/*
 * Get the comma separated value as input, split the field by comma, and
 * then emits multipletuple as output.
 */
public class Split extends BaseFunction {
	private static final long serialVersionUID = 2L;

	public void execute(TridentTuple tuple, TridentCollector collector) {
		String countries = tuple.getString(0);
		System.out.println("=============>Split:"+countries);
		for (String word : countries.split(",")) {
			collector.emit(new Values(word));
		}
	}
}
