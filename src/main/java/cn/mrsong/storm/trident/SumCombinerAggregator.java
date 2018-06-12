package cn.mrsong.storm.trident;

import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.tuple.TridentTuple;

import clojure.lang.Numbers;
import cn.mrsong.storm.myutil.MyUtil;
import cn.mrsong.storm.myutil.MyUtilNew;

/**
 * 合成聚合函数
 */
public class SumCombinerAggregator implements CombinerAggregator<Number> {


	private static final long serialVersionUID = 1L;


	public Number combine(Number number1, Number number2) {
		MyUtil.OutLog2NC(this, "combine("+number1+","+number2+")");
		return Numbers.add(number1, number2);
	}


	public Number init(TridentTuple tuple) {
		MyUtil.OutLog2NC(this, "init("+tuple.getValue(0)+")");
		return (Number)tuple.getValue(0);
	}


	public Number zero() {
		MyUtil.OutLog2NC(this, "zero()");
		return 0;
	}

}
