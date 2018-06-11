package cn.mrsong.storm.tridenttest;

import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;

/* This class extends BaseFilter andcontain isKeep method which willprint the input
tuple.*/
public class Print extends BaseFilter {
	private static final long serialVersionUID = 1L;

	public boolean isKeep(TridentTuple tuple) {
		System.out.println("---------------->"+tuple);
		return true;
	}
}
