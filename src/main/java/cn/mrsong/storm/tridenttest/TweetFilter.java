package cn.mrsong.storm.tridenttest;

import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;

/* This class extends BaseFilter and contain isKeep method which emits only those
tuple which has #FIFA in text field.*/
public class TweetFilter extends BaseFilter {
	
	private static final long serialVersionUID = 1L;

	public boolean isKeep(TridentTuple tuple) {
		if (tuple.getString(0).contains("#FIFA")) {
			return true;
		} else {
			return false;
		}
	}
}
