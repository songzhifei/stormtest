package cn.mrsong.storm.trident;

import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;

/**
 * 过滤器
 */
public class CheckEvenSumFilter extends BaseFilter {


	private static final long serialVersionUID = -9194995518256414479L;


	public boolean isKeep(TridentTuple tuple) {
		int a = tuple.getInteger(0);
		int b = tuple.getInteger(1);
//		int c = tuple.getInteger(2);
//		int d = tuple.getInteger(3);
		System.out.println("CheckEvenSumFilter:"+a+","+b);
		int sum = a+b;
		if(sum %2 == 0){
			return true;
		}
		return false;
	}
	
}
