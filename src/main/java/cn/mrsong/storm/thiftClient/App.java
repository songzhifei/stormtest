package cn.mrsong.storm.thiftClient;


import java.lang.reflect.Method;
import java.util.List;
import org.apache.storm.generated.ClusterSummary;
import org.apache.storm.generated.Nimbus.Client;
import org.apache.storm.generated.SupervisorSummary;
import org.apache.storm.thrift.TException;

public class App {

	public static void main(String[] args) throws Exception, TException {
		// TODO Auto-generated method stub
		ThriftClient tClient = new ThriftClient();
		
		Client client = tClient.getClient();
		
		ClusterSummary summary = client.getClusterInfo();
		
		List<SupervisorSummary> list = summary.get_supervisors();
		
		for(SupervisorSummary s:list)
		{
			Class<SupervisorSummary> class1 = SupervisorSummary.class;
			
			Method[] methods = class1.getDeclaredMethods();
			for(Method method:methods) {
				if(method.getName().startsWith("get")&&(method.getParameters()==null || method.getParameters().length==0)) {
					Object object = method.invoke(s);
					System.out.println(method.getName()+" : "+object);
				}
			}
			System.out.println("=======================");
		}
	}

}
