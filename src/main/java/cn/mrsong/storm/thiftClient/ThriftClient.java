package cn.mrsong.storm.thiftClient;


import org.apache.storm.generated.Nimbus.Client;
import org.apache.storm.thrift.protocol.TBinaryProtocol;
import org.apache.storm.thrift.transport.TFramedTransport;
import org.apache.storm.thrift.transport.TSocket;


public class ThriftClient {
	private static final String STORM_UI_NODE = "itcast03";
	
	public Client getClient() {
		TSocket socket = new TSocket(STORM_UI_NODE, 6627);
		
		TFramedTransport tFramedTransport = new TFramedTransport(socket);
		
		TBinaryProtocol tBinaryProtocol = new TBinaryProtocol(tFramedTransport);
		
		Client client = new Client(tBinaryProtocol);
		
		try {
			tFramedTransport.open();
		}catch (Exception e) {
			// TODO: handle exception
			throw new RuntimeException("Error occured while making connection us thrift server");
		}
		return client;
		
	}
}
