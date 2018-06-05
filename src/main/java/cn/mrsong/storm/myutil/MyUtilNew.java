package cn.mrsong.storm.myutil;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.Date;

public class MyUtilNew {

    public static void OutLog2NC(Object o,String msg){
        try {
        	Runtime rt = Runtime.getRuntime();
            Process p = rt.exec("nc 192.168.88.1 8888");
            OutputStream os = p.getOutputStream();
        	
        	String prefix = "";
            //取得系统时间
            Date date = new Date();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd hh:mm:ss");
            String time = sdf.format(date);
            //hostname
            String host = InetAddress.getLocalHost().getHostName();

            //pid
            RuntimeMXBean r = ManagementFactory.getRuntimeMXBean();

            //8888@hostname
            String pid = r.getName().split("@")[0];

            //thread
            String tname = Thread.currentThread().getName();
            long tid = Thread.currentThread().getId();
            String tinfo = "TID:"+tid;

            String oclass = o.getClass().getSimpleName();

            int hash = o.hashCode();

            String oinfo = oclass + "@"+hash;

            prefix = "["+time+" "+host+" "+pid+" "+tinfo+" "+oinfo+"] "+msg+"\n";

            os.write(prefix.getBytes());

            os.flush();

        }catch (IOException e){
            e.printStackTrace();
        }

    }
    
}