package com.kafka.kafkaSpout3;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

public class CountBolt implements IRichBolt{
   Map<String, Integer> counters;
   private OutputCollector collector;
   
   
   static Connection conn;  
   static Statement st;  
   
   /* 获取数据库连接的函数*/  
   public static Connection getConnection() {  
       Connection con = null;  //创建用于连接数据库的Connection对象  
       try {  
           Class.forName("com.mysql.jdbc.Driver");// 加载Mysql数据驱动  
             
           con = DriverManager.getConnection(  
                   "jdbc:mysql://localhost:3306/test", "root", "blackcat19");// 创建数据连接  
             
       } catch (Exception e) {  
           System.out.println("数据库连接失败" + e.getMessage());  
       }  
       return con; //返回所建立的数据库连接  
   }  
   
	public static void insert(String word,int value) {  
       
       conn = getConnection(); // 首先要获取连接，即连接到数据库  
 
       try {  
           String sql = "INSERT INTO words(word,count)"  
                   + " VALUES ('"+word+"','"+value+"')";  // 插入数据的sql语句  
             
           st = (Statement) conn.createStatement();    // 创建用于执行静态sql语句的Statement对象  
             
           int count = st.executeUpdate(sql);  // 执行插入操作的sql语句，并返回插入数据的个数  
             
           System.out.println("向words表中插入 " + count + " 條數據"); //输出插入操作的处理结果  
             
           conn.close();   //关闭数据库连接  
             
       } catch (SQLException e) {  
           System.out.println("插入数据失败" + e.getMessage());  
       }  
   }  
   
   
   
   
   @Override
   public void prepare(Map stormConf, TopologyContext context,
   OutputCollector collector) {
      this.counters = new HashMap<String, Integer>();
      this.collector = collector;
   }

   @Override
   public void execute(Tuple input) {
      String str = input.getString(0);
      
      if(!counters.containsKey(str)){
         counters.put(str, 1);
      }else {
         Integer c = counters.get(str) +1;
         counters.put(str, c);
      }
      
      
      collector.ack(input);
   }

   @Override
   public void cleanup() {
      for(Map.Entry<String, Integer> entry:counters.entrySet()){
         System.out.println(entry.getKey() + " : " + entry.getValue());
         //insert(entry.getKey(),entry.getValue());
      }
   }

   @Override
   public void declareOutputFields(OutputFieldsDeclarer declarer) {
   
   }

   @Override
   public Map<String, Object> getComponentConfiguration() {
      return null;
   }
}