package spark.stockanalysis;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.json.JSONArray;
import org.json.JSONObject;
import org.apache.spark.SparkContext;
import scala.Tuple2;

public class SparkStockAnalysis {

	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub
		
		final String path = "C:/Users/Mohini Kamat/Downloads/out";
		
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkStockAnalysis");
		
		JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(300000));
		
			
		JavaDStream<String> MyDStream = jssc.textFileStream("C:/Users/Mohini Kamat/Downloads/").cache();
			
		//JavaDStream<String> lines = MyDStream.flatMap(x->Arrays.asList(x.split(" ")).iterator());
		
		JavaPairDStream<String, MaxTuple> lines = MyDStream.flatMapToPair(
				new PairFlatMapFunction<String, String, MaxTuple>() 
				{
					private static final long serialVersionUID = 67676744;
					
					public Iterator<Tuple2<String, MaxTuple>> call(String t)
					throws Exception 
					{
						List<Tuple2<String, MaxTuple>> list = new ArrayList<Tuple2<String, MaxTuple>>();
						JSONArray jarr = new JSONArray(t);
						
						for (int i = 0; i < jarr.length(); i++) 
						{
							String symbol = jarr.getJSONObject(i).get("symbol").toString();
					
							JSONObject jobj = new JSONObject(jarr.getJSONObject(i).get("priceData").toString());
					
							list.add(new Tuple2<String, MaxTuple>(symbol,new MaxTuple(1, jobj.getDouble("close"),jobj.getDouble("open"))));
						}
						
						return list.iterator();
						
					}
				});
		
		JavaPairDStream<String, MaxTuple> result= lines.reduceByKeyAndWindow(
				new Function2<MaxTuple, MaxTuple, MaxTuple>() 
				{
					private static final long serialVersionUID = 76761212;
					
					public MaxTuple call(MaxTuple result, MaxTuple value) throws Exception 
					{
						result.setClosingPrice(result.getClosingPrice() + value.getClosingPrice());
					
						result.setOpeningPrice(result.getOpeningPrice()+value.getOpeningPrice());
					
						result.setCount(result.getCount() + value.getCount());
					
						return result;
					}
				}, new Duration(600000), new Duration(300000));
		
		result.foreachRDD(new VoidFunction<JavaPairRDD<String,MaxTuple>>() 
		{
			
			private static final long serialVersionUID = 6767679;
			
			public void call(JavaPairRDD<String, MaxTuple> t) throws Exception 
			{
				t.coalesce(1).saveAsTextFile(path+java.io.File.separator + System.currentTimeMillis());
			}
			});

						
		jssc.start();
		jssc.awaitTermination();
		jssc.close();
	}

}
