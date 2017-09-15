package org.apache.hadoop.hive.druid.io;

import java.util.HashMap;
import java.util.Map;

import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;


public class DruidAggregatorSpec {
	
   	

	public AGGTYPE aggType;
	public String fieldName;
	public String name;
	
	
	public DruidAggregatorSpec(String strAggType,String fieldName,String name){
		
		this.aggType = AGGTYPE.fromString(strAggType);
		this.fieldName = fieldName;
		this.name = name;
	}
	
	
	public static void main(String[] args){
		
		DruidAggregatorSpec spec  = new DruidAggregatorSpec("count", "a", "b");
		System.out.println(spec.buildAggregatorFactory());
	}
	
	
	public AggregatorFactory buildAggregatorFactory(){
		
		switch(aggType){
		case LONGSUM:
			return new LongSumAggregatorFactory(name, fieldName);
		case DOUBLESUM:
			return new DoubleSumAggregatorFactory(name, fieldName);
		case COUNT:
			return new CountAggregatorFactory(name);
		case HYPERUNIQUE:
			return new HyperUniquesAggregatorFactory(name, fieldName);
		default:
			throw new IllegalStateException("Agg type not support");
		}
	}
	
	
	
}
