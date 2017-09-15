package org.apache.hadoop.hive.druid;

import org.apache.hadoop.hive.conf.HiveConf;

public class Constants {
	
	 /* Constants for Druid storage handler */
	  public static final String DRUID_HIVE_STORAGE_HANDLER_ID =
	          "org.apache.hadoop.hive.druid.DruidStorageHandler";
	  public static final String DRUID_HIVE_OUTPUT_FORMAT =
	          "org.apache.hadoop.hive.druid.io.DruidOutputFormat";
	  public static final String DRUID_DATA_SOURCE = "druid.datasource";
	  public static final String DRUID_SEGMENT_GRANULARITY = "druid.segment.granularity";
	  public static final String DRUID_QUERY_GRANULARITY = "druid.query.granularity";
	  public static final String DRUID_QUERY_JSON = "druid.query.json";
	  public static final String DRUID_QUERY_TYPE = "druid.query.type";
	  public static final String DRUID_QUERY_FETCH = "druid.query.fetch";
	  public static final String DRUID_SEGMENT_DIRECTORY = "druid.storage.storageDirectory";//segment output path
	  public static final String DRUID_SEGMENT_VERSION = "druid.segment.version";//System set
	  public static final String DRUID_JOB_WORKING_DIRECTORY = "druid.job.workingDirectory";//System set
	  public static final String DRUID_DATASOURCE_DIMENSION ="druid.datasource.dimensions";//user set
	  public static final String DRUID_IS_ROLLUP="druid.rollup";
	  public static final String DRUID_DATASOURCE_AGGREGATOR_NAMES = "druid.datasource.aggregator.names";//user set
	  public static final String DRUID_DATASOURCE_AGGREGATOR_TYPES ="druid.datasource.aggregator.types";//user set
	  public static final String DRUID_DATASOURCE_AGGREGATOR_FIELDS = "druid.datasource.aggregator.fields";//user set
	  public static final String DEFAULT_TIMESTAMP_COLUMN="druid__time";
	  
	  
	  public static final String PARTITION_COLUMNS_NAME ="partition_columns_name";
	  public static final String PARTITION_COLUMNS_TYPE = "partition_columns_type";
	  
	  
	  
	  //hive conf 
	  
	  public static final String DRUID_METADATA_BASE  = "hive.druid.metadata.base";//default druid
	  public static final String DRUID_METADATA_DB_TYPE = "hive.druid.metadata.db.type";//default mysql
	  public static final String DRUID_METADATA_DB_USERNAME = "hive.druid.metadata.username";
	  public static final String DRUID_METADATA_DB_PASSWORD ="hive.druid.metadata.password";
	  public static final String DRUID_METADATA_DB_URI = "hive.druid.metadata.uri";//required
	  public static final String HIVE_DRUID_COORDINATOR_DEFAULT_ADDRESS = "hive.druid.coordinator.address.default";//required
	  public static final String HIVE_DRUID_MAX_TRIES = "hive.druid.maxTries";
	  public static final String HIVE_DRUID_PASSIVE_WAIT_TIME = "hive.druid.passiveWaitTimeMs";
	  public static final String DRUID_WORKING_DIR = "hive.druid.working.directory";
	  public static final String HIVE_DRUID_HTTP_READ_TIMEOUT = "hive.druid.http.read.timeout";//default PT1M
	  public static final String HIVE_DRUID_NUM_HTTP_CONNECTION ="hive.druid.http.numConnection";//default 25
	  
	  public static final String HIVE_DRUID_INDEXING_GRANULARITY="hive.druid.indexer.segments.granularity";
	  public static final String HIVE_DRUID_MAX_PARTITION_SIZE="hive.druid.indexer.partition.size.max";//default 5000000
	  
	  public static final String HIVE_DRUID_BASE_PERSIST_DIRECTORY="hive.druid.basePersistDirectory";//default java.io.tmpdir
	  public static final String HIVE_DRUID_MAX_ROW_IN_MEMORY= "hive.druid.indexer.memory.rownum.max";
	  public static final String HIVE_DRUID_BROKER_DEFAULT_ADDRESS = "hive.druid.broker.address.default";
	  public static final String HIVE_DRUID_SELECT_DISTRIBUTE = "hive.druid.select.distribute";//default false
	  public static final String HIVE_DRUID_SELECT_THRESHOLD="hive.druid.select.threshold";//default 10000
	  
	  public static final String DRUID_TIMESTAMP_GRANULARITY_COL_NAME="druid__partition";
	  
	  
	  
	  
	  
	  
	  
	  
	  
	  
	  
	  
	  

}
