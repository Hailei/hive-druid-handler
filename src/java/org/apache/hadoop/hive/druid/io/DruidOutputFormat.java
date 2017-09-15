/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.druid.io;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.druid.data.input.impl.DimensionSchema;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.MapInputRowParser;
import io.druid.data.input.impl.StringDimensionSchema;
import io.druid.data.input.impl.TimeAndDimsParseSpec;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.indexer.SQLMetadataStorageUpdaterJobHandler;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.indexing.granularity.GranularitySpec;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.realtime.plumber.CustomVersioningPolicy;
import io.druid.storage.hdfs.HdfsDataSegmentPusher;
import io.druid.storage.hdfs.HdfsDataSegmentPusherConfig;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.druid.Constants;
import org.apache.hadoop.hive.druid.DruidConfigure;
import org.apache.hadoop.hive.druid.DruidStorageHandlerUtils;
import org.apache.hadoop.hive.druid.MetaStorageHolder;
import org.apache.hadoop.hive.druid.serde.DruidWritable;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.druid.java.util.common.granularity.Granularity;
import io.druid.java.util.common.granularity.PeriodGranularity;
import io.druid.metadata.MetadataStorageConnectorConfig;
import io.druid.metadata.MetadataStorageTablesConfig;
import io.druid.metadata.SQLMetadataConnector;
import io.druid.metadata.storage.mysql.MySQLConnector;
import io.druid.metadata.storage.postgresql.PostgreSQLConnector;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.apache.hadoop.hive.druid.DruidStorageHandler.SEGMENTS_DESCRIPTOR_DIR_NAME;

public class DruidOutputFormat<K, V> implements HiveOutputFormat<K, DruidWritable> {

  protected static final Logger LOG = LoggerFactory.getLogger(DruidOutputFormat.class);
  private static MetaStorageHolder metaStorageHolder;
  
  private MetaStorageHolder getMetaStorageHandler(Properties tableProperties){
	  
       if(metaStorageHolder == null){
    	synchronized (this) {
			if(metaStorageHolder == null){
				final String base = tableProperties.getProperty(Constants.DRUID_METADATA_BASE,"druid");
		   	    final String dbType = tableProperties.getProperty(Constants.DRUID_METADATA_DB_TYPE,"mysql");
		   	    final String username = tableProperties.getProperty(Constants.DRUID_METADATA_DB_USERNAME,"root");
		   	    final String password = tableProperties.getProperty(Constants.DRUID_METADATA_DB_PASSWORD,"root");
		   	    final String uri = tableProperties.getProperty(Constants.DRUID_METADATA_DB_URI);
		   	    MetadataStorageTablesConfig druidMetadataStorageTablesConfig = MetadataStorageTablesConfig.fromBase(base);
		   	    SQLMetadataConnector connector;
		   	    SQLMetadataStorageUpdaterJobHandler druidSqlMetadataStorageUpdaterJobHandler;
		   	    final Supplier<MetadataStorageConnectorConfig> storageConnectorConfigSupplier = Suppliers.<MetadataStorageConnectorConfig>ofInstance(
		   	            new MetadataStorageConnectorConfig() {
		   	              @Override
		   	              public String getConnectURI() {
		   	                return uri;
		   	              }

		   	              @Override
		   	              public String getUser() {
		   	                return username;
		   	              }

		   	              @Override
		   	              public String getPassword() {
		   	                return password;
		   	              }
		   	            });

		   	    if (dbType.equals("mysql")) {
		   	      connector = new MySQLConnector(storageConnectorConfigSupplier,
		   	              Suppliers.ofInstance(druidMetadataStorageTablesConfig)
		   	      );
		   	    } else if (dbType.equals("postgresql")) {
		   	      connector = new PostgreSQLConnector(storageConnectorConfigSupplier,
		   	              Suppliers.ofInstance(druidMetadataStorageTablesConfig)
		   	      );
		   	    } else {
		   	      throw new IllegalStateException(String.format("Unknown metadata storage type [%s]", dbType));
		   	    }
		   	    druidSqlMetadataStorageUpdaterJobHandler = new SQLMetadataStorageUpdaterJobHandler(connector);
		   	    
		   	    metaStorageHolder = new MetaStorageHolder(druidSqlMetadataStorageUpdaterJobHandler, druidMetadataStorageTablesConfig);
		       }
		       
			}
		}   
    	
       return metaStorageHolder;
	   
  }

  @Override
  public FileSinkOperator.RecordWriter getHiveRecordWriter(
          JobConf jc,
          Path finalOutPath,
          Class<? extends Writable> valueClass,
          boolean isCompressed,
          Properties tableProperties,
          Progressable progress
  ) throws IOException {

	DruidConfigure dc = new DruidConfigure(tableProperties);
    final String segmentGranularity =
            tableProperties.getProperty(Constants.DRUID_SEGMENT_GRANULARITY,"HOUR");
    final String dataSource = tableProperties.getProperty(Constants.DRUID_DATA_SOURCE);
    final String segmentDirectory =
            tableProperties.getProperty(Constants.DRUID_SEGMENT_DIRECTORY,"/team/db/druid/segments");

    final HdfsDataSegmentPusherConfig hdfsDataSegmentPusherConfig = new HdfsDataSegmentPusherConfig();
    hdfsDataSegmentPusherConfig.setStorageDirectory(segmentDirectory);
    final DataSegmentPusher hdfsDataSegmentPusher = new HdfsDataSegmentPusher(
            hdfsDataSegmentPusherConfig, jc, DruidStorageHandlerUtils.JSON_MAPPER);
   //default  rollup
   boolean rollup = Boolean.valueOf(tableProperties.getProperty(Constants.DRUID_IS_ROLLUP, "true"));
    final GranularitySpec granularitySpec = new UniformGranularitySpec(
            getGranularityWithTimeZone(Granularity.fromString(segmentGranularity)),
            getGranularityWithTimeZone(Granularity.fromString(
                    tableProperties.getProperty(Constants.DRUID_QUERY_GRANULARITY,"HOUR"))),
            rollup,
            null
    );
    
    MetaStorageHolder metaStorageHolder = getMetaStorageHandler(tableProperties);

    final String columnNameProperty = tableProperties.getProperty(serdeConstants.LIST_COLUMNS);
    final String columnTypeProperty = tableProperties.getProperty(serdeConstants.LIST_COLUMN_TYPES);

    if (StringUtils.isEmpty(columnNameProperty) || StringUtils.isEmpty(columnTypeProperty)) {
      throw new IllegalStateException(
              String.format("List of columns names [%s] or columns type [%s] is/are not present",
                      columnNameProperty, columnTypeProperty
              ));
    }
    ArrayList<String> columnNames = new ArrayList<String>();
    for (String name : columnNameProperty.split(",")) {
      columnNames.add(name);
    }
    if (!columnNames.contains(Constants.DEFAULT_TIMESTAMP_COLUMN)) {
      throw new IllegalStateException("Timestamp column (' " + Constants.DEFAULT_TIMESTAMP_COLUMN +
              "') not specified in create table; list of columns is : " +
              tableProperties.getProperty(serdeConstants.LIST_COLUMNS));
      
    }
    final List<DimensionSchema> dimensions = new ArrayList<>();
    ImmutableList.Builder<AggregatorFactory> aggregatorFactoryBuilder = ImmutableList.builder();
    
    String[] hiveTableAggFileds = StringUtils.split(tableProperties.getProperty(Constants.DRUID_DATASOURCE_AGGREGATOR_FIELDS),",");
    
    String[] hiveTableAggNames = hiveTableAggFileds;
    String[] hiveTableAggTypes = StringUtils.split(tableProperties.getProperty(Constants.DRUID_DATASOURCE_AGGREGATOR_TYPES),",");
    List<String> hiveTableDimensions  = columnNames;
    Set<String> aggFields = new HashSet<>();
    if(hiveTableAggFileds != null){
    	 
    	    for(String field:hiveTableAggFileds){
    	    	aggFields.add(field);
    	    }
    	    
    	    for(int i = 0;i < hiveTableAggFileds.length;i++){

    	    	aggregatorFactoryBuilder.add(new DruidAggregatorSpec(hiveTableAggTypes[i],hiveTableAggFileds[i] , hiveTableAggNames[i]).buildAggregatorFactory());
    	    	    
    	    }
    }
    
    
    List<String> partitionCols = DruidStorageHandlerUtils.toList(tableProperties.getProperty("partition_columns"),"/");
    	
    for(String tableDimension:hiveTableDimensions){
    	if( (!tableDimension.equals(Constants.DEFAULT_TIMESTAMP_COLUMN)) && !aggFields.contains(tableDimension))
    	    dimensions.add(new StringDimensionSchema(tableDimension));
    }	
    
    
   
    List<AggregatorFactory> aggregatorFactories = aggregatorFactoryBuilder.build();
    final InputRowParser inputRowParser = new MapInputRowParser(new TimeAndDimsParseSpec(
            new TimestampSpec(Constants.DEFAULT_TIMESTAMP_COLUMN, "auto", null),
            new DimensionsSpec(dimensions,
            		partitionCols, null
            )
    ));

    Map<String, Object> inputParser = DruidStorageHandlerUtils.JSON_MAPPER
            .convertValue(inputRowParser, Map.class);

    final DataSchema dataSchema = new DataSchema(
            Preconditions.checkNotNull(dataSource, "Data source name is null"),
            inputParser,
            aggregatorFactories.toArray(new AggregatorFactory[aggregatorFactories.size()]),
            granularitySpec,
            DruidStorageHandlerUtils.JSON_MAPPER
    );

    final String workingPath = jc.get(Constants.DRUID_JOB_WORKING_DIRECTORY);
    final String version = jc.get(Constants.DRUID_SEGMENT_VERSION);
    Integer maxPartitionSize = jc.getInt(Constants.HIVE_DRUID_MAX_PARTITION_SIZE, 5000000);
    String basePersistDirectory = jc.get(Constants.HIVE_DRUID_BASE_PERSIST_DIRECTORY, "");
    if (Strings.isNullOrEmpty(basePersistDirectory)) {
      basePersistDirectory = System.getProperty("java.io.tmpdir");
    }
    Integer maxRowInMemory = jc.getInt(Constants.HIVE_DRUID_MAX_ROW_IN_MEMORY,75000);

    RealtimeTuningConfig realtimeTuningConfig = new RealtimeTuningConfig(maxRowInMemory,
            null,
            null,
            new File(basePersistDirectory, dataSource),
            new CustomVersioningPolicy(version),
            null,
            null,
            null,
            null,
            true,
            0,
            0,
            true,
            null
    );

    LOG.info(String.format("running with Data schema [%s] ", dataSchema));
    return new DruidRecordWriter(dataSchema, realtimeTuningConfig, hdfsDataSegmentPusher,
            maxPartitionSize, new Path(workingPath, SEGMENTS_DESCRIPTOR_DIR_NAME),
            finalOutPath.getFileSystem(jc),
            metaStorageHolder,partitionCols
    );
  }

  @Override
  public RecordWriter<K, DruidWritable> getRecordWriter(
          FileSystem ignored, JobConf job, String name, Progressable progress
  ) throws IOException {
    throw new UnsupportedOperationException("please implement me !");
  }

  
  private Granularity getGranularityWithTimeZone(Granularity granularity){
	  
	  PeriodGranularity pG = (PeriodGranularity)granularity;
	  return new PeriodGranularity(pG.getPeriod(), pG.getOrigin(), DateTimeZone.forID("Asia/Shanghai"));
  }
  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException {
    // NOOP
  }
}
