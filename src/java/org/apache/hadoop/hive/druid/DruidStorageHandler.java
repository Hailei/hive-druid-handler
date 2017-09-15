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
package org.apache.hadoop.hive.druid;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.metamx.http.client.HttpClient;
import com.sun.tools.doclets.formats.html.markup.HtmlConstants;

import io.druid.indexer.SQLMetadataStorageUpdaterJobHandler;
import io.druid.metadata.MetadataStorageTablesConfig;
import io.druid.metadata.SQLMetadataConnector;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.timeline.DataSegment;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.druid.io.DruidOutputFormat;
import org.apache.hadoop.hive.druid.io.DruidQueryBasedInputFormat;
import org.apache.hadoop.hive.druid.io.DruidRecordWriter;
import org.apache.hadoop.hive.druid.serde.DruidSerDe;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import org.apache.hadoop.hive.druid.io.AGGTYPE;

/**
 * DruidStorageHandler provides a HiveStorageHandler implementation for Druid.
 */
@SuppressWarnings({ "deprecation", "rawtypes" })
public class DruidStorageHandler implements HiveStorageHandler,HiveMetaHook {

  protected static final Logger LOG = LoggerFactory.getLogger(DruidStorageHandler.class);

  protected static final SessionState.LogHelper console = new SessionState.LogHelper(LOG);

  public static final String SEGMENTS_DESCRIPTOR_DIR_NAME = "segmentsDescriptorDir";




  private String uniqueId = null;

  private String rootWorkingDir = null;

  private Configuration conf;

  public DruidStorageHandler() {
  
  }

  @VisibleForTesting
  public DruidStorageHandler(SQLMetadataConnector connector,
          SQLMetadataStorageUpdaterJobHandler druidSqlMetadataStorageUpdaterJobHandler,
          MetadataStorageTablesConfig druidMetadataStorageTablesConfig,
          HttpClient httpClient
  ) {
   
  }

  @Override
  public Class<? extends InputFormat> getInputFormatClass() {
    return DruidQueryBasedInputFormat.class;
  }

  @Override
  public Class<? extends OutputFormat> getOutputFormatClass() {
    return DruidOutputFormat.class;
  }

  @Override
  public Class<? extends AbstractSerDe> getSerDeClass() {
    return DruidSerDe.class;
  }

  @Override
  public HiveMetaHook getMetaHook() {
    return null;
  }

  @Override
  public HiveAuthorizationProvider getAuthorizationProvider() throws HiveException {
    return new DefaultHiveAuthorizationProvider();
  }

  @Override
  public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties
  ) {

	  configureTableJobProperties(tableDesc, jobProperties);
  }

  
  @Override
  public void rollbackCreateTable(Table table) throws MetaException {
     //TODO
    
  }
  
  
  

 @Override
  public void commitCreateTable(Table table) throws MetaException {
	  
	  
	  if (MetaStoreUtils.isExternalTable(table)) {
	      return;
	  }
	  String dimensionStr = table.getParameters().get(Constants.DRUID_DATASOURCE_DIMENSION);
	  if(StringUtils.isEmpty(dimensionStr)){
		  throw new MetaException("must set 'druid.datasource.dimensions' when create table ,to specfiy druid's dimension list");
	  }
	  
	  String aggNameStr = table.getParameters().get(Constants.DRUID_DATASOURCE_AGGREGATOR_NAMES);
	  if(StringUtils.isEmpty(aggNameStr)){
	      throw new MetaException("must set 'druid.datasource.aggregator.names' when create table ,to specfiy druid's aggregator list");
	    	 
	  }
	  
	  String aggFieldStr = table.getParameters().get(Constants.DRUID_DATASOURCE_AGGREGATOR_FIELDS);
	  if(StringUtils.isEmpty(aggFieldStr)){
	      throw new MetaException("must set 'druid.datasource.aggregator.fields' when create table ,to specfiy druid's aggregator list");   	 
	  }
	  
	  
	  String aggTypeStr = table.getParameters().get(Constants.DRUID_DATASOURCE_AGGREGATOR_TYPES);
	  if(StringUtils.isEmpty(aggTypeStr)){
	      throw new MetaException("must set 'druid.datasource.aggregator.types' when create table ,to specfiy druid's aggregator list");   	 
	  }
	  
	  String[] aggName = StringUtils.split(aggNameStr);
	  String[] aggField = StringUtils.split(aggFieldStr);
	  String[] aggType = StringUtils.split(StringUtils.lowerCase(aggTypeStr));
	  
	  if(aggName.length != aggField.length || aggName.length != aggType.length || aggField.length != aggType.length){
		  
		  throw new MetaException("aggregator's name and type and field's length must be same");
	  }
	  
	  for(String type:aggType){
		  
		  AGGTYPE agg = AGGTYPE.fromString(type);
		  if(agg == null){
			  throw new MetaException(String.format("[%s]:aggregator type is illegality,must be longsum doublesum hyperunique",type));
		  }
	  }
	  
	  
    
    
  }

  

  
  public void commitDropTable(Table table, boolean deleteData) throws MetaException {
   //TODO 
  }

  
  public void commitInsertTable(Table table, boolean overwrite) throws MetaException {
    if (overwrite) {
      LOG.debug(String.format("commit insert overwrite into table [%s]", table.getTableName()));
      this.commitCreateTable(table);
    } else {
      throw new MetaException("Insert into is not supported yet");
    }
  }

 
  public void preInsertTable(Table table, boolean overwrite) throws MetaException {
    if (!overwrite) {
      throw new MetaException("INSERT INTO statement is not allowed by druid storage handler");
    }
  }

  
  public void rollbackInsertTable(Table table, boolean overwrite) throws MetaException {
    // do nothing
  }

  
  public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties
  ) {
	configureTableJobProperties(tableDesc,jobProperties);
    jobProperties.put(Constants.DRUID_SEGMENT_VERSION, new DateTime().toString());
    jobProperties.put(Constants.DRUID_JOB_WORKING_DIRECTORY, getStagingWorkingDir().toString());
  }

  @Override
  public void configureTableJobProperties(TableDesc tableDesc, Map<String, String> jobProperties
  ) {

	  Properties tableProperties = tableDesc.getProperties();
	  /*
	  try {
		OutputJobInfo jobInfo = (OutputJobInfo)
			        HCatUtil.deserialize(tableDesc.getJobProperties().get(
			          HCatConstants.HCAT_KEY_OUTPUT_INFO));
		
		HCatSchema  partitionColumn = jobInfo.getTableInfo().getPartitionColumns();
		
		List<HCatFieldSchema> partitions = partitionColumn.getFields();
	    if(partitions == null || partitions.size() == 0){
	    	throw  new RuntimeException("Druid external table must have partition(druid_partition timestamp,shard int)");
	    }
	    StringBuilder partitionColumnName = new StringBuilder();
	    StringBuilder partitionColumnType = new StringBuilder();
	    
		for(int i = 0;i < partitions.size();i++){
			
			partitionColumnName.append(partitions.get(i).getName());
			partitionColumnType.append(partitions.get(i).getTypeInfo().getTypeName());
			if(i < partitions.size() - 1){
				partitionColumnName.append(":");
				partitionColumnType.append(":");
			}
		}
		jobProperties.put(Constants.PARTITION_COLUMNS_NAME, partitionColumnName.toString());
		jobProperties.put(Constants.PARTITION_COLUMNS_TYPE, partitionColumnType.toString());

	
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	  
	  */
	  
	  for(Entry<Object,Object> entry:tableProperties.entrySet()){
		  String key = (String)entry.getKey();
		  if(!key.startsWith("columns.comments"))
		     jobProperties.put(key, (String)entry.getValue());
	  }
	  
	  for(Entry<String,String> entry:jobProperties.entrySet()){
		  conf.set(entry.getKey(), entry.getValue());
	  }
	  
  }

  @Override
  public void configureJobConf(TableDesc tableDesc, JobConf jobConf) {
//    try {
//      DruidStorageHandlerUtils.addDependencyJars(jobConf, DruidRecordWriter.class);
//    } catch (IOException e) {
//      Throwables.propagate(e);
//    }
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public String toString() {
    return Constants.DRUID_HIVE_STORAGE_HANDLER_ID;
  }

  public String getUniqueId() {
    if (uniqueId == null) {
      uniqueId = Preconditions.checkNotNull(
              Strings.emptyToNull(HiveConf.getVar(getConf(), HiveConf.ConfVars.HIVEQUERYID)),
              "Hive query id is null"
      );
    }
    return uniqueId;
  }

  private Path getStagingWorkingDir() {
    return new Path(getRootWorkingDir(), makeStagingName());
  }

  @VisibleForTesting
  protected String makeStagingName() {
    return ".staging-".concat(getUniqueId().replace(":", ""));
  }


  private String getRootWorkingDir() {
    if (Strings.isNullOrEmpty(rootWorkingDir)) {
    	rootWorkingDir = conf.get(Constants.DRUID_WORKING_DIR, "/team/db/druid/druid-tmp");
    }
    return rootWorkingDir;
  }


@Override
public void preCreateTable(Table arg0) throws MetaException {
	// TODO Auto-generated method stub
	
}

@Override
public void preDropTable(Table arg0) throws MetaException {
	// TODO Auto-generated method stub
	
}

@Override
public void rollbackDropTable(Table arg0) throws MetaException {
	// TODO Auto-generated method stub
	
}

}
