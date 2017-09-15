package org.apache.hadoop.hive.druid;

import java.util.List;

import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.com.esotericsoftware.minlog.Log;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.druid.indexer.SQLMetadataStorageUpdaterJobHandler;
import io.druid.metadata.MetadataStorageTablesConfig;
import io.druid.timeline.DataSegment;

public class MetaStorageHolder {

	protected static final Logger LOG = LoggerFactory.getLogger(MetaStorageHolder.class);

	private SQLMetadataStorageUpdaterJobHandler druidSqlMetadataStorageUpdaterJobHandler;

	private MetadataStorageTablesConfig druidMetadataStorageTablesConfig;

	public MetaStorageHolder(SQLMetadataStorageUpdaterJobHandler druidSqlMetadataStorageUpdaterJobHandler,
			MetadataStorageTablesConfig druidMetadataStorageTablesConfig) {

		this.druidSqlMetadataStorageUpdaterJobHandler = druidSqlMetadataStorageUpdaterJobHandler;
		this.druidMetadataStorageTablesConfig = druidMetadataStorageTablesConfig;
	}

	public void druidMetaStorageUpdate(List<DataSegment> segmentList) {

			druidSqlMetadataStorageUpdaterJobHandler.publishSegments(
					druidMetadataStorageTablesConfig.getSegmentsTable(), segmentList,
					DruidStorageHandlerUtils.JSON_MAPPER);

		

	}

}
