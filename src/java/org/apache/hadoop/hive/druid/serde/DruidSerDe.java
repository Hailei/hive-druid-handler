/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.druid.serde;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import javax.annotation.Nullable;

import org.apache.calcite.adapter.druid.DruidTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.druid.DruidStorageHandlerUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeSpec;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryArray;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import org.apache.hadoop.hive.druid.Constants;

/**
 * DruidSerDe that is used to deserialize objects from a Druid data source.
 */
@SerDeSpec(schemaProps = { Constants.DRUID_DATA_SOURCE })
public class DruidSerDe extends AbstractSerDe {

	protected static final Logger LOG = LoggerFactory.getLogger(DruidSerDe.class);

	private String[] columns;
	private TypeInfo[] types;
	private ObjectInspector inspector;

	private String[] columnWithPartitions;
	private TypeInfo[] typeWithPartitions;
	private ObjectInspector inspectorWithPartitions;

	@Override
	public void initialize(Configuration configuration, Properties properties) throws SerDeException {

		final List<String> columnNames = new ArrayList<String>();
		final List<String> columnNamesWP = new ArrayList<String>();

		List<TypeInfo> columnTypes = new ArrayList<TypeInfo>();
		List<TypeInfo> partitionTypes = new ArrayList<TypeInfo>();
		List<TypeInfo> columnTypesWP = new ArrayList<TypeInfo>();

		List<ObjectInspector> inspectors = new ArrayList<ObjectInspector>();
		List<ObjectInspector> inspectorsWP = new ArrayList<ObjectInspector>();

		columnNames.addAll(Utilities.getColumnNames(properties));

		if (!columnNames.contains(Constants.DEFAULT_TIMESTAMP_COLUMN)) {
			throw new SerDeException("Timestamp column (' " + Constants.DEFAULT_TIMESTAMP_COLUMN
					+ "') not specified in create table; list of columns is : "
					+ properties.getProperty(serdeConstants.LIST_COLUMNS));
		}
		// data and partition columns
		columnNamesWP.addAll(columnNames);
		columnNamesWP.addAll(toList(properties.getProperty("partition_columns"),"/"));

		columnTypes = TypeInfoUtils.typeInfosFromTypeNames(getColumnTypes(properties));
		partitionTypes = TypeInfoUtils
				.typeInfosFromTypeNames(toList(properties.getProperty("partition_columns.types"),":"));
		columnTypesWP.addAll(columnTypes);
		columnTypesWP.addAll(partitionTypes);

		for (TypeInfo typeInfo : columnTypes) {
			ObjectInspector inspector = TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(typeInfo);
			inspectors.add(inspector);
			inspectorsWP.add(inspector);
		}

		for (TypeInfo typeInfo : partitionTypes) {

			ObjectInspector inspector = TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(typeInfo);
			inspectorsWP.add(inspector);
		}

		columns = columnNames.toArray(new String[columnNames.size()]);
		types = columnTypes.toArray(new TypeInfo[columnTypes.size()]);
		
		columnWithPartitions = columnNamesWP.toArray(new String[columnNamesWP.size()]);
		typeWithPartitions =  columnTypesWP.toArray(new TypeInfo[columnTypesWP.size()]);
		
		inspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, inspectors);
		inspectorWithPartitions = ObjectInspectorFactory.getStandardStructObjectInspector(columnNamesWP, inspectorsWP);

		if (LOG.isDebugEnabled()) {
			LOG.debug("DruidSerDe initialized with\n" + "\t columns: " + columnNames + "\n\t types: " + columnTypes);
		}
	}

	@Override
	public Class<? extends Writable> getSerializedClass() {
		return DruidWritable.class;
	}

	@Override
	public Writable serialize(Object o, ObjectInspector objectInspector) throws SerDeException {
		if (objectInspector.getCategory() != ObjectInspector.Category.STRUCT) {
			throw new SerDeException(getClass().toString() + " can only serialize struct types, but we got: "
					+ objectInspector.getTypeName());
		}

		// Prepare the field ObjectInspectors
		StructObjectInspector soi = (StructObjectInspector) inspectorWithPartitions;
		List<? extends StructField> fields = soi.getAllStructFieldRefs();
		List<Object> values = soi.getStructFieldsDataAsList(o);
		// We deserialize the result
		Map<String, Object> value = new HashMap<>();
		for (int i = 0; i < columnWithPartitions.length; i++) {
			if (values.get(i) == null) {
				// null, we just add it
				value.put(columnWithPartitions[i], null);
				continue;
			}
			final Object res;
			if (typeWithPartitions[i].getCategory().equals(Category.LIST)) {
				//ListObjectInspector listObjectInspector = (ListObjectInspector) fields.get(i).getFieldObjectInspector();
				LazyBinaryArray	array = (LazyBinaryArray)values.get(i);
				res =array.getList();
			} else {
				PrimitiveTypeInfo primitive = (PrimitiveTypeInfo)typeWithPartitions[i];
				switch (primitive.getPrimitiveCategory()) {
				case TIMESTAMP:
					res = ((TimestampObjectInspector) fields.get(i).getFieldObjectInspector())
							.getPrimitiveJavaObject(values.get(i)).getTime();
					break;
				case BYTE:
					res = ((ByteObjectInspector) fields.get(i).getFieldObjectInspector()).get(values.get(i));
					break;
				case SHORT:
					res = ((ShortObjectInspector) fields.get(i).getFieldObjectInspector()).get(values.get(i));
					break;
				case INT:
					res = ((IntObjectInspector) fields.get(i).getFieldObjectInspector()).get(values.get(i));
					break;
				case LONG:
					res = ((LongObjectInspector) fields.get(i).getFieldObjectInspector()).get(values.get(i));
					break;
				case FLOAT:
					res = ((FloatObjectInspector) fields.get(i).getFieldObjectInspector()).get(values.get(i));
					break;
				case DOUBLE:
					res = ((DoubleObjectInspector) fields.get(i).getFieldObjectInspector()).get(values.get(i));
					break;
				case DECIMAL:
					res = ((HiveDecimalObjectInspector) fields.get(i).getFieldObjectInspector())
							.getPrimitiveJavaObject(values.get(i)).doubleValue();
					break;
				case STRING:
					res = ((StringObjectInspector) fields.get(i).getFieldObjectInspector())
							.getPrimitiveJavaObject(values.get(i));
					break;
				default:
					throw new SerDeException("Unknown type: " + primitive.getPrimitiveCategory());
				}
			}
			value.put(columnWithPartitions[i], res);
		}
		// value.put(Constants.DRUID_TIMESTAMP_GRANULARITY_COL_NAME,
		// ((TimestampObjectInspector)
		// fields.get(columns.length).getFieldObjectInspector())
		// .getPrimitiveJavaObject(values.get(columns.length)).getTime()
		// );
		return new DruidWritable(value);
	}

	private List getListObject(ListObjectInspector listObjectInspector, Object listObject) {
		List objectList = listObjectInspector.getList(listObject);
		if(objectList == null)
			return null;
		List list = null;
		ObjectInspector child = listObjectInspector.getListElementObjectInspector();
		switch (child.getCategory()) {
		case PRIMITIVE:
			final PrimitiveObjectInspector primitiveObjectInspector = (PrimitiveObjectInspector) child;
			list = Lists.transform(objectList, new Function() {
				@Nullable
				@Override
				public Object apply(@Nullable Object input) {
					return primitiveObjectInspector.getPrimitiveJavaObject(input);
				}
			});
			break;
		default:
			break;
		}

		return list;
	}

	@Override
	public SerDeStats getSerDeStats() {
		// no support for statistics
		return null;
	}

	@Override
	public Object deserialize(Writable writable) throws SerDeException {
		DruidWritable input = (DruidWritable) writable;
		List<Object> output = Lists.newArrayListWithExpectedSize(columns.length);
		for (int i = 0; i < columns.length; i++) {
			final Object value = input.getValue().get(columns[i]);
			if (value == null) {
				output.add(null);
				continue;
			}
			PrimitiveObjectInspector primitiveObjectInspector = (PrimitiveObjectInspector)types[i];
			switch (primitiveObjectInspector.getPrimitiveCategory()) {
			case TIMESTAMP:
				output.add(new TimestampWritable(new Timestamp((Long) value)));
				break;
			case BYTE:
				output.add(new ByteWritable(((Number) value).byteValue()));
				break;
			case SHORT:
				output.add(new ShortWritable(((Number) value).shortValue()));
				break;
			case INT:
				output.add(new IntWritable(((Number) value).intValue()));
				break;
			case LONG:
				output.add(new LongWritable(((Number) value).longValue()));
				break;
			case FLOAT:
				output.add(new FloatWritable(((Number) value).floatValue()));
				break;
			case DOUBLE:
				output.add(new DoubleWritable(((Number) value).doubleValue()));
				break;
			// case DECIMAL:
			// //output.add(new HiveDecimalWritable(HiveDecimal.create(((Number)
			// value).)));
			// output.add(new HiveDecimalWritable(HiveDecimal.create( ((Number)
			// value));
			// break;
			case STRING:
				output.add(new Text(value.toString()));
				break;
			default:
				throw new SerDeException("Unknown type: " + primitiveObjectInspector.getPrimitiveCategory());
			}
		}
		return output;
	}

	@Override
	public ObjectInspector getObjectInspector() throws SerDeException {
		return inspector;
	}

	// private StandardStructObjectInspector structObjectInspector(Properties
	// tableProperties) {
	// // extract column info - don't use Hive constants as they were renamed
	// // in 0.9 breaking compatibility
	// // the column names are saved as the given inspector to #serialize
	// // doesn't preserves them (maybe because it's an external table)
	// // use the class since StructType requires it ...
	// List<String> columnNames =
	// StringUtils.tokenize(tableProperties.getProperty(serdeConstants.LIST_COLUMNS),
	// ",");
	// List<TypeInfo> colTypes =
	// TypeInfoUtils.typeInfosFromTypeNames(getColumnTypes(tableProperties));
	//
	// // create a standard writable Object Inspector - used later on by
	// // serialization/deserialization
	// List<ObjectInspector> inspectors = new ArrayList<ObjectInspector>();
	//
	// for (TypeInfo typeInfo : colTypes) {
	// inspectors.add(TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(typeInfo));
	// }
	//
	// return
	// ObjectInspectorFactory.getStandardStructObjectInspector(columnNames,
	// inspectors);
	// }

	private List<String> getColumnTypes(Properties props) {
		List<String> names = new ArrayList<String>();
		String colNames = props.getProperty(serdeConstants.LIST_COLUMN_TYPES);
		String[] cols = colNames.trim().split(":");
		if (cols != null) {
			for (String col : cols) {
				if (col != null && !col.trim().equals("")) {
					names.add(col);
				}
			}
		}
		return names;
	}

	private List<String> toList(String str,String split) {
		List<String> names = new ArrayList<String>();
		String[] cols = str.trim().split(split);
		if (cols != null) {
			for (String col : cols) {
				if (col != null && !col.trim().equals("")) {
					names.add(col);
				}
			}
		}
		return names;

	}

}
