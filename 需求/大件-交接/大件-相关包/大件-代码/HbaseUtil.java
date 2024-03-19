package com.sf.realtime.hbase;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

import com.alibaba.fastjson.JSONObject;
import com.sf.realtime.common.utils.MD5Util;
import com.sf.realtime.hbase.common.ColumnType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class HbaseUtil implements Serializable{
	private static Connection connection;
	private static Configuration configuration;
	private volatile static HbaseUtil hbaseUtil;
	public HBaseAdmin admin = null; //NOSONAR

	private HbaseUtil() throws Exception { //NOSONAR
	}

	@SuppressWarnings("deprecation")
	public void init() throws Exception {
		Config config = com.sf.realtime.common.config.Config.getConfig();
		if (configuration == null) {
			configuration = HBaseConfiguration.create(); //NOSONAR
		}
		try {
			// zookeeper集群的URL配置信息
			configuration.set("hbase.zookeeper.quorum", config.getString("hbase.zk.quorum"));
			// 客户端连接zookeeper端口
			configuration.set("hbase.zookeeper.property.clientPort", config.getString("hbase.zk.client.port"));
			// hbase集群模式
			configuration.set("zookeeper.znode.parent", "/hbase");
			// HBase RPC请求超时时间，默认60s(60000)
			configuration.setInt("hbase.rpc.timeout", 120000);
			// 客户端重试最大次数，默认35
			configuration.setInt("hbase.client.retries.number", 10);
			// 客户端发起一次操作数据请求直至得到响应之间的总超时时间，可能包含多个RPC请求，默认为2min
			configuration.setInt("hbase.client.operation.timeout", 360000);
			// 客户端发起一次scan操作的rpc调用至得到响应之间的总超时时间
			configuration.setInt("hbase.client.scanner.timeout.period", 200000);
			// 获取hbase连接对象
			if (connection == null || connection.isClosed()) {
				connection = ConnectionFactory.createConnection(configuration); //NOSONAR
			}
			admin = new HBaseAdmin(configuration);
		} catch (Exception e) {
			System.out.println("[Failed to init the HBase] Error message:"+e.toString()); //NOSONAR
			throw e;
		}
	}

	public static void close() {
		try {
			if (connection != null)
				connection.close();
		} catch (IOException e) { //NOSONAR

		}
	}

	public static Connection getConn() throws Exception {
		if (connection != null){
			return connection;
		}else{
			getInstance();
			return connection;
		}

	}

	public static void batchFlush(String tableName,List<Put> puts) throws IOException {
		BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(tableName));
		params.writeBufferSize(5*1024*1024);
		BufferedMutator mutator = connection.getBufferedMutator(params);
		mutator.mutate(puts);
		mutator.flush();
		System.out.println("完成提交（success submit）："+mutator.getWriteBufferSize());
		puts.clear();
	}

	public static HbaseUtil getInstance() throws Exception {
		if (hbaseUtil == null) {
			synchronized (HbaseUtil.class) {
				if (hbaseUtil == null) {
					hbaseUtil = new HbaseUtil();
					hbaseUtil.init();
				}
			}
		}
		return hbaseUtil;
	}

	public void save(String tableName, List<Put> puts) throws IOException {
		Table table = connection.getTable(TableName.valueOf(tableName));
		table.put(puts);
		table.close();
	}

	/**
	 * 根据表名，rowKey，列族名，列名得到一行数据
	 * @param tableName 表名
	 * @param rowKey rowKey
	 * @param cfName 列族名
	 * @param qualifiers 多列名
	 * @return Result
	 * @throws IOException
	 */
	public Result getRowMultiColumns(String tableName, byte[] rowKey, byte[] cfName, List<String> qualifiers) throws Exception {
		Table table = null;
		Result result = null;
		try{
			if(connection == null){
				synchronized (HbaseUtil.class){
					if(connection == null)
						init();
				}
			}
			table = connection.getTable(TableName.valueOf(tableName));
			Get get = new Get(rowKey);
			for(String qualifier:qualifiers)
				get.addColumn(cfName, Bytes.toBytes(qualifier));
			result = table.get(get);
		}finally {
			if(table!= null)
				table.close();
		}
		return result;
	}

	public void truncate(String tableName) throws IOException {
		admin.disableTable(TableName.valueOf(tableName));
		admin.truncateTable(TableName.valueOf(tableName), true);
	}

	public void scan(String tableName)throws IOException{
		Table table = null;
		Result result = null;
		try {
			table = connection.getTable(TableName.valueOf(tableName));
			Scan scan = new Scan();
			ResultScanner scanner = table.getScanner(scan);
			while ((result = scanner.next()) != null){
				List<Cell> cells = result.listCells();
				for (Cell cell : cells) {
					// 打印rowkey,family,qualifier,value
					System.out.println(Bytes.toString(CellUtil.cloneRow(cell))
							+ "==> " + Bytes.toString(CellUtil.cloneFamily(cell))
							+ "{" + Bytes.toString(CellUtil.cloneQualifier(cell))
							+ ":" + Bytes.toString(CellUtil.cloneValue(cell)) + "}");
				}
			}
		}finally {
			if(table != null){
				table.close();
			}
		}
	}

	public boolean checkExist(String tablename, String rowKey) throws IOException {
		Table table = null;
		Result result = null;
		try {
			table = connection.getTable(TableName.valueOf(tablename));
			Get get = new Get(Bytes.toBytes(rowKey));
			get.setCheckExistenceOnly(true);
			result = table.get(get);
		} finally {
			table.close();
		}
		return result.getExists();
	}

	public Result getRow(String tablename, byte[] row) throws IOException {
		Table table = null;
		Result result = null;
		try {
			if(connection==null || connection.isClosed()){
				synchronized (HbaseUtil.class){
					if(connection == null)
						init();
				}
			}
			table = connection.getTable(TableName.valueOf(tablename));
			Get get = new Get(row);
			result = table.get(get);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(table != null) {
				table.close();
			}
		}
		return result;
	}

	public Map<String,Map<String,Object>> getListSpecialForWbInfo(List<String> waybills,Map<String,ColumnType> columns) throws Exception{
		Table table = null;
		Result[] results = null;
		Map<String,Map<String,Object>> r = new HashMap<String,Map<String,Object>>();
		try{
			if(connection==null){
				synchronized (HbaseUtil.class){
					if(connection == null)
						init();
				}
			}
			String tableName = "wb_info_data_sf";
			table = connection.getTable(TableName.valueOf(tableName));
			Set waybillSet = new HashSet<String>(waybills);
			Map waybillNewTransform = new HashMap<String,String>();
			List gets = new ArrayList<Get>();
			//1.查询新表
			for(String waybill : waybills){
				String rowKey = MD5Util.getMD5(waybill);
				Get get = new Get(Bytes.toBytes(rowKey));
				for(String column : columns.keySet()){
					get.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes(column));
				}
				gets.add(get);
				waybillNewTransform.put(rowKey,waybill);
			}
			System.out.println("查询新表：" + waybillSet.size());
			results = table.get(gets);
			analyzeResult(results,columns,waybillNewTransform,r,waybillSet);
			//2.查询旧表
			if(waybillSet.size()>0){
				columns.clear();
				columns.put("hewbCalcWeight",ColumnType.STRING);
				tableName = "wb_info_data_sx";
				table = connection.getTable(TableName.valueOf(tableName));
				gets = new ArrayList<Get>();
				for(Object waybill : waybillSet.toArray()){
					String waybillNo = waybill.toString();
					String rowKey = new StringBuffer(waybillNo).reverse().toString();
					Get get = new Get(Bytes.toBytes(rowKey));
					for(String column : columns.keySet()){
						get.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes(column));
					}
					gets.add(get);
					waybillNewTransform.put(rowKey,waybillNo);
				}
				System.out.println("查询旧表：" + waybillSet.size());
				results = table.get(gets);
				analyzeResult(results,columns,waybillNewTransform,r,waybillSet);
			}
			if(waybillSet.size()>0){
				System.out.println("有运单查询未命中：" + waybillSet.size());
				for(Object waybill : waybillSet.toArray()) {
					String waybillNo = waybill.toString();
					System.out.println(waybillNo);
				}
			}
		}finally {
			if(table!=null)
				table.close();
		}
		return r;
	}

	public Map<String,Object> getResult(String tableName, String rowKey, String familyName) throws Exception {
		Table table = connection.getTable(TableName.valueOf(tableName));
		// 获得一行
		Get get = new Get(Bytes.toBytes(rowKey));
		get.addFamily(Bytes.toBytes(familyName));
		Result set = table.get(get);
		Map<String,Object> map = new HashMap<>();
		if(set != null && set.listCells() != null) {
			for (Cell cell : set.listCells()) {
				long timestamp = cell.getTimestamp();
				String family = Bytes.toString(CellUtil.cloneFamily(cell));
				String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
				if(qualifier.equals("packageMeterageWeightQty") || qualifier.equals("meterageWeightQty")){
					Double value = Bytes.toDouble(CellUtil.cloneValue(cell));
					map.put(qualifier, value);
				}else{
					String value = Bytes.toString(CellUtil.cloneValue(cell));
					map.put(qualifier, value);
				}
			}
		}
		table.close();
		return map;
	}

	public Object getResult(String tableName, String rowKey, String family, String columnName, ColumnType type) throws Exception {
		if(connection==null || connection.isClosed()){
			synchronized (HbaseUtil.class){
				if(connection == null)
					init();
			}
		}
		Table table = connection.getTable(TableName.valueOf(tableName));
		// 获得一行
		Get get = new Get(Bytes.toBytes(rowKey));
		get.addColumn(Bytes.toBytes(family),Bytes.toBytes(columnName));
		Result set = table.get(get);
		List<Object> values = new ArrayList<>();
		if(set != null && set.listCells() != null) {
			for (Cell cell : set.listCells()) {
				long timestamp = cell.getTimestamp();
				String familyName = Bytes.toString(CellUtil.cloneFamily(cell));
				String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
				Object value;
				switch (type) {
					case STRING:
						value = Bytes.toString(CellUtil.cloneValue(cell));
						break;
					case DOUBLE:
						value = Bytes.toDouble(CellUtil.cloneValue(cell));
						break;
					case LONG:
						value = Bytes.toLong(CellUtil.cloneValue(cell));
						break;
					case INTEGER:
						value = Bytes.toInt(CellUtil.cloneValue(cell));
						break;
					default:
						value = Bytes.toString(CellUtil.cloneValue(cell));
						break;
				}
				values.add(value);
			}
		}
		table.close();
		return values.size()==0?null:values.get(0);
	}

	public Map<String,Object> getResult(String tableName, String rowKey, String family, List<String> columnNames, ColumnType type) throws Exception {
		if(connection==null || connection.isClosed()){
			synchronized (HbaseUtil.class){
				if(connection == null)
					init();
			}
		}
		Table table = connection.getTable(TableName.valueOf(tableName));
		// 获得一行
		Get get = new Get(Bytes.toBytes(rowKey));
		for(String columnName : columnNames){
			get.addColumn(Bytes.toBytes(family),Bytes.toBytes(columnName));
		}

		Result set = table.get(get);
		Map<String,Object> values = new HashMap<>();
		if(set != null && set.listCells() != null) {
			for (Cell cell : set.listCells()) {
				long timestamp = cell.getTimestamp();
				String familyName = Bytes.toString(CellUtil.cloneFamily(cell));
				String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
				Object value;
				switch (type) {
					case STRING:
						value = Bytes.toString(CellUtil.cloneValue(cell));
						break;
					case DOUBLE:
						value = Bytes.toDouble(CellUtil.cloneValue(cell));
						break;
					case LONG:
						value = Bytes.toLong(CellUtil.cloneValue(cell));
						break;
					case INTEGER:
						value = Bytes.toInt(CellUtil.cloneValue(cell));
						break;
					default:
						value = Bytes.toString(CellUtil.cloneValue(cell));
						break;
				}
				values.put(qualifier,value);
			}
		}
		table.close();
		return values;
	}

	public List<String> getAllTables() {
		List<String> tables = null;
		if (admin != null) {
			try {
				HTableDescriptor[] allTable = admin.listTables();
				if (allTable.length > 0){
					tables = new ArrayList<String>();
					for (HTableDescriptor hTableDescriptor : allTable) {
						tables.add(hTableDescriptor.getNameAsString());

					}
				}
			} catch (IOException e) { //NOSONAR

			}
		}
		return tables;
	}

	/**
	 * 根据表名，rowKey，列族名，列名得到一行数据
	 * @param tableName 表名
	 * @param rowKey rowKey
	 * @param cfName 列族名
	 * @param qualifier 列名
	 * @return Result
	 * @throws IOException
	 */
	public Result getRow(String tableName, byte[] rowKey, byte[] cfName, byte[] qualifier) throws Exception {
		Table table = null;
		Result result = null;
		try{
			if(connection == null){
				synchronized (HbaseUtil.class){
					if(connection == null)
						init();
				}
			}
			table = connection.getTable(TableName.valueOf(tableName));
			Get get = new Get(rowKey);
			get.addColumn(cfName, qualifier);
			result = table.get(get);
		}finally {
			if(table!= null)
				table.close();
		}
		return result;
	}

	public Map<String,Object> getResult(String tableName, String rowKey, String family, List<String> columnNames, Map<String,ColumnType> types) throws Exception {
		if(connection==null || connection.isClosed()){
			synchronized (HbaseUtil.class){
				if(connection == null)
					init();
			}
		}
		Table table = connection.getTable(TableName.valueOf(tableName));
		// 获得一行
		Get get = new Get(Bytes.toBytes(rowKey));
		for(String columnName : columnNames){
			get.addColumn(Bytes.toBytes(family),Bytes.toBytes(columnName));
		}

		Result set = table.get(get);
		Map<String,Object> values = new HashMap<>();
		if(set != null && set.listCells() != null) {
			for (Cell cell : set.listCells()) {
				long timestamp = cell.getTimestamp();
				String familyName = Bytes.toString(CellUtil.cloneFamily(cell));
				String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
				Object value;
				switch (types.get(qualifier)) {
					case STRING:
						value = Bytes.toString(CellUtil.cloneValue(cell));
						break;
					case DOUBLE:
						value = Bytes.toDouble(CellUtil.cloneValue(cell));
						break;
					case LONG:
						value = Bytes.toLong(CellUtil.cloneValue(cell));
						break;
					case INTEGER:
						value = Bytes.toInt(CellUtil.cloneValue(cell));
						break;
					default:
						value = Bytes.toString(CellUtil.cloneValue(cell));
						break;
				}
				values.put(qualifier,value);
			}
		}
		table.close();
		return values;
	}

	public List<Result> scan(String tableName, byte[] rowKeyStart, byte[] rowKeyEnd) throws Exception {
		Table table = null;
		List<Result> results = new ArrayList<>();
		ResultScanner scanner = null;
		try {
			if(connection==null){
				synchronized (HbaseUtil.class){
					if(connection == null)
						init();
				}
			}
			table = connection.getTable(TableName.valueOf(tableName));
			Scan scan = new Scan(rowKeyStart, rowKeyEnd);
			scanner = table.getScanner(scan);
			for(int i=0; i<10; i++){
				results.add(scanner.next());
			}

		}finally{
			if(table != null)
				table.close();
			if(scanner != null)
				scanner.close();
		}
		return results;
	}

	/**
	 * test
	 * @param tableName
	 * @throws Exception
	 */
	public void scantest(String tableName) throws Exception{
		Table table = null;
		ResultScanner scanner = null;
		try {
			if(connection==null){
				synchronized (HbaseUtil.class){
					if(connection == null)
						init();
				}
			}

			table = connection.getTable(TableName.valueOf(tableName));
			Scan scan = new Scan(Bytes.toBytes(String.valueOf(0)));
			scanner = table.getScanner(scan);

			for(int k = 0; k <1000; k++){
				Result r  = scanner.next();
				Map<String, Object> columnMap = new HashMap<>();
				String rowKey = null;

				List<Cell> list  = r.listCells();
				for(int i=0; i < list.size() && i <=2; i++){
					Cell cell = list.get(i);
					if (rowKey == null) {
						rowKey = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
					}
					columnMap.put(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()), Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
				}

				if (rowKey != null) {
					System.out.println(rowKey + "---" + new JSONObject(columnMap).toJSONString());
				}

			}
		}catch (Exception e){

		}
		finally{
			if(table != null)
				table.close();
			if(scanner != null)
				scanner.close();
		}
	}

	/**
	 * HBase 插入一条数据
	 * @param tablename 表名
	 * @param rowKey 唯一标识
	 * @param cfName 列族名
	 * @param qualifier 列标识
	 * @param data 数据
	 * @return 是否插入成功
	 * @throws IOException
	 */
	public boolean putRow(String tablename, byte[] rowKey, byte[] cfName, byte[] qualifier, byte[] data) throws IOException {
		Table table = null;
		try{
			table = connection.getTable(TableName.valueOf(tablename));
			Put put = new Put(rowKey);
			put.addColumn(cfName, qualifier, data);
			table.put(put);
		}finally{
			if(table !=null){
				table.close();
			}
		}
		return true;
	}

	public boolean putList(String tablename, List<Put> puts) throws Exception {
		Table table = null;
		try{
			if(connection==null){
				synchronized (HbaseUtil.class){
					if(connection == null)
						init();
				}
			}
			table = connection.getTable(TableName.valueOf(tablename));
			table.put(puts);
		}finally{
			if(table!=null)
				table.close();
		}
		return true;
	}

	public Result[] getList(String tableName, List<Get> gets) throws Exception{
		Table table = null;
		Result[] results = null;
		try{
			if(connection==null){
				synchronized (HbaseUtil.class){
					if(connection == null)
						init();
				}
			}
			table = connection.getTable(TableName.valueOf(tableName));
			results = table.get(gets);
		}finally {
			if(table!=null)
				table.close();
		}
		return results;
	}

	/**
	 * Append value to one or multiple columns
	 * @param tablename
	 * @param append
	 * @return
	 * @throws IOException
	 */
	public boolean appendRow(String tablename, Append append) throws IOException{
		Table table = null;
		try{
			table = connection.getTable(TableName.valueOf(tablename));
			table.append(append);
		}finally{
			if(table!=null)
				table.close();
		}
		return true;
	}

	/**
	 * Method that does a batch call on Deletes, Gets, Puts, Increments and Appends.
	 * The ordering of execution of the actions is not defined. Meaning if you do a Put and a
	 * Get in the same call, you will not necessarily be
	 * guaranteed that the Get returns what the Put had put.
	 * @param tableName
	 * @param actions
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public Object[] actBatch(String tableName, final List<? extends Row> actions) throws Exception {
		Table table = null;
		Object[] results = new Object[actions.size()];
		try {
			if(connection == null){
				synchronized (HbaseUtil.class){
					if(connection == null){
						init();
					}
				}
			}
			table = connection.getTable(TableName.valueOf(tableName));
			table.batch(actions, results);
		} finally{
			if(table!=null)
				table.close();
		}
		return results;
	}


	private void analyzeResult(Result[] results,Map<String,ColumnType> columns,Map transform, Map<String,Map<String,Object>> r,Set waybillSet){
		if(results !=null && results.length >0){
			for(Result rt : results){
				if (rt != null && !rt.isEmpty()){
					List<Cell> cells = rt.listCells();
					String rowkey = Bytes.toString(rt.getRow());
					String packageNo = (String)transform.get(rowkey);
					waybillSet.remove(packageNo);
					Map<String,Object> innerR = new HashMap<>();
					for (Cell cell : cells) {
						String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
						Object value;
						switch (columns.get(qualifier)) {
							case DOUBLE:
								value = Bytes.toDouble(CellUtil.cloneValue(cell));
								break;
							case LONG:
								value = Bytes.toLong(CellUtil.cloneValue(cell));
								break;
							case INTEGER:
								value = Bytes.toInt(CellUtil.cloneValue(cell));
								break;
							default:
								value = Bytes.toString(CellUtil.cloneValue(cell));
								break;
						}
						if(qualifier.equals("hewbCalcWeight")){
							innerR.put("packageMeterageWeightQty", Double.parseDouble((String)value));
						}else {
							innerR.put(qualifier, value);
						}
					}
					r.put(packageNo,innerR);
				}
			}
		}
	}


	public static void main(String[] args) throws Exception {

	}
}
