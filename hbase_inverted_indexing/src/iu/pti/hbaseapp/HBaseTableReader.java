package iu.pti.hbaseapp;

import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseTableReader {
	public static void usage() {
		System.out.println(
				"Usage: java iu.pti.hbaseapp.HBaseTableReader <table name> <column family name> <row type> <cf type> <qualifier type> <value type> <number of rows to read>");
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 7) {
			usage();
			System.exit(1);
		}
		String tableName = args[0];
		String columnFamily = args[1];
		String rowType = args[2].toLowerCase();
		String cfType = args[3].toLowerCase();
		String qualType = args[4].toLowerCase();
		String valType = args[5].toLowerCase();
		int rowCount = Integer.parseInt(args[6]);

		Constants.DataType rowDataType = Constants.DataType.INT;
		Constants.DataType qualDataType = Constants.DataType.INT;
		Constants.DataType cfDataType = Constants.DataType.INT;
		Constants.DataType valDataType = Constants.DataType.INT;

		if (rowType.equals("string")) {
			rowDataType = Constants.DataType.STRING;
		} else if (rowType.equals("double")) {
			rowDataType = Constants.DataType.DOUBLE;
		} else if (rowType.equals("long")) {
			rowDataType = Constants.DataType.LONG;
		}

		if (cfType.equals("string")) {
			cfDataType = Constants.DataType.STRING;
		} else if (cfType.equals("double")) {
			cfDataType = Constants.DataType.DOUBLE;
		} else if (cfType.equals("long")) {
			cfDataType = Constants.DataType.LONG;
		}

		if (qualType.equals("string")) {
			qualDataType = Constants.DataType.STRING;
		} else if (qualType.equals("double")) {
			qualDataType = Constants.DataType.DOUBLE;
		} else if (qualType.equals("long")) {
			qualDataType = Constants.DataType.LONG;
		}

		if (valType.equals("string")) {
			valDataType = Constants.DataType.STRING;
		} else if (valType.equals("double")) {
			valDataType = Constants.DataType.DOUBLE;
		} else if (valType.equals("long")) {
			valDataType = Constants.DataType.LONG;
		}

		Configuration hbaseConfig = HBaseConfiguration.create();
		Connection hbaseConn = ConnectionFactory.createConnection(hbaseConfig);
		Table table = hbaseConn.getTable(TableName.valueOf(tableName));
		// HTable table = new HTable(hbaseConfig, Bytes.toBytes(tableName));
		byte[] cfBytes = null;
		switch (cfDataType) {
		case INT:
			cfBytes = Bytes.toBytes(Integer.valueOf(columnFamily));
			break;
		case STRING:
			cfBytes = Bytes.toBytes(columnFamily);
			break;
		case DOUBLE:
			cfBytes = Bytes.toBytes(Double.valueOf(columnFamily));
			break;
		case LONG:
			cfBytes = Bytes.toBytes(Long.valueOf(columnFamily));
		case UNKNOWN:
			throw new RuntimeException("Unknown data type!!");
		}

		ResultScanner rs = table.getScanner(cfBytes);
		System.out.println("scanning table " + tableName + " on " + columnFamily + "...");
		Result r = rs.next();
		String row = "";
		String qual = "";
		String val = "";
		while (r != null && rowCount > 0) {
			switch (rowDataType) {
			case INT:
				row = Integer.toString(Bytes.toInt(r.getRow()));
				break;
			case STRING:
				row = Bytes.toString(r.getRow());
				break;
			case DOUBLE:
				row = Double.toString(Bytes.toDouble(r.getRow()));
				break;
			case LONG:
				row = Long.toString(Bytes.toLong(r.getRow()));
			case UNKNOWN:
				throw new RuntimeException("Unknown data type!!");
			}

			System.out.println("------------" + row + "------------");
			List<Cell> lkv = r.listCells();
			Iterator<Cell> iter = lkv.iterator();
			while (iter.hasNext()) {
				Cell kv = iter.next();
				switch (qualDataType) {
				case INT:
					qual = Integer.toString(Bytes.toInt(CellUtil.cloneQualifier(kv)));
					break;
				case STRING:
					qual = Bytes.toString(CellUtil.cloneQualifier(kv));
					break;
				case DOUBLE:
					qual = Double.toString(Bytes.toDouble(CellUtil.cloneQualifier(kv)));
					break;
				case LONG:
					qual = Long.toString(Bytes.toLong(CellUtil.cloneQualifier(kv)));
				case UNKNOWN:
					throw new RuntimeException("Unknown data type!!");
				}

				switch (valDataType) {
				case INT:
					val = Integer.toString(Bytes.toInt(CellUtil.cloneValue(kv)));
					break;
				case STRING:
					val = Bytes.toString(CellUtil.cloneValue(kv));
					break;
				case DOUBLE:
					val = Double.toString(Bytes.toDouble(CellUtil.cloneValue(kv)));
					break;
				case LONG:
					val = Long.toString(Bytes.toLong(CellUtil.cloneValue(kv)));
				case UNKNOWN:
					throw new RuntimeException("Unknown data type!!");
				}
				System.out.println(qual + " : " + val);
			}
			r = rs.next();
			rowCount--;
		}
		rs.close();
	}
}
