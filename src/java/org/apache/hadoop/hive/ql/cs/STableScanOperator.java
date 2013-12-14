package org.apache.hadoop.hive.ql.cs;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.TableScanOperator;

/**
 * 
 * STableScanOperator is special, its mapping from cols itself to itself, but in two different types
 * @author victor
 *
 */
public class STableScanOperator extends SOperator {
	
	/**
	 * Table original name, not alias
	 */
	String tableName;
	Long id = 0L;
	
	public STableScanOperator(TableScanOperator op) {
		super(op);
		
		id = UniqueIdGenerater.getNextId();
	}
	
	public void setTableName(String tableName){
		this.tableName = tableName;
	}
	
	public Set<String> getAllColumnsIds() {
		Set<String> hashSet = new HashSet<String>();
		for (SBaseColumn bcol: columnRootMap.values()) {
			hashSet.add(bcol.getId());
		}
		return hashSet;
	}
	
	@Override
	public void setColumnMap() {
		columnMap = new HashMap<SColumn, SColumn>();
		
		//root mapping
		for (SColumn col: columns) {
			columnMap.put(col, col);
		}
	}
	
	@Override
	public String prettyString() {
		if (columnRootMap != null) {
			return columnRootMap.values().toString();
		}
		return "null";
	}
}