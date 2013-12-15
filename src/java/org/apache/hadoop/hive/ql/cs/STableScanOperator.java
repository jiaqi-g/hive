package org.apache.hadoop.hive.ql.cs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.TableScanOperator;

class RootColumnNotFoundException extends RuntimeException {
	private static final long serialVersionUID = 1L;
}

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
	Map<SColumn, SBaseColumn> baseColumnMap;
	
	public STableScanOperator(TableScanOperator op) {
		super(op);
		
		id = UniqueIdGenerater.getNextId();
		setupBaseColumns();
	}
	
	private void setupBaseColumns() {
		baseColumnMap = new HashMap<SColumn, SBaseColumn>();
		for (SColumn scol: columns) {
			//convert sCol to sBaseCcol
			baseColumnMap.put(scol, new SBaseColumn(scol, this, scol.tableAlias));
		}
	}

	public void setTableName(String tableName){
		this.tableName = tableName;
	}
	
	@Override
	public SBaseColumn getRootColumn(SColumn scol) {
		SBaseColumn bcol =  baseColumnMap.get(scol);
		if (bcol == null) {
			throw new RootColumnNotFoundException();
		} else {
			return bcol;
		}
	}
}