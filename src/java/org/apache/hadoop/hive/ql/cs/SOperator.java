package org.apache.hadoop.hive.ql.cs;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;

/**
 * Current Supported Operator Types:
 * 
 * 0. FileSinkOperator (Ignore)
 * 1. JoinOperator -- join FD
 * 2. SelectOperator
 * 3. FilterOperator -- filter FD
 * 4. ReduceSinkOperator
 * 5. TableScanOperator
 * 6. LimitOperator
 * 7. GroupByOperator
 * 8. ExtractOperator(order by)
 * 9. 
 * 
 * @author victor
 */
@SuppressWarnings("unchecked")
public class SOperator {
	//1. initialized when built
	Operator op;
	String tableAlias;
	Set<SColumn> columns; //output columns for this operator
	
	//2. initialized with set call
	List<SOperator> parents;
	
	//3. only be called if above call success
	Map<SColumn, SColumn> columnMap; //map from *output* columns to *input* columns
	
	//4. only be called if above call success
	Map<SColumn, SBaseColumn> columnRootMap; //map from *output* columns to original info
	
	//Initial dependency satisfies keys -> all other columns
	//used only for TableScanOperator
	FD rootFD;
	
	//methods for functional dependency tests
	public void setInitialDependencies(FD fd) {
		this.rootFD = fd;
	}
	
	public void setDeterminists(Set<String> determinists) {
		rootFD.determinists = determinists;
	}
	
	public void setDependents(Set<String> dependents) {
		rootFD.dependents = dependents;
	}
	
	public SOperator(Operator op) {
		this.op = op;
		
		//setup Columns
		columns = new HashSet<SColumn>();

		RowSchema schema = op.getSchema();
		for (ColumnInfo info: schema.getSignature()) {
			tableAlias = info.getTabAlias();
			SColumn col = new SColumn(info.getInternalName(), info.getTabAlias());
			columns.add(col);
		}
	}

	public void setColumnMap() {
		columnMap = new HashMap<SColumn, SColumn>();
		
		//intermediate mapping
		setupMapping(op.getColumnExprMap());
	}

	private void setupMapping(Map<String, ExprNodeDesc> columnExprMap) {

		Set<SColumn> allParentsColumns = new HashSet<SColumn>();
		for (SOperator parent: parents) {
			allParentsColumns.addAll(parent.columns);
		}

		Map<ExprNodeDesc, SColumn> postMap = new HashMap<ExprNodeDesc, SColumn>();
		//O(n^2) solution
		for (ExprNodeDesc desc: columnExprMap.values()) {
			boolean mapped = false;
			for (SColumn scol: allParentsColumns) {
				if (scol.equalsToColumnNodeDesc((ExprNodeColumnDesc) desc)) {
					postMap.put(desc, scol);
					mapped = true;
					break;
				}
			}
			if (!mapped) {
				System.out.println("Fatal Error: Can not find mapping from Column Expr to Parent SColumn!");
			}
		}

		//generate final mapping
		for (SColumn col: columns) {
			SColumn correspond = postMap.get(columnExprMap.get(col.getName()));
			if (correspond == null) {
				System.out.println("Fatal Error: Can not find mapping from output Column to input Column!");
			}
			columnMap.put(col, correspond);
		}

	}

	//bean methods

	public void setParents(List<SOperator> parents) {
		this.parents = parents;
	}

	public Map<SColumn, SBaseColumn> getColumnRootMap() {
		return columnRootMap;
	}

	public void setColumnRootMap(Map<SColumn, SBaseColumn> columnRootMap) {
		this.columnRootMap = columnRootMap;
	}

	public String toString() {
		if (columnRootMap == null) {
			return op.getClass() + "\n" + columnMap.toString() + "\n" + "null";
		}
		else {
			return op.getClass() + "\n" + columnMap.toString() + "\n" + columnRootMap.toString();
		}
	}

	public String prettyString() {

		//return columnMap.toString();
		if (columnRootMap != null) {
			return columnRootMap.values().toString();
		}

		return "null";
	}
}