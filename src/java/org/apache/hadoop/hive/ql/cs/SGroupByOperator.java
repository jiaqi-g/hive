package org.apache.hadoop.hive.ql.cs;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.LimitOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;

/**
 * Group By Operator will probably generate new columns, if there are "having" after it
 * Group By column will use column name such as "key" "value" constructed from the previous operators,
 * without considering the table name
 * 
 * Having is represented as a filter
 * 
 * @author victor
 *
 */
public class SGroupByOperator extends SOperator {

	GroupByDesc groupByDesc;
	boolean isDistinct = false;
	
	public SGroupByOperator(GroupByOperator op) {
		super(op);
		groupByDesc = op.getConf();
		
		if ((groupByDesc.getAggregators() == null) || (groupByDesc.getAggregators().size() == 0)) {
			isDistinct = true;
		}
	}
	
	@Override
	public void setColumnMap() {
		columnMap = new HashMap<SColumn, SColumn>();
		
		//intermediate mapping
		Map<String, ExprNodeDesc> columnExprMap = op.getColumnExprMap();
		
		Set<SColumn> allParentsColumns = new HashSet<SColumn>();
		for (SOperator parent: parents) {
			allParentsColumns.addAll(parent.columns);
		}

		Map<ExprNodeDesc, SColumn> postMap = new HashMap<ExprNodeDesc, SColumn>();
		//O(n^2) solution
		for (ExprNodeDesc desc: columnExprMap.values()) {
			boolean mapped = false;
			for (SColumn scol: allParentsColumns) {
				//special -- only column name matching is enough
				if (scol.name.equals(((ExprNodeColumnDesc)desc).getColumn())) {
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
				correspond = col;
			}
			columnMap.put(col, correspond);
		}
	}
	
	@Override
	public String prettyString() {
		if (columnRootMap != null) {
			String s = columnRootMap.toString() + " Group By: ";
			if (groupByDesc.getAggregators() != null) {
				s += "Aggregators: ";
				for (AggregationDesc desc: groupByDesc.getAggregators()) {
					s += desc.getExprString() + " ";
				}
			}
			
			if (groupByDesc.getKeys() != null) {
				s += "Keys: ";
				for (ExprNodeDesc key: groupByDesc.getKeys()) {
					s += key.getExprString() + " ";
				}
			}
			
			return s;
		}
		
		return "null";
	}
}