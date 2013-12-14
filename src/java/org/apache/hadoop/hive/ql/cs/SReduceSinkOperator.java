package org.apache.hadoop.hive.ql.cs;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.LimitOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;

/**
 * ReduceSinkOperator may have constant ExprNodeDesc
 * 
 * @author victor
 *
 */
public class SReduceSinkOperator extends SOperator {
	
	public SReduceSinkOperator(ReduceSinkOperator op) {
		super(op);
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
				//special
				if ((desc instanceof ExprNodeColumnDesc) && scol.equalsToColumnNodeDesc((ExprNodeColumnDesc) desc)) {
					postMap.put(desc, scol);
					mapped = true;
					break;
				}
			}
			if (!mapped) {
				if (desc instanceof ExprNodeConstantDesc) {
					postMap.put(desc, new SColumn("Constant", "null"));
				}
				else {
					System.out.println("Fatal Error: Can not find mapping from Column Expr to Parent SColumn!");
				}
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
	
}