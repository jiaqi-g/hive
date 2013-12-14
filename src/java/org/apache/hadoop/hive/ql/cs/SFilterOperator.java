package org.apache.hadoop.hive.ql.cs;

import java.util.HashMap;

import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;

public class SFilterOperator extends SOperator {
	
	ExprNodeDesc expr;
	
	public SFilterOperator(FilterOperator op) {
		super(op);
		expr = (op).getConf().getPredicate();
	}
	
	public ExprNodeDesc getExpr() {
		return expr;
	}
	
	@Override
	public void setColumnMap() {
		columnMap = new HashMap<SColumn, SColumn>();
		if (parents.size() == 0) {
			System.out.println("Fatal Error: SFilterOperator Parents do not exist!");
		}
		else {
			for (SColumn scol: parents.get(0).columns) {
				columnMap.put(scol, scol);
			}
		}
	}
	
	@Override
	public String prettyString() {
		if (columnRootMap != null) {
			if (expr != null) {
				return columnRootMap.values().toString() + " Filter: " + expr.getExprString();
			}
			else {
				return columnRootMap.values().toString() + "Filter: null";
			}
		}

		return "null";
	}
}