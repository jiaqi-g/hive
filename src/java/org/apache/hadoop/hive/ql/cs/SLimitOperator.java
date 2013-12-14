package org.apache.hadoop.hive.ql.cs;

import java.util.HashMap;

import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.LimitOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;

import org.apache.hadoop.hive.ql.exec.LimitOperator;

public class SLimitOperator extends SOperator {
	
	int limit;
	
	public SLimitOperator(LimitOperator op) {
		super(op);
		limit = op.getConf().getLimit();
	}
	
	@Override
	public void setColumnMap() {
		columnMap = new HashMap<SColumn, SColumn>();
		if (parents.size() == 0) {
			System.out.println("Fatal Error: SLimitOperator Parents do not exist!");
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
			return columnRootMap.values().toString() + " Limit: " + limit;
		}

		return "null";
	}
}