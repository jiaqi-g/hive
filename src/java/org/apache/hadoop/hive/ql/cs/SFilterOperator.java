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
	public String prettyString() {
		if (expr != null) {
			return super.prettyString() + " Filter: " + expr.getExprString();
		}
		else {
			return super.prettyString() + "Filter: null";
		}
	}
	
}