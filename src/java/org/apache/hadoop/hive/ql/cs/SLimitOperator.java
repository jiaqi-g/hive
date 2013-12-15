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
	public String prettyString() {
		return super.prettyString() + " Limit: " + limit;
	}
}