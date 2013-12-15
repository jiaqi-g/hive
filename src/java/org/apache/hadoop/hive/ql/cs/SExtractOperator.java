package org.apache.hadoop.hive.ql.cs;

import java.util.HashMap;

import org.apache.hadoop.hive.ql.exec.ExtractOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;

/**
 * Order By Operator
 * @author victor
 *
 */
public class SExtractOperator extends SOperator {

	public SExtractOperator(ExtractOperator op) {
		super(op);
	}
	
	@Override
	public String prettyString() {
		return super.prettyString() + " Order By ";
	}
}