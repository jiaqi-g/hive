package org.apache.hadoop.hive.ql.cs;

import org.apache.hadoop.hive.ql.plan.AggregationDesc;

public class SAggregate extends SDerivedColumn {

	AggregationDesc desc;
	SOperator sop;
	
	public SAggregate(AggregationDesc desc, SOperator sop) {
		this.desc = desc;
		this.sop = sop;
	}
	
	@Override
	public boolean isBaseType() {
		return false;
	}
	
	@Override
	public String toString() {
		return desc.getExprString();
	}
}