package org.apache.hadoop.hive.ql.cs;

import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;

public class SConstant extends SDerivedColumn {
	ExprNodeConstantDesc desc;
	
	public SConstant(ExprNodeConstantDesc desc) {
		this.desc = desc;
	}

	@Override
	public boolean isBaseType() {
		return true;
	}
	
}