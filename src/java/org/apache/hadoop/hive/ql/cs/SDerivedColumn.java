package org.apache.hadoop.hive.ql.cs;

import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;

public abstract class SDerivedColumn extends SAbstractColumn {
	
	@Override
	public boolean isBaseType() {
		return false;
	}
	
}