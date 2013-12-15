package org.apache.hadoop.hive.ql.cs;

import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;

public class SFunction extends SDerivedColumn {
	
	ExprNodeGenericFuncDesc desc;
	
	boolean isGenByAggr = false;

	public SFunction(String name, String tableAlias, SOperator sop, ExprNodeGenericFuncDesc desc) {
		super(name, tableAlias, sop);
		this.desc = desc;
	}

	@Override
	public String toString() {
		return desc.getExprString();
	}
	
	@Override
	public int hashCode() {
		return desc.getExprString().hashCode();
	}

	@Override
	public void setup(int i) {
		for (ExprNodeColumnDesc cd : SDerivedColumn.extractDirectColumnDescs(desc)) {
			String n = cd.getColumn();
			String t = cd.getTabAlias();

			if (n == null) {
				n = "";
			}
			if (t == null) {
				t = "";
			}

			for (SOperator p : sop.parents) {
				for (SDerivedColumn c : p.columns) {
					if (c.equals(n, t)) {
						directlyConnected.add(c);
						return;
					}
				}
			}
		}
	}
}
