package org.apache.hadoop.hive.ql.cs;

import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;

public class SColumn extends SDerivedColumn {

	ExprNodeColumnDesc desc;

	/**
	 * construct from ExprNodeColumnDesc
	 * @param desc
	 */
	public SColumn (String name, String tableAlias, SOperator sop, ExprNodeColumnDesc desc) {
		super(name, tableAlias, sop);
		this.desc = desc;
	}

	public String getName() {
		return name;
	}

	public String getTableAlias() {
		return tableAlias;
	}

	/**
	 * rewrite hashCode() is a must since we use hashMap to store info
	 * 
	 * Potential problem:
	 * if two subqueries use a same column and a same table name, then we can not make a difference between them.
	 */
	@Override
	public int hashCode() {
		int hash = 1;
		hash = hash * 17 + ((name == null) ? 0 : name.hashCode());
		hash = hash * 31 + ((tableAlias == null) ? 0 : tableAlias.hashCode());
		return hash;
	}

	@Override
	public void setup(int i) {
		String n = desc.getColumn();
		String t = desc.getTabAlias();
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
			if (p instanceof SGroupByOperator) {
				directlyConnected.add(((SGroupByOperator)p).getAggregateAt(i));
				return;
			}
		}
	}

}