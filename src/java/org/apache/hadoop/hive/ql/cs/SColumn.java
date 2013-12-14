package org.apache.hadoop.hive.ql.cs;

import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;

public class SColumn implements IColumn {
	
	String name;
	String tableAlias;
	
	/**
	 * construct from ExprNodeColumnDesc
	 * @param desc
	 */
	public SColumn (ExprNodeColumnDesc desc) {
		//columnDesc = desc;
		this.name = desc.getColumn();
		this.tableAlias = desc.getTabAlias();
		
		if (this.name == null) {
			throw new RuntimeException("Column Name should not be null!");
		}
		if (this.tableAlias == null) {
			this.tableAlias = "null";
		}
	}

	/**
	 * construct from name and tableAlias
	 * @return
	 */
	public SColumn (String name, String tableAlias) {
		this.name = name;
		this.tableAlias = tableAlias;
		
		if (this.name == null) {
			throw new RuntimeException("Column Name should not be null!");
		}
		if (this.tableAlias == null) {
			this.tableAlias = "null";
		}
	}
	
	public String getName() {
		return name;
	}

	public String getTableAlias() {
		return tableAlias;
	}

	public boolean equalsToColumnNodeDesc(ExprNodeColumnDesc desc) {
		return this.equals(new SColumn(desc));
	}

	/**
	 * Notice: we only consider name and table alias equal, without considering types equal
	 */
	public boolean equals(Object o) {
		if (!(o instanceof SColumn)) {
			return false;
		}

		SColumn dest = (SColumn) o;
		if (!name.equals(dest.getName())) {
			return false;
		}
		if (!tableAlias.equals(dest.getTableAlias())) {
			return false;
		}
		return true;
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
		hash = hash * 17 + name.hashCode();
		hash = hash * 31 + tableAlias.hashCode();
		return hash;
	}

	public String toString() {
		return tableAlias + "[" + name + "] ";
	}
}