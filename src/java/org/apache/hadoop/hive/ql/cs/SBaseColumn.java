package org.apache.hadoop.hive.ql.cs;

public class SBaseColumn extends SAbstractColumn {
	
	STableScanOperator sop;
	String name;
	String alias;
	
	/**
	 * construct from name and tableAlias
	 * @return
	 */
	public SBaseColumn (String name, STableScanOperator sop, String alias) {
		this.name = name;
		this.sop = sop;
		this.alias = alias;
		
		if (this.name == null || this.sop == null) {
			throw new RuntimeException("Column Name or Sop should not be null!");
		}
	}
	
	public SBaseColumn (SColumn scol, STableScanOperator sop, String alias) {
		this(scol.name, sop, alias);
	}
	
	public String getName() {
		return name;
	}

	public STableScanOperator getSTableScanOperator() {
		return sop;
	}
	
	public boolean equals(Object o) {
		if (!(o instanceof SBaseColumn)) {
			return false;
		}

		SBaseColumn dest = (SBaseColumn) o;
		if (!name.equals(dest.getName())) {
			return false;
		}
		if (!sop.equals(dest.getSTableScanOperator())) {
			return false;
		}
		
		return true;
	}

	/**
	 * rewrite hashCode() is a must since we use hashMap to store info
	 * 
	 */
	@Override
	public int hashCode() {
		int hash = 1;
		hash = hash * 17 + name.hashCode();
		hash = hash * 31 + sop.hashCode();
		return hash;
	}

	public String toString() {
		return "BASE: " + sop.tableName + "(" + alias +")"+ "[" + name + "]";
	}
	
	/**
	 * Unique id for columns
	 * @return
	 */
	public String getId() {
		return sop.id + name;
	}

	@Override
	public boolean isBaseType() {
		return true;
	}
}