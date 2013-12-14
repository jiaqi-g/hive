package org.apache.hadoop.hive.ql.cs;

public class SBaseColumn implements IColumn {
	
	STableScanOperator sop;
	String name;
	
	/**
	 * construct from name and tableAlias
	 * @return
	 */
	public SBaseColumn (String name, STableScanOperator sop) {
		this.name = name;
		this.sop = sop;
		
		if (this.name == null || this.sop == null) {
			throw new RuntimeException("Column Name or Sop should not be null!");
		}
	}
	
	public SBaseColumn (SColumn scol, STableScanOperator sop) {
		this(scol.name, sop);
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
		return "BASE: " + sop.tableName + "[" + name + "]";
	}
	
	/**
	 * Unique id for columns
	 * @return
	 */
	public String getId() {
		return sop.id + name;
	}
}