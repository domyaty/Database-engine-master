package BPTree;

import java.io.Serializable;
import java.util.Vector;

public class Ref implements Serializable {

	/**
	 * This class represents a pointer to the record. It is used at the leaves of
	 * the B+ tree
	 */
	private static final long serialVersionUID = 1L;
	public int pageNo, indexInPage;
	public Vector<int[]> overflow;

	public void setElementInOverFlow(int index, int[] arr) {
		this.overflow.set(index, arr);
	}

	public int getPageNo() {
		return pageNo;
	}

	public void setPageNo(int pageNo) {
		this.pageNo = pageNo;
	}

	public int getIndexInPage() {
		return indexInPage;
	}

	public void setIndexInPage(int indexInPage) {
		this.indexInPage = indexInPage;
	}

	public Vector<int[]> getOverFlow() {
		return this.overflow;
	}

	public void setOverflow(Vector<int[]> overflow) {
		this.overflow = overflow;
	}

	public void addInOverFlow(int[] x) {
		this.overflow.add(x);
	}

	public void setRef(int pageNo, int indexInPage) {
		this.pageNo = pageNo;
		this.indexInPage = indexInPage;
	}

	public Ref(int pageNo, int indexInPage, Vector<int[]> overflow) {
		this.pageNo = pageNo;
		this.indexInPage = indexInPage;
		this.overflow = overflow;
	}

}
