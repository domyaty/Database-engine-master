package elZ3amaa;

import java.util.Date;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Hashtable;
import java.util.Vector;

import BPTree.*;

public class Table implements Serializable {
	static final long serialVersionUID = 1L;

	int page_capacity;
	int NodeSize;
	String strTableName;
	String strClusteringKeyColumn;
	Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
	Vector<String> pages = new Vector<String>();

	public Table() {

	}

	public Table(String strTableName, String strClusteringKeyColumn, Hashtable<String, String> htblColNameType,
			int page_capacity, int NodeSize) {
		this.strTableName = strTableName;
		this.strClusteringKeyColumn = strClusteringKeyColumn;
		this.htblColNameType = htblColNameType;
		this.page_capacity = page_capacity;
		this.NodeSize = NodeSize;
	}

	public int which_page(Hashtable<String, Object> htblColNameValue) {
		int i;
		for (i = 0; i < pages.size(); i++) {
			Object value_of_clusterKey = htblColNameValue.get(strClusteringKeyColumn);
			Page p = new Page();
			p = Page.deserializePage(pages.get(i));
			if (p.record.size() < page_capacity && !(i == pages.size() - 1)) {
				Page p2 = new Page();
				p2 = Page.deserializePage(pages.get(i + 1));
				if (!p2.record.isEmpty()) {
					Object smallest_element = p2.record.get(0).get(this.strClusteringKeyColumn);
					if (value_of_clusterKey instanceof Integer && smallest_element instanceof Integer) {
						int d1 = (int) value_of_clusterKey;
						int d2 = (int) smallest_element;
						if (d1 < d2) {
							return i;
						} else {
							continue;
						}
					} else if (value_of_clusterKey instanceof Double && smallest_element instanceof Double) {
						Double d1 = (Double) value_of_clusterKey;
						Double d2 = (Double) smallest_element;
						if (d1 < d2) {
							return i;
						} else {
							continue;
						}
					} else if (value_of_clusterKey instanceof String && smallest_element instanceof String) {
						String d1 = (String) value_of_clusterKey;
						String d2 = (String) smallest_element;
						if (d1.compareToIgnoreCase(d2) > 0) {
							return i;
						} else {
							continue;
						}
					} else if (value_of_clusterKey instanceof Date && smallest_element instanceof Date) {
						Date d1 = (Date) value_of_clusterKey;
						Date d2 = (Date) smallest_element;
						if (d1.compareTo(d2) < 0) {
							return i;
						} else {
							continue;
						}
					}
				}
			} else if (p.record.size() < page_capacity && (i == pages.size() - 1)) {
				return i;
			} else {
				Object max_key_in_page = p.getMaxKey();
				if (value_of_clusterKey instanceof Integer && max_key_in_page instanceof Integer) {
					int d1 = (int) value_of_clusterKey;
					int d2 = (int) max_key_in_page;
					if (d1 < d2) {
						return i;
					} else {
						continue;
					}
				} else if (value_of_clusterKey instanceof Double && max_key_in_page instanceof Double) {
					Double d1 = (Double) value_of_clusterKey;
					Double d2 = (Double) max_key_in_page;
					if (d1 < d2) {
						return i;
					} else {
						continue;
					}
				} else if (value_of_clusterKey instanceof String && max_key_in_page instanceof String) {
					String d1 = (String) value_of_clusterKey;
					String d2 = (String) max_key_in_page;
					if (d1.compareToIgnoreCase(d2) > 0) {
						return i;
					} else {
						continue;
					}
				} else if (value_of_clusterKey instanceof Date && max_key_in_page instanceof Date) {
					Date d1 = (Date) value_of_clusterKey;
					Date d2 = (Date) max_key_in_page;
					if (d1.compareTo(d2) < 0) {
						return i;
					} else {
						continue;
					}
				}
			}
		}

		return i;
	}

	public void insertIntoTable(Hashtable<String, Object> htblColNameValue) {
		Table t = new Table();
		t = Table.deserializeTable(strTableName);

//		int num = which_page(htblColNameValue);

		int num = binarySearchOnPages(getClusteringInfo().get("cluster"),
				htblColNameValue.get(getClusteringInfo().get("cluster")).toString());

		if (num == pages.size()) {
			String page_name = strTableName + (pages.size() + 1);
			Page p1 = new Page(t.strClusteringKeyColumn, page_name);
			pages.add(page_name);
			int indxRecord = p1.insertIntoPage(htblColNameValue, strClusteringKeyColumn);
			System.out.println(indxRecord);
			insertInAllBPTree(htblColNameValue, num, indxRecord);
			Page.serializePage(p1, page_name);
			return;
		}

		Page p2 = new Page();
		p2 = Page.deserializePage(pages.get(num));

		if (p2.record.size() < page_capacity) {
			int indxRecord = p2.insertIntoPage(htblColNameValue, strClusteringKeyColumn);
			insertInAllBPTree(htblColNameValue, num, indxRecord);
			System.out.println(indxRecord);
			Page.serializePage(p2, p2.page_name);
			return;
		} else {
			int indxRecord = p2.insertIntoPage(htblColNameValue, strClusteringKeyColumn);
			insertInAllBPTree(htblColNameValue, num, indxRecord);
			System.out.println(indxRecord);
			Page.serializePage(p2, p2.page_name);
			if (!(p2.record.size() <= page_capacity)) {
				boolean updated = false;
				for (int i = num; i < pages.size(); i++) {

					if (i == pages.size() - 1 && !(p2.record.size() <= page_capacity)) {
						if (indxRecord == page_capacity - 1) {
							num++;
							indxRecord = 0;
						}
						String page_name = strTableName + (pages.size() + 1);
						Page p1 = new Page(strClusteringKeyColumn, page_name);
						pages.add(page_name);
						Page.serializePage(p1, page_name);
						t.UpdateRefInTree(num, indxRecord);
						Page n = new Page();
						n = Page.deserializePage(this.pages.get(i));
						p1.insertIntoPage(n.record.get(n.record.size() - 1), strClusteringKeyColumn);
						n.record.remove(n.record.size() - 1);
						Page.serializePage(p1, p1.page_name);
						Page.serializePage(n, n.page_name);
						return;
					}

					if (i == pages.size() - 1)
						return;

					Page n = new Page();
					n = Page.deserializePage(this.pages.get(i));
					Page m = new Page();
					m = Page.deserializePage(this.pages.get(i + 1));

					m.record.add(0, n.record.get(n.record.size() - 1));
					n.record.remove(n.record.size() - 1);

					if (m.record.size() <= t.page_capacity) {
						Page.serializePage(n, n.page_name);
						Page.serializePage(m, m.page_name);
						break;
					}

					Page.serializePage(n, n.page_name);
					Page.serializePage(m, m.page_name);
				}
			}
		}

	}

//	public void UpdateRefInTree(int startPage, int startRecord) {
//		
//		for (int i = startPage; i < pages.size(); i++) {
//		
//			Page p = Page.deserializePage(this.pages.get(i));
//			if (i > startPage) {
//				startRecord = 0;
//			}
//			for (int j = startRecord + 1; j < p.record.size(); j++) {
//				
//			}
//	
//		}
//		
//	}

	public void UpdateRefInTree(int startPage, int startRecord) {

		Page p = Page.deserializePage(this.pages.get(startPage));

		int st = startRecord;

		Hashtable<String, Object> htblColNameValue = p.record.get(startRecord);
		String cluster = this.getClusteringInfo().get("cluster");
		String index = "";

		for (int a = 0; a < this.getIndex().size(); a++) {

			index = this.getIndex().get(a).get("index");

			if (htblColNameValue.get(index) instanceof Integer) {
				if ((this.getIndex().get(a).get("index")).equals(cluster)) {
					continue;
				}
				if (htblColNameValue.get(index) == null) {
					continue;
				}

				BPTree<Integer> tree = deserializeTree(strTableName + index);
				BPTreeLeafNode l = tree.getFirstLeaf();
				while (l != null) {
					for (int j1 = 0; j1 < l.numberOfKeys; j1++) {
						boolean pass = true;
						Ref r = tree.search((int) l.getKey(j1));
						int PN = r.getPageNo();
						int IIP = r.getIndexInPage();
						Vector<int[]> OV = r.getOverFlow();
						if (PN < startPage)
							pass = false;
						if (PN == startPage && IIP < st) {
							st = 0;
							pass = false;
						}
						if (IIP >= this.page_capacity - 1 && pass) {
							PN++;
							IIP = 0;
						} else if (pass) {
							IIP++;
						}
						r.setRef(PN, IIP);
						if (OV.size() != 0) {
							for (int j11 = 0; j11 < OV.size(); j11++) {
								pass = true;
								int[] arr = OV.get(j11);
								int PN2 = arr[0];
								int IIP2 = arr[1];
								if (PN2 < startPage)
									pass = false;
								if (PN2 == startPage && IIP2 < st) {
									st = 0;
									pass = false;
								}
								if (IIP2 >= this.page_capacity - 1 && pass) {
									PN2++;
									IIP2 = 0;
								} else if (pass) {
									IIP2++;
								}
								int[] arr2 = { PN2, IIP2 };
								r.setElementInOverFlow(j11, arr2);
								serializeTree(tree, index);
							}
						}
						l.setRecord(j1, r);
					}
					l = l.getNext();
				}
			} else if (htblColNameValue.get(index) instanceof Double) {
				if ((this.getIndex().get(a).get("index")).equals(cluster)) {
					continue;
				}
				if (htblColNameValue.get(index) == null) {
					continue;
				}
				BPTree<Double> tree = deserializeTree(strTableName + index);
				BPTreeLeafNode l = tree.getFirstLeaf();
				while (l != null) {

					for (int j1 = 0; j1 < l.numberOfKeys; j1++) {
						boolean pass = true;
						Ref r = tree.search((double) l.getKey(j1));
						int PN = r.getPageNo();
						int IIP = r.getIndexInPage();
						Vector<int[]> OV = r.getOverFlow();
						if (PN < startPage)
							pass = false;
						if (PN == startPage && IIP < st) {
							st = 0;
							pass = false;
						}
						if (IIP >= this.page_capacity - 1 && pass) {
							PN++;
							IIP = 0;
						} else if (pass) {
							IIP++;
						}
						r.setRef(PN, IIP);
						if (OV.size() != 0) {
							for (int j11 = 0; j11 < OV.size(); j11++) {
								pass = true;
								int[] arr = OV.get(j11);
								int PN2 = arr[0];
								int IIP2 = arr[1];
								if (PN2 < startPage)
									pass = false;
								if (PN2 == startPage && IIP2 < st) {
									st = 0;
									pass = false;
								}
								if (IIP2 >= this.page_capacity - 1 && pass) {
									PN2++;
									IIP2 = 0;
								} else if (pass) {
									IIP2++;
								}
								int[] arr2 = { PN2, IIP2 };
								r.setElementInOverFlow(j11, arr2);
								serializeTree(tree, index);
							}
						}
					}
					l = l.getNext();
				}
			} else if (htblColNameValue.get(index) instanceof String) {
				if ((this.getIndex().get(a).get("index")).equals(cluster)) {
					continue;
				}
				if (htblColNameValue.get(index) == null) {
					continue;
				}
				BPTree<String> tree = deserializeTree(strTableName + index);
				BPTreeLeafNode l = tree.getFirstLeaf();
				while (l != null) {

					for (int j1 = 0; j1 < l.numberOfKeys; j1++) {
						boolean pass = true;
						Ref r = tree.search((String) l.getKey(j1));
						int PN = r.getPageNo();
						int IIP = r.getIndexInPage();
						Vector<int[]> OV = r.getOverFlow();
						if (PN < startPage)
							pass = false;
						if (PN == startPage && IIP < st) {
							st = 0;
							pass = false;
						}
						if (IIP >= this.page_capacity - 1 && pass) {
							PN++;
							IIP = 0;
						} else if (pass) {
							IIP++;
						}
						r.setRef(PN, IIP);
						if (OV.size() != 0) {
							for (int j11 = 0; j11 < OV.size(); j11++) {
								pass = true;
								int[] arr = OV.get(j11);
								int PN2 = arr[0];
								int IIP2 = arr[1];
								if (PN2 < startPage)
									pass = false;
								if (PN2 == startPage && IIP2 < st) {
									st = 0;
									pass = false;
								}
								if (IIP2 >= this.page_capacity - 1 && pass) {
									PN2++;
									IIP2 = 0;
								} else if (pass) {
									IIP2++;
								}
								int[] arr2 = { PN2, IIP2 };
								r.setElementInOverFlow(j11, arr2);
								serializeTree(tree, index);
							}
						}
					}
					l = l.getNext();
				}
			} else {
				if ((this.getIndex().get(a).get("index")).equals(cluster)) {
					continue;
				}
				if (htblColNameValue.get(index) == null) {
					continue;
				}
				BPTree<Date> tree = deserializeTree(strTableName + index);
				BPTreeLeafNode l = tree.getFirstLeaf();
				while (l != null) {

					for (int j1 = 0; j1 < l.numberOfKeys; j1++) {
						boolean pass = true;
						Ref r = tree.search((Date) l.getKey(j1));
						int PN = r.getPageNo();
						int IIP = r.getIndexInPage();
						Vector<int[]> OV = r.getOverFlow();
						if (PN < startPage)
							pass = false;
						if (PN == startPage && IIP < st) {
							st = 0;
							pass = false;
						}
						if (IIP >= this.page_capacity - 1 && pass) {
							PN++;
							IIP = 0;
						} else if (pass) {
							IIP++;
						}
						r.setRef(PN, IIP);
						if (OV.size() != 0) {
							for (int j11 = 0; j11 < OV.size(); j11++) {
								pass = true;
								int[] arr = OV.get(j11);
								int PN2 = arr[0];
								int IIP2 = arr[1];
								if (PN2 < startPage)
									pass = false;
								if (PN2 == startPage && IIP2 < st) {
									st = 0;
									pass = false;
								}
								if (IIP2 >= this.page_capacity - 1 && pass) {
									PN2++;
									IIP2 = 0;
								} else if (pass) {
									IIP2++;
								}
								int[] arr2 = { PN2, IIP2 };
								r.setElementInOverFlow(j11, arr2);
								serializeTree(tree, index);
							}
						}
					}
					l = l.getNext();
				}
			}
		}
//		}

//		}

	}

	public void insertInAllBPTree(Hashtable<String, Object> htblValue, int pageNum, int indexRecord) {
		String cluster = this.getClusteringInfo().get("cluster");
		String index = "";
		System.out.println(pageNum + " " + indexRecord);
		for (int i = 0; i < this.getIndex().size(); i++) {
			if ((this.getIndex().get(i).get("index")).equals(cluster)) {
				continue;
			}

			index = this.getIndex().get(i).get("index");

			if (htblValue.get(index) == null)
				continue;

			if (htblValue.get(index) instanceof Integer) {
				BPTree<Integer> tree = new BPTree<Integer>(this.NodeSize);
				String TreeName = strTableName + index;
				int x = (int) htblValue.get(index);
				tree = Table.deserializeTree(TreeName);

				if (indexRecord >= this.page_capacity) {
					pageNum++;
					indexRecord = 0;
				}
				if (tree.contain(x)) {
					Ref r = tree.search(x);
					if (r.getOverFlow() == null) {
						Vector<int[]> overflow = new Vector<int[]>();
						r.setOverflow(overflow);
					}
					int[] refer = { pageNum, indexRecord };
					r.addInOverFlow(refer);

				} else {
					Vector<int[]> overflow = new Vector<int[]>();
					Ref r = new Ref(pageNum, indexRecord, overflow);
					tree.insert(x, r);
				}

				Table.serializeTree(tree, TreeName);
			}

			else if (htblValue.get(index) instanceof Double) {
				BPTree<Double> tree = new BPTree<Double>(this.NodeSize);
				String TreeName = strTableName + index;
				double x = (double) htblValue.get(index);
				tree = Table.deserializeTree(TreeName);

				if (indexRecord >= this.page_capacity) {
					pageNum++;
					indexRecord = 0;
				}
				if (tree.contain(x)) {
					Ref r = tree.search(x);
					if (r.getOverFlow() == null) {
						Vector<int[]> overflow = new Vector<int[]>();
						r.setOverflow(overflow);
					}
					int[] refer = { pageNum, indexRecord };
					r.addInOverFlow(refer);

				} else {
					Vector<int[]> overflow = new Vector<int[]>();
					Ref r = new Ref(pageNum, indexRecord, overflow);
					tree.insert(x, r);
				}

				Table.serializeTree(tree, TreeName);
			}

			else if (htblValue.get(index) instanceof String) {
				BPTree<String> tree = new BPTree<String>(this.NodeSize);
				String TreeName = strTableName + index;
				String x = (String) htblValue.get(index);
				tree = Table.deserializeTree(TreeName);

				if (indexRecord >= this.page_capacity) {
					pageNum++;
					indexRecord = 0;
				}
				if (tree.contain(x)) {
					Ref r = tree.search(x);
					if (r.getOverFlow() == null) {
						Vector<int[]> overflow = new Vector<int[]>();
						r.setOverflow(overflow);
					}
					int[] refer = { pageNum, indexRecord };
					r.addInOverFlow(refer);

				} else {
					Vector<int[]> overflow = new Vector<int[]>();
					Ref r = new Ref(pageNum, indexRecord, overflow);
					tree.insert(x, r);
				}

				Table.serializeTree(tree, TreeName);
			}

			else if (htblValue.get(index) instanceof Date) {
				BPTree<Date> tree = new BPTree<Date>(this.NodeSize);
				String TreeName = strTableName + index;
				Date x = (Date) htblValue.get(index);
				tree = Table.deserializeTree(TreeName);

				if (indexRecord >= this.page_capacity) {
					pageNum++;
					indexRecord = 0;
				}
				if (tree.contain(x)) {
					Ref r = tree.search(x);
					if (r.getOverFlow() == null) {
						Vector<int[]> overflow = new Vector<int[]>();
						r.setOverflow(overflow);
					}
					int[] refer = { pageNum, indexRecord };
					r.addInOverFlow(refer);

				} else {
					Vector<int[]> overflow = new Vector<int[]>();
					Ref r = new Ref(pageNum, indexRecord, overflow);
					tree.insert(x, r);
				}

				Table.serializeTree(tree, TreeName);
			}

		}
	}

	public void insertIntoTableUsingIndex(Hashtable<String, Object> htblColNameValue) {

		String cluster = this.getClusteringInfo().get("cluster");
		String index = "";
		for (int i = 0; i < this.getIndex().size(); i++) {
			if ((this.getIndex().get(i).get("index")).equals(cluster)) {
				index = this.getIndex().get(i).get("index");
				break;
			}
		}

		int pageNum, indexInPage;
		BPTree tree = deserializeTree(strTableName + index);
		if (tree.contain((Comparable) htblColNameValue.get(index))) {
			Ref r = tree.search((Comparable) htblColNameValue.get(index));
			Vector<int[]> overflow = (Vector<int[]>) r.getOverFlow();
			if (!overflow.isEmpty()) {
				int[] arr = (int[]) overflow.get(overflow.size() - 1);
				pageNum = arr[0];
				indexInPage = arr[1];
				if (indexInPage >= this.page_capacity) {
					pageNum++;
					indexInPage = 0;
				} else {
					indexInPage++;
				}
			} else {
				pageNum = r.getPageNo();
				indexInPage = r.getIndexInPage();
				if (indexInPage >= this.page_capacity) {
					pageNum++;
					indexInPage = 0;
				} else {
					indexInPage++;
				}
			}

			if (pageNum == this.pages.size() && indexInPage == 0) {
				String newPage = strTableName + (pages.size() + 1);
				Page p1 = new Page(strClusteringKeyColumn, newPage);
				pages.add(newPage);
				Page.serializePage(p1, p1.page_name);
			}

			String pageName = strTableName + (pageNum + 1);
			Page p = Page.deserializePage(pageName);
			p.record.add(indexInPage, htblColNameValue);
			this.UpdateRefInTree(pageNum, indexInPage);
			Page.serializePage(p, pageName);

			int[] newValue = { pageNum, indexInPage };
			r.addInOverFlow(newValue);

			BPTreeLeafNode l = tree.getLeafNode((Comparable) htblColNameValue.get(index));

			while (l != null) {
				for (int i = 0; i < l.numberOfKeys; i++) {
					if (l.getKey(i).equals(htblColNameValue.get(index)))
						continue;
					int PN = tree.search((Comparable) l.getKey(i)).getPageNo();
					int IIP = tree.search((Comparable) l.getKey(i)).getIndexInPage();
					Vector<int[]> OV = tree.search((Comparable) l.getKey(i)).getOverFlow();
					if (IIP >= this.page_capacity) {
						PN++;
						IIP = 0;
					} else {
						IIP++;
					}
					tree.search((Comparable) l.getKey(i)).setRef(PN, IIP);
					if (OV.size() != 0) {
						for (int j = 0; j < OV.size(); j++) {
							int[] arr = OV.get(j);
							int PN2 = arr[0];
							int IIP2 = arr[1];
							if (IIP2 >= this.page_capacity) {
								PN2++;
								IIP2 = 0;
							} else {
								IIP2++;
							}
							int[] arr2 = { PN2, IIP2 };
							tree.search((Comparable) l.getKey(i)).setElementInOverFlow(j, arr2);
						}
					}
				}
				l = l.getNext();
			}
		}

		if (!tree.contain((Comparable) htblColNameValue.get(index))) {

		}

		serializeTree(tree, strTableName + index);
	}

	public void updateTable(String strClusteringKey, Hashtable<String, Object> htblColNameValue) {

		int indexOfPage = binarySearchOnPages(strClusteringKeyColumn, strClusteringKey);
		int pageNumber = indexOfPage + 1;

		while (true) {

			Page p = new Page();
			p = Page.deserializePage(strTableName + pageNumber);

			p.updatePage(strClusteringKey, htblColNameValue);

			if (pageNumber == pages.size())
				break;

			pageNumber++;
			p = Page.deserializePage(strTableName + pageNumber);

			if (!(p.record.get(0).get(strClusteringKeyColumn).toString()).equals(strClusteringKey))
				break;
		}
	}

	public void deleteFromTable(Hashtable<String, Object> htblColNameValue) {

		String strClusteringKey = htblColNameValue.get(this.strClusteringKeyColumn).toString();

		int indexOfPage = binarySearchOnPages(strClusteringKeyColumn, strClusteringKey);
		int pageNumber = indexOfPage + 1;

		while (true) {

			Page p = new Page();
			p = Page.deserializePage(strTableName + pageNumber);

			p.deleteFromPage(strClusteringKey, htblColNameValue);

			if (pageNumber == pages.size())
				break;

			pageNumber++;
			p = Page.deserializePage(strTableName + pageNumber);

			if (!p.record.isEmpty()) {
				if (!(p.record.get(0).get(strClusteringKeyColumn).toString()).equals(strClusteringKey))
					break;
			} else {
				pageNumber++;
				p = Page.deserializePage(strTableName + pageNumber);
				if (!(p.record.get(0).get(strClusteringKeyColumn).toString()).equals(strClusteringKey))
					break;
			}

		}

		boolean empty = false;

		for (int i = 0; i < pages.size(); i++) {
			Page p = new Page();
			p = Page.deserializePage(pages.get(i));
			if (p.record.isEmpty()) {
				empty = true;
				Page.deletePage(p.page_name);
				this.pages.remove(i);
				i--;
			}
		}

		serializeTable(this, this.strTableName);

		if (empty) {

			for (int i = 0; i < pages.size(); i++) {

				Page p = new Page();
				p = Page.deserializePage(pages.get(i));
				String instanceName = strTableName + (i + 1);

				if ((p.page_name).equals(instanceName)) {
					continue;
				}

				Page.deletePage(p.page_name);
				p.page_name = instanceName;
				Page.serializePage(p, instanceName);
				pages.set(i, instanceName);

			}

			serializeTable(this, this.strTableName);

		}

	}

	public int binarySearchOnPages(String columnName, String clusterOfInput) {

		int first = 0;
		int last = pages.size() - 1;
		int mid = (first + last) / 2;

		while (first <= last) {

			if (first == mid && mid == last)
				break;

			if (first == mid && first == pages.size() - 2)
				return mid + 1;

			if (first == mid && last == 1)
				return mid;

			Page p = new Page();
			p = Page.deserializePage(pages.get(mid));

			if (clusterOfInput.matches("-?\\d+?")) {

				String maxInPage = p.record.get(p.record.size() - 1).get(columnName).toString();
				String minInPage = p.record.get(0).get(columnName).toString();

				int max = Integer.parseInt(maxInPage);
				int min = Integer.parseInt(minInPage);
				int cluster = Integer.parseInt(clusterOfInput);

				if ((cluster == max || cluster == min) && first != 0) {
					while (mid != 0) {
						Page p2 = new Page();
						p2 = Page.deserializePage(pages.get(mid - 1));
						String maxInPage2 = p2.record.get(p2.record.size() - 1).get(columnName).toString();
						int max2 = Integer.parseInt(maxInPage2);
						if (cluster == max2) {
							mid--;
						} else {
							break;
						}
					}
					return mid;
				}

				else if ((cluster == max || cluster == min) && first == 0) {
					while (mid != 0) {
						Page p2 = new Page();
						p2 = Page.deserializePage(pages.get(mid - 1));
						String maxInPage2 = p2.record.get(p2.record.size() - 1).get(columnName).toString();
						int max2 = Integer.parseInt(maxInPage2);
						if (cluster == max2) {
							mid--;
						} else {
							break;
						}
					}
					return mid;
				}

				else if (max < cluster) {
					first = mid;
				}

				else if (min < cluster && cluster < max && first != 0) {
					return mid;
				}

				else if (min < cluster && cluster < max && first == 0) {
					while (mid != 0) {
						Page p2 = new Page();
						p2 = Page.deserializePage(pages.get(mid - 1));
						String maxInPage2 = p2.record.get(p2.record.size() - 1).get(columnName).toString();
						int max2 = Integer.parseInt(maxInPage2);
						if (cluster == max2) {
							mid--;
						} else {
							break;
						}
					}
					return mid;
				}

				else {
					last = mid;
				}

				mid = (first + last) / 2;
				continue;

			} else if (clusterOfInput.matches("-?\\d+(\\.\\d+)?")) {

				String maxInPage = p.record.get(p.record.size() - 1).get(columnName).toString();
				String minInPage = p.record.get(0).get(columnName).toString();

				Double max = Double.parseDouble(maxInPage);
				Double min = Double.parseDouble(minInPage);
				Double cluster = Double.parseDouble(clusterOfInput);

				if ((cluster == max || cluster == min) && first != 0) {
					while (mid != 0) {
						Page p2 = new Page();
						p2 = Page.deserializePage(pages.get(mid - 1));
						String maxInPage2 = p2.record.get(p2.record.size() - 1).get(columnName).toString();
						Double max2 = Double.parseDouble(maxInPage2);
						if (cluster == max2) {
							mid--;
						} else {
							break;
						}
					}
					return mid;
				}

				else if ((cluster == max || cluster == min) && first == 0) {
					while (mid != 0) {
						Page p2 = new Page();
						p2 = Page.deserializePage(pages.get(mid - 1));
						String maxInPage2 = p2.record.get(p2.record.size() - 1).get(columnName).toString();
						Double max2 = Double.parseDouble(maxInPage2);
						if (cluster == max2) {
							mid--;
						} else {
							break;
						}
					}
					return mid;
				}

				else if (max < cluster) {
					first = mid;
				}

				else if (min < cluster && cluster < max && first != 0) {
					return mid;
				}

				else if (min < cluster && cluster < max && first == 0) {
					while (mid != 0) {
						Page p2 = new Page();
						p2 = Page.deserializePage(pages.get(mid - 1));
						String maxInPage2 = p2.record.get(p2.record.size() - 1).get(columnName).toString();
						Double max2 = Double.parseDouble(maxInPage2);
						if (cluster == max2) {
							mid--;
						} else {
							break;
						}
					}
					return mid;
				}

				else {
					last = mid;
				}

				mid = (first + last) / 2;
				continue;

			}

			else if (clusterOfInput.matches("^[ A-Za-z]+$")) {

				String maxInPage = p.record.get(p.record.size() - 1).get(columnName).toString();
				String minInPage = p.record.get(0).get(columnName).toString();

				if ((clusterOfInput.equals(maxInPage) || clusterOfInput.equals(minInPage)) && first != 0) {
					while (mid != 0) {
						Page p2 = new Page();
						p2 = Page.deserializePage(pages.get(mid - 1));
						String maxInPage2 = p2.record.get(p2.record.size() - 1).get(columnName).toString();
						if (clusterOfInput.equals(maxInPage2)) {
							mid--;
						} else {
							break;
						}
					}
					return mid;
				}

				else if ((clusterOfInput.equals(maxInPage) || clusterOfInput.equals(minInPage)) && first == 0) {
					while (mid != 0) {
						Page p2 = new Page();
						p2 = Page.deserializePage(pages.get(mid - 1));
						String maxInPage2 = p2.record.get(p2.record.size() - 1).get(columnName).toString();
						if (clusterOfInput.equals(maxInPage2)) {
							mid--;
						} else {
							break;
						}
					}
					return mid;
				}

				else if (maxInPage.compareToIgnoreCase(clusterOfInput) < 0) {
					first = mid;
				}

				else if (minInPage.compareToIgnoreCase(clusterOfInput) < 0
						&& clusterOfInput.compareToIgnoreCase(maxInPage) < 0 && first != 0) {
					return mid;
				}

				else if (minInPage.compareToIgnoreCase(clusterOfInput) < 0
						&& clusterOfInput.compareToIgnoreCase(maxInPage) < 0 && first == 0) {
					while (mid != 0) {
						Page p2 = new Page();
						p2 = Page.deserializePage(pages.get(mid - 1));
						String maxInPage2 = p2.record.get(p2.record.size() - 1).get(columnName).toString();
						if (clusterOfInput.equals(maxInPage2)) {
							mid--;
						} else {
							break;
						}
					}
					return mid;
				}

				else {
					last = mid;
				}

				mid = (first + last) / 2;
				continue;

			}

			else {

				String maxInPage = p.record.get(p.record.size() - 1).get(columnName).toString();
				String minInPage = p.record.get(0).get(columnName).toString();

				if ((clusterOfInput.equals(maxInPage) || clusterOfInput.equals(minInPage)) && first != 0) {
					while (mid != 0) {
						Page p2 = new Page();
						p2 = Page.deserializePage(pages.get(mid - 1));
						String maxInPage2 = p2.record.get(p2.record.size() - 1).get(columnName).toString();
						if (clusterOfInput.equals(maxInPage2)) {
							mid--;
						} else {
							break;
						}
					}
					return mid;
				}

				else if ((clusterOfInput.equals(maxInPage) || clusterOfInput.equals(minInPage)) && first == 0) {
					while (mid != 0) {
						Page p2 = new Page();
						p2 = Page.deserializePage(pages.get(mid - 1));
						String maxInPage2 = p2.record.get(p2.record.size() - 1).get(columnName).toString();
						if (clusterOfInput.equals(maxInPage2)) {
							mid--;
						} else {
							break;
						}
					}
					return mid;
				}

				else if (maxInPage.compareToIgnoreCase(clusterOfInput) < 0) {
					first = mid;
				}

				else if (minInPage.compareToIgnoreCase(clusterOfInput) < 0
						&& clusterOfInput.compareToIgnoreCase(maxInPage) < 0 && first != 0) {
					return mid;
				}

				else if (minInPage.compareToIgnoreCase(clusterOfInput) < 0
						&& clusterOfInput.compareToIgnoreCase(maxInPage) < 0 && first == 0) {
					while (mid != 0) {
						Page p2 = new Page();
						p2 = Page.deserializePage(pages.get(mid - 1));
						String maxInPage2 = p2.record.get(p2.record.size() - 1).get(columnName).toString();
						if (clusterOfInput.equals(maxInPage2)) {
							mid--;
						} else {
							break;
						}
					}
					return mid;
				}

				else {
					last = mid;
				}

				mid = (first + last) / 2;
				continue;

			}

		}

		return mid;

	}

	public Vector<Hashtable<String, String>> getIndex() {
		Vector<Hashtable<String, String>> v = new Vector<Hashtable<String, String>>();
		try {
			BufferedReader br = new BufferedReader(new FileReader("data/metadata.csv"));
			while (br.ready()) {
				String[] line = br.readLine().split(",");
				if (!line[0].equals(this.strTableName))
					continue;

				if (line[4].equals("true")) {
					Hashtable<String, String> x = new Hashtable<String, String>();
					x.put("index", line[1]);
					x.put("type", line[2]);
					v.add(x);
				}

			}
			br.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return v;
	}

	public Hashtable<String, String> getClusteringInfo() {
		Hashtable<String, String> x = new Hashtable<String, String>();
		try {
			BufferedReader br = new BufferedReader(new FileReader("data/metadata.csv"));
			while (br.ready()) {
				String[] line = br.readLine().split(",");
				if (!line[0].equals(this.strTableName))
					continue;

				if (line[3].equals("true")) {
					x.put("cluster", line[1]);
					x.put("type", line[2]);
					return x;
				}

			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return x;
	}

	public static void serializeTable(Table t, String name) {
		try {
			FileOutputStream fileOut = new FileOutputStream("data/" + name + ".class");
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(t);
			out.close();
			fileOut.close();
		} catch (IOException i) {
			i.printStackTrace();
		}
	}

	public static Table deserializeTable(String name) {
		Table t = new Table();
		try {
			FileInputStream fileIn = new FileInputStream("data/" + name + ".class");
			ObjectInputStream in = new ObjectInputStream(fileIn);
			t = (Table) in.readObject();
			in.close();
			fileIn.close();
		} catch (IOException i) {
			i.printStackTrace();
		} catch (ClassNotFoundException c) {
			System.out.println("Table class not found");
			c.printStackTrace();
		}
		return t;
	}

	public static void serializeTree(BPTree t, String name) {
		try {
			FileOutputStream fileOut = new FileOutputStream("data/" + name + ".class");
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(t);
			out.close();
			fileOut.close();
		} catch (IOException i) {
			i.printStackTrace();
		}
	}

	public static BPTree deserializeTree(String name) {
		BPTree t = new BPTree();
		try {
			FileInputStream fileIn = new FileInputStream("data/" + name + ".class");
			ObjectInputStream in = new ObjectInputStream(fileIn);
			t = (BPTree) in.readObject();
			in.close();
			fileIn.close();
		} catch (IOException i) {
			i.printStackTrace();
		} catch (ClassNotFoundException c) {
			System.out.println("Table class not found");
			c.printStackTrace();
		}
		return t;
	}

}