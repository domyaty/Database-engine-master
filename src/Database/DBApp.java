package elZ3amaa;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.Stack;
import java.util.Vector;
import BPTree.*;
import elZ3amaa.DBAppException;

public class DBApp implements Serializable {

	static final long serialVersionUID = 1L;
	DateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	Vector<String> tablenames = new Vector<String>();
	Vector<String> indecies = new Vector<String>();
	static Properties prop = new Properties();
	static int page_capacity;
	static int NodeSize;

	public DBApp() {
		tableNames();
		try {
			prop.load(new FileInputStream("config/DBApp.properties"));
			page_capacity = Integer.parseInt(prop.getProperty("MaximumRowsCountinPage"));
			NodeSize = Integer.parseInt(prop.getProperty("NodeSize"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void tableNames() {
		try {
			BufferedReader br = new BufferedReader(new FileReader(new File("data/metadata.csv")));
			while (br.ready()) {
				String[] line = br.readLine().split(",");
				tablenames.add(line[0]);
			}
			br.close();
		} catch (FileNotFoundException e) {
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws DBAppException {

		for (int i = 0; i < tablenames.size(); i++) {
			if (tablenames.get(i).equals(strTableName)) {
				throw new DBAppException("This table already exists in the DataBase");
			}
		}
		htblColNameType.put("TouchDate", "java.util.Date");
		tablenames.add(strTableName);
		Table t = new Table(strTableName, strClusteringKeyColumn, htblColNameType, page_capacity, NodeSize);
		Table.serializeTable(t, strTableName);
		CSV(htblColNameType, strTableName, strClusteringKeyColumn);
	}

	public void CSV(Hashtable<String, String> h, String name, String key) {
		Set entrySet = h.entrySet();
		Object[] arr = entrySet.toArray();
		FileWriter fw = null;
		try {
			fw = new FileWriter("data/metadata.csv", true);
			for (int i = 0; i < arr.length; i++) {
				String[] words = arr[i].toString().split("=");
				String colname = words[0];
				String coltype = words[1];
				fw.append(name);
				fw.append(",");
				fw.append(colname);
				fw.append(",");
				fw.append(coltype);
				fw.append(",");
				if (key.equals(colname))
					fw.append("true");
				else
					fw.append("false");
				fw.append(",");
				fw.append("false");
				fw.append("\n");
			}
			fw.flush();
			fw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {

		if (!tablenames.contains(strTableName))
			throw new DBAppException("The Table does not exist");
		checkDataTypes(strTableName, htblColNameValue);
		htblColNameValue.put("TouchDate", (new Date()));

		Table t = new Table();
		t = Table.deserializeTable(strTableName);

		String cluster = t.getClusteringInfo().get("cluster");
		String index = "";
		for (int i = 0; i < t.getIndex().size(); i++) {
			System.out.println(t.getIndex().size());
			String s = t.getIndex().get(i).get("index");
			if (s.equals(cluster)) {
				index = cluster;
				break;
			}
			else {
				index = s;
			}
		}

		if (t.pages.size() == 0) {
			String pagename = strTableName + (t.pages.size() + 1);
			Page p = new Page(t.strClusteringKeyColumn, pagename);
			p.insertIntoPage(htblColNameValue, t.strClusteringKeyColumn);
			t.pages.add(pagename);
			if (index != "") {
				BPTree tree = t.deserializeTree(strTableName + index);
				Vector<int[]> v = new Vector<int[]>();
				Ref r = new Ref(0, 0, v);
				tree.insert((Comparable) htblColNameValue.get(index), r);
				Table.serializeTree(tree, strTableName + index);
			}
			Page.serializePage(p, pagename);
		}

		else if (cluster.equals(index)) {
			t.insertIntoTableUsingIndex(htblColNameValue);
		}

		else {
			t.insertIntoTable(htblColNameValue);
		}
		Table.serializeTable(t, strTableName);
	}

	public static void checkDataTypes(String tableName, Hashtable<String, Object> htblColNameValue)
			throws DBAppException {
		Hashtable<String, String> types = getDatatypes(tableName);

		Table t = new Table();
		t = Table.deserializeTable(tableName);
		String clustring = t.getClusteringInfo().get("cluster");
		if (!htblColNameValue.containsKey(clustring))
			throw new DBAppException("There is no a clustering key in the input to insert by");

		for (String col_type : htblColNameValue.keySet()) {
			if (!types.containsKey(col_type))
				throw new DBAppException("Sorry this table does not have the coloumn " + col_type);
			if (!types.get(col_type).equals(htblColNameValue.get(col_type).getClass().toString()))
				throw new DBAppException("Sorry the coloumn " + col_type + " does not have this datatype");
		}
	}

	public static Hashtable<String, String> getDatatypes(String tablename) {
		FileReader fr;
		Hashtable<String, String> datatypes = new Hashtable<>();
		try {
			fr = new FileReader("data/metadata.csv");
			BufferedReader br = new BufferedReader(fr);
			while (br.ready()) {
				String[] tmp = br.readLine().split(",");
				if (tmp[1].equals("TouchDate") || !(tmp[0].equals(tablename))) {
					continue;
				}
				datatypes.put(tmp[1], "class " + tmp[2]);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return datatypes;
	}

	public void updateTable(String strTableName, String strClusteringKey, Hashtable<String, Object> htblColNameValue)
			throws DBAppException {

		if (!tablenames.contains(strTableName))
			throw new DBAppException("The Table does not exist");
		checkDataTypes(strTableName, htblColNameValue);
		Table t = new Table();
		t = Table.deserializeTable(strTableName);
		String typ_of_clusterKey = strClusteringKey.getClass().toString();
		if (!t.strClusteringKeyColumn.getClass().toString().equals(typ_of_clusterKey)) {
			throw new DBAppException(
					"The type of the given key does not match the type of the clustering key of the table");
		}
		htblColNameValue.put("TouchDate", (new Date()));
		t.updateTable(strClusteringKey, htblColNameValue);
		Table.serializeTable(t, t.strTableName);
	}

	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		if (!tablenames.contains(strTableName))
			throw new DBAppException("The Table does not exist");
		checkDataTypes(strTableName, htblColNameValue);
		Table t = new Table();
		t = Table.deserializeTable(strTableName);
		t.deleteFromTable(htblColNameValue);
		Table.serializeTable(t, t.strTableName);
	}

	public void createBTreeIndex(String strTableName, String strColName) throws DBAppException, IOException {

		if (!tablenames.contains(strTableName))
			throw new DBAppException("The Table does not exist");

		Table t = new Table();
		t = Table.deserializeTable(strTableName);

		if (!t.htblColNameType.containsKey(strColName))
			throw new DBAppException("The column does not exist in this table");

		if (t.pages.isEmpty()) {
			updateCSV(strTableName, strColName);
			String TreeName = strTableName + strColName;
			Hashtable<String, String> dataType = getDatatypes(strTableName);
			String type = dataType.get(strColName).substring(6);
			if (type.equals("java.lang.Integer")) {
				BPTree<Integer> tree = new BPTree<Integer>(t.NodeSize);
				Table.serializeTree(tree, TreeName);
			} else if (type.equals("java.lang.Double")) {
				BPTree<Double> tree = new BPTree<Double>(t.NodeSize);
				Table.serializeTree(tree, TreeName);
			} else if (type.equals("java.lang.String")) {
				BPTree<String> tree = new BPTree<String>(t.NodeSize);
				Table.serializeTree(tree, TreeName);
			} else if (type.equals("java.util.Date")) {
				BPTree<Date> tree = new BPTree<Date>(t.NodeSize);
				Table.serializeTree(tree, TreeName);
			}
			indecies.add(TreeName);
			return;
		}

		updateCSV(strTableName, strColName);
		String TreeName = strTableName + strColName;

		for (int i = 0; i < t.pages.size(); i++) {

			Page p = new Page();
			p = Page.deserializePage(t.pages.get(i));

			for (int j = 0; j < p.record.size(); j++) {

				if (p.record.get(j).get(strColName) instanceof Integer) {

					int x = (int) p.record.get(j).get(strColName);

					if (!indecies.contains(TreeName)) {

						BPTree<Integer> tree = new BPTree<Integer>(t.NodeSize);

						Vector<int[]> overflow = new Vector<int[]>();

						Ref r = new Ref(i, j, overflow);

						tree.insert(x, r);
						indecies.add(TreeName);
						Table.serializeTree(tree, TreeName);
					}

					else {

						BPTree<Integer> tree = new BPTree<Integer>(t.NodeSize);

						tree = Table.deserializeTree(TreeName);

						if (tree.contain(x)) {
							Ref r = tree.search(x);
							int[] index = { i, j };
							r.addInOverFlow(index);
						} else {
							Vector<int[]> overflow = new Vector<int[]>();
							Ref r = new Ref(i, j, overflow);
							tree.insert(x, r);
						}

						Table.serializeTree(tree, TreeName);

					}

				}

				else if (p.record.get(j).get(strColName) instanceof Double) {

					Double x = (Double) p.record.get(j).get(strColName);

					if (!indecies.contains(TreeName)) {

						BPTree<Double> tree = new BPTree<Double>(t.NodeSize);

						Vector<int[]> overflow = new Vector<int[]>();

						Ref r = new Ref(i, j, overflow);

						tree.insert(x, r);
						indecies.add(TreeName);
						Table.serializeTree(tree, TreeName);
					}

					else {

						BPTree<Double> tree = new BPTree<Double>(t.NodeSize);

						tree = Table.deserializeTree(TreeName);

						if (tree.contain(x)) {
							Ref r = tree.search(x);
							int[] index = { i, j };
							r.addInOverFlow(index);
						} else {
							Vector<int[]> overflow = new Vector<int[]>();
							Ref r = new Ref(i, j, overflow);
							tree.insert(x, r);
						}
						Table.serializeTree(tree, TreeName);
					}
				}

				else if (p.record.get(j).get(strColName) instanceof String) {

					String x = (String) p.record.get(j).get(strColName);

					if (!indecies.contains(TreeName)) {

						BPTree<String> tree = new BPTree<String>(t.NodeSize);

						Vector<int[]> overflow = new Vector<int[]>();

						Ref r = new Ref(i, j, overflow);

						tree.insert(x, r);
						indecies.add(TreeName);
						Table.serializeTree(tree, TreeName);
					}

					else {

						BPTree<String> tree = new BPTree<String>(t.NodeSize);

						tree = Table.deserializeTree(TreeName);

						if (tree.contain(x)) {
							Ref r = tree.search(x);
							int[] index = { i, j };
							r.addInOverFlow(index);
						} else {
							Vector<int[]> overflow = new Vector<int[]>();
							Ref r = new Ref(i, j, overflow);
							tree.insert(x, r);
						}
						Table.serializeTree(tree, TreeName);
					}
				}

				else if (p.record.get(j).get(strColName) instanceof Date) {

					Date x = (Date) p.record.get(j).get(strColName);

					if (!indecies.contains(TreeName)) {

						BPTree<Date> tree = new BPTree<Date>(t.NodeSize);

						Vector<int[]> overflow = new Vector<int[]>();

						Ref r = new Ref(i, j, overflow);

						tree.insert(x, r);
						indecies.add(TreeName);
						Table.serializeTree(tree, TreeName);
					}

					else {

						BPTree<Date> tree = new BPTree<Date>(t.NodeSize);

						tree = Table.deserializeTree(TreeName);

						if (tree.contain(x)) {
							Ref r = tree.search(x);
							int[] index = { i, j };
							r.addInOverFlow(index);
						} else {
							Vector<int[]> overflow = new Vector<int[]>();
							Ref r = new Ref(i, j, overflow);
							tree.insert(x, r);
						}
						Table.serializeTree(tree, TreeName);
					}

				}

			}

		}

	}

	public static void updateCSV(String tablename, String colName) throws IOException {
		FileReader fr;
		Vector<String[]> v = new Vector<String[]>();
		try {
			fr = new FileReader("data/metadata.csv");
			BufferedReader br = new BufferedReader(fr);
			while (br.ready()) {
				String[] tmp = br.readLine().split(",");
				v.add(tmp);
			}
			fr.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		try {
			File f = new File("data/metadata.csv");
			f.delete();
		}

		catch (Exception i) {
			i.printStackTrace();
		}

		for (int i = 0; i < v.size(); i++) {
			String[] s = v.get(i);
			if (s[0].equals(tablename) && s[1].equals(colName)) {
				s[4] = "true";
			}

			FileWriter fw = null;
			try {
				fw = new FileWriter("data/metadata.csv", true);
				fw.append(s[0]);
				fw.append(",");
				fw.append(s[1]);
				fw.append(",");
				fw.append(s[2]);
				fw.append(",");
				fw.append(s[3]);
				fw.append(",");
				fw.append(s[4]);
				fw.append("\n");
				fw.flush();
				fw.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
		Iterator iter = null;

		Hashtable<String, Object> datatypes = new Hashtable<>();

		for (int l = 0; l < arrSQLTerms.length; l++) {
			if (!tablenames.contains(arrSQLTerms[l]._strTableName)) {
				throw new DBAppException("The Table" + "'" + arrSQLTerms[l]._strTableName + "'" + " does not exist");
			}
			datatypes.put(arrSQLTerms[l]._strColumnName, arrSQLTerms[l]._objValue);

		}
		checkDataTypes(arrSQLTerms[0]._strTableName, datatypes);

		Table t = Table.deserializeTable(arrSQLTerms[0]._strTableName);

		String clusteringKey = t.getClusteringInfo().get("cluster");
		Vector<String> index = new Vector<String>();

		for (int i = 0; i < t.getIndex().size(); i++) {
			index.add(t.getIndex().get(i).get("index"));
		}

		Stack<Vector<Hashtable<String, Object>>> all = new Stack<Vector<Hashtable<String, Object>>>();

		for (int i = 0; i < arrSQLTerms.length; i++) {

			Vector<Hashtable<String, Object>> arr = new Vector<Hashtable<String, Object>>();

			if (clusteringKey.equals(arrSQLTerms[i]._strColumnName)) {

				int x = t.binarySearchOnPages(clusteringKey, arrSQLTerms[i]._objValue.toString());

				for (int j = x; j < t.pages.size(); j++) {

					String pageName = t.strTableName + (j + 1);
					Page p1 = new Page();
					p1 = Page.deserializePage(pageName);
					int y = p1.binarySearchOnRecords(clusteringKey, arrSQLTerms[i]._objValue.toString());

					for (int j2 = y; j2 < p1.record.size(); j2++) {
						String record = p1.record.get(j2).get(arrSQLTerms[i]._strColumnName).toString();
						if (record.equals(arrSQLTerms[i]._objValue.toString()))
							arr.add(p1.record.get(j2));
					}

					if (j < t.pages.size() - 1) {
						String nextPage = t.strTableName + (j + 2);
						Page p2 = new Page();
						p2 = Page.deserializePage(nextPage);
						String firstRecord = p2.record.get(0).get(arrSQLTerms[i]._strColumnName).toString();
						if (!firstRecord.equals(arrSQLTerms[i]._objValue.toString()))
							break;
					}

				}

			}

			else if (index.contains(arrSQLTerms[i]._strColumnName)) {

				BPTree tree = new BPTree();
				tree = Table.deserializeTree(t.strTableName + arrSQLTerms[i]._strColumnName);

				String sqlval = arrSQLTerms[i]._objValue.toString();

				if (sqlval.matches("-?\\d+?")) {
					int value = Integer.parseInt(sqlval);
					Ref r = tree.search(value);
					int pageNumber = r.getPageNo();
					String pageName = t.strTableName + (pageNumber + 1);
					int indexOfRecord = r.getIndexInPage();
					Page p = new Page();
					p = Page.deserializePage(pageName);
					arr.add(p.record.get(indexOfRecord));
					for (int j = 0; j < r.getOverFlow().size(); j++) {
						int[] x = (int[]) r.getOverFlow().get(i);
						int pageNumber2 = x[0];
						String pageName2 = t.strTableName + (pageNumber2 + 1);
						int indexOfRecord2 = r.getIndexInPage();
						Page p2 = new Page();
						p2 = Page.deserializePage(pageName2);
						arr.add(p2.record.get(indexOfRecord2));
					}
				}

				else if (sqlval.matches("-?\\d+(\\.\\d+)?")) {
					double value = Double.parseDouble(sqlval);
					Ref r = tree.search(value);
					int pageNumber = r.getPageNo();
					String pageName = t.strTableName + (pageNumber + 1);
					int indexOfRecord = r.getIndexInPage();
					Page p = new Page();
					p = Page.deserializePage(pageName);
					arr.add(p.record.get(indexOfRecord));
					for (int j = 0; j < r.getOverFlow().size(); j++) {
						int[] x = (int[]) r.getOverFlow().get(i);
						int pageNumber2 = x[0];
						String pageName2 = t.strTableName + (pageNumber2 + 1);
						int indexOfRecord2 = r.getIndexInPage();
						Page p2 = new Page();
						p2 = Page.deserializePage(pageName2);
						arr.add(p2.record.get(indexOfRecord2));
					}

				}

				else if (sqlval.matches("^[ A-Za-z]+$")) {

					String value = sqlval;
					Ref r = tree.search(value);
					int pageNumber = r.getPageNo();
					String pageName = t.strTableName + (pageNumber + 1);
					int indexOfRecord = r.getIndexInPage();
					Page p = new Page();
					p = Page.deserializePage(pageName);
					arr.add(p.record.get(indexOfRecord));
					for (int j = 0; j < r.getOverFlow().size(); j++) {
						int[] x = (int[]) r.getOverFlow().get(i);
						int pageNumber2 = x[0];
						String pageName2 = t.strTableName + (pageNumber2 + 1);
						int indexOfRecord2 = r.getIndexInPage();
						Page p2 = new Page();
						p2 = Page.deserializePage(pageName2);
						arr.add(p2.record.get(indexOfRecord2));
					}

				}

				else {

					Date value = (Date) arrSQLTerms[i]._objValue;
					Ref r = tree.search(value);
					int pageNumber = r.getPageNo();
					String pageName = t.strTableName + (pageNumber + 1);
					int indexOfRecord = r.getIndexInPage();
					Page p = new Page();
					p = Page.deserializePage(pageName);
					arr.add(p.record.get(indexOfRecord));
					for (int j = 0; j < r.getOverFlow().size(); j++) {
						int[] x = (int[]) r.getOverFlow().get(i);
						int pageNumber2 = x[0];
						String pageName2 = t.strTableName + (pageNumber2 + 1);
						int indexOfRecord2 = r.getIndexInPage();
						Page p2 = new Page();
						p2 = Page.deserializePage(pageName2);
						arr.add(p2.record.get(indexOfRecord2));
					}

				}

			}

			else {

				for (int k = 0; k < t.pages.size(); k++) {

					Page p = new Page();
					p = Page.deserializePage((t.pages.get(k)));

					for (int j = 0; j < p.record.size(); j++) {

						String val = p.record.get(j).get(arrSQLTerms[i]._strColumnName).toString();

						if (val.matches("-?\\d+?")) {

							int value = Integer.parseInt(val);
							int sqlval = Integer.parseInt(arrSQLTerms[i]._objValue.toString());
							if (value == sqlval) {
								arr.add(p.record.get(j));
								continue;
							}

						}

						else if (val.matches("-?\\d+(\\.\\d+)?")) {
							double value = Double.parseDouble(val);
							double sqlval = Double.parseDouble(arrSQLTerms[i]._objValue.toString());
							if (value == sqlval) {
								arr.add(p.record.get(j));
								continue;
							}

						}

						else if (val.matches("^[ A-Za-z]+$")) {

							String sqlval = arrSQLTerms[i]._objValue.toString();
							if (sqlval.equals(val)) {
								arr.add(p.record.get(j));
								continue;
							}

						}

						else {
							String sqlval = arrSQLTerms[i]._objValue.toString();
							if (sqlval.equals(val)) {
								arr.add(p.record.get(j));
								continue;
							}

						}

					}

				}

			}

			all.push(arr);

		}

		Stack<Vector<Hashtable<String, Object>>> all2 = new Stack<Vector<Hashtable<String, Object>>>();
		while (!all.isEmpty()) {
			all2.push(all.pop());

		}

		Vector<Hashtable<String, Object>> finalresult = new Vector<Hashtable<String, Object>>();

		for (int i = 0; i < strarrOperators.length; i++) {

			Vector<Hashtable<String, Object>> temp1 = new Vector<Hashtable<String, Object>>();
			Vector<Hashtable<String, Object>> temp2 = new Vector<Hashtable<String, Object>>();
			temp1.addAll(all2.pop());
			temp2.addAll(all2.pop());

			if (strarrOperators[i].equals("OR")) {

				temp1 = removeDuplicates(temp1, temp2);

				Vector<Hashtable<String, Object>> result = new Vector<Hashtable<String, Object>>();
				result.addAll(temp1);
				result.addAll(temp2);

				all2.push(result);
				finalresult = result;
				continue;

			}

			else if (strarrOperators[i].equals("AND")) {

				if (i == 0) {

					for (int j = 0; j < temp1.size(); j++) {
						if (!(temp1.get(j).get(arrSQLTerms[i + 1]._strColumnName).toString()
								.equals(arrSQLTerms[i + 1]._objValue.toString()))) {
							temp1.remove(j);
							j--;
						}

					}

					for (int j = 0; j < temp2.size(); j++) {

						if (!(temp2.get(j).get(arrSQLTerms[i]._strColumnName).toString()
								.equals(arrSQLTerms[i]._objValue.toString()))) {
							temp2.remove(j);
							j--;
						}

					}

					Vector<Hashtable<String, Object>> result2 = new Vector<Hashtable<String, Object>>();

					temp1 = removeDuplicates(temp1, temp2);

					result2.addAll(temp1);
					result2.addAll(temp2);

					all2.push(result2);
					continue;

				}

				Vector<Hashtable<String, Object>> result = new Vector<Hashtable<String, Object>>();
				result.addAll(temp1);

				for (int j = 0; j < result.size(); j++) {

					if (!(result.get(j).get(arrSQLTerms[i + 1]._strColumnName).toString()
							.equals(arrSQLTerms[i + 1]._objValue.toString()))) {
						result.remove(j);
						j--;
					}
				}

				all2.push(result);
				finalresult = result;
				continue;
			}

			else if (strarrOperators[i].equals("XOR")) {
				Vector<Hashtable<String, Object>> result = new Vector<Hashtable<String, Object>>();
				result = XOR(temp1, temp2);
				all2.push(result);
				finalresult = result;
				continue;
			}

		}

		for (int i = 0; i < finalresult.size(); i++) {
			System.out.println(finalresult.get(i));
		}
		iter = finalresult.iterator();

		return iter;
	}

	public static Vector<Hashtable<String, Object>> removeDuplicates(Vector<Hashtable<String, Object>> list1,
			Vector<Hashtable<String, Object>> list2) {

		for (int i = 0; i < list1.size(); i++) {

			if (list2.contains(list1.get(i))) {

				list1.remove(i);
				i--;
			}
		}

		return list1;
	}

	public static Vector<Hashtable<String, Object>> XOR(Vector<Hashtable<String, Object>> list1,
			Vector<Hashtable<String, Object>> list2) {

		Vector<Hashtable<String, Object>> v = new Vector<Hashtable<String, Object>>();

		for (int i = 0; i < list1.size(); i++) {

			for (int j = 0; j < list2.size(); j++) {

				if (list1.get(i).equals(list2.get(j))) {
					list1.remove(i);
					list2.remove(j);
					j--;
				}

			}

		}

		v.addAll(list1);
		v.addAll(list2);

		return v;
	}

	public static void main(String[] args) throws DBAppException, IOException {

		DBApp dbApp = new DBApp();

//		System.out.println(getIndex("shady"));
//		BPTree tree = new BPTree();
//		tree = deserializeTree("shadyid");
//		System.out.println(tree.search(4).pageNo);
//		System.out.println(tree.search(4).indexInPage);
//		System.out.println(tree.toString());

//		Ref r = tree.search(0);
//		for (int i = 0; i < r.overflow.size(); i++) {
//			int[] arr = r.overflow.get(i);
//			System.out.println("index of page:"+arr[0]+" index of record:"+arr[1]);
//		}

//		Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
//		htblColNameType.put("id", "java.lang.Integer");
//		htblColNameType.put("name", "java.lang.String");
//		htblColNameType.put("gpa", "java.lang.Double");
//		dbApp.createTable("shady", "id", htblColNameType);
//		dbApp.createBTreeIndex("mohamed", "name");
//		
//		for (int i = 0; i < 8; i++) {
//			Hashtable<String, Object> htblColNameValue4 = new Hashtable<String, Object>();
//			htblColNameValue4.put("id", new Integer(i));
//			htblColNameValue4.put("name", new String("mohamed Noor"));
//			htblColNameValue4.put("gpa", new Double(0.95));
//			dbApp.insertIntoTable("mohamed", htblColNameValue4);
//			htblColNameValue4.clear();
//		}

		Hashtable<String, Object> htblColNameValue0 = new Hashtable<String, Object>();
		htblColNameValue0.put("id", new Integer(0));
		htblColNameValue0.put("name", new String("aa"));
		htblColNameValue0.put("gpa", new Double(1.0));
		dbApp.insertIntoTable("mohamed", htblColNameValue0);

//		Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
////		htblColNameValue.put("id", new Integer(0));
//		htblColNameValue.put("name", new String("ahmed" ) );
//		htblColNameValue.put("gpa", new Double(2.0) );
//		dbApp.updateTable("shady","9", htblColNameValue);

//		Hashtable<String, Object> htblColNameValue2 = new Hashtable<String, Object>(); 
//		htblColNameValue2.put("id", new Integer(9));
//		htblColNameValue2.put("name", new String("basel" ) );
//		htblColNameValue2.put("gpa", new Double(1.0) );
//		dbApp.deleteFromTable( "shady" , htblColNameValue2);
//		
//		Hashtable<String, Object> htblColNameValue4 = new Hashtable<String, Object>(); 
//		htblColNameValue4.put("id", new Integer(48));
//		htblColNameValue4.put("name", new String("Ahmed Noor" ) );
//		htblColNameValue4.put("gpa", new Double(0.95) );
//		dbApp.deleteFromTable( "ahmed" , htblColNameValue4);
//		
//		
//		Hashtable<String, Object> htblColNameValue5 = new Hashtable<String, Object>(); 
//		htblColNameValue5.put("id", new Integer(34));
//		htblColNameValue5.put("name", new String("Ahmed Noor" ) );
//		htblColNameValue5.put("gpa", new Double(0.95) );
//		dbApp.deleteFromTable( "ahmed" , htblColNameValue5);
//		
//		
//		Hashtable<String, Object> htblColNameValue3 = new Hashtable<String, Object>(); 
//		htblColNameValue3.put("id", new Integer(35));
//		htblColNameValue3.put("name", new String("Ahmed Noor" ) );
//		htblColNameValue3.put("gpa", new Double(0.95) );
//		dbApp.deleteFromTable( "ahmed" , htblColNameValue3);

//		Hashtable<String, Object> htblColNameValue6 = new Hashtable<String, Object>(); 
//		htblColNameValue6.put("id", new Integer(43));
//		htblColNameValue6.put("name", new String("Ahmed Noor" ) );
//		htblColNameValue6.put("gpa", new Double(0.95) );
//		dbApp.deleteFromTable( "ahmed" , htblColNameValue6);

//		Page.deletePage(t.pages.get(t.pages.size()-1));
//		t.pages.remove(t.pages.size()-1);
//		Table.serializeTable(t, t.strTableName);

		String strTableName = "mohamed";
		Table t = Table.deserializeTable(strTableName);

		BPTree tree = t.deserializeTree(t.strTableName + "name");

		System.out.println(tree.toString());

		BPTreeLeafNode l = tree.getFirstLeaf();
		while (l != null) {

			System.out.println(l);
			for (int i = 0; i < l.numberOfKeys; i++) {
				System.out.println((tree.search((String) l.getKey(i))).pageNo + "  "
						+ (tree.search((String) l.getKey(i))).indexInPage);
				for (int j = 0; j < tree.search((String) l.getKey(i)).getOverFlow().size(); j++) {
					int [] arr = (int[])tree.search((String) l.getKey(i)).getOverFlow().get(j);
					System.out.println(arr[0] +"  "+arr[1]);
				}
			}
			l = l.getNext();
			System.out.println();
			System.out.println();
		}
		
		
//		System.out.println(getIndex(strTableName));

//		int x = t.binarySearchOnPages("name", "cat");
//		
//		String pageName = strTableName + (x+1) ;
//		Page p2 = new Page();
//		p2 = Page.deserializePage(pageName);
//		
//		
//		System.out.println(p2.page_name);
//		
//		int y = p2.binarySearchOnRecords("name", "cat");
//		System.out.println(y);

//		System.out.println(t.pages.size());
//		System.out.println(t.page_capacity);
//		
//
		for (int i = 0; i < t.pages.size(); i++) {
			Page p = new Page();
			p = Page.deserializePage((t.pages.get(i)));
			System.out.println(p.page_name);
			for (Hashtable<String, Object> hm : p.record) {
				for (String col : hm.keySet())
					System.out.print(col + " " + hm.get(col) + ",");
				System.out.println();
			}
			System.out.println();
		}

		System.out.println();
		System.out.println();

//		SQLTerm[] arrSQLTerms;
//		arrSQLTerms = new SQLTerm[3];
//		for (int i = 0; i < arrSQLTerms.length; i++) {
//			arrSQLTerms[i] = new SQLTerm();
//		}
//		arrSQLTerms[0]._strTableName = "mohamed";
//		arrSQLTerms[0]._strColumnName = "name";
//		arrSQLTerms[0]._strOperator = "=";
//		arrSQLTerms[0]._objValue = "aa";
//		arrSQLTerms[1]._strTableName = "shady";
//		arrSQLTerms[1]._strColumnName = "id";
//		arrSQLTerms[1]._strOperator = "=";
//		arrSQLTerms[1]._objValue = new Integer(3);
//		arrSQLTerms[2]._strTableName = "shady";
//		arrSQLTerms[2]._strColumnName = "gpa";
//		arrSQLTerms[2]._strOperator = "=";
//		arrSQLTerms[2]._objValue = new Double(1.0);
//		String[] strarrOperators = new String[2];
//		strarrOperators[0] = "OR";
//		strarrOperators[1] = "AND";
//		// select * from Student where name = John Noor or gpa = 1.5;
//		Iterator resultSet = dbApp.selectFromTable(arrSQLTerms, strarrOperators);

	}

}
