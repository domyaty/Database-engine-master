package elZ3amaa;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.sql.Date;
import java.util.Hashtable;
import java.util.Set;
import java.util.Vector;

public class Page implements Serializable {
	static final long serialVersionUID = 1L;
	Vector<Hashtable<String, Object>> record;
	String key;
	String page_name;

	public Page() {

	}

	public Page(String key, String page_name) {
		this.record = new Vector<Hashtable<String, Object>>();
		this.key = key;
		this.page_name = page_name;
	}

	public int insertIntoPage(Hashtable<String, Object> htblColNameValue, String clustringKey) {
		boolean binarySearch = false;
		if (record.size() == 0) {
			record.add(htblColNameValue);
			return 0;
		}
		if (record.size() != 0 && record.size() <= 3) {
			binarySearch = false;
		} else {
			binarySearch = true;
		}

		if (binarySearch == false) {
			for (int i = 0; i < record.size(); i++) {
				Object clusterOfInput = htblColNameValue.get(key);
				Object clusterInRecord = record.get(i).get(key);
				if (clusterOfInput instanceof Integer && clusterInRecord instanceof Integer) {
					int d1 = (int) clusterOfInput;
					int d2 = (int) clusterInRecord;
					if (d1 < d2) {
						record.add(i, htblColNameValue);
						return i;
					} else if (record.size() - 1 == i) {
						record.add(i + 1, htblColNameValue);
						return i+1;
					} else {
						continue;
					}
				} else if (clusterOfInput instanceof Double && clusterInRecord instanceof Double) {
					Double d1 = (Double) clusterOfInput;
					Double d2 = (Double) clusterInRecord;
					if (d1 < d2) {
						record.add(i, htblColNameValue);
						return i;
					} else if (record.size() - 1 == i) {
						record.add(i + 1, htblColNameValue);
						return i+1;
					} else {
						continue;
					}
				} else if (clusterOfInput instanceof String && clusterInRecord instanceof String) {
					String d1 = (String) clusterOfInput;
					String d2 = (String) clusterInRecord;
					if (d1.compareTo(d2) < 0) {
						record.add(i, htblColNameValue);
						return i;
					} else if (record.size() - 1 == i) {
						record.add(i + 1, htblColNameValue);
						return i+1;
					} else {
						continue;
					}
				} else if (clusterOfInput instanceof Date && clusterInRecord instanceof Date) {
					Date d1 = (Date) clusterOfInput;
					Date d2 = (Date) clusterInRecord;
					if (d1.compareTo(d2) < 0) {
						record.add(i, htblColNameValue);
						return i;
					} else if (record.size() - 1 == i) {
						record.add(i + 1, htblColNameValue);
						return i+1;
					} else {
						continue;
					}
				}
			}
		}

		int first = 0;
		int last = record.size() - 1;
		int mid = (first + last) / 2;
		while (first <= last && binarySearch == true) {
			if (first == last && first == mid && first == 0) {
				record.add(first, htblColNameValue);
				return first;
			} else if (first == last && first == mid && first == record.size() - 1) {
				record.add(first + 1, htblColNameValue);
				return first+1;
			}
			if (mid == 0) {
				record.add(mid, htblColNameValue);
				return mid;
			}

			Object clusterOfInput = htblColNameValue.get(clustringKey);
			Object clusterInRecord = record.get(mid).get(clustringKey);
			Object valueBeforeMid = record.get(mid - 1).get(clustringKey);
			Object ValueAfterMid = record.get(mid + 1).get(clustringKey);

			if (clusterOfInput instanceof Integer && clusterInRecord instanceof Integer) {
				if (last == 0) {
					record.add(last, htblColNameValue);
					return last;
				} else if (first == (record.size() - 1)) {
					record.add(first + 1, htblColNameValue);
					return first+1;
				} else if ((int) clusterOfInput < (int) clusterInRecord
						&& !((int) clusterOfInput >= (int) valueBeforeMid)) {
					last = mid - 1;

				} else if ((int) clusterOfInput > (int) clusterInRecord
						&& !((int) clusterOfInput < (int) ValueAfterMid)) {
					first = mid + 1;

				} else if ((int) clusterOfInput > (int) clusterInRecord) {
					record.add(mid + 1, htblColNameValue);
					return mid+1;
				} else {
					record.add(mid, htblColNameValue);
					return mid;
				}
				mid = (first + last) / 2;
			}

			else if (clusterOfInput instanceof Double && clusterInRecord instanceof Double) {
				if (last == 0) {
					record.add(last, htblColNameValue);
					return last;
				} else if (first == record.size() - 1) {
					record.add(first + 1, htblColNameValue);
					return first+1;
				} else if ((Double) clusterOfInput < (Double) clusterInRecord
						&& !((Double) clusterOfInput > (Double) valueBeforeMid)) {
					last = mid - 1;

				} else if ((Double) clusterInRecord < (Double) clusterOfInput
						&& !((Double) clusterOfInput < (Double) ValueAfterMid)) {
					first = mid + 1;

				} else if ((Double) clusterOfInput > (Double) clusterInRecord) {
					record.add(mid + 1, htblColNameValue);
					return mid+1;
				} else {
					record.add(mid, htblColNameValue);
					return mid;
				}
				mid = (first + last) / 2;
			}

			else if (clusterOfInput instanceof String && clusterInRecord instanceof String) {
				if (last == 0) {
					record.add(last, htblColNameValue);
					return last;
				} else if (first == record.size() - 1) {
					record.add(first + 1, htblColNameValue);
					return first+1;
				} else if (((String) clusterOfInput).compareTo((String) clusterInRecord) < 0
						&& !(((String) clusterOfInput).compareTo((String) valueBeforeMid) > 0)) {
					last = mid - 1;

				} else if (((String) clusterInRecord).compareTo((String) clusterOfInput) < 0
						&& !(((String) clusterOfInput).compareTo((String) ValueAfterMid) < 0)) {
					first = mid + 1;

				} else if (((String) clusterOfInput).compareTo((String) clusterInRecord) > 0) {
					record.add(mid + 1, htblColNameValue);
					return mid+1;
				} else {
					record.add(mid, htblColNameValue);
					return mid;
				}
				mid = (first + last) / 2;
			}

			else if (clusterOfInput instanceof Date && clusterInRecord instanceof Date) {
				if (last == 0) {
					record.add(last, htblColNameValue);
					return last;
				} else if (first == record.size() - 1) {
					record.add(first + 1, htblColNameValue);
					return first+1;
				} else if (((Date) clusterOfInput).compareTo((Date) clusterInRecord) < 0
						&& !(((Date) clusterOfInput).compareTo((Date) valueBeforeMid) > 0)) {
					last = mid - 1;

				} else if (((Date) clusterInRecord).compareTo((Date) clusterOfInput) < 0
						&& !(((Date) clusterOfInput).compareTo((Date) ValueAfterMid) < 0)) {
					first = mid + 1;

				} else if (((Date) clusterOfInput).compareTo((Date) clusterInRecord) > 0) {
					record.add(mid + 1, htblColNameValue);
					return mid+1;
				} else {
					record.add(mid, htblColNameValue);
					return mid;
				}
				mid = (first + last) / 2;
			}
		}
		return mid;
	}

	public void updatePage(String strClusteringKey, Hashtable<String, Object> htblColNameValue) {

		boolean binarySearch = false;

		if (record.size() == 0) {
			return;
		}

		if (record.size() != 0 && record.size() <= 3) {
			binarySearch = false;
		}

		else {
			binarySearch = true;
		}

		if (binarySearch == false) {

			for (int j = 0; j < record.size(); j++) {

				if (strClusteringKey.equals(record.get(j).get(key).toString())) {
					Set entrySet = htblColNameValue.entrySet();
					Object[] arr = entrySet.toArray();

					for (int k = 0; k < arr.length; k++) {
						String[] words = arr[k].toString().split("=");
						String colname = words[0];
						String colvalue = words[1];
						record.get(j).put(colname, colvalue);
					}

				}

			}

		}

		if (binarySearch == true) {

			int first = 0;
			int last = record.size() - 1;
			int mid = (first + last) / 2;

			while (first <= last && binarySearch == true) {

				String clusterInRecord = record.get(mid).get(key).toString();

				if (clusterInRecord.matches("-?\\d+?")) {
					int cluster = Integer.parseInt(clusterInRecord);
					int clustergiven = Integer.parseInt(strClusteringKey);

					if (clustergiven > cluster) {
						first = mid + 1;
					}

					else if (clustergiven == cluster) {
						Set entrySet = htblColNameValue.entrySet();
						Object[] arr = entrySet.toArray();

						for (int k = 0; k < arr.length; k++) {
							String[] words = arr[k].toString().split("=");
							String colname = words[0];
							String colvalue = words[1];
							record.get(mid).put(colname, colvalue);
						}

						for (int j = mid; j < record.size() && (j + 1 < record.size()); j++) {

							int clusterNext = Integer.parseInt((record.get(j + 1).get(key).toString()));

							if (clusterNext == clustergiven) {

								for (int k = 0; k < arr.length; k++) {
									String[] words = arr[k].toString().split("=");
									String colname = words[0];
									String colvalue = words[1];
									record.get(j + 1).put(colname, colvalue);
								}

							}

							else {
								break;
							}

						}

						for (int j = mid; j > 0 && (j - 1 >= 0); j--) {

							int clusterBefore = Integer.parseInt((record.get(j - 1).get(key).toString()));

							if (clusterBefore == clustergiven) {

								for (int k = 0; k < arr.length; k++) {
									String[] words = arr[k].toString().split("=");
									String colname = words[0];
									String colvalue = words[1];
									record.get(j - 1).put(colname, colvalue);
								}

							}

							else {
								break;
							}
						}

						break;

					}

					else {
						last = mid - 1;
					}

					mid = (first + last) / 2;
					continue;

				}

				else if (clusterInRecord.matches("-?\\d+(\\.\\d+)?")) {

					double cluster = Double.parseDouble(clusterInRecord);
					double clustergiven = Double.parseDouble(strClusteringKey);

					if (clustergiven > cluster) {
						first = mid + 1;
					}

					else if (clustergiven == cluster) {

						Set entrySet = htblColNameValue.entrySet();
						Object[] arr = entrySet.toArray();

						for (int k = 0; k < arr.length; k++) {
							String[] words = arr[k].toString().split("=");
							String colname = words[0];
							String colvalue = words[1];
							record.get(mid).put(colname, colvalue);
						}

						for (int j = mid; j < record.size() && (j + 1 < record.size()); j++) {

							Double clusterNext = Double.parseDouble((record.get(j + 1).get(key).toString()));

							if (clusterNext == clustergiven) {

								for (int k = 0; k < arr.length; k++) {
									String[] words = arr[k].toString().split("=");
									String colname = words[0];
									String colvalue = words[1];
									record.get(j + 1).put(colname, colvalue);
								}

							}

							else {
								break;
							}

						}

						for (int j = mid; j > 0 && (j - 1 >= 0); j--) {

							double clusterBefore = Double.parseDouble((record.get(j - 1).get(key).toString()));

							if (clusterBefore == clustergiven) {

								for (int k = 0; k < arr.length; k++) {
									String[] words = arr[k].toString().split("=");
									String colname = words[0];
									String colvalue = words[1];
									record.get(j - 1).put(colname, colvalue);
								}

							}

							else {
								break;
							}

						}

						break;
					}

					else {
						last = mid - 1;
					}

					mid = (first + last) / 2;
					continue;

				}

				else if (clusterInRecord.matches("^[ A-Za-z]+$")) {
					
					if (strClusteringKey.compareTo(clusterInRecord) > 0) {
						first = mid + 1;
					}

					else if (clusterInRecord.equals(strClusteringKey)) {

						Set entrySet = htblColNameValue.entrySet();
						Object[] arr = entrySet.toArray();

						for (int k = 0; k < arr.length; k++) {
							String[] words = arr[k].toString().split("=");
							String colname = words[0];
							String colvalue = words[1];
							record.get(mid).put(colname, colvalue);
						}

						for (int j = mid; j < record.size() && (j + 1 < record.size()); j++) {

							if ((record.get(j + 1).get(key).toString()).equals(strClusteringKey)) {

								for (int k = 0; k < arr.length; k++) {
									String[] words = arr[k].toString().split("=");
									String colname = words[0];
									String colvalue = words[1];
									record.get(j + 1).put(colname, colvalue);
								}

							}

							else {
								break;
							}

						}

						for (int j = mid; j > 0 && (j - 1 >= 0); j--) {

							if ((record.get(j - 1).get(key).toString()).equals(strClusteringKey)) {

								for (int k = 0; k < arr.length; k++) {
									String[] words = arr[k].toString().split("=");
									String colname = words[0];
									String colvalue = words[1];
									record.get(j - 1).put(colname, colvalue);
								}

							}

							else {
								break;
							}

						}

						break;
					}

					else {
						last = mid - 1;
					}

					mid = (first + last) / 2;
					continue;
				}

				else {

					if (clusterInRecord.compareTo(strClusteringKey) < 0) {
						first = mid + 1;
					}

					else if (clusterInRecord.equals(strClusteringKey)) {

						Set entrySet = htblColNameValue.entrySet();
						Object[] arr = entrySet.toArray();

						for (int k = 0; k < arr.length; k++) {
							String[] words = arr[k].toString().split("=");
							String colname = words[0];
							String colvalue = words[1];
							record.get(mid).put(colname, colvalue);
						}

						for (int j = mid; j < record.size() && (j + 1 < record.size()); j++) {

							if ((record.get(j + 1).get(key).toString()).equals(strClusteringKey)) {

								for (int k = 0; k < arr.length; k++) {
									String[] words = arr[k].toString().split("=");
									String colname = words[0];
									String colvalue = words[1];
									record.get(j + 1).put(colname, colvalue);
								}

							}

							else {
								break;
							}

						}

						for (int j = mid; j > 0 && (j - 1 >= 0); j--) {

							if ((record.get(j - 1).get(key).toString()).equals(strClusteringKey)) {

								for (int k = 0; k < arr.length; k++) {
									String[] words = arr[k].toString().split("=");
									String colname = words[0];
									String colvalue = words[1];
									record.get(j - 1).put(colname, colvalue);
								}

							}

							else {
								break;
							}

						}

						break;
					}

					else {
						last = mid - 1;
					}

					mid = (first + last) / 2;
					continue;
				}

			}

		}
		Page.serializePage(this, page_name);
	}

	public void deleteFromPage(String strClusteringKey, Hashtable<String, Object> htblColNameValue) {

		boolean binarySearch = false;

		if (record.size() == 0) {
			return;
		}

		if (record.size() != 0 && record.size() <= 3) {
			binarySearch = false;
		}

		else {
			binarySearch = true;
		}

		if (binarySearch == false) {

			for (int j = 0; j < record.size(); j++) {

				if (strClusteringKey.equals(record.get(j).get(key).toString())) {
					Hashtable<String, Object> h = new Hashtable<String, Object>();

					for (String col_type : record.get(j).keySet()) {

						if (col_type.equals("TouchDate")) {
							continue;
						}
						h.put(col_type, record.get(j).get(col_type));
					}

					boolean equal = true;
					for (String col_type : record.get(j).keySet()) {
						if (col_type.equals("TouchDate"))
							continue;

						if (!(htblColNameValue.get(col_type).toString()).equals(h.get(col_type).toString())) {
							equal = false;
							break;
						}

					}

					if (equal) {
						record.remove(j);
						j--;
					}

				}

			}

		}

		if (binarySearch == true) {

			int first = 0;
			int last = record.size() - 1;
			int mid = (first + last) / 2;

			while (first <= last && binarySearch == true) {

				String clusterInRecord = record.get(mid).get(key).toString();

				if (clusterInRecord.matches("-?\\d+?")) {
					int cluster = Integer.parseInt(clusterInRecord);
					int clustergiven = Integer.parseInt(strClusteringKey);

					if (clustergiven > cluster) {
						first = mid + 1;
					}

					else if (clustergiven == cluster) {
						int start = mid;

						while ((record.get(start).get(key).toString()).equals(strClusteringKey) && start != 0) {

							if ((record.get(start - 1).get(key).toString()).equals(strClusteringKey)) {
								start--;
							} else {
								break;
							}
						}

						while ((start != record.size())
								&& (record.get(start).get(key).toString()).equals(strClusteringKey)) {

							Hashtable<String, Object> h = new Hashtable<String, Object>();

							for (String col_type : record.get(start).keySet()) {

								if (col_type.equals("TouchDate")) {
									continue;
								}

								h.put(col_type, record.get(start).get(col_type));
							}

							boolean equal = true;

							for (String col_type : record.get(start).keySet()) {

								if (col_type.equals("TouchDate"))
									continue;

								if (!(htblColNameValue.get(col_type).toString()).equals(h.get(col_type).toString())) {
									equal = false;
									break;
								}

							}

							if (equal) {
								record.remove(start);
							}

							else {
								start++;
							}

						}

						break;

					}

					else {
						last = mid - 1;
					}

					mid = (first + last) / 2;
					continue;

				}

				else if (clusterInRecord.matches("-?\\d+(\\.\\d+)?")) {

					double cluster = Double.parseDouble(clusterInRecord);
					double clustergiven = Double.parseDouble(strClusteringKey);

					if (clustergiven > cluster) {
						first = mid + 1;
					}

					else if (clustergiven == cluster) {

						int start = mid;

						while ((record.get(start).get(key).toString()).equals(strClusteringKey) && start != 0) {

							if ((record.get(start - 1).get(key).toString()).equals(strClusteringKey)) {
								start--;
							}

							else {
								break;
							}

						}

						while ((start != record.size())
								&& (record.get(start).get(key).toString()).equals(strClusteringKey)) {

							Hashtable<String, Object> h = new Hashtable<String, Object>();

							for (String col_type : record.get(start).keySet()) {

								if (col_type.equals("TouchDate")) {
									continue;
								}

								h.put(col_type, record.get(start).get(col_type));
							}

							boolean equal = true;

							for (String col_type : record.get(start).keySet()) {

								if (col_type.equals("TouchDate"))
									continue;

								if (!(htblColNameValue.get(col_type).toString()).equals(h.get(col_type).toString())) {
									equal = false;
									break;
								}

							}

							if (equal) {
								record.remove(start);
							}

							else {
								start++;
							}

						}

						break;
					}

					else {
						last = mid - 1;
					}

					mid = (first + last) / 2;
					continue;

				}

				else if (clusterInRecord.matches("^[ A-Za-z]+$")) {

					if (clusterInRecord.compareToIgnoreCase(strClusteringKey) < 0) {
						first = mid + 1;
					}

					else if (clusterInRecord.equals(strClusteringKey)) {

						int start = mid;

						while ((record.get(start).get(key).toString()).equals(strClusteringKey) && start != 0) {

							if ((record.get(start - 1).get(key).toString()).equals(strClusteringKey)) {
								start--;
							} else {
								break;
							}
						}

						while ((start != record.size())
								&& (record.get(start).get(key).toString()).equals(strClusteringKey)) {

							Hashtable<String, Object> h = new Hashtable<String, Object>();

							for (String col_type : record.get(start).keySet()) {

								if (col_type.equals("TouchDate")) {
									continue;
								}

								h.put(col_type, record.get(start).get(col_type));
							}

							boolean equal = true;

							for (String col_type : record.get(start).keySet()) {

								if (col_type.equals("TouchDate"))
									continue;

								if (!(htblColNameValue.get(col_type).toString()).equals(h.get(col_type).toString())) {
									equal = false;
									break;
								}

							}

							if (equal) {
								record.remove(start);
							}

							else {
								start++;
							}

						}

						break;
					}

					else {
						last = mid - 1;
					}

					mid = (first + last) / 2;
					continue;
				}

				else {

					if (clusterInRecord.compareTo(strClusteringKey) < 0) {
						first = mid + 1;
					}

					else if (clusterInRecord.equals(strClusteringKey)) {

						int start = mid;

						while ((record.get(start).get(key).toString()).equals(strClusteringKey) && start != 0) {

							if ((record.get(start - 1).get(key).toString()).equals(strClusteringKey)) {
								start--;
							} else {
								break;
							}
						}

						while ((start != record.size())
								&& (record.get(start).get(key).toString()).equals(strClusteringKey)) {

							Hashtable<String, Object> h = new Hashtable<String, Object>();

							for (String col_type : record.get(start).keySet()) {

								if (col_type.equals("TouchDate")) {
									continue;
								}

								h.put(col_type, record.get(start).get(col_type));
							}

							boolean equal = true;

							for (String col_type : record.get(start).keySet()) {

								if (col_type.equals("TouchDate"))
									continue;

								if (!(htblColNameValue.get(col_type).toString()).equals(h.get(col_type).toString())) {
									equal = false;
									break;
								}

							}

							if (equal) {
								record.remove(start);
							}

							else {
								start++;
							}

						}

						break;
					}

					else {
						last = mid - 1;
					}

					mid = (first + last) / 2;
					continue;
				}

			}

		}

		serializePage(this, this.page_name);

	}

	public int binarySearchOnRecords(String columnName, String strClusteringKey) {
		int first = 0;
		int last = record.size() - 1;
		int mid = (first + last) / 2;

		while (first <= last) {

			String clusterInRecord = record.get(mid).get(columnName).toString();

			if (clusterInRecord.matches("-?\\d+?")) {
				int cluster = Integer.parseInt(clusterInRecord);
				int clustergiven = Integer.parseInt(strClusteringKey);

				if (clustergiven > cluster) {
					first = mid + 1;
				}

				else if (clustergiven == cluster) {

					while ((record.get(mid).get(columnName).toString()).equals(strClusteringKey) && mid != 0) {

						if ((record.get(mid - 1).get(columnName).toString()).equals(strClusteringKey)) {
							mid--;
						} else {
							break;
						}
					}
					break;

				}

				else {
					last = mid - 1;
				}

				mid = (first + last) / 2;
				continue;

			}

			else if (clusterInRecord.matches("-?\\d+(\\.\\d+)?")) {

				double cluster = Double.parseDouble(clusterInRecord);
				double clustergiven = Double.parseDouble(strClusteringKey);

				if (clustergiven > cluster) {
					first = mid + 1;
				}

				else if (clustergiven == cluster) {

					while ((record.get(mid).get(columnName).toString()).equals(strClusteringKey) && mid != 0) {

						if ((record.get(mid - 1).get(columnName).toString()).equals(strClusteringKey)) {
							mid--;
						}

						else {
							break;
						}

					}

					break;
				}

				else {
					last = mid - 1;
				}

				mid = (first + last) / 2;
				continue;

			}

			else if (clusterInRecord.matches("^[ A-Za-z]+$")) {

				if (clusterInRecord.compareToIgnoreCase(strClusteringKey) < 0) {
					first = mid + 1;
				}

				else if (clusterInRecord.equals(strClusteringKey)) {

					while ((record.get(mid).get(columnName).toString()).equals(strClusteringKey) && mid != 0) {

						if ((record.get(mid - 1).get(columnName).toString()).equals(strClusteringKey)) {
							mid--;
						} else {
							break;
						}
					}

					break;
				}

				else {
					last = mid - 1;
				}

				mid = (first + last) / 2;
				continue;
			}

			else {

				if (clusterInRecord.compareTo(strClusteringKey) < 0) {
					first = mid + 1;
				}

				else if (clusterInRecord.equals(strClusteringKey)) {

					while ((record.get(mid).get(columnName).toString()).equals(strClusteringKey) && mid != 0) {

						if ((record.get(mid - 1).get(columnName).toString()).equals(strClusteringKey)) {
							mid--;
						} else {
							break;
						}
					}

					break;
				}

				else {
					last = mid - 1;
				}

				mid = (first + last) / 2;
				continue;
			}

		}

		return mid;
	}
	
	public static void main(String[] args) {
		System.out.println("ahmed".compareToIgnoreCase("mohamed"));
	}

	public static void serializePage(Page p, String name) {

		try {

			FileOutputStream fileOut = new FileOutputStream("data/" + name + ".class");
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(p);
			out.close();
			fileOut.close();

		}

		catch (IOException i) {
			i.printStackTrace();
		}

	}

	public static Page deserializePage(String name) {

		Page p = new Page();

		try {

			FileInputStream fileIn = new FileInputStream("data/" + name + ".class");
			ObjectInputStream in = new ObjectInputStream(fileIn);
			p = (Page) in.readObject();
			in.close();
			fileIn.close();

		}

		catch (IOException i) {
			i.printStackTrace();
		}

		catch (ClassNotFoundException c) {
			System.out.println("Page class not found");
			c.printStackTrace();
		}
		return p;
	}

	public static void deletePage(String name) {
		try {
			File f = new File("data/" + name + ".class");
			f.delete();
		}

		catch (Exception i) {
			i.printStackTrace();
		}

	}

	public Object getMaxKey() {
		int index = record.size() - 1;
		return record.get(index).get(key);
	}
}
