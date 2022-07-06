package org.fog.test.perfeval;

public class CSV {

	public static StringBuilder csv = new StringBuilder();
	private static int id = 0;

	public static void write(String lat, String lon, String block, String level, String parent, String details) {
		csv.append(id);
		csv.append(",");
		csv.append(lat);
		csv.append(",");
		csv.append(lon);
		csv.append(",");
		csv.append(block);
		csv.append(",");
		csv.append(level);
		csv.append(",");
		csv.append(parent);
		csv.append(",");
		csv.append("VIC");
		csv.append(",");
		csv.append(details);
		csv.append("\n");
		id++;
	}

	public static StringBuilder getCsv() {
		return csv;
	}

}
