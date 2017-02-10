package indiana.cgl.hadoop.pagerank;

import java.util.ArrayList;

public class RankRecord {
	public Long sourceUrl;
	public double rankValue;
	public ArrayList<Long> targetUrlsList;

	public RankRecord(String strLine) {
		String[] strArray = strLine.split("#");
		sourceUrl = Long.parseLong(strArray[0].split("\t")[0]);
		rankValue = Double.parseDouble(strArray[0].split("\t")[1]);
		targetUrlsList = new ArrayList<Long>();
		for (int i = 1; i < strArray.length; i++) {
			targetUrlsList.add(Long.parseLong(strArray[i]));
		}
	}
}
