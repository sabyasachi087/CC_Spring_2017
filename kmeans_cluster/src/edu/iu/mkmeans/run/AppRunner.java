package edu.iu.mkmeans.run;

import edu.iu.mbkmeans.util.DataFileGenerator;

public class AppRunner {

	public static void main(String args[]) {
		DataFileGenerator.generate(1000, 4, new int[] { 10, 10, 10, 10 });
	}

}
