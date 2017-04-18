package edu.iu.mbkmeans.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class DataFileGenerator {

	private final String FILE_NAME = "DataSet_" + System.currentTimeMillis() + ".txt";

	private DataFileGenerator() {
	}

	public synchronized static void generate(int nbrOfPoints, int dimensions, int[] rangePerDimensions) {
		dimensions = dimensions == 0 ? 2 : dimensions;
		if (rangePerDimensions.length != dimensions) {
			rangePerDimensions = new int[dimensions];
			for (int i = 0; i < dimensions; i++)
				rangePerDimensions[i] = 10;
		}
		DataFileGenerator dfg = new DataFileGenerator();
		dfg.writeToFile(nbrOfPoints, dimensions, rangePerDimensions);
	}

	private void writeToFile(int nbrOfPoints, int dimensions, int[] rangePerDimensions) {
		try {
			double point;
			File file = new File(FILE_NAME);
			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);
			Random random = new Random(System.currentTimeMillis());
			int flushCount = 0;
			for (int i = 0; i < nbrOfPoints; i++) {
				for (int j = 0; j < dimensions; j++) {
					point = random.nextDouble() * rangePerDimensions[j];
					// System.out.println(point+"\t");
					if (j == dimensions - 1) {
						bw.write(point + "");
						if (i != (nbrOfPoints - 1))
							bw.newLine();
					} else {
						bw.write(point + " ");
					}
				}
				if (++flushCount == 100) {
					bw.flush();
					flushCount = 0;
				}
			}
			bw.close();
			System.out.println("Done writing " + nbrOfPoints + " points" + "to file " + FILE_NAME);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
