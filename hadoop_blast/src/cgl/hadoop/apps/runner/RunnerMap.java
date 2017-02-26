/**
 * Software License, Version 1.0
 * 
 * Copyright 2003 The Trustees of Indiana University.  All rights reserved.
 * 
 *
 *Redistribution and use in source and binary forms, with or without 
 *modification, are permitted provided that the following conditions are met:
 *
 *1) All redistributions of source code must retain the above copyright notice,
 * the list of authors in the original source code, this list of conditions and
 * the disclaimer listed in this license;
 *2) All redistributions in binary form must reproduce the above copyright 
 * notice, this list of conditions and the disclaimer listed in this license in
 * the documentation and/or other materials provided with the distribution;
 *3) Any documentation included with all redistributions must include the 
 * following acknowledgement:
 *
 *"This product includes software developed by the Community Grids Lab. For 
 * further information contact the Community Grids Lab at 
 * http://communitygrids.iu.edu/."
 *
 * Alternatively, this acknowledgement may appear in the software itself, and 
 * wherever such third-party acknowledgments normally appear.
 * 
 *4) The name Indiana University or Community Grids Lab or NaradaBrokering, 
 * shall not be used to endorse or promote products derived from this software 
 * without prior written permission from Indiana University.  For written 
 * permission, please contact the Advanced Research and Technology Institute 
 * ("ARTI") at 351 West 10th Street, Indianapolis, Indiana 46202.
 *5) Products derived from this software may not be called NaradaBrokering, 
 * nor may Indiana University or Community Grids Lab or NaradaBrokering appear
 * in their name, without prior written permission of ARTI.
 * 
 *
 * Indiana University provides no reassurances that the source code provided 
 * does not infringe the patent or any other intellectual property rights of 
 * any other entity.  Indiana University disclaims any liability to any 
 * recipient for claims brought by any other entity based on infringement of 
 * intellectual property rights or otherwise.  
 *
 *LICENSEE UNDERSTANDS THAT SOFTWARE IS PROVIDED "AS IS" FOR WHICH NO 
 *WARRANTIES AS TO CAPABILITIES OR ACCURACY ARE MADE. INDIANA UNIVERSITY GIVES
 *NO WARRANTIES AND MAKES NO REPRESENTATION THAT SOFTWARE IS FREE OF 
 *INFRINGEMENT OF THIRD PARTY PATENT, COPYRIGHT, OR OTHER PROPRIETARY RIGHTS. 
 *INDIANA UNIVERSITY MAKES NO WARRANTIES THAT SOFTWARE IS FREE FROM "BUGS", 
 *"VIRUSES", "TROJAN HORSES", "TRAP DOORS", "WORMS", OR OTHER HARMFUL CODE.  
 *LICENSEE ASSUMES THE ENTIRE RISK AS TO THE PERFORMANCE OF SOFTWARE AND/OR 
 *ASSOCIATED MATERIALS, AND TO THE PERFORMANCE AND VALIDITY OF INFORMATION 
 *GENERATED USING SOFTWARE.
 */
package cgl.hadoop.apps.runner;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Thilina Gunarathne (tgunarat@cs.indiana.edu)
 * 
 * @editor Stephen, TAK-LON WU (taklwu@indiana.edu)
 */

public class RunnerMap extends Mapper<String, String, IntWritable, Text> {

	private String localDB = "";
	private String localBlastProgram = "";
	// This should have been come from the input, but for now it has been
	// hardcoded, change as per the installation folder of the executables
	private String localPathDir = "/home/sabyasachi/Documents/Indiana_University/Spring_2017/Cloud_Computing/Lab3/dwl/";
	private Integer INPUT_FILE_COUNT = 1;

	@Override
	public void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		// /Path[] local = DistributedCache.getLocalCacheArchives(conf);
		// test for 2.6 + version
		{
			for (URI fileUri : context.getCacheFiles()) {
				localDB = fileUri.toString();
				localBlastProgram = localDB;
			}
		}
		localBlastProgram = localPathDir;

		/**
		 * Write your code here get two absolute filepath for localDB and
		 * localBlastBinary
		 **/
	}

	public void map(String key, String value, Context context) throws IOException, InterruptedException {

		long startTime = System.currentTimeMillis();
		String endTime = "";

		Configuration conf = context.getConfiguration();
		String programDir = conf.get(DataAnalysis.PROGRAM_DIR);
		String execName = conf.get(DataAnalysis.EXECUTABLE);
		String cmdArgs = conf.get(DataAnalysis.PARAMETERS);
		String outputDir = conf.get(DataAnalysis.OUTPUT_DIR);
		String workingDir = conf.get(DataAnalysis.WORKING_DIR);
		String dbName = conf.get(DataAnalysis.DB_NAME);
		this.localDB = localPathDir + "db" + File.separator + dbName;
		System.out.println("the map key : " + key);
		System.out.println("the value path : " + value.toString());
		System.out.println("Local DB : " + this.localDB);
		System.out.println("programDir : " + programDir);
		System.out.println("execName : " + execName);

		// We have the full file names in the value.
		String inputFileName = "";
		String fileNameOnly = "final_file";
		String outFile = "sabyroyc_out";
		String stdOutFile = "sabyroyc_stdout";
		String stdErrFile = "sabyroyc_stderr";

		INPUT_FILE_COUNT = getCounter(value);
		fileNameOnly = fileNameOnly + "_" + INPUT_FILE_COUNT + ".txt";
		stdOutFile = stdOutFile + "_" + INPUT_FILE_COUNT + ".txt";
		outFile = outFile + "_" + INPUT_FILE_COUNT + ".txt";
		stdErrFile = stdErrFile + "_" + INPUT_FILE_COUNT + ".txt";
		inputFileName = "input_" + (INPUT_FILE_COUNT) + ".txt";

		String localInputFile = localPathDir + inputFileName;
		System.out.println("Local input path = " + localInputFile);

		/*
		 * if (10 % 2 == 0) { System.out.println("Returning for now .... ");
		 * return; }
		 */
		/**
		 * Write your code to get localInputFile, outFile, stdOutFile and
		 * stdErrFile
		 **/

		// download the file from HDFS
		Path inputFilePath = new Path(value);
		System.out.println("Input file path : " + inputFilePath.getName());
		FileSystem fs = inputFilePath.getFileSystem(conf);
		fs.copyToLocalFile(inputFilePath, new Path(localInputFile));

		// Prepare the arguments to the executable
		String execCommand = cmdArgs.replaceAll("#_INPUTFILE_#", localInputFile);
		if (cmdArgs.indexOf("#_OUTPUTFILE_#") > -1) {
			execCommand = execCommand.replaceAll("#_OUTPUTFILE_#", outFile);
		} else {
			outFile = stdOutFile;
		}

		endTime = Double.toString(((System.currentTimeMillis() - startTime) / 1000.0));
		System.out.println("Before running the executable Finished in " + endTime + " seconds");

		execCommand = this.localBlastProgram + File.separator + execName + " " + execCommand + " -db " + this.localDB;
		// Create the external process
		System.out.println("Final execute command = " + execCommand);
		startTime = System.currentTimeMillis();

		Process p = Runtime.getRuntime().exec(execCommand);

		OutputHandler inputStream = new OutputHandler(p.getInputStream(), "INPUT", stdOutFile);
		OutputHandler errorStream = new OutputHandler(p.getErrorStream(), "ERROR", stdErrFile);

		// start the stream threads.
		inputStream.start();
		errorStream.start();

		p.waitFor();
		// end time of this procress
		endTime = Double.toString(((System.currentTimeMillis() - startTime) / 1000.0));
		System.out.println("Program Finished in " + endTime + " seconds");

		// Upload the results to HDFS
		startTime = System.currentTimeMillis();

		Path outputDirPath = new Path(outputDir);
		Path outputFileName = new Path(outputDirPath, fileNameOnly);
		fs.copyFromLocalFile(new Path(outFile), outputFileName);

		endTime = Double.toString(((System.currentTimeMillis() - startTime) / 1000.0));
		System.out.println("Upload Result Finished in " + endTime + " seconds");

	}

	private Integer getCounter(String value) {
		System.out.println("Getting file counter from " + value);
		String cnt = value.charAt(value.length() - 4) + "";
		System.out.println("Counter found = " + cnt);
		return Integer.parseInt(cnt);
	}
}
