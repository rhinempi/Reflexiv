package uni.bielefeld.cmg.reflexiv.util;


import org.apache.commons.cli.*;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by rhinempi on 22.07.2017.
 *
 *       Reflexiv
 *
 * Copyright (c) 2017.
 *       Liren Huang     <huanglr at cebitec.uni-bielefeld.de>
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
 * Returns an object for parsing the input options for Reflexiv counter.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
 */
public class ParameterOfCounter {
    private String[] arguments;
    private InfoDumper info = new InfoDumper();

    /**
     * A constructor that construct an object of {@link Parameter} class.
     *
     * @param arguments an array of strings containing commandline options
     * @throws IOException
     * @throws ParseException
     */
    public ParameterOfCounter(String[] arguments) throws IOException, ParseException {
        this.arguments = arguments;
    }

    private static final Options parameter = new Options();

    DefaultParam param = new DefaultParam();

    private static final String
            INPUT_FASTQ = "fastq",
            INPUT_FASTA = "fasta",
            INPUT_CODEC = "infmt",
            READNUMBER= "reads",
            OUTPUT_FILE = "outfile",
            OUTPUT_CODEC="gzip",
            KMER_SIZE = "kmer",
            OVERLAP = "overlap",
            FRONTCLIP = "clipf",
            ENDCLIP = "clipe",
            MINCOVER= "cover",
            MAXCOVER= "maxcov",
            MINLENGTH = "minlength",
            PARTITIONS = "partition",
            SHUFFLEPARTITIONS = "partitionredu",
            CACHE = "cache",
            VERSION = "version",
            HELP2 = "h",
            HELP = "help";

    private static final Map<String, Integer> parameterMap = new HashMap<String, Integer>();

    /**
     * This method places all input parameters into a hashMap.
     */
    public void putParameterID() {
        int o = 0;

        parameterMap.put(INPUT_FASTQ, o++);
        parameterMap.put(INPUT_FASTA, o++);
        parameterMap.put(INPUT_CODEC, o++);
        parameterMap.put(READNUMBER, o++);
        parameterMap.put(OUTPUT_FILE, o++);
        parameterMap.put(OUTPUT_CODEC, o++);
        parameterMap.put(KMER_SIZE, o++);
        parameterMap.put(OVERLAP, o++);
        parameterMap.put(FRONTCLIP, o++);
        parameterMap.put(ENDCLIP, o++);
        parameterMap.put(MINCOVER, o++);
        parameterMap.put(MAXCOVER, o++);
        parameterMap.put(MINLENGTH, o++);
        parameterMap.put(PARTITIONS, o++);
        parameterMap.put(SHUFFLEPARTITIONS, o++);
        parameterMap.put(CACHE, o++);
        parameterMap.put(VERSION, o++);
        parameterMap.put(HELP2, o++);
        parameterMap.put(HELP, o ++);
    }

    /**
     * This method adds descriptions to each parameter.
     */
    public void addParameterInfo(){

        /*  use Object parameter of Options class to store parameter information   */

        parameter.addOption(OptionBuilder.withArgName("input fastq file")
                .hasArg().withDescription("Input NGS data, fastq file format, four line per unit")
                .create(INPUT_FASTQ));

        parameter.addOption(OptionBuilder.withArgName("input fasta file")
                .hasArg().withDescription("Also input NGS data, but in fasta file format, two line per unit")
                .create(INPUT_FASTA));

        parameter.addOption(OptionBuilder.withArgName("input compression format")
                .hasArg().withDescription("Compression format of the input files. Default mc4. !!! set this to anything other than mc4, e.g. gzip, if you did not pre-process the input data")
                .create(INPUT_CODEC));

        parameter.addOption(OptionBuilder.withArgName("input read number")
                .hasArg().withDescription("limit input read number for processing")
                .create(READNUMBER));

        parameter.addOption(OptionBuilder.withArgName("output file")
                .hasArg().withDescription("Output assembly result")
                .create(OUTPUT_FILE));

        parameter.addOption(OptionBuilder.withArgName("output compression")
                .hasArg(false).withDescription("Set to compress output files")
                .create(OUTPUT_CODEC));

        parameter.addOption(OptionBuilder.withArgName("kmer size")
                .hasArg().withDescription("Kmer length for reads mapping")
                .create(KMER_SIZE));

        parameter.addOption(OptionBuilder.withArgName("kmer overlap")
                .hasArg().withDescription("Overlap size between two adjacent kmers")
                .create(OVERLAP));

        parameter.addOption(OptionBuilder.withArgName("clip front nt")
                .hasArg().withDescription("Clip N number of nucleotides from the beginning of the reads")
                .create(FRONTCLIP));

        parameter.addOption(OptionBuilder.withArgName("clip end nt")
                .hasArg().withDescription("Clip N number of nucleotides from the end of the reads")
                .create(ENDCLIP));

        parameter.addOption(OptionBuilder.withArgName("minimal kmer coverage")
                .hasArg().withDescription("Minimal coverage to filter low freq kmers")
                .create(MINCOVER));

        parameter.addOption(OptionBuilder.withArgName("maximal kmer coverage")
                .hasArg().withDescription("Maximal coverage to filter high freq kmers")
                .create(MAXCOVER));

        parameter.addOption(OptionBuilder.withArgName("minimal read length")
                .hasArg().withDescription("Minimal read length required for assembly")
                .create(MINLENGTH));

        parameter.addOption(OptionBuilder.withArgName("re-partition number")
                .hasArg().withDescription("re generate N number of partitions")
                .create(PARTITIONS));

        parameter.addOption(OptionBuilder.withArgName("re-partition number")
                .hasArg().withDescription("re generate N number of partitions for reducer")
                .create(SHUFFLEPARTITIONS));

        parameter.addOption(OptionBuilder.withArgName("cache data RAM")
                .hasArg(false).withDescription("weather to store data in memory or not")
                .create(CACHE));

        parameter.addOption(OptionBuilder
                .hasArg(false).withDescription("show version information")
                .create(VERSION));

        parameter.addOption(OptionBuilder
                .hasArg(false).withDescription("print and show this information")
                .create(HELP));

        parameter.addOption(OptionBuilder
                .hasArg(false).withDescription("")
                .create(HELP2));
    }

    /* main method */

    /**
     * This method parses input commandline arguments and sets correspond
     * parameters.
     *
     * @return {@link DefaultParam}.
     */
    public DefaultParam importCommandLine(){

        /* Assigning Parameter ID to an ascending number */
        putParameterID();

        /* Assigning parameter descriptions to each parameter ID */
        addParameterInfo();

        /* need a Object parser of PosixParser class for the function parse of CommandLine class */
        PosixParser parser = new PosixParser();

        /* print out help information */
        HelpParam help = new HelpParam(parameter, parameterMap);

        /* check each parameter for assignment */
        try {
            long input_limit = -1;
            int threads = Runtime.getRuntime().availableProcessors();

			/* Set Object cl of CommandLine class for Parameter storage */
            CommandLine cl = parser.parse(parameter, arguments, true);
            if (cl.hasOption(HELP)) {
                help.printHelpOfCounter();
                System.exit(0);
            }

            if (cl.hasOption(HELP2)) {
                help.printHelpOfCounter();
                System.exit(0);
            }

            if (cl.hasOption(VERSION)) {
                System.exit(0);
            }

            if (cl.hasOption(OUTPUT_CODEC)) {
                param.gzip =true;
            }

			/* Checking all parameters */

            String value;

            if ((value = cl.getOptionValue(KMER_SIZE)) != null) {
                if (Integer.decode(value) >= 1 || Integer.decode(value) <= 100) {
                    param.kmerSize = Integer.decode(value);
                    param.setKmerSize(param.kmerSize);
                    param.setSubKmerSize(param.kmerSize-1);
                    param.setKmerBinarySlots(param.kmerSize);
                    param.setKmerSizeResidue(param.kmerSize);
                } else {
                    throw new RuntimeException("Parameter " + KMER_SIZE +
                            " should be set between 1-100");
                }
            }

            if ((value = cl.getOptionValue(OVERLAP)) != null) {
                if (Integer.decode(value) >= 0 || Integer.decode(value) <= param.kmerSize) {
                    param.kmerOverlap = Integer.decode(value);
                } else {
                    throw new RuntimeException("Parameter " + OVERLAP +
                            " should not be bigger than kmer size or smaller than 0");
                }
            }

            if ((value = cl.getOptionValue(PARTITIONS)) != null){
                if (Integer.decode(value) >= 0 ) {
                    param.partitions = Integer.decode(value);
                }else{
                    throw new RuntimeException("Parameter " + PARTITIONS+
                            " should be larger than 0");
                }
            }

            if ((value = cl.getOptionValue(READNUMBER)) != null){
                if (Long.decode(value) >= 0 ){
                    param.readLimit = Long.decode(value);
                }else{
                    throw new RuntimeException("Parameter " + READNUMBER +
                            " should be larger than 0");
                }
            }

            if ((value = cl.getOptionValue(SHUFFLEPARTITIONS)) != null){
                if (Integer.decode(value) >= 0 ) {
                    param.shufflePartition = Integer.decode(value);
                }else{
                    throw new RuntimeException("Parameter " + SHUFFLEPARTITIONS+
                            " should be larger than 0");
                }
            }

            if ((value = cl.getOptionValue(FRONTCLIP)) != null){
                if (Integer.decode(value) >0 ) {
                    param.frontClip = Integer.decode(value);
                }else{
                    throw new RuntimeException("Parameter " + FRONTCLIP +
                            " should be larger than 1");
                }
            }

            if ((value = cl.getOptionValue(ENDCLIP)) != null){
                if (Integer.decode(value) > 0 ) {
                    param.endClip = Integer.decode(value);
                }else{
                    throw new RuntimeException("Parameter " + ENDCLIP +
                            " should be larger than 1");
                }
            }

            if ((value = cl.getOptionValue(MINCOVER)) != null){
                if (Integer.decode(value) >= 0 ) {
                    param.minKmerCoverage = Integer.decode(value);
                }else{
                    throw new RuntimeException("Parameter " + MINCOVER+
                            " should be larger than 0");
                }
            }

            if ((value = cl.getOptionValue(MAXCOVER)) != null){
                if (Integer.decode(value) >= 0 ) {
                    param.maxKmerCoverage = Integer.decode(value);
                }else{
                    throw new RuntimeException("Parameter " + MAXCOVER+
                            " should be smaller than 1000000");
                }
            }

            if ((value = cl.getOptionValue(MINLENGTH)) != null){
                if (Integer.decode(value) >= 0 ){
                    param.minReadSize = Integer.decode(value);
                }else{
                    throw new RuntimeException("Parameter " + MINLENGTH +
                            " should be larger than 0");
                }
            }

            if ((value = cl.getOptionValue(INPUT_CODEC)) != null){
                    param.inputFormat = value;
            }


            if ((value = cl.getOptionValue(INPUT_FASTQ)) != null) {
                param.inputFqPath = value;
            } else if ((value = cl.getOptionValue(INPUT_FASTA)) != null){
                param.inputFaPath = value;
            } else{
                help.printHelpOfCounter();
                System.exit(0);
                //throw new IOException("Input query file not specified.\nUse -help for list of options");
            }

            if ((value = cl.getOptionValue(OUTPUT_FILE)) != null){
                param.outputPath = value;
            }else{
                info.readMessage("Output file not set of -outfile options");
                info.screenDump();
            }

            File outfile = new File(param.outputPath).getAbsoluteFile();
            if (outfile.exists()) {
                info.readParagraphedMessages("Output file : \n\t" + param.outputPath + "\nalready exists, will be overwrite.");
                info.screenDump();
                // Runtime.getRuntime().exec("rm -rf " + param.outputPath);
            }


        } /*catch (IOException e) { // Don`t catch this, NaNaNaNa, U can`t touch this.
            info.readMessage("Parameter settings incorrect.");
            info.screenDump();
            e.printStackTrace();
            System.exit(0);
        }*/ catch (RuntimeException e) {
            info.readMessage("Parameter settings incorrect.");
            info.screenDump();
            e.printStackTrace();
            System.exit(0);
        } catch (ParseException e) {
            info.readMessage("Parameter settings incorrect.");
            info.screenDump();
            e.printStackTrace();
            System.exit(0);
        }

        return param;
    }
}
