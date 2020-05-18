package uni.bielefeld.cmg.reflexiv.util;


import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.cli.ParseException;

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
 * Returns an object for parsing the input options for Reflexiv run.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
 */
public class Parameter {
    private String[] arguments;
    private InfoDumper info = new InfoDumper();

    /**
     * A constructor that construct an object of {@link Parameter} class.
     *
     * @param arguments an array of strings containing commandline options
     * @throws IOException
     * @throws ParseException
     */
    public Parameter(String[] arguments) throws IOException, ParseException {
        this.arguments = arguments;
    }

    private static final Options parameter = new Options();

    DefaultParam param = new DefaultParam();

    private static final String
            INPUT_FASTQ = "fastq",
            INPUT_FASTA = "fasta",
            INPUT_CONTIG= "contig",
            INPUT_KMER= "kmerc",
            OUTPUT_FILE = "outfile",
            KMER_SIZE = "kmer",
            OUTPUT_CODEC="gzip",
            OVERLAP = "overlap",
            MINITERATIONS = "miniter",
            MAXITERATIONS = "maxiter",
            FRONTCLIP = "clipf",
            ENDCLIP = "clipe",
            MINCOVER= "cover",
            MAXCOVER= "maxcov",
            MINERROR= "error",
            BUBBLE = "bubble",
            MERCY = "mercy",
            MINLENGTH = "minlength",
            MINCONTIG = "mincontig",
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
        parameterMap.put(INPUT_CONTIG, o++);
        parameterMap.put(INPUT_KMER, o++);
        parameterMap.put(OUTPUT_FILE, o++);
        parameterMap.put(KMER_SIZE, o++);
        parameterMap.put(OVERLAP, o++);
        parameterMap.put(MINITERATIONS, o++);
        parameterMap.put(MAXITERATIONS, o++);
        parameterMap.put(FRONTCLIP, o++);
        parameterMap.put(ENDCLIP, o++);
        parameterMap.put(MINCOVER, o++);
        parameterMap.put(MAXCOVER, o++);
        parameterMap.put(MINERROR, o++);
        parameterMap.put(MINLENGTH, o++);
        parameterMap.put(MINCONTIG, o++);
        parameterMap.put(PARTITIONS, o++);
        parameterMap.put(SHUFFLEPARTITIONS, o++);
        parameterMap.put(OUTPUT_CODEC, o++);
        parameterMap.put(BUBBLE, o++);
        parameterMap.put(MERCY, o++);
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

        parameter.addOption(OptionBuilder.withArgName("input contig file")
                .hasArg().withDescription("Also input NGS data, but in fasta file format, two line per unit")
                .create(INPUT_CONTIG));

        parameter.addOption(OptionBuilder.withArgName("input Kmer file")
                .hasArg().withDescription("Input counted kmer file, tabular file format or spark RDD pair text file")
                .create(INPUT_KMER));

        parameter.addOption(OptionBuilder.withArgName("output file")
                .hasArg().withDescription("Output assembly result")
                .create(OUTPUT_FILE));

        parameter.addOption(OptionBuilder.withArgName("kmer size")
                .hasArg().withDescription("Kmer length for reads mapping")
                .create(KMER_SIZE));

        parameter.addOption(OptionBuilder.withArgName("kmer overlap")
                .hasArg().withDescription("Overlap size between two adjacent kmers")
                .create(OVERLAP));

        parameter.addOption(OptionBuilder.withArgName("remove bubbles")
                .hasArg(false).withDescription("Set to NOT remove bubbles.")
                .create(BUBBLE));

        parameter.addOption(OptionBuilder.withArgName("without mercy k-mer")
                .hasArg(false).withDescription("Set to DISable mercy k-mer.")
                .create(MERCY));

        parameter.addOption(OptionBuilder.withArgName("output compression")
                .hasArg(false).withDescription("Set to compress output files")
                .create(OUTPUT_CODEC));

        parameter.addOption(OptionBuilder.withArgName("minimum iterations")
                .hasArg().withDescription("Minimum iterations for contig construction")
                .create(MINITERATIONS));

        parameter.addOption(OptionBuilder.withArgName("maximum iterations")
                .hasArg().withDescription("Maximum iterations for contig construction")
                .create(MAXITERATIONS));

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

        parameter.addOption(OptionBuilder.withArgName("minimum error correction cover")
                .hasArg().withDescription("Minimum coverage for correcting sequencing errors. Used for low coverage sequencing. Should be higher than minimal kmer coverage -cover")
                .create(MINERROR));

        parameter.addOption(OptionBuilder.withArgName("minimal read length")
                .hasArg().withDescription("Minimal read length required for assembly")
                .create(MINLENGTH));

        parameter.addOption(OptionBuilder.withArgName("minimal contig length")
                .hasArg().withDescription("Minimal contig length to be reported")
                .create(MINCONTIG));

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
                help.printHelp();
                System.exit(0);
            }

            if (cl.hasOption(HELP2)) {
                help.printHelp();
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
                    param.setSubKmerBinarySlots(param.subKmerSize);
                    param.setSubKmerSizeResidue(param.subKmerSize);
                    param.setKmerSizeResidueAssemble(param.kmerSize);
                    param.setKmerBinarySlotsAssemble(param.kmerSize);
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

            if ((value = cl.getOptionValue(SHUFFLEPARTITIONS)) != null){
                if (Integer.decode(value) >= 0 ) {
                    param.shufflePartition = Integer.decode(value);
                }else{
                    throw new RuntimeException("Parameter " + SHUFFLEPARTITIONS+
                            " should be larger than 0");
                }
            }

            if (cl.hasOption(BUBBLE)){
                param.bubble =false;
            }

            if (cl.hasOption(MERCY)){
                param.mercy = false;
            }

            if (cl.hasOption(CACHE)){
                param.cache = true;
            }

            if ((value = cl.getOptionValue(MINITERATIONS)) != null){
                if (Integer.decode(value) >= 0 ) {
                    param.minimumIteration = Integer.decode(value);
                }else{
                    throw new RuntimeException("Parameter " + MINITERATIONS +
                            " should be larger than 0");
                }
            }

            if ((value = cl.getOptionValue(MAXITERATIONS)) != null){
                if (Integer.decode(value) <= 100000 ) {
                    param.maximumIteration = Integer.decode(value);
                }else{
                    throw new RuntimeException("Parameter " + MAXITERATIONS +
                            " should be smaller than 100000");
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
                    param.setMinErrorCoverage(param.minKmerCoverage); // default minErrorCoverage is equal to minKmerCoverage
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

            if ((value = cl.getOptionValue(MINERROR)) != null){
                if (Integer.decode(value) >= 0 ) {
                    param.minErrorCoverage = Integer.decode(value);
                }else{
                    throw new RuntimeException("Parameter " + MINERROR+
                            " should be larger than 0");
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

            if ((value = cl.getOptionValue(MINCONTIG)) != null){

                if (Integer.decode(value) >= 0 ){
                    param.minContig = Integer.decode(value);
                }else{
                    throw new RuntimeException("Parameter " + MINCONTIG +
                            " should be larger than 0");
                }
            }

            if ((value = cl.getOptionValue(INPUT_FASTQ)) != null) {
                param.inputFqPath = value;
            } else if ((value = cl.getOptionValue(INPUT_FASTA)) != null){
                param.inputFaPath = value;
            } else if ((value = cl.getOptionValue(INPUT_KMER)) != null) {
                param.inputKmerPath = value;
            } else{
                help.printHelp();
                System.exit(0);
                //throw new IOException("Input query file not specified.\nUse -help for list of options");
            }

            // both kmer count and input fastq exist, input fastq will be used for mercy k-mer
            if (cl.getOptionValue(INPUT_KMER) != null &&  cl.getOptionValue(INPUT_FASTQ) != null) {
                param.inputKmerPath = cl.getOptionValue(INPUT_KMER);
                param.inputFqPath = cl.getOptionValue(INPUT_FASTQ);
            }

            if (cl.getOptionValue(INPUT_CONTIG) !=null){
                param.inputContigPath = cl.getOptionValue(INPUT_CONTIG);
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
