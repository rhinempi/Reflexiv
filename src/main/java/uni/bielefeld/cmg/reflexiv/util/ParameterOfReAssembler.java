package uni.bielefeld.cmg.reflexiv.util;


import org.apache.commons.cli.*;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Liren Huang on 24.08.17.
 *
 *      Reflexiv
 *
 * Copyright (c) 2015-2015
 *      Liren Huang      <huanglr at cebitec.uni-bielefeld.de>
 * 
 * Reflexiv is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; Without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more detail.
 * 
 * You should have received a copy of the GNU General Public License along
 * with this program. If not, see <http://www.gnu.org/licenses>.
 *
 */


public class ParameterOfReAssembler {
    private String[] arguments;
    private InfoDumper info = new InfoDumper();

    public ParameterOfReAssembler(String[] arguments) throws IOException, ParseException {
        this.arguments = arguments;
    }

    private static final Options parameter = new Options();

    DefaultParam param = new DefaultParam();

    private static final String
            INPUT_FASTQ = "fastq",
            INPUT_KMER= "kmerc",
            INPUT_FASTA = "fasta",
            INPUT_FRAG = "frag",
            OUTPUT_FILE = "outfile",
            KMER_SIZE = "kmer",
            OVERLAP = "overlap",
            MINITERATIONS = "miniter",
            MAXITERATIONS = "maxiter",
            FRONTCLIP = "clipf",
            ENDCLIP = "clipe",
            MINCOVER= "cover",
            MAXCOVER= "maxcov",
            BUBBLE = "bubble",
            MINLENGTH = "minlength",
            MINCONTIG = "mincontig",
            PARTITIONS = "partition",
            CACHE = "cache",
            VERSION = "version",
            HELP2 = "h",
            HELP = "help";

    private static final Map<String, Integer> parameterMap = new HashMap<String, Integer>();

    public void putParameterID() {
        int o = 0;

        parameterMap.put(INPUT_FASTQ, o++);
        parameterMap.put(INPUT_KMER, o++);
        parameterMap.put(INPUT_FASTA, o++);
        parameterMap.put(INPUT_FRAG, o++);
        parameterMap.put(OUTPUT_FILE, o++);
        parameterMap.put(KMER_SIZE, o++);
        parameterMap.put(OVERLAP, o++);
        parameterMap.put(MINITERATIONS, o++);
        parameterMap.put(MAXITERATIONS, o++);
        parameterMap.put(FRONTCLIP, o++);
        parameterMap.put(ENDCLIP, o++);
        parameterMap.put(MINCOVER, o++);
        parameterMap.put(MAXCOVER, o++);
        parameterMap.put(MINLENGTH, o++);
        parameterMap.put(MINCONTIG, o++);
        parameterMap.put(PARTITIONS, o++);
        parameterMap.put(BUBBLE, o++);
        parameterMap.put(CACHE, o++);
        parameterMap.put(VERSION, o++);
        parameterMap.put(HELP2, o++);
        parameterMap.put(HELP, o ++);
    }

    public void addParameterInfo(){

        /*  use Object parameter of Options class to store parameter information   */

        parameter.addOption(OptionBuilder.withArgName("input fastq file")
                .hasArg().withDescription("Input NGS data, fastq file format, four line per unit")
                .create(INPUT_FASTQ));

        parameter.addOption(OptionBuilder.withArgName("input Kmer file")
                .hasArg().withDescription("Input counted kmer file, tabular file format or spark RDD pair text file")
                .create(INPUT_KMER));

        parameter.addOption(OptionBuilder.withArgName("input fasta file")
                .hasArg().withDescription("Also input NGS data, but in fasta file format, two line per unit")
                .create(INPUT_FASTA));

        parameter.addOption(OptionBuilder.withArgName("input pre assemblies")
                .hasArg().withDescription("Input pre-assembled contig fragments to be extended")
                .create(INPUT_FRAG));

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

        parameter.addOption(OptionBuilder.withArgName("minimal read length")
                .hasArg().withDescription("Minimal read length required for assembly")
                .create(MINLENGTH));

        parameter.addOption(OptionBuilder.withArgName("minimal contig length")
                .hasArg().withDescription("Minimal contig length to be reported")
                .create(MINCONTIG));

        parameter.addOption(OptionBuilder.withArgName("re-partition number")
                .hasArg().withDescription("re generate N number of partitions")
                .create(PARTITIONS));

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
                help.printHelpOfReAssembler();
                System.exit(0);
            }

            if (cl.hasOption(HELP2)) {
                help.printHelpOfReAssembler();
                System.exit(0);
            }

            if (cl.hasOption(VERSION)) {
                System.exit(0);
            }

			/* Checking all parameters */

            String value;

            if ((value = cl.getOptionValue(KMER_SIZE)) != null) {
                if (Integer.decode(value) >= 1 || Integer.decode(value) <= 100) {
                    param.kmerSize = Integer.decode(value);
                    param.setKmerSize(param.kmerSize);
                    param.setSubKmerSize(param.kmerSize-1);
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

            if (cl.hasOption(BUBBLE)){
                param.bubble =false;
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
            } else if ((value = cl.getOptionValue(INPUT_KMER)) != null){
                param.inputKmerPath = value;
            }else{
                help.printHelpOfReAssembler();
                System.exit(0);
                //throw new IOException("Input query file not specified.\nUse -help for list of options");
            }

            if ((value = cl.getOptionValue(INPUT_FRAG)) != null) {
                param.inputContigPath = value;
            } else {
                help.printHelpOfReAssembler();
                System.exit(0);
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
                Runtime.getRuntime().exec("rm -rf " + param.outputPath);
            }


        } catch (IOException e) { // Don`t catch this, NaNaNaNa, U can`t touch this.
            info.readMessage("Parameter settings incorrect.");
            info.screenDump();
            e.printStackTrace();
            System.exit(0);
        } catch (RuntimeException e) {
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
