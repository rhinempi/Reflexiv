package uni.bielefeld.cmg.reflexiv.util;


import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.PrintWriter;
import java.util.Comparator;
import java.util.Map;

import static java.lang.System.err;

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
 * Returns an object for dumping help information to the screen.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
 */
public class HelpParam {
    private final Options parameter;
    private final Map<String, Integer> parameterMap;

    /**
     * A constructor that construct an object of {@link HelpParam} class.
     *
     * @param parameter {@link Options} the commandline options.
     * @param parameterMap a {@link Map} that stores the parameter.
     */
    public HelpParam(Options parameter, Map<String, Integer> parameterMap){
        /**
         * utilizes HelpFormatter to dump command line information for using the pipeline
         */
        this.parameter = parameter;
        this.parameterMap = parameterMap;
    }

    /**
     * This method prints out help info for Reflexiv run
     */
    public void printHelp(){
        HelpFormatter formatter =new HelpFormatter();
        formatter.setOptionComparator(new Comparator<Option>(){
            public int compare(Option o1, Option o2){
                return Integer.compare(parameterMap.get(o1.getOpt()),
                        parameterMap.get(o2.getOpt()));
            }
        });

        final String executable = System.getProperty("executable", "spark-submit [spark parameter] --class uni.bielefeld.cmg.reflexiv.main.Main reflexiv.jar");
        final String executable2 = System.getProperty("executable2", "reflexiv run [spark parameter]");
        err.println("Name:");
        err.println("\tReflexiv Main");
        err.println();
        err.println("Options:");
        formatter.printOptions(new PrintWriter(err, true), 85, parameter, 2, 3); /* print formatted parameters */
        err.println();
        err.println("Usage:");
        err.println("\trun de novo genome assembly : ");
        err.println(executable + " [parameters] -fastq input.fq -kmer 31 -outfile output_file");
        err.println(executable + " [parameters] -fasta input.txt -kmer 31 -outfile output_file");
        err.println(executable2 + " [parameters] -fastq input.fq -kmer 31 -outfile output_file");
        err.println();
    }

    /**
     * This method prints out help info for Reflexiv counter
     */
    public void printHelpOfCounter(){
        HelpFormatter formatter =new HelpFormatter();
        formatter.setOptionComparator(new Comparator<Option>(){
            public int compare(Option o1, Option o2){
                return Integer.compare(parameterMap.get(o1.getOpt()),
                        parameterMap.get(o2.getOpt()));
            }
        });

        final String executable = System.getProperty("executable", "spark-submit [spark parameter] --class uni.bielefeld.cmg.reflexiv.main.MainOfCounter reflexiv.jar");
        final String executable2 = System.getProperty("executable2", "reflexiv counter [spark parameter]");
        err.println("Name:");
        err.println("\tReflexiv Kmer Counter");
        err.println();
        err.println("Options:");
        formatter.printOptions(new PrintWriter(err, true), 85, parameter, 2, 3); /* print formatted parameters */
        err.println();
        err.println("Usage:");
        err.println("\trun K-mer counting : ");
        err.println(executable + " [parameters] -fastq input.fq -kmer 31 -outfile kmerCount.out");
        err.println(executable + " [parameters] -fasta input.txt -kmer 31 -outfile kmerCount.out");
        err.println(executable2 + " [parameters] -fastq input.fq -kmer 31 -outfile kmerCount.out");
        err.println();
    }

    /**
     * This method prints out help info for Reflexiv reassembler
     */
    public void printHelpOfReAssembler(){
        HelpFormatter formatter =new HelpFormatter();
        formatter.setOptionComparator(new Comparator<Option>(){
            public int compare(Option o1, Option o2){
                return Integer.compare(parameterMap.get(o1.getOpt()),
                        parameterMap.get(o2.getOpt()));
            }
        });

        final String executable = System.getProperty("executable", "spark-submit [spark parameter] --class uni.bielefeld.cmg.reflexiv.main.MainOfReAssembler reflexiv.jar");
        final String executable2 = System.getProperty("executable2", "reflexiv reassembler [spark parameter]");
        err.println("Name:");
        err.println("\tReflexiv Re-Assembler");
        err.println();
        err.println("Options:");
        formatter.printOptions(new PrintWriter(err, true), 85, parameter, 2, 3); /* print formatted parameters */
        err.println();
        err.println("Usage:");
        err.println("\trun de novo genome Re-assembly : ");
        err.println(executable + " [parameters] -fastq input.fq -frag gene_frag.fa -kmer 31 -outfile reassemble.out");
        err.println(executable + " [parameters] -fasta input.txt -frag gene_frag.fa -kmer 31 -outfile reassemble.out");
        err.println(executable2 + " [parameters] -fastq input.fq -frag gene_frag.fa -kmer 31 -outfile reassemble.out");
        err.println();
    }
}
