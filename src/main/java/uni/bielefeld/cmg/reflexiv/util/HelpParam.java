package uni.bielefeld.cmg.reflexiv.util;


import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.PrintWriter;
import java.util.Comparator;
import java.util.Map;

import static java.lang.System.err;

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


public class HelpParam {
    private final Options parameter;
    private final Map<String, Integer> parameterMap;

    /**
     *
     * @param parameter
     * @param parameterMap
     */
    public HelpParam(Options parameter, Map<String, Integer> parameterMap){
        /**
         * utilizes HelpFormatter to dump command line information for using the pipeline
         */
        this.parameter = parameter;
        this.parameterMap = parameterMap;
    }

    /**
     * print out help info with parameters
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
        final String executable2 = System.getProperty("executable2", "reflexiv mapper [spark parameter]");
        err.println("Name:");
        err.println("\tReflexiv Main");
        err.println();
        err.println("Options:");
        formatter.printOptions(new PrintWriter(err, true), 85, parameter, 2, 3); /* print formatted parameters */
        err.println();
        err.println("Usage:");
        err.println("\trun de novo genome assembly : ");
        err.println(executable + " [parameters] -fastq input.fq -kmer 63 -outfile output_file");
        err.println(executable + " [parameters] -fasta input.txt -kmer 63 -outfile output_file");
        err.println(executable2 + " [parameters] -fastq input.fq -kmer 63 -outfile output_file\"");
        err.println();
    }
}
