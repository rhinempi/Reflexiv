package uni.bielefeld.cmg.reflexiv.pipeline;


import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.expressions.IntegerLiteral;
import scala.Tuple2;
import scala.Tuple4;
import uni.bielefeld.cmg.reflexiv.util.DefaultParam;
import uni.bielefeld.cmg.reflexiv.util.InfoDumper;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


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
 * Returns an object for running the Reflexiv counter pipeline.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
 */
public class ReflexivCounter implements Serializable{
    private long time;
    private DefaultParam param;

    private InfoDumper info = new InfoDumper();

    /**
     *
     */
    private void clockStart() {
        time = System.currentTimeMillis();
    }

    /**
     *
     * @return
     */
    private long clockCut() {
        long tmp = time;
        time = System.currentTimeMillis();
        return time - tmp;
    }

    /**wc
     *
     * @return
     */
    private SparkConf setSparkConfiguration(){
        SparkConf conf = new SparkConf().setAppName("Reflexiv");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "uni.bielefeld.cmg.reflexiv.serializer.SparkKryoRegistrator");

        return conf;
    }

    private SparkSession setSparkSessionConfiguration(){
        SparkSession spark = SparkSession
                .builder()
                .appName("Reflexiv")
                .config("spark.kryo.registrator", "uni.bielefeld.cmg.reflexiv.serializer.SparkKryoRegistrator")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .getOrCreate();

        return spark;
    }

    /**
     *
     */
    public void assembly(){
        SparkConf conf = setSparkConfiguration();
        info.readMessage("Initiating Spark context ...");
        info.screenDump();
        info.readMessage("Start Spark framework");
        info.screenDump();
        JavaSparkContext sc = new JavaSparkContext(conf);

        SparkSession spark = setSparkSessionConfiguration();

        JavaRDD<String> FastqRDD;
        JavaPairRDD<String, Integer> KmerRDD;
        JavaPairRDD<Long, Integer> KmerBinaryRDD;

        /* Tuple4 data struct (reflexiv marker, rest of the string, coverage of prefix, coverage of suffix)*/
        JavaPairRDD<Long, Tuple4<Integer, Long, Integer, Integer>> ReflexivSubKmerRDD; // both
        JavaPairRDD<Long, Tuple4<Integer, Long[], Integer, Integer>> ReflexivLongSubKmerRDD;

        JavaPairRDD<String, Tuple4<Integer, String, Integer, Integer>> ReflexivSubKmerStringRDD; // Generates strings, for testing
        //      JavaPairRDD<String, Tuple4<Integer, String, Integer, Integer>> ForwardSubKmerRDD;
  //      JavaPairRDD<String, Tuple4<Integer, String, Integer, Integer>> ReflectedSubKmerRDD;

        JavaPairRDD<String, String> ContigTuple2RDD;
        JavaPairRDD<Tuple2<String, String>, Long> ContigTuple2IndexRDD;
        JavaRDD<String> ContigRDD;


        FastqRDD = sc.textFile(param.inputFqPath);

        /**
         * Step 1: filter and check input fastq file
         */
        clockStart();
        FastqFilterWithQual RDDFastqFilter = new FastqFilterWithQual();
        FastqRDD = FastqRDD.map(RDDFastqFilter);
        long zeit = clockCut();

        /**
         * Step 2: filter null units introduced in the above step
         */
        FastqUnitFilter RDDFastqUnitFilter = new FastqUnitFilter();
        FastqRDD = FastqRDD.filter(RDDFastqUnitFilter);
        if (param.partitions > 0) {
            FastqRDD = FastqRDD.repartition(param.partitions);
        }
        if (param.cache) {
            FastqRDD.cache();
        }

        /**
         * Step 3: extract kmers from sequencing reads and
         *          and build <kmer, count> tuples.
         */

        ReverseComplementKmerBinaryExtraction RDDExtractRCKmerBinaryFromFastq = new ReverseComplementKmerBinaryExtraction();
        KmerBinaryRDD = FastqRDD.mapPartitionsToPair(RDDExtractRCKmerBinaryFromFastq);


        /**
         * Step 4: counting kmer frequencies with reduceByKey function
         */

        KmerCounting RDDCountingKmerFreq = new KmerCounting();
        KmerBinaryRDD = KmerBinaryRDD.reduceByKey(RDDCountingKmerFreq);

        /**
         * Step 5: filter kmers by coverage
         */
        if (param.minKmerCoverage >1) {
            KmerCoverageFilter RDDKmerFilter = new KmerCoverageFilter();
            KmerBinaryRDD = KmerBinaryRDD.filter(RDDKmerFilter);
        }

        BinaryKmerToString KmerStringOutput = new BinaryKmerToString();

        KmerRDD = KmerBinaryRDD.mapPartitionsToPair(KmerStringOutput);

        if (param.gzip) {
            KmerRDD.saveAsTextFile(param.outputPath, GzipCodec.class);
        }else{
            KmerRDD.saveAsTextFile(param.outputPath);
        }

        sc.stop();
    }


    class BinaryKmerToString implements PairFlatMapFunction<Iterator<Tuple2<Long, Integer>>, String, Integer>, Serializable{
        List<Tuple2<String, Integer>> reflexivKmerStringList = new ArrayList<Tuple2<String, Integer>>();

        public Iterator<Tuple2<String, Integer>> call(Iterator<Tuple2<Long, Integer>> sIterator){
            while (sIterator.hasNext()){
                String subKmer = "";
                Tuple2<Long, Integer> s = sIterator.next();
                for (int i=1; i<=param.kmerSize;i++){
                    Long currentNucleotideBinary = s._1 >>> 2*(param.kmerSize - i);
                    currentNucleotideBinary &= 3L;
                    char currentNucleotide =  BinaryToNucleotide(currentNucleotideBinary);
                    subKmer += currentNucleotide;
                }

                reflexivKmerStringList.add (
                        new Tuple2<String, Integer>(subKmer, s._2)
                );
            }
            return reflexivKmerStringList.iterator();
        }

        private char BinaryToNucleotide (Long twoBits){
            char nucleotide;
            if (twoBits == 0){
                nucleotide = 'A';
            }else if (twoBits == 1){
                nucleotide = 'C';
            }else if (twoBits == 2){
                nucleotide = 'G';
            }else{
                nucleotide = 'T';
            }
            return nucleotide;
        }
    }

    /**
     * interface class for RDD implementation, used in step 4
     */
    class KmerCounting implements Function2<Integer, Integer, Integer>, Serializable{
        public Integer call (Integer i1, Integer i2) {
            return i1 + i2;
        }
    }

    class ReverseComplementKmerBinaryExtraction implements PairFlatMapFunction<Iterator<String>, Long, Integer>, Serializable{
        long maxKmerBits= ~((~0L) << (2*param.kmerSize));

        List<Tuple2<Long, Integer>> kmerList = new ArrayList<Tuple2<Long, Integer>>();
        int readLength;
        String[] units;
        String read;
        char nucleotide;
        long nucleotideInt;
        long nucleotideIntComplement;

        public Iterator<Tuple2<Long, Integer>> call(Iterator<String> s){

            while (s.hasNext()) {
                units = s.next().split("\\n");
                read = units[1];
                readLength = read.length();

                if (readLength - param.kmerSize - param.endClip <= 1 || param.frontClip > readLength) {
                    continue;
                }

                Long nucleotideBinary = 0L;
                Long nucleotideBinaryReverseComplement = 0L;

                for (int i = param.frontClip; i < readLength - param.endClip; i++) {
                    nucleotide = read.charAt(i);
                    if (nucleotide >= 256) nucleotide = 255;
                    nucleotideInt = nucleotideValue(nucleotide);
                    // forward kmer in bits
                    nucleotideBinary <<= 2;
                    nucleotideBinary |= nucleotideInt;
                    if (i - param.frontClip >= param.kmerSize) {
                        nucleotideBinary &= maxKmerBits;
                    }

                    // reverse kmer binarizationalitivities :) non English native speaking people making fun of English
                    nucleotideIntComplement = nucleotideInt ^ 3;  // 3 is binary 11; complement: 11(T) to 00(A), 10(G) to 01(C)

                    if (i - param.frontClip >= param.kmerSize) {
                        nucleotideBinaryReverseComplement >>>= 2;
                        nucleotideIntComplement <<= 2 * (param.kmerSize - 1);
                    } else {
                        nucleotideIntComplement <<= 2 * (i - param.frontClip);
                    }
                    nucleotideBinaryReverseComplement |= nucleotideIntComplement;

                    // reach the first complete K-mer
                    if (i - param.frontClip >= param.kmerSize - 1) {
                        if (nucleotideBinary.compareTo(nucleotideBinaryReverseComplement) < 0) {
                            kmerList.add(new Tuple2<Long, Integer>(nucleotideBinary, 1));
                        } else {
                            kmerList.add(new Tuple2<Long, Integer>(nucleotideBinaryReverseComplement, 1));
                        }
                    }
                }
            }
            return kmerList.iterator();
        }

        private long nucleotideValue(char a) {
            long value;
            if (a == 'A') {
                value = 0L;
            } else if (a == 'C') {
                value = 1L;
            } else if (a == 'G') {
                value = 2L;
            } else { // T
                value = 3L;
            }
            return value;
        }
    }

    /**
     * interface class for RDD implementation, Used in step 1
     */
    class FastqUnitFilter implements Function<String, Boolean>, Serializable{
        public Boolean call(String s){
            return s != null;
        }
    }

    /**
     * interface class for RDD implementation, used in step 2
     */
    class FastqFilterWithQual implements Function<String, String>, Serializable{
        String line = "";
        int lineMark = 0;
        public String call(String s) {
            if (lineMark == 2) {
                lineMark++;
                line = line + "\n" + s;
                return null;
            } else if (lineMark == 3) {
                lineMark++;
                line = line + "\n" + s;
                return line;
            } else if (s.startsWith("@")) {
                line = s;
                lineMark = 1;
                return null;
            } else if (lineMark == 1) {
                line = line + "\n" + s;
                lineMark++;
                return null;
            }else{
                return null;
            }
        }
    }

    class KmerCoverageFilter implements Function<Tuple2<Long, Integer>, Boolean>, Serializable{
        public Boolean call(Tuple2<Long, Integer> s){
            return s._2 >= param.minKmerCoverage && s._2 <= param.maxKmerCoverage;
        }

    }

    /**
     *
     * @param param
     */
    public void setParam(DefaultParam param) {
        this.param = param;
    }
}
