package uni.bielefeld.cmg.reflexiv.pipeline;


import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import uni.bielefeld.cmg.reflexiv.io.ContigReader;
import uni.bielefeld.cmg.reflexiv.io.ReadFasta;
import uni.bielefeld.cmg.reflexiv.util.DefaultParam;
import uni.bielefeld.cmg.reflexiv.util.InfoDumper;

import java.io.Serializable;
import java.util.*;


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
 * Returns an object for running the Reflexiv reassembler pipeline.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
 */
public class ReflexivReAssembler implements Serializable{
    private long time;
    private DefaultParam param;
  //  Broadcast<String[]> broadcastedContigIDArray;
    Broadcast<String[]> broadcastedPrimerArray;

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

        ArrayList<Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>> reflexivContigList;

        JavaRDD<String> FastqRDD;
        JavaRDD<String> InputKmerRDD;
        JavaPairRDD<String, Integer> KmerRDD;
        JavaPairRDD<Long, Integer> KmerBinaryRDD;

        /* Tuple4 data struct (reflexiv marker, rest of the string, coverage of prefix, coverage of suffix)*/
        JavaPairRDD<Long, Tuple4<Integer, Long, Integer, Integer>> ReflexivSubKmerRDD; // both
        JavaPairRDD<Long, Tuple4<Integer, Long[], Integer, Integer>> ReflexivLongSubKmerRDD;
        JavaPairRDD<Long, Tuple4<Integer, Long[], Integer, Integer>> ReflexivLongContigRDD;

        JavaPairRDD<String, Tuple4<Integer, String, Integer, Integer>> ReflexivSubKmerStringRDD; // Generates strings, for testing
        //      JavaPairRDD<String, Tuple4<Integer, String, Integer, Integer>> ForwardSubKmerRDD;
        //      JavaPairRDD<String, Tuple4<Integer, String, Integer, Integer>> ReflectedSubKmerRDD;

        JavaPairRDD<String, String> ContigTuple2RDD;
        JavaPairRDD<Tuple2<String, String>, Long> ContigTuple2IndexRDD;
        JavaRDD<String> ContigRDD;
        JavaRDD<String> ReConstructedContigRDD;

        FastqRDD = sc.textFile(param.inputFqPath);

        ContigReader contigReader = new ContigReader();
        contigReader.setParameter(param);
        contigReader.loadRef(param.inputContigPath);
        reflexivContigList = contigReader.getReflexivContigList();
        String[] primerArray = contigReader.getPrimerArray();
        broadcastedPrimerArray = sc.broadcast(primerArray);

        int partitions = reflexivContigList.size() / 2000 + 1;
        if (partitions > 50) {
            partitions = 50;
        }

        ReflexivLongContigRDD = sc.parallelizePairs(reflexivContigList, partitions);

        BinaryReflexivKmerArrayToString ArrayStringOutput = new BinaryReflexivKmerArrayToString();
        ReflexivSubKmerStringRDD = ReflexivLongContigRDD.mapPartitionsToPair(ArrayStringOutput);

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

        LoadCountedKmerToLongArray kmerLoader = new LoadCountedKmerToLongArray();

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

        /**
         * Generate reverse complement Kmers
         */
        KmerReverseComplement RDDRCKmer = new KmerReverseComplement();
        KmerBinaryRDD = KmerBinaryRDD.mapPartitionsToPair(RDDRCKmer);

        /**
         * Step : filter forks
         */

        ForwardSubKmerLongExtraction RDDextractForwardLongSubKmer = new ForwardSubKmerLongExtraction();
        ReflexivLongSubKmerRDD = KmerBinaryRDD.mapPartitionsToPair(RDDextractForwardLongSubKmer);

        ReflexivLongSubKmerRDD = ReflexivLongSubKmerRDD.union(ReflexivLongContigRDD);

        if (param.bubble == true) {
            ReflexivLongSubKmerRDD = ReflexivLongSubKmerRDD.sortByKey();
            FilterForkSubKmerLong RDDhighCoverageLongSelector = new FilterForkSubKmerLong();
            ReflexivLongSubKmerRDD = ReflexivLongSubKmerRDD.mapPartitionsToPair(RDDhighCoverageLongSelector);

            ReflectedSubKmerExtractionFromForwardLong RDDreflectionExtractorLong = new ReflectedSubKmerExtractionFromForwardLong();
            ReflexivLongSubKmerRDD = ReflexivLongSubKmerRDD.mapPartitionsToPair(RDDreflectionExtractorLong);

            ReflexivLongSubKmerRDD = ReflexivLongSubKmerRDD.sortByKey();
            FilterForkReflectedSubKmerLong RDDhighCoverageReflectedLongSelector = new FilterForkReflectedSubKmerLong();
            ReflexivLongSubKmerRDD = ReflexivLongSubKmerRDD.mapPartitionsToPair(RDDhighCoverageReflectedLongSelector);

        }

        /**
         * first three extensions fit in one Long 1 2 4 8 16 32(x)
         */

        int iterations = 0;

        /**
         * Step 10: iteration: repeat step 6, 7 and 8 until convergence is reached
         */
        ExtendReflexivKmerToArrayLoop KmerExtenstionArrayToArray = new ExtendReflexivKmerToArrayLoop();

        int partitionNumber = ReflexivLongSubKmerRDD.getNumPartitions();
        long contigNumber = 0;
        while (iterations <= param.maximumIteration) {
            iterations++;
            if (iterations >= param.minimumIteration){
                if (iterations % 3 == 0) {

                    long currentContigNumber = ReflexivLongSubKmerRDD.count();
                    if (contigNumber == currentContigNumber) {
                        break;
                    } else {
                        contigNumber = currentContigNumber;
                    }

                    if (partitionNumber >= 16) {
                        if (currentContigNumber / partitionNumber <= 20) {
                            partitionNumber = partitionNumber / 4 + 1;
                            ReflexivLongSubKmerRDD = ReflexivLongSubKmerRDD.coalesce(partitionNumber);
                        }
                    }
                }
            }

            ReflexivLongSubKmerRDD = ReflexivLongSubKmerRDD.sortByKey();

//            ReflexivSubKmerStringRDD = ReflexivLongSubKmerRDD.mapPartitionsToPair(ArrayStringOutput);
//            ReflexivSubKmerStringRDD.saveAsTextFile(param.outputPath + iterations);

            ReflexivLongSubKmerRDD = ReflexivLongSubKmerRDD.mapPartitionsToPair(KmerExtenstionArrayToArray);

//            ReflexivSubKmerStringRDD = ReflexivLongSubKmerRDD.mapPartitionsToPair(ArrayStringOutput);
//            ReflexivSubKmerStringRDD.saveAsTextFile(param.outputPath + iterations + "Extend");

        }


        /**
         * Binary to string
         */
        ReflexivSubKmerStringRDD = ReflexivLongSubKmerRDD.mapPartitionsToPair(ArrayStringOutput);
        /**
         * Step 11: change reflexiv kmers to contig
         */

        KmerToContig contigformater = new KmerToContig();
        ContigTuple2RDD = ReflexivSubKmerStringRDD.mapPartitionsToPair(contigformater);

        ContigTuple2IndexRDD = ContigTuple2RDD.zipWithIndex();

        TagContigID IdLabeling = new TagContigID();
        ContigRDD = ContigTuple2IndexRDD.flatMap(IdLabeling);

        ExtractReContig ReContigfilter = new ExtractReContig();
        ReConstructedContigRDD = ContigRDD.filter(ReContigfilter);

        /**
         * Step N: save result
         */

        ReConstructedContigRDD.saveAsTextFile(param.outputPath + ".extended");
        ContigRDD.saveAsTextFile(param.outputPath);

        /**
         * Step N+1: Stop
         */
        sc.stop();
    }

    public void assemblyFromKmer(){
        SparkConf conf = setSparkConfiguration();
        info.readMessage("Initiating Spark context ...");
        info.screenDump();
        info.readMessage("Start Spark framework");
        info.screenDump();
        JavaSparkContext sc = new JavaSparkContext(conf);

        ArrayList<Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>> reflexivContigList;

        JavaRDD<String> FastqRDD;
        JavaRDD<String> InputKmerRDD;
        JavaPairRDD<String, Integer> KmerRDD;
        JavaPairRDD<Long, Integer> KmerBinaryRDD;

        /* Tuple4 data struct (reflexiv marker, rest of the string, coverage of prefix, coverage of suffix)*/
        JavaPairRDD<Long, Tuple4<Integer, Long, Integer, Integer>> ReflexivSubKmerRDD; // both
        JavaPairRDD<Long, Tuple4<Integer, Long[], Integer, Integer>> ReflexivLongSubKmerRDD;
        JavaPairRDD<Long, Tuple4<Integer, Long[], Integer, Integer>> ReflexivLongContigRDD;

        JavaPairRDD<String, Tuple4<Integer, String, Integer, Integer>> ReflexivSubKmerStringRDD; // Generates strings, for testing
        //      JavaPairRDD<String, Tuple4<Integer, String, Integer, Integer>> ForwardSubKmerRDD;
        //      JavaPairRDD<String, Tuple4<Integer, String, Integer, Integer>> ReflectedSubKmerRDD;

        JavaPairRDD<String, String> ContigTuple2RDD;
        JavaPairRDD<Tuple2<String, String>, Long> ContigTuple2IndexRDD;
        JavaRDD<String> ContigRDD;
        JavaRDD<String> ReConstructedContigRDD;

        InputKmerRDD = sc.textFile(param.inputKmerPath);

        ContigReader contigReader = new ContigReader();
        contigReader.setParameter(param);
        contigReader.loadRef(param.inputContigPath);
        reflexivContigList = contigReader.getReflexivContigList();
        String[] primerArray = contigReader.getPrimerArray();
        broadcastedPrimerArray = sc.broadcast(primerArray);

        int partitions = reflexivContigList.size() / 2000 + 1;
        if (partitions > 50) {
            partitions = 50;
        }

        ReflexivLongContigRDD = sc.parallelizePairs(reflexivContigList, partitions);

        BinaryReflexivKmerArrayToString ArrayStringOutput = new BinaryReflexivKmerArrayToString();
        ReflexivSubKmerStringRDD = ReflexivLongContigRDD.mapPartitionsToPair(ArrayStringOutput);

        /**
         * Step 1: filter and check input fastq file
         */

        /**
         * Step 2: filter null units introduced in the above step
         */
        if (param.partitions > 0) {
            InputKmerRDD = InputKmerRDD.repartition(param.partitions);
        }
        if (param.cache) {
            InputKmerRDD.cache();
        }

        /**
         * Step 3: extract kmers from sequencing reads and
         *          and build <kmer, count> tuples.
         */

        LoadCountedKmerToLongArray kmerLoader = new LoadCountedKmerToLongArray();

 //       ReverseComplementKmerBinaryExtraction RDDExtractRCKmerBinaryFromFastq = new ReverseComplementKmerBinaryExtraction();
        KmerBinaryRDD = InputKmerRDD.mapPartitionsToPair(kmerLoader);

        /**
         * Step 4: counting kmer frequencies with reduceByKey function
         */

   //     KmerCounting RDDCountingKmerFreq = new KmerCounting();
   //     KmerBinaryRDD = KmerBinaryRDD.reduceByKey(RDDCountingKmerFreq);

        /**
         * Step 5: filter kmers by coverage
         */
        if (param.minKmerCoverage >1) {
            KmerCoverageFilter RDDKmerFilter = new KmerCoverageFilter();
            KmerBinaryRDD = KmerBinaryRDD.filter(RDDKmerFilter);
        }

        /**
         * Generate reverse complement Kmers
         */
        KmerReverseComplement RDDRCKmer = new KmerReverseComplement();
        KmerBinaryRDD = KmerBinaryRDD.mapPartitionsToPair(RDDRCKmer);

        /**
         * Step : filter forks
         */

        ForwardSubKmerLongExtraction RDDextractForwardLongSubKmer = new ForwardSubKmerLongExtraction();
        ReflexivLongSubKmerRDD = KmerBinaryRDD.mapPartitionsToPair(RDDextractForwardLongSubKmer);

        ReflexivLongSubKmerRDD = ReflexivLongSubKmerRDD.union(ReflexivLongContigRDD);

        if (param.bubble == true) {
            ReflexivLongSubKmerRDD = ReflexivLongSubKmerRDD.sortByKey();
            FilterForkSubKmerLong RDDhighCoverageLongSelector = new FilterForkSubKmerLong();
            ReflexivLongSubKmerRDD = ReflexivLongSubKmerRDD.mapPartitionsToPair(RDDhighCoverageLongSelector);

            ReflectedSubKmerExtractionFromForwardLong RDDreflectionExtractorLong = new ReflectedSubKmerExtractionFromForwardLong();
            ReflexivLongSubKmerRDD = ReflexivLongSubKmerRDD.mapPartitionsToPair(RDDreflectionExtractorLong);

            ReflexivLongSubKmerRDD = ReflexivLongSubKmerRDD.sortByKey();
            FilterForkReflectedSubKmerLong RDDhighCoverageReflectedLongSelector = new FilterForkReflectedSubKmerLong();
            ReflexivLongSubKmerRDD = ReflexivLongSubKmerRDD.mapPartitionsToPair(RDDhighCoverageReflectedLongSelector);

        }

        /**
         * Step 9: filter extended Kmers
         */


        /**
         * first three extensions fit in one Long 1 2 4 8 16 32(x)
         */

        int iterations = 0;

        /**
         * Step 10: iteration: repeat step 6, 7 and 8 until convergence is reached
         */
        ExtendReflexivKmerToArrayLoop KmerExtenstionArrayToArray = new ExtendReflexivKmerToArrayLoop();

        int partitionNumber = ReflexivLongSubKmerRDD.getNumPartitions();
        long contigNumber = 0;
        while (iterations <= param.maximumIteration) {
            iterations++;
            if (iterations >= param.minimumIteration){
                if (iterations % 3 == 0) {

                    long currentContigNumber = ReflexivLongSubKmerRDD.count();
                    if (contigNumber == currentContigNumber) {
                        break;
                    } else {
                        contigNumber = currentContigNumber;
                    }

                    if (partitionNumber >= 16) {
                        if (currentContigNumber / partitionNumber <= 20) {
                            partitionNumber = partitionNumber / 4 + 1;
                            ReflexivLongSubKmerRDD = ReflexivLongSubKmerRDD.coalesce(partitionNumber);
                        }
                    }
                }
            }

            ReflexivLongSubKmerRDD = ReflexivLongSubKmerRDD.sortByKey();

//            ReflexivSubKmerStringRDD = ReflexivLongSubKmerRDD.mapPartitionsToPair(ArrayStringOutput);
//            ReflexivSubKmerStringRDD.saveAsTextFile(param.outputPath + iterations);

            ReflexivLongSubKmerRDD = ReflexivLongSubKmerRDD.mapPartitionsToPair(KmerExtenstionArrayToArray);

//            ReflexivSubKmerStringRDD = ReflexivLongSubKmerRDD.mapPartitionsToPair(ArrayStringOutput);
//            ReflexivSubKmerStringRDD.saveAsTextFile(param.outputPath + iterations + "Extend");

        }

        /**
         * Binary to string
         */
        ReflexivSubKmerStringRDD = ReflexivLongSubKmerRDD.mapPartitionsToPair(ArrayStringOutput);

        /**
         * Step 11: change reflexiv kmers to contig
         */

        KmerToContig contigformater = new KmerToContig();
        ContigTuple2RDD = ReflexivSubKmerStringRDD.mapPartitionsToPair(contigformater);

        ContigTuple2IndexRDD = ContigTuple2RDD.zipWithIndex();

        TagContigID IdLabeling = new TagContigID();
        ContigRDD = ContigTuple2IndexRDD.flatMap(IdLabeling);

        ExtractReContig ReContigfilter = new ExtractReContig();
        ReConstructedContigRDD = ContigRDD.filter(ReContigfilter);

        /**
         * Step N: save result
         */

        ReConstructedContigRDD.saveAsTextFile(param.outputPath + ".extended");
        ContigRDD.saveAsTextFile(param.outputPath);

        /**
         * Step N+1: Stop
         */
        sc.stop();
    }


    class TagContigID implements FlatMapFunction<Tuple2<Tuple2<String, String>, Long>, String>, Serializable {

        public Iterator<String> call(Tuple2<Tuple2<String, String>, Long> s) {


            List<String> contigList = new ArrayList<String>();

            contigList.add(s._1._1 + "-" + s._2 + "\n" + s._1._2);

            return contigList.iterator();
        }
    }

    class ExtractReContig implements Function<String, Boolean>, Serializable{
        public Boolean call (String contig){
            if (contig.startsWith(">ReCon")) {
                return true;
            }else{
                return false;
            }
        }
    }


    /**
     * interface class for RDD implementation, used in step 5
     */
    class KmerToContig implements PairFlatMapFunction<Iterator<Tuple2<String, Tuple4<Integer, String, Integer, Integer>>>, String, String>, Serializable{
        String[] primerArray = broadcastedPrimerArray.value();


        public Iterator<Tuple2<String, String>> call (Iterator<Tuple2<String, Tuple4<Integer, String, Integer, Integer>>> sIterator){
            String[][] primerArrayTuple3 = new String[primerArray.length][3];

            for (int j=0; j< primerArray.length ; j++){
                primerArrayTuple3[j][0] = primerArray[j].substring(0,param.subKmerSize);
                primerArrayTuple3[j][1] = primerArray[j].substring(param.subKmerSize, 2*param.subKmerSize);
                primerArrayTuple3[j][2] = primerArray[j].substring(2*param.subKmerSize);
            }

            List<Tuple2<String, String>> contigList = new ArrayList<Tuple2<String, String>>();
            while(sIterator.hasNext()) {
                Tuple2<String, Tuple4<Integer, String, Integer, Integer>> s = sIterator.next();
                String contig;

                if (s._2._1() == 1) {
                    contig = s._1 + s._2._2();
                } else {
                    contig = s._2._2() + s._1;
                }

                int length = contig.length();
                if (length >= param.minContig) {
                    String ID = ">ReCon-length:" + length;

                    boolean reAssembleMarker = false;
                    for (int i = 0; i < primerArray.length; i++) {
                        int begin = contig.indexOf(primerArrayTuple3[i][0]);
                        int end = contig.indexOf(primerArrayTuple3[i][1]);
                        if (begin >= 0 && end >= 0) {
                            reAssembleMarker = true;
                            end+=param.subKmerSize;
                            ID += "-begin:" + begin + "-end:" + end + "-" + primerArrayTuple3[i][2];
                        } else {

                        }
                    }

                    if (!reAssembleMarker) {
                        ID = ">Contig-length:" + length + "-";
                    }

                    String formatedContig = changeLine(contig, length, 100);
                    contigList.add(new Tuple2<String, String>(ID, formatedContig));
                }
            }
            return contigList.iterator();
        }

        public String changeLine(String oneLine, int lineLength, int limitedLength){
            String blockLine = "";
            int fold = lineLength / limitedLength;
            int remainder = lineLength % limitedLength;
            if (fold ==0) {
                blockLine = oneLine;
            }else if (fold == 1 && remainder == 0){
                blockLine = oneLine;
            }else if (fold >1 && remainder == 0){
                for (int i =0 ; i<fold-1 ; i++ ){
                    blockLine += oneLine.substring(i*limitedLength, (i+1)*limitedLength) + "\n";
                }
                blockLine += oneLine.substring((fold-1)*limitedLength);
            }else {
                for (int i =0 ; i<fold ; i++ ){
                    blockLine += oneLine.substring(i*limitedLength, (i+1)*limitedLength) + "\n";
                }
                blockLine += oneLine.substring(fold*limitedLength);
            }

            return blockLine;
        }
    }

    /**
     *
     */
    class BinaryReflexivKmerArrayToString implements PairFlatMapFunction<Iterator<Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>>, String, Tuple4<Integer, String, Integer, Integer>>, Serializable{
        List<Tuple2<String, Tuple4<Integer, String, Integer, Integer>>> reflexivKmerStringList = new ArrayList<Tuple2<String, Tuple4<Integer, String, Integer, Integer>>>();

        public Iterator<Tuple2<String, Tuple4<Integer, String, Integer, Integer>>> call(Iterator<Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>> sIterator) {
            while (sIterator.hasNext()) {
                String subKmer = "";
                String subString = "";
                Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>> s = sIterator.next();

                int firstSuffixBlockLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros(s._2._2()[0]) / 2 + 1);

                for (int i = 1; i <= param.subKmerSize; i++) {
                    Long currentNucleotideBinary = s._1 >>> 2 * (param.subKmerSize - i);
                    currentNucleotideBinary &= 3L;
                    char currentNucleotide = BinaryToNucleotide(currentNucleotideBinary);
                    subKmer += currentNucleotide;
                }

                for (int i = 0; i < s._2._2().length; i++) {
                    if (i == 0) {
                        for (int j = 1; j <= firstSuffixBlockLength; j++) { // j=0 including the C marker; for debug
                            Long currentNucleotideBinary = s._2._2()[i] >>> 2 * (firstSuffixBlockLength - j);
                            currentNucleotideBinary &= 3L;
                            char currentNucleotide = BinaryToNucleotide(currentNucleotideBinary);
                            subString += currentNucleotide;
                        }
                    } else {
                        for (int j = 1; j <= 31; j++) {
                            if (s._2._2()[i] == null){
                                System.out.println(subKmer + "\t" + subString);
                                continue;
                            }
                            Long currentNucleotideBinary = s._2._2()[i] >>> 2 * (31 - j);
                            currentNucleotideBinary &= 3L;
                            char currentNucleotide = BinaryToNucleotide(currentNucleotideBinary);
                            subString += currentNucleotide;
                        }
                    }
                    //     subString += ">----<";
                }

                reflexivKmerStringList.add(
                        new Tuple2<String, Tuple4<Integer, String, Integer, Integer>>(
                                subKmer, new Tuple4<Integer, String, Integer, Integer>(s._2._1(), subString, s._2._3(), s._2._4())
                        )
                );

            }
            return reflexivKmerStringList.iterator();
        }

        private char BinaryToNucleotide(Long twoBits) {
            char nucleotide;
            if (twoBits == 0) {
                nucleotide = 'A';
            } else if (twoBits == 1) {
                nucleotide = 'C';
            } else if (twoBits == 2) {
                nucleotide = 'G';
            } else {
                nucleotide = 'T';
            }
            return nucleotide;

        }
    }
    /**
     *
     */
    class ExtendReflexivKmerToArrayLoop implements PairFlatMapFunction<Iterator<Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>>, Long, Tuple4<Integer, Long[], Integer, Integer>>, Serializable{

        /* marker to identify similar SubKmers in the loop sequence */
        private int lineMarker=1;

        /* 1 stands for forward sub-kmer */
        /* 2 stands for reflexiv sub-kmer */
        // private int randomReflexivMarker = ThreadLocalRandom.current().nextInt(1, 3);
        private int randomReflexivMarker = 2;

        long maxSubKmerBinary = ~((~0L) << 2*param.subKmerSize);
        long maxBlockBinary = ~((~0L) << 2*31); // a block has 31 nucleotide


        /* temporary capsule to store identical SubKmer units */
        List<Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>> tmpReflexivKmerExtendList = new ArrayList<Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>>();

        /* return capsule of extend Tuples for next iteration*/
        List<Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>> reflexivKmerConcatList = new ArrayList<Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>>();

        /**
         *
         * @param sIterator is the input data structure Tuple2<SubKmer, Tuple2<Marker, TheRestSequence>>
         *          s._1 represents sub kmer sequence
         *          s._2._1 represents sub kmer marker: 1, for forward sub kmer;
         *                                              2, for reverse (reflexiv) sub kmer;
         *          s._2._2 represents the rest sequence.
         *          s._2._2 represents the coverage of the K-mer
         * @return a list of extended Tuples for next iteration
         */
        public Iterator<Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>> call (Iterator<Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>> sIterator) {

            while (sIterator.hasNext()) {
                Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>> s = sIterator.next();

                /* receive the first sub-kmer, set new units */
                if (lineMarker == 1) {
                    resetSubKmerGroup(s);

                    // return reflexivKmerConcatList.iterator();
                }

                /* removal condition */
                /**
                 * Deprecated function for killer k-mers
                 */

                /* next element of RDD */
                else {/* if (lineMarker >= 2){ */
                /* initiate a new capsule for the current sub-kmer group */
                    //      reflexivKmerConcatList = new ArrayList<Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>>();

                    if (tmpReflexivKmerExtendList.size() == 0) {
                        directKmerComparison(s);
                    } else { /* tmpReflexivKmerExtendList.size() != 0 */
                        for (int i = 0; i < tmpReflexivKmerExtendList.size(); i++) { // the tmpReflexivKmerExtendList is changing dynamically
                            if (s._1.equals(tmpReflexivKmerExtendList.get(i)._1)) {
                                if (s._2._1() == 1) {
                                    if (tmpReflexivKmerExtendList.get(i)._2._1() == 2) {
                                        int tmpReflexivKmerSuffixLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros(tmpReflexivKmerExtendList.get(i)._2._2()[0]) / 2 + 1);
                                        int tmpBlockSize = (tmpReflexivKmerExtendList.get(i)._2._2().length - 1) * 31 + tmpReflexivKmerSuffixLength;
                                        int currentReflexivKmerSuffixLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros(s._2._2()[0]) / 2 + 1);
                                        int currentBlockSize = (s._2._2().length - 1) * 31 + currentReflexivKmerSuffixLength;

                                        if (s._2._3() < 0 && tmpReflexivKmerExtendList.get(i)._2._4() < 0) {
                                            reflexivExtend(s, tmpReflexivKmerExtendList.get(i), -1);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else if (s._2._3() >= 0 && tmpReflexivKmerExtendList.get(i)._2._4() >= 0) {
                                            reflexivExtend(s, tmpReflexivKmerExtendList.get(i), -1);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else if (s._2._3() >= 0 && s._2._3() - tmpBlockSize >= 0) {
                                            reflexivExtend(s, tmpReflexivKmerExtendList.get(i), s._2._3() - tmpBlockSize);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else if (tmpReflexivKmerExtendList.get(i)._2._4() >= 0 && tmpReflexivKmerExtendList.get(i)._2._4() - currentBlockSize >= 0) {
                                            reflexivExtend(s, tmpReflexivKmerExtendList.get(i), tmpReflexivKmerExtendList.get(i)._2._4() - currentBlockSize);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else {
                                            singleKmerRandomizer(s);
                                            break;
                                        }
                                    } else if (tmpReflexivKmerExtendList.get(i)._2._1() == 1) {
                                        singleKmerRandomizer(s);
                                        //directKmerComparison(s);
                                        break;
                                    }
                                } else { /* if (s._2._1() == 2) { */
                                    if (tmpReflexivKmerExtendList.get(i)._2._1() == 2) {
                                        singleKmerRandomizer(s);
                                        //directKmerComparison(s);
                                        break;
                                    } else if (tmpReflexivKmerExtendList.get(i)._2._1() == 1) {
                                        int tmpReflexivKmerSuffixLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros(tmpReflexivKmerExtendList.get(i)._2._2()[0]) / 2 + 1);
                                        int tmpBlockSize = (tmpReflexivKmerExtendList.get(i)._2._2().length - 1) * 31 + tmpReflexivKmerSuffixLength;
                                        int currentReflexivKmerSuffixLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros(s._2._2()[0]) / 2 + 1);
                                        int currentBlockSize = (s._2._2().length - 1) * 31 + currentReflexivKmerSuffixLength;
                                        if (s._2._4() < 0 && tmpReflexivKmerExtendList.get(i)._2._3() < 0) {
                                            reflexivExtend(tmpReflexivKmerExtendList.get(i), s, -1);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else if (s._2._4() >= 0 && tmpReflexivKmerExtendList.get(i)._2._3() >= 0) {
                                            reflexivExtend(tmpReflexivKmerExtendList.get(i), s, -1);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else if (s._2._4() >= 0 && s._2._4() - tmpBlockSize >= 0) {
                                            reflexivExtend(tmpReflexivKmerExtendList.get(i), s, s._2._4() - tmpBlockSize);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else if (tmpReflexivKmerExtendList.get(i)._2._3() >= 0 && tmpReflexivKmerExtendList.get(i)._2._4() - currentBlockSize >= 0) {
                                            reflexivExtend(tmpReflexivKmerExtendList.get(i), s, tmpReflexivKmerExtendList.get(i)._2._4() - currentBlockSize);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else {
                                            singleKmerRandomizer(s);
                                            break;
                                        }
                                    }
                                }
                            /* return reflexivKmerConcatList.iterator(); */
                            }

                        /* new Sub-kmer group section */
                            else { /* s._1 != tmpReflexivKmerExtendList.get(i)._1()*/
                                //  if (lineMarker == 2) { // lineMarker == 2 represents the second line of the partition
                                //     singleKmerRandomizer(tmpReflexivKmerExtendList.get(i));
                                // }
                                //  singleKmerRandomizer(s);
                                tmpKmerRandomizer();
                                resetSubKmerGroup(s);
                                break;
                            }
                        } /* end of the while loop */
                    }// end of else condition

                    lineMarker++;
                    // return reflexivKmerConcatList.iterator();
                }
            } // while loop
            tmpKmerRandomizer();
            return reflexivKmerConcatList.iterator();
        }

        /**
         *
         * @param currentSubKmer
         */
        public void singleKmerRandomizer(Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>> currentSubKmer){
            int blockSize = currentSubKmer._2._2().length;
            Long[] newReflexivLongArray= new Long[blockSize];

            if (currentSubKmer._2._1() == 1){
                int firstSuffixBlockLength = Long.SIZE/2 - (Long.numberOfLeadingZeros(currentSubKmer._2._2()[0])/2 + 1);
                long maxSuffixLengthBinary = ~((~0L) << (2*firstSuffixBlockLength));
                Long newReflexivSubKmer;
                Long newReflexivLong;

                if (randomReflexivMarker == 2) {
                    if ( blockSize > 1) {
                        newReflexivSubKmer = currentSubKmer._2._2()[blockSize-1] & maxSubKmerBinary;

                        // 3rd block and so on
                        for (int i=blockSize-1; i>1; i--){
                            newReflexivLong=currentSubKmer._2._2()[i] >>> 2*param.subKmerSize;
                            newReflexivLong|= (currentSubKmer._2._2()[i-1] << 2*(31-param.subKmerSize));
                            newReflexivLong&= maxBlockBinary;
                            newReflexivLongArray[i] = newReflexivLong;
                        }

                        // 2nd block
                        newReflexivLong=currentSubKmer._2._2()[1] >>> 2*param.subKmerSize;
                        newReflexivLong|= ((currentSubKmer._2._2()[0] & maxSuffixLengthBinary) << 2*(31-param.subKmerSize));
                        if (firstSuffixBlockLength < param.subKmerSize){
                            newReflexivLong |= (currentSubKmer._1 << 2*(31 - param.subKmerSize + firstSuffixBlockLength));
                        }
                        newReflexivLong&= maxBlockBinary;
                        newReflexivLongArray[1] = newReflexivLong;

                        // 1st block
                        if (firstSuffixBlockLength < param.subKmerSize){
                            newReflexivLong = currentSubKmer._1 >>> 2*(param.subKmerSize - firstSuffixBlockLength);
                            newReflexivLong |= (1L << 2 * firstSuffixBlockLength);
                        }else {
                            newReflexivLong = currentSubKmer._2._2()[0] & maxSuffixLengthBinary; //remove C marker
                            newReflexivLong >>>= 2 * param.subKmerSize;
                            newReflexivLong |= (currentSubKmer._1 << 2 * (firstSuffixBlockLength - param.subKmerSize));
                            newReflexivLong |= (1L << 2 * firstSuffixBlockLength); // add C marker
                        }
                        newReflexivLongArray[0] = newReflexivLong;
                    }else{
                        if (firstSuffixBlockLength >= param.subKmerSize){
                            newReflexivSubKmer = currentSubKmer._2._2()[0] & maxSubKmerBinary;

                            newReflexivLong = currentSubKmer._2._2()[0] & maxSuffixLengthBinary;
                            newReflexivLong >>>= 2*param.subKmerSize;
                            newReflexivLong |= (currentSubKmer._1 << 2*(firstSuffixBlockLength-param.subKmerSize));
                            newReflexivLong |= (1L << (2 * firstSuffixBlockLength)); // add C marker in the front
                        }else {
                            newReflexivSubKmer = currentSubKmer._1 << (firstSuffixBlockLength * 2);
                            newReflexivSubKmer &= maxSubKmerBinary;
                            newReflexivSubKmer |= (currentSubKmer._2._2()[0] & maxSuffixLengthBinary);

                            newReflexivLong = currentSubKmer._1 >>> (2 * (param.subKmerSize - firstSuffixBlockLength));
                            newReflexivLong |= (1L << (2 * firstSuffixBlockLength)); // add C marker in the front
                        }

                        newReflexivLongArray[0]=newReflexivLong;
                    }

                    reflexivKmerConcatList.add(
                            new Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>(newReflexivSubKmer,
                                    new Tuple4<Integer, Long[], Integer, Integer>(
                                            randomReflexivMarker, newReflexivLongArray, currentSubKmer._2._3(), currentSubKmer._2._4()
                                    )
                            )
                    );
                }else{
                    reflexivKmerConcatList.add(currentSubKmer);
                }
            }else{ /* currentSubKmer._2._1() == 2 */
                int firstPrefixLength = Long.SIZE/2 - (Long.numberOfLeadingZeros(currentSubKmer._2._2()[0])/2 + 1);
                long maxPrefixLengthBinary = ~((~0L) << (2*firstPrefixLength));

                Long newReflexivSubKmer;
                Long newReflexivLong;

                if (randomReflexivMarker == 2) {
                    reflexivKmerConcatList.add(currentSubKmer);
                }else{ /* randomReflexivMarker == 1 */
                    if (blockSize > 1){
                        // the subKmer
                        if (firstPrefixLength >= param.subKmerSize) {
                            newReflexivSubKmer = (currentSubKmer._2._2()[0] & maxPrefixLengthBinary) >>> 2* (firstPrefixLength-param.subKmerSize); // also removed C marker
                        }else{
                            newReflexivSubKmer = currentSubKmer._2._2()[0] & maxPrefixLengthBinary; // remove C marker
                            newReflexivSubKmer <<= 2*(param.subKmerSize-firstPrefixLength);
                            newReflexivSubKmer |= (currentSubKmer._2._2()[1] >>> 2*(31- param.subKmerSize + firstPrefixLength));
                        }

                        // the last block
                        newReflexivLong = currentSubKmer._2._2()[blockSize - 1] << 2 * param.subKmerSize;
                        newReflexivLong |= currentSubKmer._1;
                        newReflexivLong &= maxBlockBinary;
                        newReflexivLongArray[blockSize - 1] = newReflexivLong;

                        // 2nd and so on
                        for (int i=blockSize-2; i>=1;i--){
                            newReflexivLong = currentSubKmer._2._2()[i] << 2*param.subKmerSize;
                            newReflexivLong |= (currentSubKmer._2._2()[i+1] >>> 2*(31- param.subKmerSize));
                            newReflexivLong &=maxBlockBinary;
                            newReflexivLongArray[i] = newReflexivLong;
                        }

                        // 1st
                        newReflexivLong = currentSubKmer._2._2()[1] >>> 2*(31-param.subKmerSize);
                        if (firstPrefixLength >= param.subKmerSize) {
                            newReflexivLong |= ((currentSubKmer._2._2()[0] & maxPrefixLengthBinary) << 2 * param.subKmerSize);
                        }
                        newReflexivLong &= maxPrefixLengthBinary;
                        newReflexivLong |= (1L << 2*firstPrefixLength); // add C marker
                        newReflexivLongArray[0] = newReflexivLong;

                    }else{ /* blockSize = 1)*/
                        if (firstPrefixLength >= param.subKmerSize) {
                            newReflexivSubKmer = (currentSubKmer._2._2()[0] & maxPrefixLengthBinary) >>> 2*(firstPrefixLength - param.subKmerSize);
                            newReflexivSubKmer &= maxSubKmerBinary; // remove header, including C marker

                            newReflexivLong = (currentSubKmer._2._2()[0] & maxPrefixLengthBinary) << 2*param.subKmerSize;
                            newReflexivLong |= currentSubKmer._1;
                            newReflexivLong &= maxPrefixLengthBinary; // remove header, including C marker
                            newReflexivLong |= (1L << 2*firstPrefixLength); // add C marker
                        }else {
                            newReflexivSubKmer = (currentSubKmer._2._2()[0] & maxPrefixLengthBinary) << (2 * (param.subKmerSize - firstPrefixLength));
                            newReflexivSubKmer |= (currentSubKmer._1 >>> (2 * firstPrefixLength));

                            newReflexivLong = currentSubKmer._1 & maxPrefixLengthBinary;
                            newReflexivLong |= (1L << 2 * firstPrefixLength); // add C marker in the front
                        }

                        newReflexivLongArray[0]=newReflexivLong;
                    }

                    reflexivKmerConcatList.add(
                            new Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>(newReflexivSubKmer,
                                    new Tuple4<Integer, Long[], Integer, Integer>(
                                            randomReflexivMarker, newReflexivLongArray, currentSubKmer._2._3(), currentSubKmer._2._4()
                                    )
                            )
                    );
                }

            }

            /* an action of randomization */

            if (randomReflexivMarker == 1 ){
                randomReflexivMarker = 2;
            }else { /* randomReflexivMarker == 2 */
                randomReflexivMarker = 1;
            }
        }
        /**
         *
         * @param currentSubKmer
         */
        public void directKmerComparison(Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>> currentSubKmer){
            tmpReflexivKmerExtendList.add(currentSubKmer);
        }

        /**
         *
         * @param forwardSubKmer
         * @param reflexedSubKmer
         */
        public void reflexivExtend(Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>> forwardSubKmer, Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>> reflexedSubKmer, int bubbleDistance) {

             /* forward   ATCGATCG, 1, ------ */
             /* reflexed  ------, 2, ATCGATCG */

            int forwardFirstSuffixLength = Long.SIZE/2 - (Long.numberOfLeadingZeros(forwardSubKmer._2._2()[0])/2 + 1);
            int forwardBlockSize = forwardSubKmer._2._2().length;
            int forwardKmerLength= (forwardSubKmer._2._2().length - 1)*31 + forwardFirstSuffixLength;
            int reflexedFirstPrefixLength = Long.SIZE/2 - (Long.numberOfLeadingZeros(reflexedSubKmer._2._2()[0])/2 + 1);
            int reflexedBlockSize = reflexedSubKmer._2._2().length;
            int reflexedKmerLength=(reflexedSubKmer._2._2().length -1)*31 + reflexedFirstPrefixLength;
            int concatenateLength = forwardKmerLength + reflexedKmerLength;
            int concatBlockSize = concatenateLength/31;
            if (concatenateLength%31 !=0 ){
                concatBlockSize++;
            }

            long maxSuffixLengthBinary = ~((~0L) << 2*forwardFirstSuffixLength);
            long maxPrefixLengthBinary = ~((~0L) << 2*reflexedFirstPrefixLength);


            if (randomReflexivMarker == 2) {
                Long newReflexivSubKmer;
                Long newReflexivLong;
                Long[] newReflexivLongArray = new Long[concatBlockSize];

                if (forwardBlockSize > 1) {
                    newReflexivSubKmer = forwardSubKmer._2._2()[forwardBlockSize - 1] & maxSubKmerBinary;
                } else {
                    newReflexivSubKmer = forwardSubKmer._1 << (2 * forwardFirstSuffixLength);
                    newReflexivSubKmer |= (forwardSubKmer._2._2()[0] & maxSuffixLengthBinary);
                    newReflexivSubKmer &= maxSubKmerBinary;
                }

                // 3rd of forward and so on
                int j = concatBlockSize; // extended array index. Initiating with one more as -1 in the loop
                for (int i = forwardBlockSize - 1; i > 1; i--) {
                    j--;
                    newReflexivLong = forwardSubKmer._2._2()[i] >>> 2 * param.subKmerSize;
                    newReflexivLong |= (forwardSubKmer._2._2()[i - 1] << 2 * (31 - param.subKmerSize));
                    newReflexivLong &= maxBlockBinary;
                    newReflexivLongArray[j] = newReflexivLong;
                }

                // 2nd of forward
                if (forwardBlockSize >1) {
                    newReflexivLong = forwardSubKmer._2._2()[1] >>> 2 * param.subKmerSize;
                    newReflexivLong |= ((forwardSubKmer._2._2()[0] & maxSuffixLengthBinary) << 2 * (31 - param.subKmerSize)); // remove C marker
                    if (forwardFirstSuffixLength < param.subKmerSize) {
                        newReflexivLong |= (forwardSubKmer._1 << 2 * (31 - param.subKmerSize + forwardFirstSuffixLength));
                    }
                    newReflexivLong &= maxBlockBinary;
                    newReflexivLongArray[concatBlockSize - forwardBlockSize + 1] = newReflexivLong;
                }

                // 1st of forward
                /**
                 *                    forward               |----------|  |-------||------------||------------|
                 *                    reflected             |----------|  |-------||------------||------------|
                 *     |-------||------------||------------|
                 *           |------------||------------||------------||------------||------------||----------|
                 */
                if (forwardFirstSuffixLength < param.subKmerSize) {
                    newReflexivLong = forwardSubKmer._1 >>> 2 * (param.subKmerSize - forwardFirstSuffixLength);
                } else {
                    newReflexivLong = (forwardSubKmer._2._2()[0] & maxSuffixLengthBinary) >>> 2 * param.subKmerSize;
                    newReflexivLong |= (forwardSubKmer._1 << 2 * (forwardFirstSuffixLength - param.subKmerSize));
                }

                if (forwardFirstSuffixLength < 31) {  // well, current version forwardFirstSuffixLength will not be larger than 31
                    if (reflexedBlockSize > 1) {
                        newReflexivLong |= (reflexedSubKmer._2._2()[reflexedBlockSize - 1] << 2 * forwardFirstSuffixLength);
                        newReflexivLong &= maxBlockBinary;
                        newReflexivLongArray[concatBlockSize - forwardBlockSize] = newReflexivLong;
                    } else if (reflexedFirstPrefixLength > (31 - forwardFirstSuffixLength) && reflexedBlockSize == 1) {
                        newReflexivLong |= (reflexedSubKmer._2._2()[reflexedBlockSize - 1] << 2 * forwardFirstSuffixLength);
                        newReflexivLong &= maxBlockBinary;
                        newReflexivLongArray[concatBlockSize - forwardBlockSize] = newReflexivLong;
                    } else { //reflexedFirstPrefixLength <= (31-forwardFirstSuffixLength)
                        newReflexivLong |= (reflexedSubKmer._2._2()[reflexedBlockSize - 1] << 2 * forwardFirstSuffixLength);
                        newReflexivLong &= maxBlockBinary;
                        newReflexivLong |= (1L << 2 * (reflexedFirstPrefixLength + forwardFirstSuffixLength)); // add C marker
                        newReflexivLongArray[concatBlockSize - forwardBlockSize] = newReflexivLong;
                    }
                }else{ // forwardFirstSuffixLength == 31
                    newReflexivLong &= maxBlockBinary;
                    newReflexivLongArray[concatBlockSize- forwardBlockSize] = newReflexivLong;
                }

                // 3rd of reflected and so on
                int k= concatBlockSize - forwardBlockSize;
                for (int i = reflexedBlockSize-1; i >1; i--) {
                    k--;
                    if (forwardFirstSuffixLength < 31) {
                        newReflexivLong = reflexedSubKmer._2._2()[i] >>> 2 * (31 - forwardFirstSuffixLength);
                        newReflexivLong |= (reflexedSubKmer._2._2()[i - 1] << 2 * forwardFirstSuffixLength);
                        newReflexivLong &= maxBlockBinary;
                    } else { // forwardFirstSuffixLength == 31
                        newReflexivLong = reflexedSubKmer._2._2()[i];
                    }
                    newReflexivLongArray[k] = newReflexivLong;
                }

                // 2nd of reflected or the 1st if reflexedFirstPrefixLength < (31-forwardFirstSuffixLength)
                if (reflexedBlockSize > 1) {
                    if (forwardFirstSuffixLength < 31) {
                        newReflexivLong = reflexedSubKmer._2._2()[1] >>> 2 * (31 - forwardFirstSuffixLength);
                        newReflexivLong |= ((reflexedSubKmer._2._2()[0] & maxPrefixLengthBinary) << 2 * forwardFirstSuffixLength);
                        newReflexivLong &= maxBlockBinary;

                        //reflexedFirstPrefixLength + forwardFirstSuffixLength <= 31
                        if (reflexedFirstPrefixLength <= (31 - forwardFirstSuffixLength)) {
                            newReflexivLong |= (1L << 2 * (forwardFirstSuffixLength + reflexedFirstPrefixLength)); // add C marker
                            newReflexivLongArray[0] = newReflexivLong;
                        } else {
                            newReflexivLongArray[1] = newReflexivLong;
                            newReflexivLong = reflexedSubKmer._2._2()[0] >>> 2 * (31 - forwardFirstSuffixLength); // keep the header
                            newReflexivLongArray[0] = newReflexivLong;
                        }
                    } else { // forwardFirstSuffixLength == 31
                        newReflexivLong = reflexedSubKmer._2._2()[1];
                        newReflexivLongArray[1] = newReflexivLong;
                        newReflexivLongArray[0] = reflexedSubKmer._2._2()[0]; // include the C maker
                    }
                } else { // reflexedBlockSize ==1
                    if (forwardFirstSuffixLength < 31) {
                        if (reflexedFirstPrefixLength <= (31 - forwardFirstSuffixLength)) {
                            // the first element is already included above
                        } else {
                            newReflexivLong = reflexedSubKmer._2._2()[0] >>> 2 * (31 - forwardFirstSuffixLength);
                            newReflexivLongArray[0] = newReflexivLong;
                        }
                    } else { // forwardFirstSuffixLength ==31
                        newReflexivLongArray[0] = reflexedSubKmer._2._2()[0];
                    }
                }

                if (bubbleDistance < 0) {
                    reflexivKmerConcatList.add(
                            new Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>(newReflexivSubKmer,
                                    new Tuple4<Integer, Long[], Integer, Integer>(
                                            randomReflexivMarker, newReflexivLongArray, reflexedSubKmer._2._3(), forwardSubKmer._2._4()
                                    )
                            )
                    );
                } else {
                    if (forwardSubKmer._2._3() > 0) {
                        reflexivKmerConcatList.add(
                                new Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>(newReflexivSubKmer,
                                        new Tuple4<Integer, Long[], Integer, Integer>(
                                                randomReflexivMarker, newReflexivLongArray, bubbleDistance, forwardSubKmer._2._4()
                                        )
                                )
                        );
                    } else {
                        reflexivKmerConcatList.add(
                                new Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>(newReflexivSubKmer,
                                        new Tuple4<Integer, Long[], Integer, Integer>(
                                                randomReflexivMarker, newReflexivLongArray, reflexedSubKmer._2._3(), bubbleDistance
                                        )
                                )
                        );
                    }
                }

                randomReflexivMarker = 1; /* an action of randomization */
            }else { /* randomReflexivMarker == 1 */
                Long newForwardSubKmer;
                Long newForwardLong;
                Long[] newForwardLongArray = new Long[concatBlockSize];

                if (reflexedFirstPrefixLength >= param.subKmerSize) {
                    newForwardSubKmer = (reflexedSubKmer._2._2()[0] & maxPrefixLengthBinary) >>> 2 * (reflexedFirstPrefixLength - param.subKmerSize);
                } else {
                    newForwardSubKmer = (reflexedSubKmer._2._2()[0] & maxPrefixLengthBinary) << 2 * (param.subKmerSize - reflexedFirstPrefixLength);
                    if (reflexedBlockSize > 1) {
                        newForwardSubKmer |= reflexedSubKmer._2._2()[1] >>> 2 * (31 - param.subKmerSize + reflexedFirstPrefixLength);
                    } else {//if (reflexedBlockSize == 1) {
                        newForwardSubKmer |= reflexedSubKmer._1 >>> 2 * reflexedFirstPrefixLength;
                    }
                }


                // 2nd and so on
                int j = concatBlockSize; // the concatenated array index. With one more as -1 in the loop
                for (int i = forwardBlockSize - 1; i >= 1; i--) {
                    j--;
                    newForwardLongArray[j] = forwardSubKmer._2._2()[i];
                }

                // 1st
                if (forwardFirstSuffixLength + param.subKmerSize < 31) { // forwardFirstSuffixLength < 31
                    newForwardLong = (forwardSubKmer._2._2()[0] & maxSuffixLengthBinary);
                    newForwardLong |= (forwardSubKmer._1 << 2 * (forwardFirstSuffixLength));
                    /**
                     *                    forward        |--------|  |-||------------||------------|
                     *                    reflected      |--------|  |--||------------|
                     *                  |-||------------|
                     *                  |--------||------------||------------||------------|
                     */

                    if (reflexedBlockSize == 1 && reflexedFirstPrefixLength >=param.subKmerSize) {
                        Long maxFirstBlockRestBinary = ~((~0L) << 2 * (reflexedFirstPrefixLength-param.subKmerSize));
                        Long maxSecondBlockRestBinary = ~((~0L) << 2 * (reflexedFirstPrefixLength+forwardFirstSuffixLength));
                        newForwardLong |= ((reflexedSubKmer._2._2()[0] & maxFirstBlockRestBinary) << 2 * (param.subKmerSize + forwardFirstSuffixLength));
                        if (forwardFirstSuffixLength + reflexedFirstPrefixLength >31) {
                            newForwardLong &= maxBlockBinary;
                            newForwardLongArray[1] = newForwardLong;

                            newForwardLong = ((reflexedSubKmer._2._2()[0] & maxFirstBlockRestBinary) << 2*param.subKmerSize);
                            newForwardLong >>>= 2*(31-forwardFirstSuffixLength);
                            newForwardLong |= (1L << 2*(forwardFirstSuffixLength + reflexedFirstPrefixLength -31)); // add C marker
                            newForwardLongArray[0] = newForwardLong;
                        }else{
                            newForwardLong &= maxSecondBlockRestBinary;
                            newForwardLong |= (1L << 2*(forwardFirstSuffixLength + reflexedFirstPrefixLength)); // add C marker
                            newForwardLongArray[0] = newForwardLong;
                        }
                    }else if (reflexedBlockSize == 1 && reflexedFirstPrefixLength < param.subKmerSize){
                        Long maxFirstBlockRestBinary = ~((~0L) << 2 * (forwardFirstSuffixLength + reflexedFirstPrefixLength));
                        newForwardLong &= maxFirstBlockRestBinary;
                        newForwardLong |= (1L << 2*(forwardFirstSuffixLength + reflexedFirstPrefixLength));
                        newForwardLongArray[0] =newForwardLong;
                    }else {
                        newForwardLong |= (reflexedSubKmer._2._2()[reflexedBlockSize - 1] << 2 * (param.subKmerSize + forwardFirstSuffixLength));
                        newForwardLong &= maxBlockBinary;
                        newForwardLongArray[concatBlockSize - forwardBlockSize] = newForwardLong;
                    }

                    // reflected 3rd or 2nd and so on
                    int k = concatBlockSize - forwardBlockSize; // concatenate array index
                    for (int i = reflexedBlockSize - 1; i > 1; i--) {
                        k--;
                        newForwardLong = reflexedSubKmer._2._2()[i] >>> 2 * (31 - param.subKmerSize - forwardFirstSuffixLength);
                        newForwardLong |= reflexedSubKmer._2._2()[i - 1] << 2 * (param.subKmerSize + forwardFirstSuffixLength);
                        newForwardLong &= maxBlockBinary;
                        newForwardLongArray[k] = newForwardLong;
                    }

                    // reflected 2nd or 1st
                    if (reflexedBlockSize >1) {
                        if (reflexedFirstPrefixLength >= param.subKmerSize && reflexedFirstPrefixLength - param.subKmerSize <= 31 - param.subKmerSize - forwardFirstSuffixLength) { // 1st
                            newForwardLong = reflexedSubKmer._2._2()[1] >>> 2 * (31 - param.subKmerSize - forwardFirstSuffixLength);
                            Long maxfirstBlockRestBinary = ~((~0L) << 2 * (reflexedFirstPrefixLength - param.subKmerSize));
                            newForwardLong |= ((reflexedSubKmer._2._2()[0] & maxfirstBlockRestBinary) << 2 * (param.subKmerSize + forwardFirstSuffixLength));
                            newForwardLong |= 1L << 2 * (forwardFirstSuffixLength + reflexedFirstPrefixLength); // add C marker
                            newForwardLongArray[0] = newForwardLong;
                        } else if (reflexedFirstPrefixLength >= param.subKmerSize && reflexedFirstPrefixLength - param.subKmerSize > 31 - param.subKmerSize - forwardFirstSuffixLength) {
                            newForwardLong = reflexedSubKmer._2._2()[1] >>> 2 * (31 - param.subKmerSize - forwardFirstSuffixLength);
                            newForwardLong |= (reflexedSubKmer._2._2()[0] << 2 * (param.subKmerSize + forwardFirstSuffixLength));
                            newForwardLong &= maxBlockBinary;
                            newForwardLongArray[1] = newForwardLong;

                            Long maxfirstBlockRestBinary = ~((~0L) << 2 * (reflexedFirstPrefixLength - param.subKmerSize));
                            newForwardLong = reflexedSubKmer._2._2()[0] & maxfirstBlockRestBinary >>> 2 * (31 - param.subKmerSize - forwardFirstSuffixLength);
                            newForwardLong |= 1L << 2 * (reflexedFirstPrefixLength + forwardFirstSuffixLength - 31); // add C marker
                            newForwardLongArray[0] = newForwardLong;
                        } else { // reflexedFirstPrefixLength < param.subKmerSize
                            Long maxSecondBlockRestBinary = ~((~0L) << 2 * (31 - param.subKmerSize + reflexedFirstPrefixLength));
                            newForwardLong |= (reflexedSubKmer._2._2()[1] & maxSecondBlockRestBinary) >>> 2 * (31 - param.subKmerSize - forwardFirstSuffixLength);
                            newForwardLong |= 1L << 2 * (forwardFirstSuffixLength + reflexedFirstPrefixLength); // add C marker
                            newForwardLongArray[1] = newForwardLong;
                        }
                    }

                } else if (forwardFirstSuffixLength < 31 && forwardFirstSuffixLength + param.subKmerSize >= 31) {
                    if (reflexedBlockSize >1) {
                        newForwardLong = (forwardSubKmer._2._2()[0] & maxSuffixLengthBinary);
                        newForwardLong |= (forwardSubKmer._1 << 2 * (forwardFirstSuffixLength));
                        newForwardLong &= maxBlockBinary;
                        newForwardLongArray[concatBlockSize - forwardBlockSize] = newForwardLong;

                        newForwardLong = forwardSubKmer._1 >>> 2 * (31 - forwardFirstSuffixLength);
                        newForwardLong |= (reflexedSubKmer._2._2()[reflexedBlockSize - 1] << 2 * (param.subKmerSize + forwardFirstSuffixLength - 31));
                        if (reflexedBlockSize == 2 && forwardFirstSuffixLength + reflexedFirstPrefixLength <= 31){
                            Long maxFirstBlockRestBinary=  ~((~0L) << 2 * (reflexedFirstPrefixLength + forwardFirstSuffixLength));
                            newForwardLong &= maxFirstBlockRestBinary;
                            newForwardLong |= (1L << 2*(forwardFirstSuffixLength + reflexedFirstPrefixLength));
                        }else {
                            newForwardLong &= maxBlockBinary;
                        }
                        newForwardLongArray[concatBlockSize - forwardBlockSize - 1] = newForwardLong;
                    }else if (reflexedBlockSize == 1 && reflexedFirstPrefixLength >param.subKmerSize){ // forwardFirstSuffixLength + reflexedFirstPrefixLength >31
                        newForwardLong = (forwardSubKmer._2._2()[0] & maxSuffixLengthBinary);
                        newForwardLong |= (forwardSubKmer._1 << 2 * (forwardFirstSuffixLength));
                        newForwardLong &= maxBlockBinary;
                        newForwardLongArray[concatBlockSize - forwardBlockSize] = newForwardLong; // concatBlockSize - forwardBlockSize = 1

                        newForwardLong = forwardSubKmer._1 >>> 2 * (31 - forwardFirstSuffixLength);
                        Long maxFirstBlockRestBinary = ~((~0L) << 2 * (reflexedFirstPrefixLength - param.subKmerSize));
                        newForwardLong |= ((reflexedSubKmer._2._2()[0] & maxFirstBlockRestBinary) << 2*(param.subKmerSize + forwardFirstSuffixLength -31));
                        newForwardLong |= (1L << 2*(forwardFirstSuffixLength+reflexedFirstPrefixLength -31));
                        newForwardLongArray[concatBlockSize - forwardBlockSize -1] = newForwardLong; // concateBlockSize - forwardBlockSize = 0
                    }else if (reflexedBlockSize == 1 && reflexedFirstPrefixLength <= param.subKmerSize && forwardFirstSuffixLength + reflexedFirstPrefixLength > 31){ // reflexedBlockSize == 1 && reflexedFirstPrefixLength <= param.subKmerSize
                        newForwardLong = (forwardSubKmer._2._2()[0] & maxSuffixLengthBinary);
                        newForwardLong |= (forwardSubKmer._1 << 2 * (forwardFirstSuffixLength));
                        newForwardLong &= maxBlockBinary;
                        newForwardLongArray[concatBlockSize - forwardBlockSize] = newForwardLong;

                        newForwardLong = forwardSubKmer._1 >>> 2*(31 - forwardFirstSuffixLength);
                        Long maxFirstBlockRestBinary = ~((~0L) << 2 * (reflexedFirstPrefixLength + forwardFirstSuffixLength -31));
                        newForwardLong &= maxFirstBlockRestBinary;
                        newForwardLong |= (1L << 2*(forwardFirstSuffixLength+reflexedFirstPrefixLength-31));
                        newForwardLongArray[concatBlockSize - forwardBlockSize -1] =newForwardLong;
                    } else {
                        newForwardLong = (forwardSubKmer._2._2()[0] & maxSuffixLengthBinary);
                        newForwardLong |= (forwardSubKmer._1 << (2 * forwardFirstSuffixLength));
                        Long maxFirstBlockRestBinary = ~((~0L) << 2 * (reflexedFirstPrefixLength + forwardFirstSuffixLength));
                        newForwardLong &= maxFirstBlockRestBinary;
                        newForwardLong |= (1L << 2*(reflexedFirstPrefixLength + forwardFirstSuffixLength));
                        newForwardLongArray[concatBlockSize - forwardBlockSize] = newForwardLong; // concatBlockSize - forwardBlockSize = 0
                    }

                    // reflected 3rd or 2nd and so on
                    int k = concatBlockSize - forwardBlockSize - 1; // concatenate array index
                    for (int i = reflexedBlockSize - 1; i > 2; i--) {
                        k--;
                        newForwardLong = reflexedSubKmer._2._2()[i] >>> 2 * (62 - param.subKmerSize - forwardFirstSuffixLength);
                        newForwardLong |= (reflexedSubKmer._2._2()[i - 1] << 2 * (param.subKmerSize + forwardFirstSuffixLength - 31));
                        newForwardLong &= maxBlockBinary;
                        newForwardLongArray[k] = newForwardLong;
                    }

                    if (reflexedBlockSize >1) {
                        if (reflexedFirstPrefixLength > param.subKmerSize) { // && param.subKmerSize - reflexedFirstPrefixLength + (param.subKmerSize + forwardFirstSuffixLength -31) > 31 is impossible
                            if (reflexedBlockSize > 2) {
                                newForwardLong = reflexedSubKmer._2._2()[2] >>> 2 * (62 - param.subKmerSize - forwardFirstSuffixLength);
                                newForwardLong |= (reflexedSubKmer._2._2()[1] << 2 * (param.subKmerSize + forwardFirstSuffixLength - 31));
                                newForwardLong &= maxBlockBinary;
                                newForwardLongArray[1] = newForwardLong;
                            }

                            newForwardLong = reflexedSubKmer._2._2()[1] >>> 2 * (62 - param.subKmerSize - forwardFirstSuffixLength);
                            Long maxfirstBlockRestBinary = ~((~0L) << 2 * (reflexedFirstPrefixLength - param.subKmerSize));
                            newForwardLong |= ((reflexedSubKmer._2._2()[0] & maxfirstBlockRestBinary) << 2 * (param.subKmerSize + forwardFirstSuffixLength - 31));  // also removed C marker
                            newForwardLong |= 1L << 2 * (reflexedFirstPrefixLength + forwardFirstSuffixLength - 31); // add C marker
                            newForwardLongArray[0] = newForwardLong;
                        } else if (reflexedFirstPrefixLength <= param.subKmerSize && forwardFirstSuffixLength + reflexedFirstPrefixLength > 31) {
                            if (reflexedBlockSize >2) {
                                newForwardLong = reflexedSubKmer._2._2()[2] >>> 2 * (62 - param.subKmerSize - forwardFirstSuffixLength);
                                newForwardLong |= (reflexedSubKmer._2._2()[1] << 2 * (param.subKmerSize + forwardFirstSuffixLength - 31));
                                newForwardLong &= maxBlockBinary;
                                newForwardLongArray[1] = newForwardLong;
                            }

                            newForwardLong = reflexedSubKmer._2._2()[1] >>> 2 * (62 - param.subKmerSize - forwardFirstSuffixLength);
                            Long maxfirstBlockRestBinary = ~((~0L) << 2 * (reflexedFirstPrefixLength + forwardFirstSuffixLength - 31));
                            newForwardLong &= maxfirstBlockRestBinary;
                            newForwardLong |= (1L << 2 * (reflexedFirstPrefixLength + forwardFirstSuffixLength - 31));
                            newForwardLongArray[0] = newForwardLong;
                        } else { // forwardFirstSuffixLength+reflexedFirstPrefixLength <= 31
                            if (reflexedBlockSize >= 3) {
                                newForwardLong = reflexedSubKmer._2._2()[2] >>> 2 * (62 - param.subKmerSize - forwardFirstSuffixLength);
                                newForwardLong |= (reflexedSubKmer._2._2()[1] << 2 * (param.subKmerSize + forwardFirstSuffixLength - 31));
                                Long maxfirstBlockRestBinary = ~((~0L) << 2 * (reflexedFirstPrefixLength + forwardFirstSuffixLength));
                                newForwardLong &= maxfirstBlockRestBinary;
                                newForwardLong |= (1L << 2 * (reflexedFirstPrefixLength + forwardFirstSuffixLength)); // add C marker
                                newForwardLongArray[0] = newForwardLong;
                            }
                        }
                    }

                } else {// forwardFirstSuffixLength == 31
                    newForwardLong = forwardSubKmer._2._2()[0] & maxBlockBinary; // remove C marker
                    newForwardLongArray[concatBlockSize - forwardBlockSize] = newForwardLong;

                    if (reflexedBlockSize >1) {
                        newForwardLong = forwardSubKmer._1;
                        newForwardLong |= (reflexedSubKmer._2._2()[reflexedBlockSize-1] << 2*param.subKmerSize);
                        newForwardLong &= maxBlockBinary;
                        newForwardLongArray[concatBlockSize - forwardBlockSize - 1] = newForwardLong;
                    }else{
                        newForwardLong = forwardSubKmer._1;
                        if (reflexedFirstPrefixLength > param.subKmerSize){
                            newForwardLong |= ((reflexedSubKmer._2._2()[0] & maxPrefixLengthBinary) << 2* param.subKmerSize);
                        }
                        Long maxfirstBlockRestBinary = ~((~0L) << 2 * (reflexedFirstPrefixLength + forwardFirstSuffixLength -31));
                        newForwardLong &= maxfirstBlockRestBinary;
                        newForwardLong |= (1L << 2*(reflexedFirstPrefixLength + forwardFirstSuffixLength -31));
                        newForwardLongArray[0] = newForwardLong;
                    }

                    int k = concatBlockSize - forwardBlockSize - 1; // concatenate array index
                    for (int i = reflexedBlockSize - 1; i > 1; i--) {
                        k--;
                        newForwardLong = reflexedSubKmer._2._2()[i] >>> 2*(31-param.subKmerSize);
                        newForwardLong |= reflexedSubKmer._2._2()[i-1] << 2*param.subKmerSize;
                        newForwardLong &= maxBlockBinary;
                        newForwardLongArray[k] = newForwardLong;
                    }

                    if (reflexedBlockSize >1) {
                        if (reflexedFirstPrefixLength >= param.subKmerSize) {
                            newForwardLong = reflexedSubKmer._2._2()[1] >>> 2 * (31 - param.subKmerSize);
                            newForwardLong |= (reflexedSubKmer._2._2()[0] << 2 * param.subKmerSize);
                            Long maxfirstBlockRestBinary = ~((~0L) << 2 * (reflexedFirstPrefixLength + forwardFirstSuffixLength - 31));
                            newForwardLong &= maxfirstBlockRestBinary;
                            newForwardLong |= (1L << 2 * (reflexedFirstPrefixLength + forwardFirstSuffixLength - 31)); // add C marker
                            newForwardLongArray[0] = newForwardLong;
                        } else { // reflexedFirstPrefixLength < param.subKmerSize
                            newForwardLong = reflexedSubKmer._2._2()[1] >>> 2 * (31 - param.subKmerSize);
                            Long maxfirstBlockRestBinary = ~((~0L) << 2 * (reflexedFirstPrefixLength + forwardFirstSuffixLength - 31));
                            newForwardLong &= maxfirstBlockRestBinary;
                            newForwardLong |= (1L << 2 * (reflexedFirstPrefixLength + forwardFirstSuffixLength - 31)); // add C marker
                            newForwardLongArray[0] = newForwardLong;
                        }
                    }
                }

                if (bubbleDistance < 0) {
                    reflexivKmerConcatList.add(
                            new Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>(newForwardSubKmer,
                                    new Tuple4<Integer, Long[], Integer, Integer>(
                                            randomReflexivMarker, newForwardLongArray, reflexedSubKmer._2._3(), forwardSubKmer._2._4()
                                    )
                            )
                    );
                } else {
                    if (forwardSubKmer._2._3() > 0) {
                        reflexivKmerConcatList.add(
                                new Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>(newForwardSubKmer,
                                        new Tuple4<Integer, Long[], Integer, Integer>(
                                                randomReflexivMarker, newForwardLongArray, bubbleDistance, forwardSubKmer._2._4()
                                        )
                                )
                        );
                    } else { // reflexedSubKmer._2._4() >0
                        reflexivKmerConcatList.add(
                                new Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>(newForwardSubKmer,
                                        new Tuple4<Integer, Long[], Integer, Integer>(
                                                randomReflexivMarker, newForwardLongArray, reflexedSubKmer._2._3(), bubbleDistance
                                        )
                                )
                        );
                    }
                }

                randomReflexivMarker = 2;
            }

             /* add current sub kmer to temporal storage */
            // tmpReflexivKmerExtendList.add(reflexedSubKmer);
        }

        /**
         *
         * @param S
         */
        public void resetSubKmerGroup(Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>> S) {
            if (lineMarker == 1) {
                lineMarker = 2;
            }else {
                lineMarker = 3; /* reset to new sub-kmer group */
            }

            tmpReflexivKmerExtendList = new ArrayList<Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>>();
            tmpReflexivKmerExtendList.add(
                    new Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>(S._1,
                            new Tuple4<Integer, Long[], Integer, Integer>(
                                    S._2._1(), S._2._2(), S._2._3(), S._2._4()
                            )
                    )
            );
        }

        /**
         *
         */
        public void tmpKmerRandomizer(){
            if (tmpReflexivKmerExtendList.size() != 0) {
                for (int i = 0; i < tmpReflexivKmerExtendList.size(); i++) {
                    singleKmerRandomizer(tmpReflexivKmerExtendList.get(i));
                }
            }
        }
    }

    /**
     *  choose one kmer from a fork with higher coverage.
     */
    class FilterForkSubKmerLong implements PairFlatMapFunction<Iterator<Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>>, Long, Tuple4<Integer, Long[], Integer, Integer>>, Serializable {
        List<Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>> HighCoverageSubKmer = new ArrayList<Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>>();
//        Tuple2<String, Tuple4<Integer, String, Integer, Integer>> HighCoverKmer=null;
//                new Tuple2<String, Tuple4<Integer, String, Integer, Integer>>("",
        //                       new Tuple4<Integer, String, Integer, Integer>(0, "", 0, 0));

        public Iterator<Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>> call(Iterator<Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>> s) {
            while (s.hasNext()) {
                Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>> subKmer = s.next();
                if (HighCoverageSubKmer.size() == 0) {
                    if (subKmer._2._4() >= 1000000000) {
                        HighCoverageSubKmer.add(
                                new Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>(subKmer._1,
                                        new Tuple4<Integer, Long[], Integer, Integer>(subKmer._2._1(), subKmer._2._2(), subKmer._2._3(), -subKmer._2._4())
                                )
                        );
                    }else if (subKmer._2._4() <= -1000000000) {
                        HighCoverageSubKmer.add(
                                new Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>(subKmer._1,
                                        new Tuple4<Integer, Long[], Integer, Integer>(subKmer._2._1(), subKmer._2._2(), subKmer._2._3(), subKmer._2._4())
                                )
                        );
                    }else {
                        HighCoverageSubKmer.add(
                                new Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>(subKmer._1,
                                        new Tuple4<Integer, Long[], Integer, Integer>(subKmer._2._1(), subKmer._2._2(), subKmer._2._3(), -1)
                                )
                        );
                    }
                } else {
                    if (subKmer._1.equals(HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1)._1)) {
                        if (subKmer._2._3().compareTo(HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1)._2._3()) > 0) {
                            if (subKmer._2._4() >= 1000000000) {
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        new Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>(subKmer._1,
                                                new Tuple4<Integer, Long[], Integer, Integer>(subKmer._2._1(), subKmer._2._2(), subKmer._2._3(), -subKmer._2._4())
                                        )
                                );
                            }else if (subKmer._2._4() <= -1000000000){
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        new Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>(subKmer._1,
                                                new Tuple4<Integer, Long[], Integer, Integer>(subKmer._2._1(), subKmer._2._2(), subKmer._2._3(), subKmer._2._4())
                                        )
                                );
                            }else {
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        new Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>(subKmer._1,
                                                new Tuple4<Integer, Long[], Integer, Integer>(subKmer._2._1(), subKmer._2._2(), subKmer._2._3(), param.subKmerSize)
                                        )
                                );
                            }
                        } else if (subKmer._2._3().equals(HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1)._2._3())) {
                            int subKmerFirstSuffixLength = Long.SIZE/2 - (Long.numberOfLeadingZeros(subKmer._2._2()[0])/2 + 1);
                            int HighCoverageSubKmerFirstSuffixLength = Long.SIZE/2 - ((Long.numberOfLeadingZeros(HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1)._2._2()[0]))/2 + 1);
                            Long subKmerFirstSuffix = subKmer._2._2()[0] >>> 2*(subKmerFirstSuffixLength-1);
                            Long HighCoverageSubKmerFirstSuffix = HighCoverageSubKmer.get(HighCoverageSubKmer.size()-1)._2._2()[0] >>> 2*(HighCoverageSubKmerFirstSuffixLength);

                            if (subKmerFirstSuffix.compareTo(HighCoverageSubKmerFirstSuffix) > 0) {
                                if (subKmer._2._4() >= 1000000000) {
                                    HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                            new Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>(subKmer._1,
                                                    new Tuple4<Integer, Long[], Integer, Integer>(subKmer._2._1(), subKmer._2._2(), subKmer._2._3(), -subKmer._2._4())
                                            )
                                    );
                                }else if (subKmer._2._4() <= -1000000000){
                                    HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                            new Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>(subKmer._1,
                                                    new Tuple4<Integer, Long[], Integer, Integer>(subKmer._2._1(), subKmer._2._2(), subKmer._2._3(), subKmer._2._4())
                                            )
                                    );
                                }else {
                                    HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                            new Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>(subKmer._1,
                                                    new Tuple4<Integer, Long[], Integer, Integer>(subKmer._2._1(), subKmer._2._2(), subKmer._2._3(), param.subKmerSize)
                                            )
                                    );
                                }
                            } else {
                                subKmer = HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1);
                                if (subKmer._2._4() >= 1000000000) {
                                    HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                            new Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>(subKmer._1,
                                                    new Tuple4<Integer, Long[], Integer, Integer>(subKmer._2._1(), subKmer._2._2(), subKmer._2._3(), -subKmer._2._4())
                                            )
                                    );
                                }else if (subKmer._2._4() <= -1000000000){
                                    HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                            new Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>(subKmer._1,
                                                    new Tuple4<Integer, Long[], Integer, Integer>(subKmer._2._1(), subKmer._2._2(), subKmer._2._3(), subKmer._2._4())
                                            )
                                    );
                                }else {
                                    HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                            new Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>(subKmer._1,
                                                    new Tuple4<Integer, Long[], Integer, Integer>(subKmer._2._1(), subKmer._2._2(), subKmer._2._3(), param.subKmerSize)
                                            )
                                    );
                                }
                            }
                        } else {
                            subKmer = HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1);
                            if (subKmer._2._4() >= 1000000000) {
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        new Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>(subKmer._1,
                                                new Tuple4<Integer, Long[], Integer, Integer>(subKmer._2._1(), subKmer._2._2(), subKmer._2._3(), -subKmer._2._4())
                                        )
                                );
                            }else if (subKmer._2._4() <= -1000000000){
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        new Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>(subKmer._1,
                                                new Tuple4<Integer, Long[], Integer, Integer>(subKmer._2._1(), subKmer._2._2(), subKmer._2._3(), subKmer._2._4())
                                        )
                                );
                            }else {
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        new Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>(subKmer._1,
                                                new Tuple4<Integer, Long[], Integer, Integer>(subKmer._2._1(), subKmer._2._2(), subKmer._2._3(), param.subKmerSize)
                                        )
                                );
                            }
                        }
                    } else {
                        if (subKmer._2._4() >= 1000000000) {
                            HighCoverageSubKmer.add(
                                    new Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>(subKmer._1,
                                            new Tuple4<Integer, Long[], Integer, Integer>(subKmer._2._1(), subKmer._2._2(), subKmer._2._3(), -subKmer._2._4())
                                    )
                            );
                        }else if (subKmer._2._4() <= -1000000000) {
                            HighCoverageSubKmer.add(
                                    new Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>(subKmer._1,
                                            new Tuple4<Integer, Long[], Integer, Integer>(subKmer._2._1(), subKmer._2._2(), subKmer._2._3(), subKmer._2._4())
                                    )
                            );
                        }else {
                            HighCoverageSubKmer.add(
                                    new Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>(subKmer._1,
                                            new Tuple4<Integer, Long[], Integer, Integer>(subKmer._2._1(), subKmer._2._2(), subKmer._2._3(), -1)
                                    )
                            );
                        }
                    }
                }
            }

            return HighCoverageSubKmer.iterator();
        }
    }

    class FilterForkReflectedSubKmerLong implements PairFlatMapFunction<Iterator<Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>>, Long, Tuple4<Integer, Long[], Integer, Integer>>, Serializable{
        List<Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>> HighCoverageSubKmer = new ArrayList<Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>>();
        Integer HighCoverLastCoverage = 0;
//        Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>> HighCoverKmer=null;
//                new Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>("",
        //                       new Tuple4<Integer, Long, Integer, Integer>(0, "", 0, 0));

        public Iterator<Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>> call (Iterator<Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>> s){
            while (s.hasNext()){
                Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>> subKmer = s.next();
                if (HighCoverageSubKmer.size() == 0){
                    HighCoverLastCoverage = subKmer._2._3();
                    if (subKmer._2._3() >= 1000000000){
                        HighCoverageSubKmer.add(
                                new Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>(subKmer._1,
                                        new Tuple4<Integer, Long[], Integer, Integer>(subKmer._2._1(), subKmer._2._2(), -subKmer._2._3(), subKmer._2._4())
                                )
                        );
                    }else if (subKmer._2._3() <= -1000000000){
                        HighCoverageSubKmer.add(
                                new Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>(subKmer._1,
                                        new Tuple4<Integer, Long[], Integer, Integer>(subKmer._2._1(), subKmer._2._2(), subKmer._2._3(), subKmer._2._4())
                                )
                        );
                    }else {
                        HighCoverageSubKmer.add(
                                new Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>(subKmer._1,
                                        new Tuple4<Integer, Long[], Integer, Integer>(subKmer._2._1(), subKmer._2._2(), -1, subKmer._2._4())
                                )
                        );
                    }
                }else {
                    if (subKmer._1.equals(HighCoverageSubKmer.get(HighCoverageSubKmer.size()-1)._1)) {
                        if (subKmer._2._3().compareTo(HighCoverLastCoverage) >0) {
                            HighCoverLastCoverage = subKmer._2._3();
                            if (subKmer._2._3() >= 1000000000){
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        new Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>(subKmer._1,
                                                new Tuple4<Integer, Long[], Integer, Integer>(subKmer._2._1(), subKmer._2._2(), -subKmer._2._3(), subKmer._2._4())
                                        )
                                );
                            }else if (subKmer._2._3() <= -1000000000){
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        new Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>(subKmer._1,
                                                new Tuple4<Integer, Long[], Integer, Integer>(subKmer._2._1(), subKmer._2._2(), subKmer._2._3(), subKmer._2._4())
                                        )
                                );
                            }else {
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        new Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>(subKmer._1,
                                                new Tuple4<Integer, Long[], Integer, Integer>(subKmer._2._1(), subKmer._2._2(), param.subKmerSize, subKmer._2._4())
                                        )
                                );
                            }
                        } else if (subKmer._2._3().equals(HighCoverLastCoverage)){
                            int subKmerFirstSuffixLength = Long.SIZE/2 - (Long.numberOfLeadingZeros(subKmer._2._2()[0])/2 + 1);
                            int HighCoverageSubKmerFirstSuffixLength = Long.SIZE/2 - ((Long.numberOfLeadingZeros(HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1)._2._2()[0]))/2 + 1);
                            Long subKmerFirstSuffix = subKmer._2._2()[0] >>> 2*(subKmerFirstSuffixLength-1);
                            Long HighCoverageSubKmerFirstSuffix = HighCoverageSubKmer.get(HighCoverageSubKmer.size()-1)._2._2()[0] >>> 2*(HighCoverageSubKmerFirstSuffixLength);

                            if (subKmerFirstSuffix.compareTo(HighCoverageSubKmerFirstSuffix) >0){
                                if (subKmer._2._3() >= 1000000000){
                                    HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                            new Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>(subKmer._1,
                                                    new Tuple4<Integer, Long[], Integer, Integer>(subKmer._2._1(), subKmer._2._2(), -subKmer._2._3(), subKmer._2._4())
                                            )
                                    );
                                }else if (subKmer._2._3() <= -1000000000){
                                    HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                            new Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>(subKmer._1,
                                                    new Tuple4<Integer, Long[], Integer, Integer>(subKmer._2._1(), subKmer._2._2(), subKmer._2._3(), subKmer._2._4())
                                            )
                                    );
                                }else {
                                    HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                            new Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>(subKmer._1,
                                                    new Tuple4<Integer, Long[], Integer, Integer>(subKmer._2._1(), subKmer._2._2(), param.subKmerSize, subKmer._2._4())
                                            )
                                    );
                                }
                            }else{
                                subKmer = HighCoverageSubKmer.get(HighCoverageSubKmer.size()-1); // re assign
                                if (subKmer._2._3() >= 1000000000){
                                    HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                            new Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>(subKmer._1,
                                                    new Tuple4<Integer, Long[], Integer, Integer>(subKmer._2._1(), subKmer._2._2(), -subKmer._2._3(), subKmer._2._4())
                                            )
                                    );
                                }else if (subKmer._2._3() <= -1000000000){
                                    HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                            new Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>(subKmer._1,
                                                    new Tuple4<Integer, Long[], Integer, Integer>(subKmer._2._1(), subKmer._2._2(), subKmer._2._3(), subKmer._2._4())
                                            )
                                    );
                                }else {
                                    HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                            new Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>(subKmer._1,
                                                    new Tuple4<Integer, Long[], Integer, Integer>(subKmer._2._1(), subKmer._2._2(), param.subKmerSize, subKmer._2._4())
                                            )
                                    );
                                }
                            }
                        } else {
                            subKmer = HighCoverageSubKmer.get(HighCoverageSubKmer.size()-1);
                            if (subKmer._2._3() >= 1000000000){
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        new Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>(subKmer._1,
                                                new Tuple4<Integer, Long[], Integer, Integer>(subKmer._2._1(), subKmer._2._2(), -subKmer._2._3(), subKmer._2._4())
                                        )
                                );
                            }else if (subKmer._2._3() <= -1000000000){
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        new Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>(subKmer._1,
                                                new Tuple4<Integer, Long[], Integer, Integer>(subKmer._2._1(), subKmer._2._2(), subKmer._2._3(), subKmer._2._4())
                                        )
                                );
                            }else {
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        new Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>(subKmer._1,
                                                new Tuple4<Integer, Long[], Integer, Integer>(subKmer._2._1(), subKmer._2._2(), param.subKmerSize, subKmer._2._4())
                                        )
                                );
                            }
                        }
                    }else{
                        HighCoverLastCoverage = subKmer._2._3();
                        if (subKmer._2._3() >= 1000000000){
                            HighCoverageSubKmer.add(
                                    new Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>(subKmer._1,
                                            new Tuple4<Integer, Long[], Integer, Integer>(subKmer._2._1(), subKmer._2._2(), -subKmer._2._3(), subKmer._2._4())
                                    )
                            );
                        }else if (subKmer._2._3() <= -1000000000){
                            HighCoverageSubKmer.add(
                                    new Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>(subKmer._1,
                                            new Tuple4<Integer, Long[], Integer, Integer>(subKmer._2._1(), subKmer._2._2(), subKmer._2._3(), subKmer._2._4())
                                    )
                            );
                        }else {
                            HighCoverageSubKmer.add(
                                    new Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>(subKmer._1,
                                            new Tuple4<Integer, Long[], Integer, Integer>(subKmer._2._1(), subKmer._2._2(), -1, subKmer._2._4())
                                    )
                            );
                        }
                    }
                }
            }

            return HighCoverageSubKmer.iterator();
        }
    }

    /**
     *
     */
    class ForwardSubKmerLongExtraction implements PairFlatMapFunction<Iterator<Tuple2<Long, Integer>>, Long, Tuple4<Integer, Long[], Integer, Integer>>, Serializable {
        List<Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>> TupleList = new ArrayList<Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>>();
        Long suffixBinary;
        Long prefixBinary;
        Tuple2<Long, Integer> kmerTuple;

        public Iterator<Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>> call(Iterator<Tuple2<Long, Integer>> s) {

            while (s.hasNext()) {
                kmerTuple = s.next();
                /**
                 * normal Sub-kmer
                 *        Kmer      ATGCACGTTATG
                 *        Sub-Kmer  ATGCACGTTAT         marked as Integer 1 in Tuple2
                 *        Left      -----------G
                 */
                suffixBinary = kmerTuple._1 & 3L;
                suffixBinary |= (1L << 2); // add C marker
                Long[] suffixBinaryLong = new Long[1];
                suffixBinaryLong[0] = suffixBinary;

                prefixBinary = kmerTuple._1 >>> 2;


                TupleList.add(
                        new Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>(
                                prefixBinary, new Tuple4<Integer, Long[], Integer, Integer>(1, suffixBinaryLong, kmerTuple._2, kmerTuple._2)
                        )
                );
            }

            return TupleList.iterator();
        }
    }

    /**
     *
     */

    class ReflectedSubKmerExtractionFromForwardLong implements PairFlatMapFunction<Iterator<Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>>, Long, Tuple4<Integer, Long[], Integer, Integer>>, Serializable {

        /* 1 stands for forward sub-kmer */
        /* 2 stands for reflexiv sub-kmer */
        // private int randomReflexivMarker = ThreadLocalRandom.current().nextInt(1, 3);
        private int randomReflexivMarker = 2;

        long maxSubKmerBinary = ~((~0L) << 2*param.subKmerSize);
        long maxBlockBinary = ~((~0L) << 2*31); // a block has 31 nucleotide

        /* return capsule of extend Tuples for next iteration*/
        List<Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>> reflexivKmerConcatList = new ArrayList<Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>>();

        public Iterator<Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>> call(Iterator<Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>> s){

            while (s.hasNext()) {
                Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>> currentSubKmer = s.next();

                int blockSize = currentSubKmer._2._2().length;
                Long[] newReflexivLongArray = new Long[blockSize];

                if (currentSubKmer._2._1() == 1) {
                    int firstSuffixBlockLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros(currentSubKmer._2._2()[0]) / 2 + 1);
                    long maxSuffixLengthBinary = ~((~0L) << (2 * firstSuffixBlockLength));
                    Long newReflexivSubKmer;
                    Long newReflexivLong;

                    if (randomReflexivMarker == 2) {
                        if (blockSize > 1) {
                            newReflexivSubKmer = currentSubKmer._2._2()[blockSize - 1] & maxSubKmerBinary;

                            // 3rd block and so on
                            for (int i = blockSize - 1; i > 1; i--) {
                                newReflexivLong = currentSubKmer._2._2()[i] >>> 2 * param.subKmerSize;
                                newReflexivLong |= (currentSubKmer._2._2()[i - 1] << 2 * (31 - param.subKmerSize));
                                newReflexivLong &= maxBlockBinary;
                                newReflexivLongArray[i] = newReflexivLong;
                            }

                            // 2nd block
                            newReflexivLong = currentSubKmer._2._2()[1] >>> 2 * param.subKmerSize;
                            newReflexivLong |= ((currentSubKmer._2._2()[0] & maxSuffixLengthBinary) << 2 * (31 - param.subKmerSize));
                            if (firstSuffixBlockLength < param.subKmerSize) {
                                newReflexivLong |= (currentSubKmer._1 << 2 * (31 - param.subKmerSize + firstSuffixBlockLength));
                            }
                            newReflexivLong &= maxBlockBinary;
                            newReflexivLongArray[1] = newReflexivLong;

                            // 1st block
                            if (firstSuffixBlockLength < param.subKmerSize) {
                                newReflexivLong = currentSubKmer._1 >>> 2 * (param.subKmerSize - firstSuffixBlockLength);
                                newReflexivLong |= (1L << 2 * firstSuffixBlockLength);
                            } else {
                                newReflexivLong = currentSubKmer._2._2()[0] & maxSuffixLengthBinary; //remove C marker
                                newReflexivLong >>>= 2 * param.subKmerSize;
                                newReflexivLong |= (currentSubKmer._1 << 2 * (firstSuffixBlockLength - param.subKmerSize));
                                newReflexivLong |= (1L << 2 * firstSuffixBlockLength); // add C marker
                            }
                            newReflexivLongArray[0] = newReflexivLong;
                        } else {
                            if (firstSuffixBlockLength >= param.subKmerSize) {
                                newReflexivSubKmer = currentSubKmer._2._2()[0] & maxSubKmerBinary;

                                newReflexivLong = currentSubKmer._2._2()[0] & maxSuffixLengthBinary;
                                newReflexivLong >>>= 2 * param.subKmerSize;
                                newReflexivLong |= (currentSubKmer._1 << 2 * (firstSuffixBlockLength - param.subKmerSize));
                                newReflexivLong |= (1L << (2 * firstSuffixBlockLength)); // add C marker in the front
                            } else {
                                newReflexivSubKmer = currentSubKmer._1 << (firstSuffixBlockLength * 2);
                                newReflexivSubKmer &= maxSubKmerBinary;
                                newReflexivSubKmer |= (currentSubKmer._2._2()[0] & maxSuffixLengthBinary);

                                newReflexivLong = currentSubKmer._1 >>> (2 * (param.subKmerSize - firstSuffixBlockLength));
                                newReflexivLong |= (1L << (2 * firstSuffixBlockLength)); // add C marker in the front
                            }

                            newReflexivLongArray[0] = newReflexivLong;
                        }

                        reflexivKmerConcatList.add(
                                new Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>(newReflexivSubKmer,
                                        new Tuple4<Integer, Long[], Integer, Integer>(
                                                randomReflexivMarker, newReflexivLongArray, currentSubKmer._2._3(), currentSubKmer._2._4()
                                        )
                                )
                        );
                    } else {
                        reflexivKmerConcatList.add(currentSubKmer);
                    }
                } else { /* currentSubKmer._2._1() == 2 */
                    int firstPrefixLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros(currentSubKmer._2._2()[0]) / 2 + 1);
                    long maxPrefixLengthBinary = ~((~0L) << (2 * firstPrefixLength));

                    Long newReflexivSubKmer;
                    Long newReflexivLong;

                    if (randomReflexivMarker == 2) {
                        reflexivKmerConcatList.add(currentSubKmer);
                    } else { /* randomReflexivMarker == 1 */
                        if (blockSize > 1) {
                            // the subKmer
                            if (firstPrefixLength >= param.subKmerSize) {
                                newReflexivSubKmer = (currentSubKmer._2._2()[0] & maxPrefixLengthBinary) >>> 2 * (firstPrefixLength - param.subKmerSize); // also removed C marker
                            } else {
                                newReflexivSubKmer = currentSubKmer._2._2()[0] & maxPrefixLengthBinary; // remove C marker
                                newReflexivSubKmer <<= 2 * (param.subKmerSize - firstPrefixLength);
                                newReflexivSubKmer |= (currentSubKmer._2._2()[1] >>> 2 * (31 - param.subKmerSize + firstPrefixLength));
                            }

                            // the last block
                            newReflexivLong = currentSubKmer._2._2()[blockSize - 1] << 2 * param.subKmerSize;
                            newReflexivLong |= currentSubKmer._1;
                            newReflexivLong &= maxBlockBinary;
                            newReflexivLongArray[blockSize - 1] = newReflexivLong;

                            // 2nd and so on
                            for (int i = blockSize - 2; i >= 1; i--) {
                                newReflexivLong = currentSubKmer._2._2()[i] << 2 * param.subKmerSize;
                                newReflexivLong |= (currentSubKmer._2._2()[i + 1] >>> 2 * (31 - param.subKmerSize));
                                newReflexivLong &= maxBlockBinary;
                                newReflexivLongArray[i] = newReflexivLong;
                            }

                            // 1st
                            newReflexivLong = currentSubKmer._2._2()[1] >>> 2 * (31 - param.subKmerSize);
                            if (firstPrefixLength >= param.subKmerSize) {
                                newReflexivLong |= ((currentSubKmer._2._2()[0] & maxPrefixLengthBinary) << 2 * param.subKmerSize);
                            }
                            newReflexivLong &= maxPrefixLengthBinary;
                            newReflexivLong |= (1L << 2 * firstPrefixLength); // add C marker
                            newReflexivLongArray[0] = newReflexivLong;

                        } else { /* blockSize = 1)*/
                            if (firstPrefixLength >= param.subKmerSize) {
                                newReflexivSubKmer = (currentSubKmer._2._2()[0] & maxPrefixLengthBinary) >>> 2 * (firstPrefixLength - param.subKmerSize);
                                newReflexivSubKmer &= maxSubKmerBinary; // remove header, including C marker

                                newReflexivLong = (currentSubKmer._2._2()[0] & maxPrefixLengthBinary) << 2 * param.subKmerSize;
                                newReflexivLong |= currentSubKmer._1;
                                newReflexivLong &= maxPrefixLengthBinary; // remove header, including C marker
                                newReflexivLong |= (1L << 2 * firstPrefixLength); // add C marker
                            } else {
                                newReflexivSubKmer = (currentSubKmer._2._2()[0] & maxPrefixLengthBinary) << (2 * (param.subKmerSize - firstPrefixLength));
                                newReflexivSubKmer |= (currentSubKmer._1 >>> (2 * firstPrefixLength));

                                newReflexivLong = currentSubKmer._1 & maxPrefixLengthBinary;
                                newReflexivLong |= (1L << 2 * firstPrefixLength); // add C marker in the front
                            }

                            newReflexivLongArray[0] = newReflexivLong;
                        }

                        reflexivKmerConcatList.add(
                                new Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>(newReflexivSubKmer,
                                        new Tuple4<Integer, Long[], Integer, Integer>(
                                                randomReflexivMarker, newReflexivLongArray, currentSubKmer._2._3(), currentSubKmer._2._4()
                                        )
                                )
                        );
                    }
                }

            }

            return reflexivKmerConcatList.iterator();
        }
    }

    /**
     * interface class for RDD implementation, used in step 5
     */

    /**
     * interface class for RDD implementation, used in step 4
     */
    class KmerCounting implements Function2<Integer, Integer, Integer>, Serializable{
        public Integer call (Integer i1, Integer i2) {
            return i1 + i2;
        }
    }

    class KmerReverseComplement implements PairFlatMapFunction<Iterator<Tuple2<Long, Integer>>, Long, Integer>, Serializable{
        /* a capsule for all Kmers and reverseComplementKmers */
        List<Tuple2<Long, Integer>> kmerList = new ArrayList<Tuple2<Long, Integer>>();
        Long reverseComplement;
        Tuple2<Long, Integer> kmerTuple;
        Long lastTwoBits;
        Long kmerBinary;


        public Iterator<Tuple2<Long, Integer>> call(Iterator<Tuple2<Long, Integer>> s){


            while (s.hasNext()) {
                kmerTuple = s.next();
                kmerBinary = kmerTuple._1;
                reverseComplement=0L;
                for (int i = 0; i < param.kmerSize; i++) {
                    reverseComplement<<=2;

                    lastTwoBits = kmerBinary & 3L ^ 3L;
                    kmerBinary >>>=2;
                    reverseComplement|=lastTwoBits;
                }

                kmerList.add(kmerTuple);
                kmerList.add(new Tuple2<Long, Integer>(reverseComplement, kmerTuple._2));
            }

            return kmerList.iterator();
        }
    }

    /**
     * interface class for RDD implementation, used in step 3
     *      -----------
     *      ------
     *       ------
     *        ------
     *         ------
     *          ------
     *           ------
     */

    class LoadCountedKmerToLongArray implements PairFlatMapFunction<Iterator<String>, Long, Integer>, Serializable {

        List<Tuple2<Long, Integer>> kmerList = new ArrayList<Tuple2<Long, Integer>>();
        String[] units;
        String kmer;
        int cover;
        char nucleotide;
        long nucleotideInt;
   //     Long suffixBinary;
   //     Long[] suffixBinaryArray;

        public Iterator<Tuple2<Long, Integer >> call(Iterator<String> s) {

            while (s.hasNext()) {
                units = s.next().split(",");

                kmer = units[0].substring(1);

                cover = Integer.parseInt(StringUtils.chop(units[1]));

                Long nucleotideBinary = 0L;

                for (int i = 0; i < param.kmerSize; i++) {
                    nucleotide = kmer.charAt(i);
                    if (nucleotide >= 256) nucleotide = 255;
                    nucleotideInt = nucleotideValue(nucleotide);
                    // forward kmer in bits
                    nucleotideBinary <<= 2;
                    nucleotideBinary |= nucleotideInt;
                }

                kmerList.add(
                        new Tuple2<Long, Integer>(
                                nucleotideBinary, cover
                        )
                );
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
