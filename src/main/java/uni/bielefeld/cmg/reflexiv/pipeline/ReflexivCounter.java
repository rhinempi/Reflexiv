package uni.bielefeld.cmg.reflexiv.pipeline;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;
import scala.Tuple4;
import uni.bielefeld.cmg.reflexiv.util.DefaultParam;
import uni.bielefeld.cmg.reflexiv.util.InfoDumper;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;


/**
 * Created by Liren Huang on 25.08.17.
 *
 *      Reflexiv
 *
 * Copyright (c) 2015-2015
 * Liren Huang      <huanglr at cebitec.uni-bielefeld.de>
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

        /**
         * Generate reverse complement Kmers
         */
        KmerReverseComplement RDDRCKmer = new KmerReverseComplement();
        KmerBinaryRDD = KmerBinaryRDD.mapPartitionsToPair(RDDRCKmer);

        /**
         * Step : filter forks
         */

        ForwardSubKmerExtraction RDDextractForwardSubKmer = new ForwardSubKmerExtraction();
        ReflexivSubKmerRDD = KmerBinaryRDD.mapPartitionsToPair(RDDextractForwardSubKmer);   // all forward

        if (param.bubble == true) {
            ReflexivSubKmerRDD = ReflexivSubKmerRDD.sortByKey();
            FilterForkSubKmer RDDhighCoverageSelector = new FilterForkSubKmer();
            ReflexivSubKmerRDD = ReflexivSubKmerRDD.mapPartitionsToPair(RDDhighCoverageSelector);

            ReflectedSubKmerExtractionFromForward RDDreflectionExtractor =  new ReflectedSubKmerExtractionFromForward();
            ReflexivSubKmerRDD = ReflexivSubKmerRDD.mapPartitionsToPair(RDDreflectionExtractor); // all reflected

            ReflexivSubKmerRDD = ReflexivSubKmerRDD.sortByKey();
            FilterForkReflectedSubKmer RDDhighCoverageReflectedSelector = new FilterForkReflectedSubKmer();
            ReflexivSubKmerRDD = ReflexivSubKmerRDD.mapPartitionsToPair(RDDhighCoverageReflectedSelector);
        }

        /**
         * Step 6: extract sub-kmers from each K-mer
         */
        kmerRandomReflection RDDrandomizeSubKmer = new kmerRandomReflection();
        ReflexivSubKmerRDD = ReflexivSubKmerRDD.mapPartitionsToPair(RDDrandomizeSubKmer);

        /**
         * Step 7: sort all sub-kmers
         */

        ReflexivSubKmerRDD = ReflexivSubKmerRDD.sortByKey();

        BinaryReflexivKmerToString StringOutput = new BinaryReflexivKmerToString();

        ReflexivSubKmerStringRDD = ReflexivSubKmerRDD.mapPartitionsToPair(StringOutput);
        ReflexivSubKmerStringRDD.saveAsTextFile(param.outputPath + 0);

        /**
         * Step 8: connect and extend overlap kmers
         */

        ExtendReflexivKmer KmerExtention = new ExtendReflexivKmer();
        ReflexivSubKmerRDD = ReflexivSubKmerRDD.mapPartitionsToPair(KmerExtention);

        /**
         * Step 9: filter extended Kmers
         */


        /**
         * first three extensions fit in one Long 1 2 4 8 16 32(x)
         */
        int iterations = 0;
        for (int i = 0; i < 4; i++) {
            iterations++;
            ReflexivSubKmerRDD = ReflexivSubKmerRDD.sortByKey();

            ReflexivSubKmerStringRDD = ReflexivSubKmerRDD.mapPartitionsToPair(StringOutput);
            ReflexivSubKmerStringRDD.saveAsTextFile(param.outputPath + iterations);

            ReflexivSubKmerRDD = ReflexivSubKmerRDD.mapPartitionsToPair(KmerExtention);
        }


        /**
         * first extension to array
         */
        ReflexivSubKmerRDD = ReflexivSubKmerRDD.sortByKey();

        iterations++;
        ReflexivSubKmerStringRDD = ReflexivSubKmerRDD.mapPartitionsToPair(StringOutput);
        ReflexivSubKmerStringRDD.saveAsTextFile(param.outputPath + iterations);

        ExtendReflexivKmerToArrayFirstTime KmerExtentionToArrayFirst = new ExtendReflexivKmerToArrayFirstTime();
        ReflexivLongSubKmerRDD = ReflexivSubKmerRDD.mapPartitionsToPair(KmerExtentionToArrayFirst);

        BinaryReflexivKmerArrayToString ArrayStringOutput = new BinaryReflexivKmerArrayToString();

        /**
         * Step 10: iteration: repeat step 6, 7 and 8 until convergence is reached
         */
        ExtendReflexivKmerToArrayLoop KmerExtenstionArrayToArray = new ExtendReflexivKmerToArrayLoop();

        int partitionNumber = ReflexivSubKmerRDD.getNumPartitions();
        long contigNumber = 0;
        while (iterations <= param.maximumIteration) {
            iterations++;
            if (iterations >= param.minimumIteration){
                if (iterations % 3 == 0) {

                    long currentContigNumber = ReflexivSubKmerRDD.count();
                    if (contigNumber == currentContigNumber) {
                        break;
                    } else {
                        contigNumber = currentContigNumber;
                    }

                    if (partitionNumber >= 16) {
                        if (currentContigNumber / partitionNumber <= 20) {
                            partitionNumber = partitionNumber / 4 + 1;
                            ReflexivSubKmerRDD = ReflexivSubKmerRDD.coalesce(partitionNumber);
                        }
                    }
                }
            }

            ReflexivLongSubKmerRDD = ReflexivLongSubKmerRDD.sortByKey();

            ReflexivSubKmerStringRDD = ReflexivLongSubKmerRDD.mapPartitionsToPair(ArrayStringOutput);
            ReflexivSubKmerStringRDD.saveAsTextFile(param.outputPath + iterations);

            ReflexivLongSubKmerRDD = ReflexivLongSubKmerRDD.mapPartitionsToPair(KmerExtenstionArrayToArray);

            ReflexivSubKmerStringRDD = ReflexivLongSubKmerRDD.mapPartitionsToPair(ArrayStringOutput);
            ReflexivSubKmerStringRDD.saveAsTextFile(param.outputPath + iterations + "Extend");

        }

        /**
         * Step 11: change reflexiv kmers to contig
         */
        /*
        KmerToContig contigformater = new KmerToContig();
        ContigTuple2RDD = ReflexivSubKmerRDD.flatMapToPair(contigformater);

        ContigTuple2IndexRDD = ContigTuple2RDD.zipWithIndex();

        TagContigID IdLabeling = new TagContigID();
        ContigRDD = ContigTuple2IndexRDD.flatMap(IdLabeling);

        /**
         * Step N: save result
         */
        /*
        ContigRDD.saveAsTextFile(param.outputPath);

        /**
         * Step N+1: Stop
         */
        sc.stop();
    }


    class TagContigID implements FlatMapFunction<Tuple2<Tuple2<String, String>, Long[]>, String>, Serializable {

        public Iterator<String> call(Tuple2<Tuple2<String, String>, Long[]> s) {


            List<String> contigList = new ArrayList<String>();

            contigList.add(s._1._1 + "-" + s._2 + "\n" + s._1._2);

            return contigList.iterator();
        }
    }


    /**
     * interface class for RDD implementation, used in step 5
     */
    class KmerToContig implements PairFlatMapFunction<Tuple2<String, Tuple4<Integer, String, Integer, Integer>>, String, String>, Serializable{

        public Iterator<Tuple2<String, String>> call (Tuple2<String, Tuple4<Integer, String, Integer, Integer>> s){

            List<Tuple2<String, String>> contigList = new ArrayList<Tuple2<String, String>>();
            if (s._2._1() == 1) {
                String contig= s._1 + s._2._2();
                int length = contig.length();
                if (length >= param.minContig) {
                    String ID = ">Contig-" + length;
                    String formatedContig = changeLine(contig, length, 100);
                    contigList.add(new Tuple2<String, String>(ID, formatedContig));
                }
            }

            else{ // (randomReflexivMarker == 2) {
                String contig= s._2._2() + s._1;
                int length = contig.length();
                if (length >= param.minContig){
                    String ID = ">Contig-" + length;
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


    class ReflexivExtensionToArray implements PairFlatMapFunction<Iterator<Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>>, Long, Tuple4<Integer, Long[], Integer, Integer>>, Serializable{
        public Iterator<Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>> call (Iterator<Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>> sIterator) {
            List<Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>> reflexivKmerConcatList = new ArrayList<Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>>();
            return reflexivKmerConcatList.iterator();
        }
    }

    /**
     *
     */
    class BinaryReflexivKmerToString implements PairFlatMapFunction<Iterator<Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>>, String, Tuple4<Integer, String, Integer, Integer>>, Serializable{
        List<Tuple2<String, Tuple4<Integer, String, Integer, Integer>>> reflexivKmerStringList = new ArrayList<Tuple2<String, Tuple4<Integer, String, Integer, Integer>>>();

        public Iterator<Tuple2<String, Tuple4<Integer, String, Integer, Integer>>> call(Iterator<Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>> sIterator){
            while (sIterator.hasNext()){
                String subKmer = "";
                String subString ="";
                Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>> s = sIterator.next();
                int currentSuffixLength = Long.SIZE/2 - (Long.numberOfLeadingZeros(s._2._2())/2 + 1);
                for (int i=1; i<=param.subKmerSize;i++){
                    Long currentNucleotideBinary = s._1 >>> 2*(param.subKmerSize - i);
                    currentNucleotideBinary &= 3L;
                    char currentNucleotide =  BinaryToNucleotide(currentNucleotideBinary);
                    subKmer += currentNucleotide;
                }

                for (int i=1; i<=currentSuffixLength; i++){
                    Long currentNucleotideBinary = s._2._2() >>> 2*(currentSuffixLength - i);
                    currentNucleotideBinary &= 3L;
                    char currentNucleotide =  BinaryToNucleotide(currentNucleotideBinary);
                    subString += currentNucleotide;
                }

                reflexivKmerStringList.add (
                        new Tuple2<String, Tuple4<Integer, String, Integer, Integer>>(
                                subKmer, new Tuple4<Integer, String, Integer, Integer>(s._2._1(), subString, s._2._3(), s._2._4())
                        )
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
/*
 *           else if (lineMarker == -1){
 *               if (s._2._1() == 0) {
 *                   reflexivKmerConcatList.add(s);
 *                   return reflexivKmerConcatList.iterator();
 *               } else if (s._1 == tmpReflexivKmerExtendList.get(0)._1()) { /* the same Sub-kmer, than kill
 *
 *                   return null;
 *               } else { /* not the sam Sub-kmer, than reset to new sub-kmer group
 *                   resetSubKmerGroup(s);
 *                   return null;
 *               }
 *           }
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
                                        int tmpReflexivKmerSuffixLength = Long.SIZE/2 - (Long.numberOfLeadingZeros(tmpReflexivKmerExtendList.get(i)._2._2()[0])/2 + 1);
                                        int tmpBlockSize = (tmpReflexivKmerExtendList.get(i)._2._2().length - 1)*31 + tmpReflexivKmerSuffixLength;
                                        int currentReflexivKmerSuffixLength = Long.SIZE/2 - (Long.numberOfLeadingZeros(s._2._2()[0])/2 + 1);
                                        int currentBlockSize = (s._2._2().length -1)*31 + currentReflexivKmerSuffixLength;

                                        if (s._2._3() < 0 && tmpReflexivKmerExtendList.get(i)._2._4() < 0) {
                                            reflexivExtend(s, tmpReflexivKmerExtendList.get(i), -1);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        }else if (s._2._3()>=0 && tmpReflexivKmerExtendList.get(i)._2._4()>=0) {
                                            reflexivExtend(s, tmpReflexivKmerExtendList.get(i), -1);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        }else if (s._2._3()>=0 && s._2._3()-tmpBlockSize>=0) {
                                            reflexivExtend(s, tmpReflexivKmerExtendList.get(i), s._2._3()-tmpBlockSize);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        }else if (tmpReflexivKmerExtendList.get(i)._2._4() >=0 && tmpReflexivKmerExtendList.get(i)._2._4()-currentBlockSize>=0){
                                            reflexivExtend(s, tmpReflexivKmerExtendList.get(i), tmpReflexivKmerExtendList.get(i)._2._4()-currentBlockSize);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        }else{
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
                                        int tmpReflexivKmerSuffixLength = Long.SIZE/2 - (Long.numberOfLeadingZeros(tmpReflexivKmerExtendList.get(i)._2._2()[0])/2 + 1);
                                        int tmpBlockSize = (tmpReflexivKmerExtendList.get(i)._2._2().length - 1)*31 + tmpReflexivKmerSuffixLength;
                                        int currentReflexivKmerSuffixLength = Long.SIZE/2 - (Long.numberOfLeadingZeros(s._2._2()[0])/2 + 1);
                                        int currentBlockSize = (s._2._2().length -1)*31 + currentReflexivKmerSuffixLength;
                                        if (s._2._4() < 0 && tmpReflexivKmerExtendList.get(i)._2._3() < 0) {
                                            reflexivExtend(tmpReflexivKmerExtendList.get(i), s, -1);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        }else if (s._2._4()>=0 && tmpReflexivKmerExtendList.get(i)._2._3()>=0) {
                                            reflexivExtend(tmpReflexivKmerExtendList.get(i), s, -1);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        }else if (s._2._4()>=0 && s._2._4()-tmpBlockSize>=0) {
                                            reflexivExtend(tmpReflexivKmerExtendList.get(i), s, s._2._4()-tmpBlockSize);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        }else if (tmpReflexivKmerExtendList.get(i)._2._3() >=0 && tmpReflexivKmerExtendList.get(i)._2._4()-currentBlockSize >=0){
                                            reflexivExtend(tmpReflexivKmerExtendList.get(i), s, tmpReflexivKmerExtendList.get(i)._2._4()-currentBlockSize);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        }else{
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
            /* re-reflex all single kmers in the sub-kmer group */
//            if (tmpReflexivKmerExtendList.size() != 0) {
//                for (int i = 0; i < tmpReflexivKmerExtendList.size(); i++) {
            //                   singleKmerRandomizer(tmpReflexivKmerExtendList.get(i));
            //               }
            //          }

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
     *
     */
    class ExtendReflexivKmerToArrayFirstTime implements PairFlatMapFunction<Iterator<Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>>, Long, Tuple4<Integer, Long[], Integer, Integer>>, Serializable{

        /* marker to identify similar SubKmers in the loop sequence */
        private int lineMarker=1;

        /* 1 stands for forward sub-kmer */
        /* 2 stands for reflexiv sub-kmer */
 //       private int randomReflexivMarker = ThreadLocalRandom.current().nextInt(1, 3);
        private int randomReflexivMarker =2;

        long maxSubKmerBinary = ~((~0L) << 2*param.subKmerSize);
        long maxBlockBinary = ~((~0L) << 2*31); // a block has 31 nucleotide


        /* temporary capsule to store identical SubKmer units */
        List<Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>> tmpReflexivKmerExtendList = new ArrayList<Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>>();

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
        public Iterator<Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>> call (Iterator<Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>> sIterator) {

            while (sIterator.hasNext()) {
                Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>> s = sIterator.next();
            /* receive the first sub-kmer, set new units */
                if (lineMarker == 1) {
                    resetSubKmerGroup(s);

                    // return reflexivKmerConcatList.iterator();
                }

            /* removal condition */
                /**
                 * Deprecated function for killer k-mers
                 */
/*
 *           else if (lineMarker == -1){
 *               if (s._2._1() == 0) {
 *                   reflexivKmerConcatList.add(s);
 *                   return reflexivKmerConcatList.iterator();
 *               } else if (s._1 == tmpReflexivKmerExtendList.get(0)._1()) { /* the same Sub-kmer, than kill
 *
 *                   return null;
 *               } else { /* not the sam Sub-kmer, than reset to new sub-kmer group
 *                   resetSubKmerGroup(s);
 *                   return null;
 *               }
 *           }
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
                                        int tmpReflexivKmerSuffixLength = Long.SIZE/2 - (Long.numberOfLeadingZeros(tmpReflexivKmerExtendList.get(i)._2._2())/2 + 1);
                                        int currentReflexivKmerSuffixLength = Long.SIZE/2 - (Long.numberOfLeadingZeros(s._2._2())/2 + 1);
                                        if (s._2._3() < 0 && tmpReflexivKmerExtendList.get(i)._2._4() < 0) {
                                            reflexivExtend(s, tmpReflexivKmerExtendList.get(i), -1);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        }else if (s._2._3()>=0 && tmpReflexivKmerExtendList.get(i)._2._4()>=0) {
                                            reflexivExtend(s, tmpReflexivKmerExtendList.get(i), -1);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        }else if (s._2._3()>=0 && s._2._3()-tmpReflexivKmerSuffixLength>=0) {
                                            reflexivExtend(s, tmpReflexivKmerExtendList.get(i), s._2._3()-tmpReflexivKmerSuffixLength);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        }else if (tmpReflexivKmerExtendList.get(i)._2._4() >=0 && tmpReflexivKmerExtendList.get(i)._2._4()-currentReflexivKmerSuffixLength>=0){
                                            reflexivExtend(s, tmpReflexivKmerExtendList.get(i), tmpReflexivKmerExtendList.get(i)._2._4()-currentReflexivKmerSuffixLength);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        }else{
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
                                        int tmpReflexivKmerSuffixLength = Long.SIZE/2 - (Long.numberOfLeadingZeros(tmpReflexivKmerExtendList.get(i)._2._2())/2 + 1);
                                        int currentReflexivKmerSuffixLength = Long.SIZE/2 - (Long.numberOfLeadingZeros(s._2._2())/2 + 1);
                                        if (s._2._4() < 0 && tmpReflexivKmerExtendList.get(i)._2._3() < 0) {
                                            reflexivExtend(tmpReflexivKmerExtendList.get(i), s, -1);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        }else if (s._2._4()>=0 && tmpReflexivKmerExtendList.get(i)._2._3()>=0) {
                                            reflexivExtend(tmpReflexivKmerExtendList.get(i), s, -1);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        }else if (s._2._4()>=0 && s._2._4()-tmpReflexivKmerSuffixLength>=0) {
                                            reflexivExtend(tmpReflexivKmerExtendList.get(i), s, s._2._4()-tmpReflexivKmerSuffixLength);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        }else if (tmpReflexivKmerExtendList.get(i)._2._3() >=0 && tmpReflexivKmerExtendList.get(i)._2._4()-currentReflexivKmerSuffixLength >=0){
                                            reflexivExtend(tmpReflexivKmerExtendList.get(i), s, tmpReflexivKmerExtendList.get(i)._2._4()-currentReflexivKmerSuffixLength);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        }else{
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
        public void singleKmerRandomizer(Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>> currentSubKmer){


            if (currentSubKmer._2._1() == 1){
                int currentSuffixLength = Long.SIZE/2 - (Long.numberOfLeadingZeros(currentSubKmer._2._2())/2 + 1);
                long maxSuffixLengthBinary = ~((~0L) << (2*currentSuffixLength));
                Long newReflexivSubKmer=0L;
                Long newReflexivLong=0L;
                Long[] newReflexivLongArray;

                if (randomReflexivMarker == 2) {
                    if (currentSuffixLength > param.subKmerSize) {
                        System.out.println("what? not possible. Tell the author to check his program. He knows");
                        newReflexivLongArray = new Long[1];
                    }else if (currentSuffixLength == param.subKmerSize){
                        System.out.println("what? not possible. Tell the author to check his program. He knows");
                        newReflexivLongArray = new Long[1];
                    }else{
                        newReflexivSubKmer = currentSubKmer._1 << (currentSuffixLength*2);
                        newReflexivSubKmer &= maxSubKmerBinary;
                        newReflexivSubKmer |= (currentSubKmer._2._2() & maxSuffixLengthBinary);


                        newReflexivLong = currentSubKmer._1 >>> (2*(param.subKmerSize- currentSuffixLength));
                        newReflexivLong |= (1L<<(2*currentSuffixLength)); // add C marker in the front
                        /**
                         * to array
                         */
                        newReflexivLongArray = new Long[1];
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
                    newReflexivLongArray = new Long[1];
                    newReflexivLongArray[0] = currentSubKmer._2._2();
                    reflexivKmerConcatList.add(
                            new Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>(currentSubKmer._1,
                                    new Tuple4<Integer, Long[], Integer, Integer>(
                                            currentSubKmer._2._1(), newReflexivLongArray, currentSubKmer._2._3(), currentSubKmer._2._4()
                                    )
                            )
                    );
                }
            }else{ /* currentSubKmer._2._1() == 2 */
                int currentPrefixLength = Long.SIZE/2 - (Long.numberOfLeadingZeros(currentSubKmer._2._2())/2 + 1);
                long maxSuffixLengthBinary = ~((~0L) << (2*currentPrefixLength));
                Long newReflexivSubKmer=0L;
                Long newReflexivLong=0L;
                Long[] newReflexivLongArray;
                if (randomReflexivMarker == 2) {
                    newReflexivLongArray = new Long[1];
                    newReflexivLongArray[0] = currentSubKmer._2._2();
                    reflexivKmerConcatList.add(
                            new Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>(currentSubKmer._1,
                                    new Tuple4<Integer, Long[], Integer, Integer>(
                                            currentSubKmer._2._1(), newReflexivLongArray, currentSubKmer._2._3(), currentSubKmer._2._4()
                                    )
                            )
                    );
                }else{ /* randomReflexivMarker == 1 */
                    if (currentPrefixLength > param.subKmerSize){
                        System.out.println("what? not possible. Tell the author to check his program. He knows");
                        newReflexivLongArray = new Long[1];
                    }else if (currentPrefixLength == param.subKmerSize){
                        System.out.println("what? not possible. Tell the author to check his program. He knows");
                        newReflexivLongArray = new Long[1];
                    }else{ /* currentPreffixLength < param.subKmerSize */
                        newReflexivSubKmer = (currentSubKmer._2._2() & maxSuffixLengthBinary) << (2*(param.subKmerSize - currentPrefixLength));;
                        //newReflexivSubKmer <<= (2*(param.subKmerSize - currentPrefixLength));
                        newReflexivSubKmer |=(currentSubKmer._1 >>> (2*currentPrefixLength));

                        newReflexivLong = currentSubKmer._1 & maxSuffixLengthBinary;
                        newReflexivLong |= (1L<<2*currentPrefixLength); // add C marker in the front
                        /**
                         * to array
                         */
                        newReflexivLongArray = new Long[1];
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
        public void directKmerComparison(Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>> currentSubKmer){
            tmpReflexivKmerExtendList.add(currentSubKmer);
        }

        /**
         *
         * @param forwardSubKmer
         * @param reflexedSubKmer
         */
        public void reflexivExtend(Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>> forwardSubKmer, Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>> reflexedSubKmer, int bubbleDistance) {

             /* forward   ATCGATCG, 1, ------ */
             /* reflexed  ------, 2, ATCGATCG */

            int forwardSuffixLength = Long.SIZE/2 - (Long.numberOfLeadingZeros(forwardSubKmer._2._2())/2 + 1);
            int reflexedPrefixLength = Long.SIZE/2 - (Long.numberOfLeadingZeros(reflexedSubKmer._2._2())/2 + 1);
            int concatenateLength = forwardSuffixLength + reflexedPrefixLength;
            long maxSuffixLengthBinary = ~((~0L) << 2*forwardSuffixLength);
            long maxPrefixLengthBinary = ~((~0L) << 2*reflexedPrefixLength);


            if (randomReflexivMarker == 2) {
                Long newReflexivSubKmer=0L;
                Long newReflexivLong=0L;
                Long[] newReflexivLongArray;

                if (forwardSuffixLength > param.subKmerSize) {
                    System.out.println("what? not possible. Tell the author to check his program. He knows");
                    newReflexivLongArray = new Long[1];
                } else if (forwardSuffixLength == param.subKmerSize) {
                    System.out.println("what? not possible. Tell the author to check his program. He knows");
                    newReflexivLongArray = new Long[1];
                } else { /* forwardSuffixLength < param.subKmerSize */

                    newReflexivSubKmer = forwardSubKmer._1 << (2*forwardSuffixLength);
                    newReflexivSubKmer &= maxSubKmerBinary;
                    newReflexivSubKmer |= (forwardSubKmer._2._2() & maxSuffixLengthBinary);
                    if (concatenateLength > 31){ // 31 for one block
                        Long newReflexivLonghead = reflexedSubKmer._2._2() >>> (2*(31-forwardSuffixLength)); // do not remove the C maker
                        newReflexivLong= reflexedSubKmer._2._2() << (2*forwardSuffixLength);
                        newReflexivLong &= maxBlockBinary;
                        newReflexivLong |= (forwardSubKmer._1 >>> 2*(param.subKmerSize - forwardSuffixLength));

                        newReflexivLongArray = new Long[concatenateLength/31+1];
                        newReflexivLongArray[0] = newReflexivLonghead;
                        newReflexivLongArray[1] = newReflexivLong;
                    }else {
                        newReflexivLong = reflexedSubKmer._2._2() << (2 * forwardSuffixLength); // do not remove the C marker as it will be used again
                        newReflexivLong |= (forwardSubKmer._1 >>> (2 * (param.subKmerSize - forwardSuffixLength))); // do not have to add the C marker

                        // the first time only one element in the array
                        newReflexivLongArray = new Long[1];
                        newReflexivLongArray[0] = newReflexivLong;
                    }
                }

                if (bubbleDistance <0) {
                    reflexivKmerConcatList.add(
                            new Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>(newReflexivSubKmer,
                                    new Tuple4<Integer, Long[], Integer, Integer>(
                                            randomReflexivMarker, newReflexivLongArray, reflexedSubKmer._2._3(), forwardSubKmer._2._4()
                                    )
                            )
                    );
                }else {
                    if (forwardSubKmer._2._3() > 0) {
                        reflexivKmerConcatList.add(
                                new Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>(newReflexivSubKmer,
                                        new Tuple4<Integer, Long[], Integer, Integer>(
                                                randomReflexivMarker, newReflexivLongArray, bubbleDistance, forwardSubKmer._2._4()
                                        )
                                )
                        );
                    }else{
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
                Long newForwardSubKmer=0L;
                Long newForwardLong=0L;
                Long[] newForwardLongArray;

                if (reflexedPrefixLength > param.subKmerSize) {
                    System.out.println("what? not possible. Tell the author to check his program. He knows");
                    newForwardLongArray = new Long[1];
                } else if (reflexedPrefixLength == param.subKmerSize) {
                    System.out.println("what? not possible. Tell the author to check his program. He knows");
                    newForwardLongArray = new Long[1];
                } else { /* reflexedPreffixLength < param.subKmerSize */
                    newForwardSubKmer = (reflexedSubKmer._2._2() & maxPrefixLengthBinary) << (2*(param.subKmerSize - reflexedPrefixLength));
                    newForwardSubKmer |= reflexedSubKmer._1 >>> 2*reflexedPrefixLength;

                    if (concatenateLength>31){
                        Long newForwardLonghead = forwardSubKmer._1 & maxPrefixLengthBinary;
                        newForwardLonghead >>>= 2*(31 - forwardSuffixLength);
                        newForwardLonghead |= (1L << 2*(concatenateLength -31)); // add the C maker

                        newForwardLong = forwardSubKmer._1 << 2*forwardSuffixLength;
                        newForwardLong |= (forwardSubKmer._2._2() & maxSuffixLengthBinary);
                        newForwardLong &= maxBlockBinary;

                        newForwardLongArray = new Long[concatenateLength/31+1];
                        newForwardLongArray[0] = newForwardLonghead;
                        newForwardLongArray[1] = newForwardLong;
                    }else {
                        newForwardLong = reflexedSubKmer._1 & maxPrefixLengthBinary;
                        newForwardLong |= (1L << (2 * reflexedPrefixLength)); // add the C marker
                        newForwardLong <<= (2 * forwardSuffixLength);
                        newForwardLong |= (forwardSubKmer._2._2() & maxSuffixLengthBinary);

                        // the first time only one element
                        newForwardLongArray = new Long[1];
                        newForwardLongArray[0] = newForwardLong;
                    }
                }

                if (bubbleDistance <0) {
                    reflexivKmerConcatList.add(
                            new Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>(newForwardSubKmer,
                                    new Tuple4<Integer, Long[], Integer, Integer>(
                                            randomReflexivMarker, newForwardLongArray, reflexedSubKmer._2._3(), forwardSubKmer._2._4()
                                    )
                            )
                    );
                }else {
                    if (forwardSubKmer._2._3() > 0) {
                        reflexivKmerConcatList.add(
                                new Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>(newForwardSubKmer,
                                        new Tuple4<Integer, Long[], Integer, Integer>(
                                                randomReflexivMarker, newForwardLongArray, bubbleDistance, forwardSubKmer._2._4()
                                        )
                                )
                        );
                    }else{ // reflexedSubKmer._2._4() >0
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
        public void resetSubKmerGroup(Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>> S) {
            if (lineMarker == 1) {
                lineMarker = 2;
            }else {
                lineMarker = 3; /* reset to new sub-kmer group */
            }
            /* re-reflex all single kmers in the sub-kmer group */
//            if (tmpReflexivKmerExtendList.size() != 0) {
//                for (int i = 0; i < tmpReflexivKmerExtendList.size(); i++) {
            //                   singleKmerRandomizer(tmpReflexivKmerExtendList.get(i));
            //               }
            //          }

            tmpReflexivKmerExtendList = new ArrayList<Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>>();
            tmpReflexivKmerExtendList.add(
                    new Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>(S._1,
                            new Tuple4<Integer, Long, Integer, Integer>(
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
     *
     */
    class ExtendReflexivKmer implements PairFlatMapFunction<Iterator<Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>>, Long, Tuple4<Integer, Long, Integer, Integer>>, Serializable{

        /* marker to identify similar SubKmers in the loop sequence */
        private int lineMarker=1;

        /* 1 stands for forward sub-kmer */
        /* 2 stands for reflexiv sub-kmer */
   //     private int randomReflexivMarker = ThreadLocalRandom.current().nextInt(1, 3);
        private int randomReflexivMarker=2;

        long maxSubKmerBinary = ~((~0L) << 2*param.subKmerSize);


        /* temporary capsule to store identical SubKmer units */
        List<Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>> tmpReflexivKmerExtendList = new ArrayList<Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>>();

        /* return capsule of extend Tuples for next iteration*/
        List<Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>> reflexivKmerConcatList = new ArrayList<Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>>();

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
        public Iterator<Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>> call (Iterator<Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>> sIterator) {

            while (sIterator.hasNext()) {
                Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>> s = sIterator.next();
            /* receive the first sub-kmer, set new units */
                if (lineMarker == 1) {
                    resetSubKmerGroup(s);

                   // return reflexivKmerConcatList.iterator();
                }

            /* removal condition */
                /**
                 * Deprecated function for killer k-mers
                 */
/*
 *           else if (lineMarker == -1){
 *               if (s._2._1() == 0) {
 *                   reflexivKmerConcatList.add(s);
 *                   return reflexivKmerConcatList.iterator();
 *               } else if (s._1 == tmpReflexivKmerExtendList.get(0)._1()) { /* the same Sub-kmer, than kill
 *
 *                   return null;
 *               } else { /* not the sam Sub-kmer, than reset to new sub-kmer group
 *                   resetSubKmerGroup(s);
 *                   return null;
 *               }
 *           }
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
                                        int tmpReflexivKmerSuffixLength = Long.SIZE/2 - (Long.numberOfLeadingZeros(tmpReflexivKmerExtendList.get(i)._2._2())/2 + 1);
                                        int currentReflexivKmerSuffixLength = Long.SIZE/2 - (Long.numberOfLeadingZeros(s._2._2())/2 + 1);
                                        if (s._2._3() < 0 && tmpReflexivKmerExtendList.get(i)._2._4() < 0) {
                                            reflexivExtend(s, tmpReflexivKmerExtendList.get(i), -1);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        }else if (s._2._3()>=0 && tmpReflexivKmerExtendList.get(i)._2._4()>=0) {
                                            reflexivExtend(s, tmpReflexivKmerExtendList.get(i), -1);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        }else if (s._2._3()>=0 && s._2._3()-tmpReflexivKmerSuffixLength>=0) {
                                            reflexivExtend(s, tmpReflexivKmerExtendList.get(i), s._2._3()-tmpReflexivKmerSuffixLength);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        }else if (tmpReflexivKmerExtendList.get(i)._2._4() >=0 && tmpReflexivKmerExtendList.get(i)._2._4()-currentReflexivKmerSuffixLength>=0){
                                            reflexivExtend(s, tmpReflexivKmerExtendList.get(i), tmpReflexivKmerExtendList.get(i)._2._4()-currentReflexivKmerSuffixLength);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        }else{
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
                                        int tmpReflexivKmerSuffixLength = Long.SIZE/2 - (Long.numberOfLeadingZeros(tmpReflexivKmerExtendList.get(i)._2._2())/2 + 1);
                                        int currentReflexivKmerSuffixLength = Long.SIZE/2 - (Long.numberOfLeadingZeros(s._2._2())/2 + 1);
                                        if (s._2._4() < 0 && tmpReflexivKmerExtendList.get(i)._2._3() < 0) {
                                            reflexivExtend(tmpReflexivKmerExtendList.get(i), s, -1);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        }else if (s._2._4()>=0 && tmpReflexivKmerExtendList.get(i)._2._3()>=0) {
                                            reflexivExtend(tmpReflexivKmerExtendList.get(i), s, -1);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        }else if (s._2._4()>=0 && s._2._4()-tmpReflexivKmerSuffixLength>=0) {
                                            reflexivExtend(tmpReflexivKmerExtendList.get(i), s, s._2._4()-tmpReflexivKmerSuffixLength);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        }else if (tmpReflexivKmerExtendList.get(i)._2._3() >=0 && tmpReflexivKmerExtendList.get(i)._2._4()-currentReflexivKmerSuffixLength >=0){
                                            reflexivExtend(tmpReflexivKmerExtendList.get(i), s, tmpReflexivKmerExtendList.get(i)._2._4()-currentReflexivKmerSuffixLength);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        }else{
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
        public void singleKmerRandomizer(Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>> currentSubKmer){


            if (currentSubKmer._2._1() == 1){
                int currentSuffixLength = Long.SIZE/2 - (Long.numberOfLeadingZeros(currentSubKmer._2._2())/2 + 1);
                long maxSuffixLengthBinary = ~((~0L) << (2*currentSuffixLength));
                Long newReflexivSubKmer=0L;
                Long newReflexivLong=0L;

                if (randomReflexivMarker == 2) {
                    if (currentSuffixLength > param.subKmerSize) {
                        System.out.println("what? not possible. Tell the author to check his program. He knows");

                    }else if (currentSuffixLength == param.subKmerSize){
                        System.out.println("what? not possible. Tell the author to check his program. He knows");

                    }else{
                        newReflexivSubKmer = currentSubKmer._1 << (currentSuffixLength*2);
                        newReflexivSubKmer &= maxSubKmerBinary;
                        newReflexivSubKmer |= (currentSubKmer._2._2() & maxSuffixLengthBinary);


                        newReflexivLong = currentSubKmer._1 >>> (2*(param.subKmerSize- currentSuffixLength));
                        newReflexivLong |= (1L<<(2*currentSuffixLength)); // add C marker in the front


                    }

                    reflexivKmerConcatList.add(
                            new Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>(newReflexivSubKmer,
                                    new Tuple4<Integer, Long, Integer, Integer>(
                                            randomReflexivMarker, newReflexivLong, currentSubKmer._2._3(), currentSubKmer._2._4()
                                    )
                            )
                    );
                }else{
                    reflexivKmerConcatList.add(currentSubKmer);
                }
            }else{ /* currentSubKmer._2._1() == 2 */
                int currentPrefixLength = Long.SIZE/2 - (Long.numberOfLeadingZeros(currentSubKmer._2._2())/2 + 1);
                long maxSuffixLengthBinary = ~((~0L) << (2*currentPrefixLength));
                Long newReflexivSubKmer=0L;
                Long newReflexivLong=0L;
                if (randomReflexivMarker == 2) {
                    reflexivKmerConcatList.add(currentSubKmer);
                }else{ /* randomReflexivMarker == 1 */
                    if (currentPrefixLength > param.subKmerSize){
                        System.out.println("what? not possible. Tell the author to check his program. He knows");

                    }else if (currentPrefixLength == param.subKmerSize){
                        System.out.println("what? not possible. Tell the author to check his program. He knows");

                    }else{ /* currentPreffixLength < param.subKmerSize */
                        newReflexivSubKmer = (currentSubKmer._2._2() & maxSuffixLengthBinary) << (2*(param.subKmerSize - currentPrefixLength));;
                        //newReflexivSubKmer <<= (2*(param.subKmerSize - currentPrefixLength));
                        newReflexivSubKmer |=(currentSubKmer._1 >>> (2*currentPrefixLength));

                        newReflexivLong = currentSubKmer._1 & maxSuffixLengthBinary;
                        newReflexivLong |= (1L<<2*currentPrefixLength); // add C marker in the front
                    }

                    reflexivKmerConcatList.add(
                            new Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>(newReflexivSubKmer,
                                    new Tuple4<Integer, Long, Integer, Integer>(
                                            randomReflexivMarker, newReflexivLong, currentSubKmer._2._3(), currentSubKmer._2._4()
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
        public void directKmerComparison(Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>> currentSubKmer){
            tmpReflexivKmerExtendList.add(currentSubKmer);
        }

        /**
         *
         * @param forwardSubKmer
         * @param reflexedSubKmer
         */
         public void reflexivExtend(Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>> forwardSubKmer, Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>> reflexedSubKmer, int bubbleDistance) {

             /* forward   ATCGATCG, 1, ------ */
             /* reflexed  ------, 2, ATCGATCG */

             int forwardSuffixLength = Long.SIZE/2 - (Long.numberOfLeadingZeros(forwardSubKmer._2._2())/2 + 1);
             int reflexedPrefixLength = Long.SIZE/2 - (Long.numberOfLeadingZeros(reflexedSubKmer._2._2())/2 + 1);
             long maxSuffixLengthBinary = ~((~0L) << 2*forwardSuffixLength);
             long maxPrefixLengthBinary = ~((~0L) << 2*reflexedPrefixLength);


             if (randomReflexivMarker == 2) {
                 Long newReflexivSubKmer=0L;
                 Long newReflexivLong=0L;

                 if (forwardSuffixLength > param.subKmerSize) {
                     System.out.println("what? not possible. Tell the author to check his program. He knows");
                 } else if (forwardSuffixLength == param.subKmerSize) {
                     System.out.println("what? not possible. Tell the author to check his program. He knows");
                 } else { /* forwardSuffixLength < param.subKmerSize */
                     newReflexivSubKmer = forwardSubKmer._1 << (2*forwardSuffixLength);
                     newReflexivSubKmer &= maxSubKmerBinary;
                     newReflexivSubKmer |= (forwardSubKmer._2._2() & maxSuffixLengthBinary);

                     newReflexivLong = reflexedSubKmer._2._2() << (2*forwardSuffixLength); // do not remove the C marker as it will be used again
                     newReflexivLong |= (forwardSubKmer._1 >>> (2*(param.subKmerSize - forwardSuffixLength))); // do not have to add the C marker
                 }

                 if (bubbleDistance <0) {
                     reflexivKmerConcatList.add(
                             new Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>(newReflexivSubKmer,
                                     new Tuple4<Integer, Long, Integer, Integer>(
                                             randomReflexivMarker, newReflexivLong, reflexedSubKmer._2._3(), forwardSubKmer._2._4()
                                     )
                             )
                     );
                 }else {
                     if (forwardSubKmer._2._3() > 0) {
                         reflexivKmerConcatList.add(
                                 new Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>(newReflexivSubKmer,
                                         new Tuple4<Integer, Long, Integer, Integer>(
                                                 randomReflexivMarker, newReflexivLong, bubbleDistance, forwardSubKmer._2._4()
                                         )
                                 )
                         );
                     }else{
                         reflexivKmerConcatList.add(
                                 new Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>(newReflexivSubKmer,
                                         new Tuple4<Integer, Long, Integer, Integer>(
                                                 randomReflexivMarker, newReflexivLong, reflexedSubKmer._2._3(), bubbleDistance
                                         )
                                 )
                         );
                     }
                 }

                 randomReflexivMarker = 1; /* an action of randomization */
             }else { /* randomReflexivMarker == 1 */
                 Long newForwardSubKmer=0L;
                 Long newForwardLong=0L;

                 if (reflexedPrefixLength > param.subKmerSize) {
                     System.out.println("what? not possible. Tell the author to check his program. He knows");
                 } else if (reflexedPrefixLength == param.subKmerSize) {
                     System.out.println("what? not possible. Tell the author to check his program. He knows");
                 } else { /* reflexedPreffixLength < param.subKmerSize */
                     newForwardSubKmer = (reflexedSubKmer._2._2() & maxPrefixLengthBinary) << (2*(param.subKmerSize - reflexedPrefixLength));
                     newForwardSubKmer |= reflexedSubKmer._1 >>> 2*reflexedPrefixLength;

                     newForwardLong = reflexedSubKmer._1 & maxPrefixLengthBinary;
                     newForwardLong |= (1L << (2*reflexedPrefixLength)); // add the C marker
                     newForwardLong <<= (2*forwardSuffixLength);
                     newForwardLong |= (forwardSubKmer._2._2() & maxSuffixLengthBinary);
                 }

                 if (bubbleDistance <0) {
                     reflexivKmerConcatList.add(
                             new Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>(newForwardSubKmer,
                                     new Tuple4<Integer, Long, Integer, Integer>(
                                             randomReflexivMarker, newForwardLong, reflexedSubKmer._2._3(), forwardSubKmer._2._4()
                                     )
                             )
                     );
                 }else {
                     if (forwardSubKmer._2._3() > 0) {
                         reflexivKmerConcatList.add(
                                 new Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>(newForwardSubKmer,
                                         new Tuple4<Integer, Long, Integer, Integer>(
                                                 randomReflexivMarker, newForwardLong, bubbleDistance, forwardSubKmer._2._4()
                                         )
                                 )
                         );
                     }else{ // reflexedSubKmer._2._4() >0
                         reflexivKmerConcatList.add(
                                 new Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>(newForwardSubKmer,
                                         new Tuple4<Integer, Long, Integer, Integer>(
                                                 randomReflexivMarker, newForwardLong, reflexedSubKmer._2._3(), bubbleDistance
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
        public void resetSubKmerGroup(Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>> S) {
            if (lineMarker == 1) {
                lineMarker = 2;
            }else {
                lineMarker = 3; /* reset to new sub-kmer group */
            }
            /* re-reflex all single kmers in the sub-kmer group */
//            if (tmpReflexivKmerExtendList.size() != 0) {
//                for (int i = 0; i < tmpReflexivKmerExtendList.size(); i++) {
 //                   singleKmerRandomizer(tmpReflexivKmerExtendList.get(i));
 //               }
  //          }

            tmpReflexivKmerExtendList = new ArrayList<Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>>();
            tmpReflexivKmerExtendList.add(
                    new Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>(S._1,
                            new Tuple4<Integer, Long, Integer, Integer>(
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
    class FilterForkSubKmer implements PairFlatMapFunction<Iterator<Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>>, Long, Tuple4<Integer, Long, Integer, Integer>>, Serializable {
        List<Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>> HighCoverageSubKmer = new ArrayList<Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>>();
//        Tuple2<String, Tuple4<Integer, String, Integer, Integer>> HighCoverKmer=null;
//                new Tuple2<String, Tuple4<Integer, String, Integer, Integer>>("",
        //                       new Tuple4<Integer, String, Integer, Integer>(0, "", 0, 0));

        public Iterator<Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>> call(Iterator<Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>> s) {
            while (s.hasNext()) {
                Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>> subKmer = s.next();
                if (HighCoverageSubKmer.size() == 0) {
                    HighCoverageSubKmer.add(
                            new Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>(subKmer._1,
                                    new Tuple4<Integer, Long, Integer, Integer>(subKmer._2._1(), subKmer._2._2(), subKmer._2._3(), -1)
                            )
                    );
                } else {
                    if (subKmer._1.equals(HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1)._1)) {
                        if (subKmer._2._3().compareTo(HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1)._2._3()) > 0) {
                            HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                    new Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>(subKmer._1,
                                            new Tuple4<Integer, Long, Integer, Integer>(subKmer._2._1(), subKmer._2._2(), subKmer._2._3(), param.subKmerSize)
                                    )
                            );
                        } else if (subKmer._2._3().equals(HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1)._2._3())) {
                            if (subKmer._2._2().compareTo(HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1)._2._2()) > 0) {
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        new Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>(subKmer._1,
                                                new Tuple4<Integer, Long, Integer, Integer>(subKmer._2._1(), subKmer._2._2(), subKmer._2._3(), param.subKmerSize)
                                        )
                                );
                            } else {
                                subKmer = HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1);
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        new Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>(subKmer._1,
                                                new Tuple4<Integer, Long, Integer, Integer>(subKmer._2._1(), subKmer._2._2(), subKmer._2._3(), param.subKmerSize)
                                        )
                                );
                            }
                        } else {
                            subKmer = HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1);
                            HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                    new Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>(subKmer._1,
                                            new Tuple4<Integer, Long, Integer, Integer>(subKmer._2._1(), subKmer._2._2(), subKmer._2._3(), param.subKmerSize)
                                    )
                            );
                        }
                    } else {
                        HighCoverageSubKmer.add(
                                new Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>(subKmer._1,
                                        new Tuple4<Integer, Long, Integer, Integer>(subKmer._2._1(), subKmer._2._2(), subKmer._2._3(), -1)
                                )
                        );
                    }
                }
            }

            return HighCoverageSubKmer.iterator();
        }
    }

    class FilterForkReflectedSubKmer implements PairFlatMapFunction<Iterator<Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>>, Long, Tuple4<Integer, Long, Integer, Integer>>, Serializable{
        List<Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>> HighCoverageSubKmer = new ArrayList<Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>>();
//        Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>> HighCoverKmer=null;
//                new Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>("",
        //                       new Tuple4<Integer, Long, Integer, Integer>(0, "", 0, 0));

        public Iterator<Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>> call (Iterator<Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>> s){
            while (s.hasNext()){
                Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>> subKmer = s.next();
                if (HighCoverageSubKmer.size() == 0){
                    HighCoverageSubKmer.add(
                            new Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>(subKmer._1,
                                    new Tuple4<Integer, Long, Integer, Integer>(subKmer._2._1(), subKmer._2._2(), -1, subKmer._2._4())
                            )
                    );
                }else {
                    if (subKmer._1.equals(HighCoverageSubKmer.get(HighCoverageSubKmer.size()-1)._1)) {
                        if (subKmer._2._3().compareTo(HighCoverageSubKmer.get(HighCoverageSubKmer.size()-1)._2._3()) >0) {
                            HighCoverageSubKmer.set(HighCoverageSubKmer.size()-1,
                                    new Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>(subKmer._1,
                                            new Tuple4<Integer, Long, Integer, Integer>(subKmer._2._1(), subKmer._2._2(), param.subKmerSize, subKmer._2._4())
                                    )
                            );
                        } else if (subKmer._2._3().equals(HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1)._2._3())){
                            if (subKmer._2._2().compareTo(HighCoverageSubKmer.get(HighCoverageSubKmer.size()-1)._2._2()) >0){
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size()-1,
                                        new Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>(subKmer._1,
                                                new Tuple4<Integer, Long, Integer, Integer>(subKmer._2._1(), subKmer._2._2(), param.subKmerSize, subKmer._2._4())
                                        )
                                );
                            }else{
                                subKmer = HighCoverageSubKmer.get(HighCoverageSubKmer.size()-1); // re assign
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size()-1,
                                        new Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>(subKmer._1,
                                                new Tuple4<Integer, Long, Integer, Integer>(subKmer._2._1(), subKmer._2._2(), param.subKmerSize, subKmer._2._4())
                                        )
                                );
                            }
                        } else {
                            subKmer = HighCoverageSubKmer.get(HighCoverageSubKmer.size()-1);
                            HighCoverageSubKmer.set(HighCoverageSubKmer.size()-1,
                                    new Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>(subKmer._1,
                                            new Tuple4<Integer, Long, Integer, Integer>(subKmer._2._1(), subKmer._2._2(), param.subKmerSize, subKmer._2._4())
                                    )
                            );
                        }
                    }else{
                        HighCoverageSubKmer.add(
                                new Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>(subKmer._1,
                                        new Tuple4<Integer, Long, Integer, Integer>(subKmer._2._1(), subKmer._2._2(), -1, subKmer._2._4())
                                )
                        );
                    }
                }
            }

            return HighCoverageSubKmer.iterator();
        }
    }


    /**
     *
     */
    class ForwardSubKmerExtraction implements PairFlatMapFunction<Iterator<Tuple2<Long, Integer>>, Long, Tuple4<Integer, Long, Integer, Integer>>, Serializable {
        List<Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>> TupleList = new ArrayList<Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>>();
        Long suffixBinary;
        Long prefixBinary;
        Tuple2<Long, Integer> kmerTuple;

        public Iterator<Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>> call(Iterator<Tuple2<Long, Integer>> s) {

            while (s.hasNext()) {
                kmerTuple = s.next();
                /**
                 * normal Sub-kmer
                 *        Kmer      ATGCACGTTATG
                 *        Sub-Kmer  ATGCACGTTAT         marked as Integer 1 in Tuple2
                 *        Left      -----------G
                 */
                suffixBinary = kmerTuple._1 & 3L;
                prefixBinary = kmerTuple._1 >>> 2;

                TupleList.add(
                        new Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>(
                                prefixBinary, new Tuple4<Integer, Long, Integer, Integer>(1, suffixBinary, kmerTuple._2, kmerTuple._2)
                        )
                );
            }

            return TupleList.iterator();
        }
    }


    class ReflectedSubKmerExtractionFromForward implements PairFlatMapFunction<Iterator<Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>>, Long, Tuple4<Integer, Long, Integer, Integer>>, Serializable {
        List<Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>> TupleList = new ArrayList<Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>>();
        Long suffixBinary;
        Long prefixBinary;
        Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>> kmerTuple;
        int shift =(2*(param.subKmerSize-1));
        Long maxSubKmerBinary = ~((~0L)<<2*param.subKmerSize);

        public Iterator<Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>> call(Iterator<Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>> s) {

            while (s.hasNext()) {
                kmerTuple = s.next();
                /**
                 * reflected Sub-kmer
                 *        Kmer      ATGCACGTTATG
                 *        Sub-Kmer  ATGCACGTTAT         marked as Integer 1 in Tuple2
                 *        Left      -----------G
                 */
                suffixBinary = 3L << shift;
                suffixBinary = kmerTuple._1 & suffixBinary;
                suffixBinary >>>= shift;
                suffixBinary |=4L; // add C marker in the front 0100 = 4L

                prefixBinary = kmerTuple._1 <<2 & maxSubKmerBinary;
                prefixBinary |= kmerTuple._2._2();

                TupleList.add(
                        new Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>(
                                prefixBinary, new Tuple4<Integer, Long, Integer, Integer>(2, suffixBinary, kmerTuple._2._3(), kmerTuple._2._4())
                        )
                );
            }

            return TupleList.iterator();
        }
    }

    /**
     *
     */
    class kmerRandomReflection implements PairFlatMapFunction<Iterator<Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>>, Long, Tuple4<Integer, Long, Integer, Integer>>, Serializable{
        /* 0 stands for forward sub-kmer */
        /* 1 stands for reflexiv sub-kmer */
        private int randomReflexivMarker = 2;

        List<Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>> reflexivKmerConcatList = new ArrayList<Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>>();
        Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>> kmerTuple;
        long maxSubKmerBinary = ~((~0L) << 2*param.subKmerSize);

        public Iterator<Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>> call (Iterator<Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>> s){
            while (s.hasNext()) {
                kmerTuple = s.next();

                singleKmerRandomizer(kmerTuple);
            }
            return reflexivKmerConcatList.iterator();
        }

        public void singleKmerRandomizer(Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>> currentSubKmer){

            if (currentSubKmer._2._1() == 1){
                /**
                 * 00000000000000110010111010010   Long.SIZE
                 * --------------C-G-G-G-T-C-A-G   Long.SIZE - (Long.numberOfLeadingZeros / 2 + 1)
                 * --------------^-Length marker
                 */
                int currentSuffixLength = Long.SIZE/2 - (Long.numberOfLeadingZeros(currentSubKmer._2._2())/2 + 1);
                long maxSuffixLengthBinary = ~(~0L << 2*currentSuffixLength);
                Long newReflexivSubKmer;
                Long newReflexivLong;

                if (randomReflexivMarker == 2) {
                    if (currentSuffixLength > param.subKmerSize) { // not possible in the first five (include initial) rounds
                        newReflexivSubKmer = currentSubKmer._1 << currentSuffixLength*2 & maxSubKmerBinary;
                        newReflexivSubKmer |= (currentSubKmer._2._2() & maxSuffixLengthBinary);

                        newReflexivLong = currentSubKmer._1 >>> 2*currentSuffixLength;
                        newReflexivLong |= (1L<<2*currentSuffixLength); // add C marker in front
                        // not finished
                    }else if (currentSuffixLength == param.subKmerSize){ // not possible in the first five (include initial) rounds
                        newReflexivSubKmer = currentSubKmer._1 << currentSuffixLength*2 & maxSubKmerBinary;
                        newReflexivSubKmer |= (currentSubKmer._2._2() & maxSuffixLengthBinary);

                        newReflexivLong = currentSubKmer._1 >>> 2*currentSuffixLength;
                        newReflexivLong |= (1L<<2*currentSuffixLength); // add C marker in front
                        // not finished
                    }else{ // now this is possible in the first five
                        newReflexivSubKmer = currentSubKmer._1 << currentSuffixLength*2;
                        newReflexivSubKmer &= maxSubKmerBinary;
                        newReflexivSubKmer |= (currentSubKmer._2._2() & maxSuffixLengthBinary);

                        newReflexivLong = currentSubKmer._1 >>> 2*(param.subKmerSize - currentSuffixLength);
                        newReflexivLong |= (1L<<2*currentSuffixLength); // add C marker in the front
                    }

                    reflexivKmerConcatList.add(
                            new Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>(newReflexivSubKmer,
                                    new Tuple4<Integer, Long, Integer, Integer>(
                                            randomReflexivMarker, newReflexivLong, currentSubKmer._2._3(), currentSubKmer._2._4()
                                    )
                            )
                    );
                }else{
                    reflexivKmerConcatList.add(currentSubKmer);
                }
            }else{ /* currentSubKmer._2._1() == 2 */
                int currentPrefixLength = Long.SIZE/2 - (Long.numberOfLeadingZeros(currentSubKmer._2._2())/2 + 1);
                long maxSuffixLengthBinary = ~((~0L) << (2*currentPrefixLength));
                Long newReflexivSubKmer;
                Long newReflexivLong;

                if (randomReflexivMarker == 2) {
                    reflexivKmerConcatList.add(currentSubKmer);
                }else{ /* randomReflexivMarker == 1 */
                    if (currentPrefixLength > param.subKmerSize){ //
                        newReflexivSubKmer = currentSubKmer._2._2() << 2*(param.subKmerSize - currentPrefixLength);

                        newReflexivLong = currentSubKmer._1 & maxSuffixLengthBinary;
                        newReflexivLong |= (1L<<2*currentPrefixLength); // add C marker in the front
                    }else if (currentPrefixLength == param.subKmerSize){ //
                        newReflexivSubKmer = currentSubKmer._2._2() << 2*(param.subKmerSize - currentPrefixLength);

                        newReflexivLong = currentSubKmer._1 & maxSuffixLengthBinary;
                        newReflexivLong |= (1L<<2*currentPrefixLength); // add C marker in the front
                    }else{ /* currentPreffixLength < param.subKmerSize */
                        newReflexivSubKmer = (currentSubKmer._2._2() & maxSuffixLengthBinary) << (2*(param.subKmerSize - currentPrefixLength));;
                        //newReflexivSubKmer <<= (2*(param.subKmerSize - currentPrefixLength));
                        newReflexivSubKmer |=(currentSubKmer._1 >>> (2*currentPrefixLength));

                        newReflexivLong = currentSubKmer._1 & maxSuffixLengthBinary;
                        newReflexivLong |= (1L<<2*currentPrefixLength); // add C marker in the front
                    }

                    reflexivKmerConcatList.add(
                            new Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>(newReflexivSubKmer,
                                    new Tuple4<Integer, Long, Integer, Integer>(
                                            randomReflexivMarker, newReflexivLong, currentSubKmer._2._3(), currentSubKmer._2._4()
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
    }

    /**
     * interface class for RDD implementation, used in step 5
     */
    class SubKmerExtraction implements PairFlatMapFunction<Tuple2<String, Integer>, String, Tuple4<Integer, String, Integer, Integer>>, Serializable{
        /* 0 stands for forward sub-kmer */
        /* 1 stands for reflexiv sub-kmer */
        private int randomReflexivMarker = 2;

        public Iterator<Tuple2<String, Tuple4<Integer, String, Integer, Integer>>> call (Tuple2<String, Integer> s){

            List<Tuple2<String, Tuple4<Integer, String, Integer, Integer>>> subKmerList = new ArrayList<Tuple2<String, Tuple4<Integer, String, Integer, Integer>>>();
            int stringLength = s._1.length();
            /**
             * normal Sub-kmer
             *        Kmer      ATGCACGTTATG
             *        Sub-Kmer  ATGCACGTTAT         marked as Integer 1 in Tuple2
             *        Left      -----------G
             */
            if (randomReflexivMarker == 1) {
                String kmerPrefix = s._1.substring(0, param.subKmerSize);
                String stringSuffix = s._1.substring(param.subKmerSize, stringLength);
                subKmerList.add(
                        new Tuple2<String, Tuple4<Integer, String, Integer, Integer>>(
                                kmerPrefix, new Tuple4<Integer, String, Integer, Integer>(1, stringSuffix, s._2, s._2)
                        )
                );
                randomReflexivMarker = 2;
            }

            /**
             * reflexiv Sub-kmer
             *          Kmer        ATGCACGTTATG
             * reflexiv Sub-kmer     TGCACGTTATG      marked as Integer 2 in Tuple2
             *          Left        A-----------
             */
            else{ // (randomReflexivMarker == 2) {
                String kmerSuffix = s._1.substring(stringLength - param.subKmerSize, stringLength);
                String stringPrefix = s._1.substring(0, stringLength - param.subKmerSize);
                subKmerList.add(
                        new Tuple2<String, Tuple4<Integer, String, Integer, Integer>>(
                                kmerSuffix, new Tuple4<Integer, String, Integer, Integer>(2, stringPrefix, s._2, s._2)
                        )
                );
                randomReflexivMarker = 1;
            }

            return subKmerList.iterator();
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
    class KmerExtraction implements PairFlatMapFunction<String, String, Integer>, Serializable{
        public Iterator<Tuple2<String, Integer>> call(String s){

            /* a capsule for all Kmers */
            List<Tuple2<String, Integer>> kmerList = new ArrayList<Tuple2<String, Integer>>();

            String[] units = s.split("\\n");
            String read = units[1];
            int readlength = read.length();

            if (readlength- param.kmerSize - param.endClip <= 1 || param.frontClip > readlength - param.kmerSize){
                return kmerList.iterator();
            }

            for (int i=param.frontClip ; i<readlength- param.kmerSize - param.endClip; i++){
                String kmer = read.substring(i, param.kmerSize + i);
                Tuple2<String, Integer> kmerTuple = new Tuple2<String, Integer>(kmer, 1);
                kmerList.add(kmerTuple);
            }

            return kmerList.iterator();
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

    class ReverseComplementKmerExtraction implements PairFlatMapFunction<String, String, Integer>, Serializable{
        public Iterator<Tuple2<String, Integer>> call(String s){

            List<Tuple2<String, Integer>> kmerList = new ArrayList<Tuple2<String, Integer>>();

            String[] units = s.split("\\n");
            String read = units[1];
            int readlength = read.length();

            if (readlength - param.kmerSize - param.endClip <= 1 || param.frontClip > readlength){
                return kmerList.iterator();
            }

            for (int i = param.frontClip; i < readlength - param.kmerSize - param.endClip; i++){
                String Kmer = read.substring(i, param.kmerSize + i);
                String reverseKmer="";
                long forwardValue = 0;
                long reverseValue = 0;
                boolean forwardMarker = false;
                for (int j = 0 ; j < param.kmerSize ; j++) {
                    char nucleotide = Kmer.charAt(j);
                    char reverseComplement = complement(Kmer.charAt(param.kmerSize - 1 -j));
                    forwardValue += nucleotideValue(nucleotide, param.kmerSize - 1 - j);
                    reverseValue += nucleotideValue(reverseComplement, param.kmerSize - 1 - j);
                    if (forwardValue < reverseValue){
                        Tuple2<String, Integer> kmerTuple2 = new Tuple2<String, Integer>(Kmer, 1);
                        kmerList.add(kmerTuple2);
                        forwardMarker = true;
                        break;
                    } else { // forwardValue >= reverseValue
                        reverseKmer += reverseComplement;
                    }
                }

                if (!forwardMarker) {
                    kmerList.add(new Tuple2<String, Integer>(reverseKmer, 1)); // 10000 is a marker for reverse Complement Kmer
                }
            }

            return kmerList.iterator();
        }

        public char complement(char a){
            if (a == 'A') {
                return 'T';
            } else if (a == 'C') {
                return 'G';
            } else if (a == 'G') {
                return 'C';
            } else { // T
                return 'A';
            }
        }

        /**
         *
         * @param a
         * @param c
         * @return
         */
        public long nucleotideValue(char a, int c){
            long value;
            if (a == 'A') {
                value = 0;
            } else if (a == 'C') {
                value = 1;
            } else if (a == 'G') {
                value = 2;
            } else { // T
                value = 3;
            }

            value = value << (2*c);
            return value;
        }
    }

    class ReverseComplementHashTableKmerExtraction implements PairFlatMapFunction<Iterator<String>, String, Integer>, Serializable{
        HashMap<String, Integer> kmerHash = new HashMap<String, Integer>();

        public Iterator<Tuple2<String, Integer>> call(Iterator<String> k){
            List<Tuple2<String, Integer>> kmerList = new ArrayList<Tuple2<String, Integer>>();

            while (k.hasNext()) {
                String s = k.next();

                String[] units = s.split("\\n");
                String read = units[1];
                int readlength = read.length();

                if (readlength - param.kmerSize - param.endClip <= 1 || param.frontClip > readlength) {
                    return kmerList.iterator();
                }

                for (int i = param.frontClip; i < readlength - param.kmerSize - param.endClip; i++) {
                    String Kmer = read.substring(i, param.kmerSize + i);
                    String reverseKmer = "";
                    long forwardValue = 0;
                    long reverseValue = 0;
                    boolean forwardMarker = false;
                    for (int j = 0; j < param.kmerSize; j++) {
                        char nucleotide = Kmer.charAt(j);
                        char reverseComplement = complement(Kmer.charAt(param.kmerSize - 1 - j));
                        forwardValue += nucleotideValue(nucleotide, param.kmerSize - 1 - j);
                        reverseValue += nucleotideValue(reverseComplement, param.kmerSize - 1 - j);
                        if (forwardValue < reverseValue) {
                            Tuple2<String, Integer> kmerTuple2 = new Tuple2<String, Integer>(Kmer, 1);
                            int oldValue;
                            if (kmerHash.containsKey(Kmer)){
                                oldValue = kmerHash.get(Kmer);
                            }else{
                                oldValue = 0;
                            }
                            kmerHash.put(Kmer, oldValue + 1);
                            forwardMarker = true;
                            break;
                        } else { // forwardValue >= reverseValue
                            reverseKmer += reverseComplement;
                        }
                    }

                    if (!forwardMarker) {
                        int oldReverseValue;
                        if (kmerHash.containsKey(reverseKmer)){
                            oldReverseValue = kmerHash.get(reverseKmer);
                        }else{
                            oldReverseValue = 0;
                        }

                        kmerHash.put(reverseKmer, oldReverseValue + 1); // 10000 is a marker for reverse Complement Kmer
                    }
                }
            }

            Set<String> keys = kmerHash.keySet();

            Iterator<String> itr = keys.iterator();

            String distinctKmer;

            while(itr.hasNext()){
                distinctKmer = itr.next();
                kmerList.add(new Tuple2<String, Integer>(distinctKmer, kmerHash.get(distinctKmer)));

            }
            kmerHash.clear();

            return kmerList.iterator();
        }

        public char complement(char a){
            if (a == 'A') {
                return 'T';
            } else if (a == 'C') {
                return 'G';
            } else if (a == 'G') {
                return 'C';
            } else { // T
                return 'A';
            }
        }

        /**
         *
         * @param a
         * @param c
         * @return
         */
        public long nucleotideValue(char a, int c){
            long value;
            if (a == 'A') {
                value = 0;
            } else if (a == 'C') {
                value = 1;
            } else if (a == 'G') {
                value = 2;
            } else { // T
                value = 3;
            }

            value = value << (2*c);
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

    class ExtendedKmerFilter implements Function<Tuple2<String, Tuple4<Integer, String, Integer, Integer>>, Boolean>, Serializable{
        public Boolean call(Tuple2<String, Tuple4<Integer, String, Integer, Integer>> s){
            return s != null;
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
