package uni.bielefeld.cmg.reflexiv.pipeline;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import uni.bielefeld.cmg.reflexiv.util.DefaultParam;
import uni.bielefeld.cmg.reflexiv.util.InfoDumper;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
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


public class ReflexivMain implements Serializable{
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

    /**
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

        /* Tuple4 data struct (reflexiv marker, rest of the string, coverage of prefix, coverage of suffix)*/
        JavaPairRDD<String, Tuple4<Integer, String, Integer, Integer>> ReflexivSubKmerRDD;

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
        KmerExtraction RDDExtractKmerFromFastq = new KmerExtraction();
        KmerRDD = FastqRDD.flatMapToPair(RDDExtractKmerFromFastq);

        /**
         * Step 4: filter kmers by coverage
         */
        KmerCoverageFilter RDDKmerFilter = new KmerCoverageFilter();
        KmerRDD = KmerRDD.filter(RDDKmerFilter);

        /**
         * Step 5: counting kmer frequencies with reduceByKey function
         */

        KmerCounting RDDCountingKmerFreq = new KmerCounting();
        KmerRDD = KmerRDD.reduceByKey(RDDCountingKmerFreq);

        /**
         * Step 6: extract sub-kmers from each K-mer
         */

        SubKmerExtraction RDDextractSubKmer = new SubKmerExtraction();
        ReflexivSubKmerRDD = KmerRDD.flatMapToPair(RDDextractSubKmer);


        /**
         * Step 7: sort all sub-kmers
         */

        ReflexivSubKmerRDD = ReflexivSubKmerRDD.sortByKey();

        /**
         * Step 8: connect and extend overlap kmers
         */
        ExtendReflexivKmer KmerExtention = new ExtendReflexivKmer();
        ReflexivSubKmerRDD = ReflexivSubKmerRDD.mapPartitionsToPair(KmerExtention);

        /**
         * Step 9: filter extended Kmers
         */
  //      ExtendedKmerFilter KmerExtendedFilter = new ExtendedKmerFilter();
     //   ReflexivSubKmerRDD = ReflexivSubKmerRDD.filter(KmerExtendedFilter);

        /**
         * Step 10: iteration: repeat step 6, 7 and 8 until convergence is reached
         */
        int partitionNumber = ReflexivSubKmerRDD.getNumPartitions();
        int iterations = 0;
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

                    if (partitionNumber >= 32) {
                        if (currentContigNumber / partitionNumber <= 40) {
                            ReflexivSubKmerRDD = ReflexivSubKmerRDD.coalesce(partitionNumber / 4 + 1);
                        }
                    }
                }
            }

            ReflexivSubKmerRDD = ReflexivSubKmerRDD.sortByKey();
 //           ReflexivSubKmerRDD.saveAsTextFile(param.outputPath + iterations);
            KmerExtention = new ExtendReflexivKmer();
            ReflexivSubKmerRDD = ReflexivSubKmerRDD.mapPartitionsToPair(KmerExtention);
 //           ReflexivSubKmerRDD = ReflexivSubKmerRDD.filter(KmerExtendedFilter);
        }

        /**
         * Step 11: change reflexiv kmers to contig
         */


        /**
         * Step N: save result
         */
        ReflexivSubKmerRDD.saveAsTextFile(param.outputPath);

      /*  KmerRDD = KmerRDD.sortByKey();
        KmerRDD.saveAsTextFile(param.outputPath);

*/
        /**
         * Step N+1: Stop
         */
        sc.stop();
    }

    /**
     *
     */
    class ExtendReflexivKmer implements PairFlatMapFunction<Iterator<Tuple2<String, Tuple4<Integer, String, Integer, Integer>>>, String, Tuple4<Integer, String, Integer, Integer>>, Serializable{

        /* marker to identify similar SubKmers in the loop sequence */
        private int lineMarker=1;

        /* 1 stands for forward sub-kmer */
        /* 2 stands for reflexiv sub-kmer */
        private int randomReflexivMarker = ThreadLocalRandom.current().nextInt(1, 3);

        /* temporary capsule to store identical SubKmer units */
        List<Tuple2<String, Tuple4<Integer, String, Integer, Integer>>> tmpReflexivKmerExtendList = new ArrayList<Tuple2<String, Tuple4<Integer, String, Integer, Integer>>>();

        /* return capsule of extend Tuples for next iteration*/
        List<Tuple2<String, Tuple4<Integer, String, Integer, Integer>>> reflexivKmerConcatList = new ArrayList<Tuple2<String, Tuple4<Integer, String, Integer, Integer>>>();

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
        public Iterator<Tuple2<String, Tuple4<Integer, String, Integer, Integer>>> call (Iterator<Tuple2<String, Tuple4<Integer, String, Integer, Integer>>> sIterator) {

            while (sIterator.hasNext()) {
                Tuple2<String, Tuple4<Integer, String, Integer, Integer>> s = sIterator.next();
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
              //      reflexivKmerConcatList = new ArrayList<Tuple2<String, Tuple4<Integer, String, Integer, Integer>>>();

                    if (tmpReflexivKmerExtendList.size() == 0) {
                        directKmerComparison(s);
                    } else { /* tmpReflexivKmerExtendList.size() != 0 */
                        for (int i = 0; i < tmpReflexivKmerExtendList.size(); i++) { // the tmpReflexivKmerExtendList is changing dynamically
                            if (s._1.equals(tmpReflexivKmerExtendList.get(i)._1)) {
                                if (s._2._1() == 1) {
                                    if (tmpReflexivKmerExtendList.get(i)._2._1() == 2) {
                                        reflexivExtend(s, tmpReflexivKmerExtendList.get(i));
                                        tmpReflexivKmerExtendList.remove(i); /* already extended */
                                        break;
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
                                        reflexivExtend(tmpReflexivKmerExtendList.get(i), s);
                                        tmpReflexivKmerExtendList.remove(i); /* already extended */
                                        break;
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
        public void singleKmerRandomizer(Tuple2<String, Tuple4<Integer, String, Integer, Integer>> currentSubKmer){

            if (currentSubKmer._2._1() == 1){
                int currentSuffixLength = currentSubKmer._2._2().length();
                if (randomReflexivMarker == 2) {
                    String newReflexivSubKmer;
                    String newReflexivString;

                    if (currentSuffixLength > param.subKmerSize) {
                        newReflexivSubKmer = currentSubKmer._2._2().substring(currentSuffixLength - param.subKmerSize, currentSuffixLength);
                        newReflexivString = currentSubKmer._1
                                + currentSubKmer._2._2().substring(0, currentSuffixLength - param.subKmerSize);

                    }else if (currentSuffixLength == param.subKmerSize){
                        newReflexivSubKmer = currentSubKmer._2._2();
                        newReflexivString = currentSubKmer._1;

                    }else{
                        newReflexivSubKmer = currentSubKmer._1.substring(currentSuffixLength, param.subKmerSize)
                                + currentSubKmer._2._2();
                        newReflexivString = currentSubKmer._1.substring(0, currentSuffixLength);

                    }

                    reflexivKmerConcatList.add(
                            new Tuple2<String, Tuple4<Integer, String, Integer, Integer>>(newReflexivSubKmer,
                                    new Tuple4<Integer, String, Integer, Integer>(
                                            randomReflexivMarker, newReflexivString, currentSubKmer._2._3(), currentSubKmer._2._4()
                                    )
                            )
                    );
                }else{
                    reflexivKmerConcatList.add(currentSubKmer);
                }
            }else{ /* currentSubKmer._2._1() == 2 */
                int currentPreffixLength = currentSubKmer._2._2().length();
                if (randomReflexivMarker == 2) {
                    reflexivKmerConcatList.add(currentSubKmer);
                }else{ /* randomReflexivMarker == 1 */
                    String newReflexivSubKmer;
                    String newReflexivString;
                    if (currentPreffixLength > param.subKmerSize){
                        newReflexivSubKmer = currentSubKmer._2._2().substring(0, param.subKmerSize);
                        newReflexivString = currentSubKmer._2._2().substring(param.subKmerSize, currentPreffixLength)
                                + currentSubKmer._1;

                    }else if (currentPreffixLength == param.subKmerSize){
                        newReflexivSubKmer = currentSubKmer._2._2();
                        newReflexivString = currentSubKmer._1;

                    }else{ /* currentPreffixLength < param.subKmerSize */
                        newReflexivSubKmer = currentSubKmer._2._2()
                                + currentSubKmer._1.substring(0, param.subKmerSize - currentPreffixLength);
                        newReflexivString = currentSubKmer._1.substring(param.subKmerSize - currentPreffixLength, param.subKmerSize);
                    }

                    reflexivKmerConcatList.add(
                            new Tuple2<String, Tuple4<Integer, String, Integer, Integer>>(newReflexivSubKmer,
                                    new Tuple4<Integer, String, Integer, Integer>(
                                            randomReflexivMarker, newReflexivString, currentSubKmer._2._3(), currentSubKmer._2._4()
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
        public void directKmerComparison(Tuple2<String, Tuple4<Integer, String, Integer, Integer>> currentSubKmer){
            tmpReflexivKmerExtendList.add(currentSubKmer);
        }

        /**
         *
         * @param forwardSubKmer
         * @param reflexedSubKmer
         */
         public void reflexivExtend(Tuple2<String, Tuple4<Integer, String, Integer, Integer>> forwardSubKmer, Tuple2<String, Tuple4<Integer, String, Integer, Integer>> reflexedSubKmer) {

             /* forward   ATCGATCG, 1, ------ */
             /* reflexed  ------, 2, ATCGATCG */

             int forwardSuffixLength = forwardSubKmer._2._2().length();
             int reflexedPreffixLength = reflexedSubKmer._2._2().length();


             if (randomReflexivMarker == 2) {
                 String newReflexivSubKmer;
                 String newReflexivString;

                 if (forwardSuffixLength > param.subKmerSize) {
                     newReflexivSubKmer = forwardSubKmer._2._2().substring(forwardSuffixLength - param.subKmerSize, forwardSuffixLength);
                     newReflexivString = reflexedSubKmer._2._2()
                             + reflexedSubKmer._1 /* or forwardSubKmer._1 */
                             + forwardSubKmer._2._2().substring(0, forwardSuffixLength - param.subKmerSize);
                 } else if (forwardSuffixLength == param.subKmerSize) {
                     newReflexivSubKmer = forwardSubKmer._2._2();
                     newReflexivString = reflexedSubKmer._2._2()
                             + reflexedSubKmer._1;
                 } else { /* forwardSuffixLength < param.subKmerSize */
                     newReflexivSubKmer = forwardSubKmer._1.substring(/* param.subKmerSize - param.subKmerSize + */ forwardSuffixLength, param.subKmerSize)
                             + forwardSubKmer._2._2();
                     newReflexivString = reflexedSubKmer._2._2()
                             + reflexedSubKmer._1.substring(0, forwardSuffixLength);
                 }

                 reflexivKmerConcatList.add(
                         new Tuple2<String, Tuple4<Integer, String, Integer, Integer>>(newReflexivSubKmer,
                                 new Tuple4<Integer, String, Integer, Integer>(
                                         randomReflexivMarker, newReflexivString, forwardSubKmer._2._3(), reflexedSubKmer._2._4()
                                 )
                         )
                 );

                 randomReflexivMarker = 1; /* an action of randomization */
             }else { /* randomReflexivMarker == 1 */
                 String newForwardSubKmer;
                 String newForwardString;

                 if (reflexedPreffixLength > param.subKmerSize) {
                     newForwardSubKmer = reflexedSubKmer._2._2().substring(0, param.subKmerSize);
                     newForwardString = reflexedSubKmer._2._2().substring(param.subKmerSize, reflexedPreffixLength)
                             + forwardSubKmer._1
                             + forwardSubKmer._2._2();
                 } else if (reflexedPreffixLength == param.subKmerSize) {
                     newForwardSubKmer = reflexedSubKmer._2._2();
                     newForwardString = forwardSubKmer._1
                             + forwardSubKmer._2._2();
                 } else { /* reflexedPreffixLength < param.subKmerSize */
                     newForwardSubKmer = reflexedSubKmer._2._2()
                             + reflexedSubKmer._1.substring(0, param.subKmerSize - reflexedPreffixLength);
                     newForwardString = reflexedSubKmer._1.substring(param.subKmerSize - reflexedPreffixLength, param.subKmerSize)
                             + forwardSubKmer._2._2();
                 }

                 reflexivKmerConcatList.add(
                         new Tuple2<String, Tuple4<Integer, String, Integer, Integer>>(newForwardSubKmer,
                                 new Tuple4<Integer, String, Integer, Integer>(
                                         randomReflexivMarker, newForwardString, forwardSubKmer._2._3(), reflexedSubKmer._2._4()
                                 )
                         )
                 );

                 randomReflexivMarker = 2;
             }

             /* add current sub kmer to temporal storage */
             // tmpReflexivKmerExtendList.add(reflexedSubKmer);
        }

        /**
         *
         * @param S
         */
        public void resetSubKmerGroup(Tuple2<String, Tuple4<Integer, String, Integer, Integer>> S) {
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

            tmpReflexivKmerExtendList = new ArrayList<Tuple2<String, Tuple4<Integer, String, Integer, Integer>>>();
            tmpReflexivKmerExtendList.add(
                    new Tuple2<String, Tuple4<Integer, String, Integer, Integer>>(S._1,
                            new Tuple4<Integer, String, Integer, Integer>(
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
            for (int i=0; i<readlength- param.kmerSize; i++){
                String kmer = read.substring(i, param.kmerSize + i);
                Tuple2<String, Integer> kmerTuple = new Tuple2<String, Integer>(kmer, 1);
                kmerList.add(kmerTuple);
            }

            return kmerList.iterator();
        }
    }
    /**
     * interface class for RDD implementation, Used in step 1
     */
    class FastqUnitFilter implements Function<String, Boolean>, Serializable{
        public Boolean call(String s){
            if (s != null) {
                return true;
            }
            else{
                return false;
            }
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

    class KmerCoverageFilter implements Function<Tuple2<String, Integer>, Boolean>, Serializable{
        public Boolean call(Tuple2<String, Integer> s){
            if (s._2 >= param.minKmerCoverage){
                return true;
            }else{
                return false;
            }
        }

    }

    class ExtendedKmerFilter implements Function<Tuple2<String, Tuple4<Integer, String, Integer, Integer>>, Boolean>, Serializable{
        public Boolean call(Tuple2<String, Tuple4<Integer, String, Integer, Integer>> s){
            if (s != null){
                return true;
            }else{
                return false;
            }
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
