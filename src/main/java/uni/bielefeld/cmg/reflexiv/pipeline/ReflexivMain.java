package uni.bielefeld.cmg.reflexiv.pipeline;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple3;
import uni.bielefeld.cmg.reflexiv.util.DefaultParam;
import uni.bielefeld.cmg.reflexiv.util.InfoDumper;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


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
        JavaPairRDD<String, Tuple2<Integer, String>> ReflexivSubKmerRDD;

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

        /**
         * Step 3: extract kmers from sequencing reads and
         *          and build <kmer, count> tuples.
         */
        KmerExtraction RDDExtractKmerFromFastq = new KmerExtraction();
        KmerRDD = FastqRDD.flatMapToPair(RDDExtractKmerFromFastq);
        KmerRDD = KmerRDD.repartition(47);

        /**
         * Step 4: counting kmer frequencies with reduceByKey function
         */

        KmerCounting RDDCountingKmerFreq = new KmerCounting();
        KmerRDD = KmerRDD.reduceByKey(RDDCountingKmerFreq);

        /**
         * Step 5: extract sub-kmers from each K-mer
         */

        SubKmerExtraction RDDextractSubKmer = new SubKmerExtraction();
        ReflexivSubKmerRDD = KmerRDD.flatMapToPair(RDDextractSubKmer);


        /**
         * Step 6: sort all sub-kmers
         */

        ReflexivSubKmerRDD = ReflexivSubKmerRDD.sortByKey();

        /**
         * Step N: save result
         */

        ReflexivSubKmerRDD.saveAsTextFile(param.outputPath);

      /*  KmerRDD = KmerRDD.sortByKey();
        KmerRDD.saveAsTextFile(param.outputPath);
*/
    }

    /**
     *
     */
    class ExtendReflexivKmer implements PairFlatMapFunction<Tuple2<String, Tuple2<Integer, String>>, String, Tuple2<Integer, String>>, Serializable{

        /* marker to identify similar SubKmers in the loop sequence */
        private int lineMarker=1;

        /* temporary capsule to store identical SubKmer units */
        List<Tuple3<String, Integer, String>> tmpReflexivKmerExtendList;

        /* return capsule of extend Tuples for next iteration*/
        List<Tuple2<String, Tuple2<Integer, String>>> reflexivKmerExtendList;

        /**
         *
         * @param s is the input data structure Tuple2<SubKmer, Tuple2<Marker, TheRestSequence>>
         * @return a list of extended Tuples for next iteration
         */
        public Iterator<Tuple2<String, Tuple2<Integer, String>>> call (Tuple2<String, Tuple2<Integer, String>> s){

            /* receive the first subkmer group, set new units */
            if (lineMarker == 1){
                resetSubKmerGroup(s);

                return null;
            }

            /* removal condition */
            else if (lineMarker == -1){
                if (s._2._1 == 4) {
                    reflexivKmerExtendList.add(s);
                    return reflexivKmerExtendList.iterator();
                } else if (s._1 == tmpReflexivKmerExtendList.get(0)._1()) { /* the same Sub-kmer, than kill */
                    return null;
                } else { /* not the sam Sub-kmer, than reset to new sub-kmer group */
                    resetSubKmerGroup(s);
                    return null;
                }
            }

            /* next element of RDD */
            else if (lineMarker >= 2){
                lineMarker++;
                /* initiate a new capsule for the current sub-kmer group */
                reflexivKmerExtendList = new ArrayList<Tuple2<String, Tuple2<Integer, String>>>();

                for (int i = 0; i < tmpReflexivKmerExtendList.size() ; i ++){
                    if (s._1 == tmpReflexivKmerExtendList.get(i)._1()){
                        if (s._2._1 ==1){
                            if (tmpReflexivKmerExtendList.get(i)._2() == 2){
                                reflexivExtend(tmpReflexivKmerExtendList.get(i), s);
                            }else if (tmpReflexivKmerExtendList.get(i)._2() == 1){
                                directKmerComparison(tmpReflexivKmerExtendList.get(i), s);
                            }
                        } else if (s._2._1 == 2){
                            if (tmpReflexivKmerExtendList.get(i)._2() == 2){
                                reflexivExtend(tmpReflexivKmerExtendList.get(i), s);
                            }else if (tmpReflexivKmerExtendList.get(i)._2() == 1){
                                directKmerComparison(tmpReflexivKmerExtendList.get(i), s);
                            }
                        } else if (s._2._1 == 3){
                            resetSubKmerGroup(s);

                            return null;
                        }
                        return reflexivKmerExtendList.iterator();
                    }

                    /* new Sub-kmer group section */
                    else if (s._1 != tmpReflexivKmerExtendList.get(i)._1() && lineMarker == 2){
                        resetSubKmerGroup(s);

                        /* return only the first sub kmer */
                        reflexivKmerExtendList.add(s);
                        return reflexivKmerExtendList.iterator();
                    }else{
                        resetSubKmerGroup(s);

                        /* new sub-kmer group found, return all extended sub-kmer */
                        return reflexivKmerExtendList.iterator();
                    }
                }
            }
            return null;
        }

        /**
         *
         * @param previousSubKmer
         * @param currentSubKmer
         */
        public void directKmerComparison(Tuple3<String, Integer, String> previousSubKmer, Tuple2<String, Tuple2<Integer, String>> currentSubKmer){

        }

        /**
         *
         * @param previousSubKmer
         * @param currentSubKmer
         */
        public void reflexivExtend(Tuple3<String, Integer, String> previousSubKmer, Tuple2<String, Tuple2<Integer, String>> currentSubKmer){

        }

        /**
         *
         * @param S
         */
        public void resetSubKmerGroup(Tuple2<String, Tuple2<Integer, String>> S) {
            lineMarker = 2; /* reset to new sub-kmer group */
            tmpReflexivKmerExtendList = new ArrayList<Tuple3<String, Integer, String>>();
            tmpReflexivKmerExtendList.add(new Tuple3<String, Integer, String>(S._1, S._2._1, S._2._2));
            if (S._2._1 == 3){  /* Killer Kmer is found */
                lineMarker = -1; /* into all removal condition */
            }
        }
    }

    /**
     * interface class for RDD implementation, used in step 5
     */
    class SubKmerExtraction implements PairFlatMapFunction<Tuple2<String, Integer>, String, Tuple2<Integer, String>>, Serializable{
        public Iterator<Tuple2<String, Tuple2<Integer, String>>> call (Tuple2<String, Integer> s){

            List<Tuple2<String, Tuple2<Integer, String>>> subKmerList = new ArrayList<Tuple2<String, Tuple2<Integer, String>>>();

            /**
             * normal Sub-kmer
             *        Kmer      ATGCACGTTATG
             *        Sub-Kmer  ATGCACGTTAT         marked as Integer 1 in Tuple2
             *        Left      -----------G
             */
            int stringLength = s._1.length();

            String kmerPrefix = s._1.substring(0, param.subKmerSize);
            String stringSuffix = s._1.substring(param.subKmerSize,stringLength);
            subKmerList.add(
                    new Tuple2<String, Tuple2<Integer, String>>(
                            kmerPrefix, new Tuple2<Integer, String>(1, stringSuffix)
                    )
            );

            /**
             * reflexiv Sub-kmer
             *          Kmer        ATGCACGTTATG
             * reflexiv Sub-kmer     TGCACGTTATG      marked as Integer 2 in Tuple2
             *          Left        A-----------
             */
            String kmerSuffix = s._1.substring(stringLength - param.subKmerSize, stringLength);
            String stringPrefix = s._1.substring(0, stringLength - param.subKmerSize);
            subKmerList.add(
                    new Tuple2<String, Tuple2<Integer, String>>(
                            kmerSuffix, new Tuple2<Integer, String>(2, stringPrefix)
                    )
            );

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
            if (s != null){
                return true;
            }else{
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

    /**
     *
     * @param param
     */
    public void setParam(DefaultParam param) {
        this.param = param;
    }
}
