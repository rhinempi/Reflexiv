package uni.bielefeld.cmg.reflexiv.pipeline;


import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.collection.Seq;
import uni.bielefeld.cmg.reflexiv.util.DefaultParam;
import uni.bielefeld.cmg.reflexiv.util.InfoDumper;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.*;

import static org.apache.spark.sql.functions.col;


/**
 * Created by rhinempi on 22.07.2017.
 * <p>
 * Reflexiv
 * <p>
 * Copyright (c) 2017.
 * Liren Huang     <huanglr at cebitec.uni-bielefeld.de>
 * <p>
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


/**
 * Returns an object for running the Reflexiv main pipeline.
 *
 * @author Liren Huang
 * @version %I%, %G%
 * @see
 */
public class ReflexivDSKmerLeftAndRightSorting implements Serializable {
    private long time;
    private DefaultParam param;

    private InfoDumper info = new InfoDumper();

    /**
     *
     * @return
     */
    private SparkConf setSparkConfiguration() {
        SparkConf conf = new SparkConf().setAppName("Reflexiv");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "uni.bielefeld.cmg.reflexiv.serializer.SparkKryoRegistrator");

        return conf;
    }

    private SparkSession setSparkSessionConfiguration(int shufflePartitions) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Reflexiv")
                .config("spark.kryo.registrator", "uni.bielefeld.cmg.reflexiv.serializer.SparkKryoRegistrator")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.cleaner.referenceTracking.cleanCheckpoints", true)
                .config("spark.checkpoint.compress",true)
                .config("spark.sql.shuffle.partitions", String.valueOf(shufflePartitions))
                .config("spark.sql.files.maxPartitionBytes", "12000000")
                .config("spark.sql.adaptive.advisoryPartitionSizeInBytes","12mb")
                .config("spark.driver.maxResultSize","1000g")
                .config("spark.memory.fraction","0.8")
                .config("spark.network.timeout","60000s")
                .config("spark.executor.heartbeatInterval","20000s")
                .getOrCreate();

        return spark;
    }

    /**
     *
     */
    public void assemblyFromKmer() {
        SparkSession spark = setSparkSessionConfiguration(param.shufflePartition);
        SparkContext sc = spark.sparkContext();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc);

        info.readMessage("Initiating Spark context ...");
        info.screenDump();
        info.readMessage("Start Spark framework: Reflexiv k-mer sorting: " + param.kmerSize);
        info.screenDump();

        sc.setCheckpointDir("/tmp/checkpoints");
        String checkpointDir= sc.getCheckpointDir().get();

        Dataset<Row> KmerCountDS;
        Dataset<Row> LongerKmerCountDS;

        Dataset<Row> KmerBinaryCountDS;
        Dataset<Row>LongerKmerBinaryCountDS;

        StructType kmerCountTupleStruct = new StructType();
        kmerCountTupleStruct = kmerCountTupleStruct.add("kmer", DataTypes.createArrayType(DataTypes.LongType), false);
        kmerCountTupleStruct = kmerCountTupleStruct.add("count", DataTypes.IntegerType, false);
        ExpressionEncoder<Row> KmerBinaryCountEncoder = RowEncoder.apply(kmerCountTupleStruct);


        Dataset<Row> ReflexivSubKmerDS;
        StructType ReflexivKmerStruct = new StructType();
        ReflexivKmerStruct = ReflexivKmerStruct.add("k-1", DataTypes.createArrayType(DataTypes.LongType), false);
        ReflexivKmerStruct = ReflexivKmerStruct.add("reflection", DataTypes.IntegerType, false);
        ReflexivKmerStruct = ReflexivKmerStruct.add("extension", DataTypes.LongType, false);
        ReflexivKmerStruct = ReflexivKmerStruct.add("left", DataTypes.IntegerType, false);
        ReflexivKmerStruct = ReflexivKmerStruct.add("right", DataTypes.IntegerType, false);
        ExpressionEncoder<Row> ReflexivSubKmerEncoder = RowEncoder.apply(ReflexivKmerStruct);

        Dataset<Row> ReflexivSubKmerDSCompressed;
        StructType ReflexivKmerStructCompressedStruct = new StructType();
        ReflexivKmerStructCompressedStruct = ReflexivKmerStructCompressedStruct.add("k-1", DataTypes.createArrayType(DataTypes.LongType), false);
        ReflexivKmerStructCompressedStruct = ReflexivKmerStructCompressedStruct.add("reflection", DataTypes.LongType, false);
        ReflexivKmerStructCompressedStruct = ReflexivKmerStructCompressedStruct.add("extension", DataTypes.LongType, false);
        ExpressionEncoder<Row> ReflexivSubKmerCompressedEncoder= RowEncoder.apply(ReflexivKmerStructCompressedStruct);

        Dataset<Row> ReflexivFullKmerDS;
        Dataset<Row> MixedFullKmerDS;
        Dataset<Row> MixedReflexivSubkmerDS;
        StructType FullKmerWithAttributeStruct = new StructType();
        FullKmerWithAttributeStruct = FullKmerWithAttributeStruct.add("k", DataTypes.createArrayType(DataTypes.LongType), false);
        FullKmerWithAttributeStruct = FullKmerWithAttributeStruct.add("reflection", DataTypes.LongType, false);
        ExpressionEncoder<Row> ReflexivFullKmerEncoder= RowEncoder.apply(FullKmerWithAttributeStruct);


        Dataset<Row> DSFullKmerStringShort;
        Dataset<Row> DSFullKmerString;
        StructType ReflexivFullKmerStringStruct = new StructType();
        ReflexivFullKmerStringStruct = ReflexivFullKmerStringStruct.add("k", DataTypes.StringType, false);
        ReflexivFullKmerStringStruct = ReflexivFullKmerStringStruct.add("reflection", DataTypes.StringType, false);
        ExpressionEncoder<Row> ReflexivFullKmerStringEncoder = RowEncoder.apply(ReflexivFullKmerStringStruct);





        /**
         * loading Kmer counts
         */
        KmerCountDS = spark.read().csv(param.inputKmerPath);

        if (param.partitions > 0) {
            KmerCountDS = KmerCountDS.repartition(param.partitions);
        }

        DynamicKmerBinarizer DSBinarizer = new DynamicKmerBinarizer();
        DSKmerReverseComplement DSRCKmer = new DSKmerReverseComplement();
        DSForwardSubKmerExtraction DSextractForwardSubKmer = new DSForwardSubKmerExtraction();
        DSSubKmerToFullKmer DSSubKmerToFullLengthKmer = new DSSubKmerToFullKmer();



        KmerBinaryCountDS = KmerCountDS.mapPartitions(DSBinarizer, KmerBinaryCountEncoder);
        
        KmerBinaryCountDS = KmerBinaryCountDS.filter(col("count")
                .geq(param.minKmerCoverage)
                .and(col("count")
                        .leq(param.maxKmerCoverage)
                )
        );

     //   if (param.partitions > 0) {
     //       LongerKmerBinaryCountDS = LongerKmerBinaryCountDS.repartition(param.partitions);
     //   }


        if (param.cache) {
            KmerBinaryCountDS.cache();
        }

        KmerBinaryCountDS = KmerBinaryCountDS.mapPartitions(DSRCKmer, KmerBinaryCountEncoder);

//        KmerBinaryCountDS.show();

        ReflexivSubKmerDS = KmerBinaryCountDS.mapPartitions(DSextractForwardSubKmer, ReflexivSubKmerCompressedEncoder);

//        ReflexivSubKmerDS.show();

        if (param.bubble == true) {
            ReflexivSubKmerDS = ReflexivSubKmerDS.sort("k-1");
            if (param.minErrorCoverage == 0) {
                DSFilterForkSubKmer DShighCoverageSelector = new DSFilterForkSubKmer();
                ReflexivSubKmerDS = ReflexivSubKmerDS.mapPartitions(DShighCoverageSelector, ReflexivSubKmerEncoder);
            } else {
                DSFilterForkSubKmerWithErrorCorrection DShighCoverageErrorRemovalSelector = new DSFilterForkSubKmerWithErrorCorrection();
                ReflexivSubKmerDS = ReflexivSubKmerDS.mapPartitions(DShighCoverageErrorRemovalSelector, ReflexivSubKmerCompressedEncoder);
            }

            DSReflectedSubKmerExtractionFromForward DSreflectionExtractor = new DSReflectedSubKmerExtractionFromForward();
            ReflexivSubKmerDS = ReflexivSubKmerDS.mapPartitions(DSreflectionExtractor, ReflexivSubKmerCompressedEncoder);

            ReflexivSubKmerDS = ReflexivSubKmerDS.sort("k-1");
            if (param.minErrorCoverage == 0) {
                DSFilterForkReflectedSubKmer DShighCoverageReflectedSelector = new DSFilterForkReflectedSubKmer();
                ReflexivSubKmerDS = ReflexivSubKmerDS.mapPartitions(DShighCoverageReflectedSelector, ReflexivSubKmerEncoder);
            } else {
                DSFilterForkReflectedSubKmerWithErrorCorrection DShighCoverageReflectedErrorRemovalSelector = new DSFilterForkReflectedSubKmerWithErrorCorrection();
                ReflexivSubKmerDS = ReflexivSubKmerDS.mapPartitions(DShighCoverageReflectedErrorRemovalSelector, ReflexivSubKmerCompressedEncoder);
            }
        }

        /**
         *
         */

        ReflexivFullKmerDS= ReflexivSubKmerDS.mapPartitions(DSSubKmerToFullLengthKmer, ReflexivFullKmerEncoder);
/*
        LongerKmerToEnglightenKmer LongerKmerEnlightmentPreparation = new LongerKmerToEnglightenKmer();
        ReflexivFullKmerDS =ReflexivSubKmerDS.mapPartitions(LongerKmerEnlightmentPreparation, ReflexivFullKmerEncoder);
*/

        DSBinaryFullKmerArrayToString FullKmerToStringLong = new DSBinaryFullKmerArrayToString();

        DSFullKmerString = ReflexivFullKmerDS.mapPartitions(FullKmerToStringLong, ReflexivFullKmerStringEncoder);

        if (param.gzip) {
            DSFullKmerString.write().
                    mode(SaveMode.Overwrite).
                    format("csv").
                    option("codec", "org.apache.hadoop.io.compress.GzipCodec").
                    save(param.outputPath + "/Count_" + param.kmerSize + "_sorted");
        }else {
            DSFullKmerString.write().
                    mode(SaveMode.Overwrite).
                    format("csv").
                    save(param.outputPath + "/Count_" + param.kmerSize + "_sorted");

        }

        spark.stop();
    }


    class TagContigID implements FlatMapFunction<Tuple2<Tuple2<String, String>, Long>, String>, Serializable {

        public Iterator<String> call(Tuple2<Tuple2<String, String>, Long> s) {


            List<String> contigList = new ArrayList<String>();

            contigList.add(s._1._1 + "-" + s._2 + "\n" + s._1._2);

            return contigList.iterator();
        }
    }

    class DSBinaryFullKmerArrayToString implements MapPartitionsFunction<Row, Row>, Serializable {
        List<Row> reflexivKmerStringList = new ArrayList<Row>();

        public Iterator<Row> call(Iterator<Row> sIterator) {
        //    Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        //    System.out.println(timestamp + "RepeatCheck DSBinaryFullKmerArrayToStringLong: " + param.kmerSize1);

            while (sIterator.hasNext()) {
                Row s = sIterator.next();

                int kmerLength= currentKmerSizeFromBinaryBlockArray(
                        (long[])s.get(0)
                );

                if( kmerLength== param.kmerSize) {

                    String kmerString = BinaryBlocksToString(
                            (long[])s.get(0)
                    );

                    String attributeString = getReflexivMarker(s.getLong(1))+"|"+getLeftMarker(s.getLong(1))+ "|"+getRightMarker(s.getLong(1));
                    reflexivKmerStringList.add(
                            RowFactory.create(kmerString, attributeString
                            )
                    );
                } // else not return

            }
            return reflexivKmerStringList.iterator();
        }

        private String BinaryBlocksToString (long[] binaryBlocks){
            //           String KmerString="";
            int KmerLength = currentKmerSizeFromBinaryBlockArray(binaryBlocks);
            StringBuilder sb= new StringBuilder();
            char currentNucleotide;

            for (int i=0; i< KmerLength; i++){
                Long currentNucleotideBinary = binaryBlocks[i/31] >>> 2 * (32 - (i%31+1));
                currentNucleotideBinary &= 3L;
                currentNucleotide = BinaryToNucleotide(currentNucleotideBinary);
                sb.append(currentNucleotide);
            }

            return sb.toString();
        }

        private long[] seq2array(Seq a){
            long[] array =new long[a.length()];
            for (int i = 0; i < a.length(); i++) {
                array[i] = (Long) a.apply(i);
            }
            return array;
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
        private int currentKmerSizeFromBinaryBlockArray(long[] binaryBlocks){
            int kmerSize;
            int blockSize = binaryBlocks.length;
            kmerSize= (blockSize-1) *31;
            final int suffix0s = Long.numberOfTrailingZeros(binaryBlocks[blockSize - 1]); // ATCG...01---
            int lastMers = Long.SIZE/2-suffix0s/2-1;

            kmerSize+=lastMers;
            return kmerSize;

        }

        private int getReflexivMarker(long attribute){
            int reflexivMarker = (int) (attribute >>> 2*(32-1)); // 01-------- -> ---------01 reflexiv marker
            return reflexivMarker;
        }

        private int getLeftMarker(long attribute){
            int leftMarker = (int) (attribute >>> 2*(16)); // 01--xxxx-----xxxx -> 01--xxxx shift out right marker
            int leftMarkerBinaryBits= ~(3 << 30) ; // ---------11 -> 11---------- -> 0011111111111
            leftMarker &= leftMarkerBinaryBits; // remove reflexivMarker

            if (leftMarker>30000){
                leftMarker=30000-leftMarker;
            }

            return leftMarker;
        }

        private int getRightMarker(long attribute){
            int rightMarker = (int) attribute;

            if (rightMarker>30000){
                rightMarker=30000-rightMarker;
            }

            return rightMarker;
        }

    }

    /**
     *
     */

    class DSFilterForkSubKmer implements MapPartitionsFunction<Row, Row>, Serializable {
        List<Row> HighCoverageSubKmer = new ArrayList<Row>();
//        Tuple2<String, Tuple4<Integer, String, Integer, Integer>> HighCoverKmer=null;
//                new Tuple2<String, Tuple4<Integer, String, Integer, Integer>>("",
        //                       new Tuple4<Integer, String, Integer, Integer>(0, "", 0, 0));

        public Iterator<Row> call(Iterator<Row> s) {
            while (s.hasNext()) {
                Row subKmer = s.next();
                if (HighCoverageSubKmer.size() == 0) {
                    HighCoverageSubKmer.add(
                            RowFactory.create(subKmer.getSeq(0), subKmer.getInt(1), subKmer.getLong(2), subKmer.getInt(3), -1)
                    );
                } else {
                    if (subKmerSlotComparator(subKmer.getSeq(0), HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1).getSeq(0)) == true) {
                        if (subKmer.getInt(3) > HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1).getInt(3)) {
                            HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                    RowFactory.create(subKmer.getSeq(0), subKmer.getInt(1), subKmer.getLong(2), subKmer.getInt(3), param.subKmerSize)
                            );
                        } else if (subKmer.getInt(3) == HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1).getInt(3)) {
                            if (subKmer.getLong(2) > HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1).getLong(2)) {
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        RowFactory.create(subKmer.getSeq(0), subKmer.getInt(1), subKmer.getLong(2), subKmer.getInt(3), param.subKmerSize)
                                );
                            } else {
                                /**
                                 * can be optimized
                                 */
                                subKmer = HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1);
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        RowFactory.create(subKmer.getSeq(0), subKmer.getInt(1), subKmer.getLong(2), subKmer.getInt(3), param.subKmerSize)
                                );
                            }
                        } else {
                            subKmer = HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1);
                            HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                    RowFactory.create(subKmer.getSeq(0), subKmer.getInt(1), subKmer.getLong(2), subKmer.getInt(3), param.subKmerSize)
                            );
                        }
                    } else {
                        HighCoverageSubKmer.add(
                                RowFactory.create(subKmer.getSeq(0), subKmer.getInt(1), subKmer.getLong(2), subKmer.getInt(3), -1)
                        );
                    }
                }
            }

            return HighCoverageSubKmer.iterator();
        }

        private boolean subKmerSlotComparator(Seq a, Seq b) {
            for (int i = 0; i < a.length(); i++) {
                if (!a.apply(i).equals(b.apply(i))) {
                    return false;
                }
            }

            return true;
        }
    }

    /**
     *  choose one kmer from a fork with higher coverage.
     */


    class DSFilterForkSubKmerWithErrorCorrection implements MapPartitionsFunction<Row, Row>, Serializable {
        List<Row> HighCoverageSubKmer = new ArrayList<Row>();
//        Tuple2<String, Tuple4<Integer, String, Integer, Integer>> HighCoverKmer=null;
//                new Tuple2<String, Tuple4<Integer, String, Integer, Integer>>("",
        //                       new Tuple4<Integer, String, Integer, Integer>(0, "", 0, 0));

        public Iterator<Row> call(Iterator<Row> s) {

            while (s.hasNext()) {
                Row subKmer = s.next();
                int reflexivMarker = getReflexivMarker(subKmer.getLong(1));
                int leftMarker = getLeftMarker(subKmer.getLong(1));
                int rightMarker = getRightMarker(subKmer.getLong(1));

                long[] subKmerArray = seq2array(subKmer.getSeq(0));
                long attribute=0;

                int currentSubKmerSize= currentKmerSizeFromBinaryBlockArray(subKmerArray);
                int maxKmerSize = param.kmerListInt[param.kmerListInt.length-1];

                if (HighCoverageSubKmer.size() == 0) {
                    attribute = buildingAlongFromThreeInt(reflexivMarker,leftMarker, -1);
                    HighCoverageSubKmer.add(
                            RowFactory.create(subKmer.getSeq(0), attribute, subKmer.getLong(2))
                    );
                } else {
                    int highestLeftMarker = getLeftMarker(HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1).getLong(1));
                    if (subKmerSlotComparator(subKmer.getSeq(0), HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1).getSeq(0)) == true) {
                        if (leftMarker > highestLeftMarker) {
                            if (highestLeftMarker <= param.minErrorCoverage && leftMarker >= param.minRepeatFold * highestLeftMarker) {
                                attribute = buildingAlongFromThreeInt(reflexivMarker, leftMarker, -1);
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        RowFactory.create(subKmer.getSeq(0), attribute, subKmer.getLong(2))
                                );
                            } else {
                                attribute = buildingAlongFromThreeInt(reflexivMarker, leftMarker, maxKmerSize+3);
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        RowFactory.create(subKmer.getSeq(0), attribute, subKmer.getLong(2))
                                );
                            }
                        } else if (leftMarker == highestLeftMarker) {
                            if (subKmer.getLong(2) > HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1).getLong(2)) {
                                attribute = buildingAlongFromThreeInt(reflexivMarker, leftMarker, maxKmerSize+3);
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        RowFactory.create(subKmer.getSeq(0), attribute, subKmer.getLong(2))
                                );
                            } else {
                                subKmer = HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1);
                                reflexivMarker=getReflexivMarker(subKmer.getLong(1));
                                leftMarker=getLeftMarker(subKmer.getLong(1));
                              //  rightMarker=getRightMarker(subKmer.getLong(1));
                                currentSubKmerSize=currentKmerSizeFromBinaryBlockArray(subKmerArray);
                                attribute = buildingAlongFromThreeInt(reflexivMarker,leftMarker,maxKmerSize+3);
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        RowFactory.create(subKmer.getSeq(0), attribute, subKmer.getLong(2))
                                );
                            }
                        } else {
                            if (leftMarker <= param.minErrorCoverage && highestLeftMarker >= param.minRepeatFold * leftMarker) {
                                subKmer = HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1);
                                reflexivMarker=getReflexivMarker(subKmer.getLong(1));
                                leftMarker=getLeftMarker(subKmer.getLong(1));
                                rightMarker=getRightMarker(subKmer.getLong(1));
                               // currentSubKmerSize=currentKmerSizeFromBinaryBlockArray((long[])subKmer.get(0));
                                attribute = buildingAlongFromThreeInt(reflexivMarker,leftMarker,-1);
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        RowFactory.create(subKmer.getSeq(0), attribute, subKmer.getLong(2))
                                );
                            } else {
                                subKmer = HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1);
                                reflexivMarker=getReflexivMarker(subKmer.getLong(1));
                                leftMarker=getLeftMarker(subKmer.getLong(1));
                              //  rightMarker=getRightMarker(subKmer.getLong(1));
                                currentSubKmerSize=currentKmerSizeFromBinaryBlockArray(subKmerArray);
                                attribute = buildingAlongFromThreeInt(reflexivMarker,leftMarker,maxKmerSize+3);
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        RowFactory.create(subKmer.getSeq(0), attribute, subKmer.getLong(2))
                                );
                            }
                        }
                    } else {
                        attribute = buildingAlongFromThreeInt(reflexivMarker, leftMarker, -1);
                        HighCoverageSubKmer.add(
                                RowFactory.create(subKmer.getSeq(0), attribute, subKmer.getLong(2))
                        );
                    }
                }

               // System.out.println("first leftMarker: " + leftMarker + " new leftMarker: " + getLeftMarker(attribute));
            }

            return HighCoverageSubKmer.iterator();
        }

        private boolean subKmerSlotComparator(Seq a, Seq b) {
            for (int i = 0; i < a.length(); i++) {
                if (!a.apply(i).equals(b.apply(i))) {
                    return false;
                }
            }

            return true;
        }

        private long[] seq2array(Seq a){
            long[] array =new long[a.length()];
            for (int i = 0; i < a.length(); i++) {
               array[i] = (Long) a.apply(i);
            }
            return array;
        }

        private int currentKmerSizeFromBinaryBlockArray(long[] binaryBlocks){
            int kmerSize;
            int blockSize = binaryBlocks.length;
            kmerSize= (blockSize-1) *31;
            final int suffix0s = Long.numberOfTrailingZeros(binaryBlocks[blockSize - 1]); // ATCG...01---
            int lastMers = Long.SIZE/2-suffix0s/2-1;

            kmerSize+=lastMers;
            return kmerSize;

        }

        private int getReflexivMarker(long attribute){
            int reflexivMarker = (int) (attribute >>> 2*(32-1)); // 01-------- -> ---------01 reflexiv marker
            return reflexivMarker;
        }

        private int getLeftMarker(long attribute){
            int leftMarker = (int) (attribute >>> 2*(16)); // 01--xxxx-----xxxx -> 01--xxxx shift out right marker
            int leftMarkerBinaryBits= ~(3 << 30) ; // ---------11 -> 11---------- -> 0011111111111
            leftMarker &= leftMarkerBinaryBits; // remove reflexivMarker

            if (leftMarker>30000){
                leftMarker=30000-leftMarker;
            }

            return leftMarker;
        }

        private int getRightMarker(long attribute){
            int rightMarker = (int) attribute;

            if (rightMarker>30000){
                rightMarker=30000-rightMarker;
            }

            return rightMarker;
        }

        private long buildingAlongFromThreeInt(int ReflexivMarker, int leftCover, int rightCover){
            long info = (long) ReflexivMarker <<2*(32-1);  //move to the left most

            /**
             * shorten the int and change negative to positive to avoid two's complementary
             */
            if (leftCover>=30000){
                leftCover=30000;
            }else if (leftCover<=-30000){
                leftCover=30000-(-30000);
            }else if (leftCover<0){
                leftCover=30000-leftCover;
            }

            if (rightCover>=30000){
                rightCover=30000;
            }else if (rightCover<=-30000){
                rightCover=30000-(-30000);
            }else if (rightCover<0){
                rightCover=30000-rightCover;
            }

            info |= ((long) leftCover << 32) ; // move one integer (32 bits) to the left
            info |= ((long) rightCover); //  01--LeftCover---RightCover

            return info;
        }

    }

    class DSFilterForkReflectedSubKmer implements MapPartitionsFunction<Row, Row>, Serializable {
        List<Row> HighCoverageSubKmer = new ArrayList<Row>();
        Integer HighCoverLastCoverage = 0;
//        Row HighCoverKmer=null;
//                new Row("",
        //                       new Tuple4<Integer, Long, Integer, Integer>(0, "", 0, 0));

        public Iterator<Row> call(Iterator<Row> s) {
            while (s.hasNext()) {
                Row subKmer = s.next();
                if (HighCoverageSubKmer.size() == 0) {
                    HighCoverLastCoverage = subKmer.getInt(3);
                    HighCoverageSubKmer.add(
                            RowFactory.create(subKmer.getSeq(0), subKmer.getInt(1), subKmer.getLong(2), -1, subKmer.getInt(4))
                    );
                } else {
                    if (subKmerSlotComparator(subKmer.getSeq(0), HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1).getSeq(0)) == true) {
                        if (subKmer.getInt(3) > HighCoverLastCoverage) {
                            HighCoverLastCoverage = subKmer.getInt(3);
                            HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                    RowFactory.create(subKmer.getSeq(0), subKmer.getInt(1), subKmer.getLong(2), param.subKmerSize, subKmer.getInt(4))
                            );
                        } else if (subKmer.getInt(3) == HighCoverLastCoverage) {
                            int subKmerFirstSuffixLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros(subKmer.getLong(2)) / 2 + 1);
                            int HighCoverageSubKmerFirstSuffixLength = Long.SIZE / 2 - ((Long.numberOfLeadingZeros(HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1).getLong(2))) / 2 + 1);
                            Long subKmerFirstSuffix = subKmer.getLong(2) >>> 2 * (subKmerFirstSuffixLength - 1);
                            Long HighCoverageSubKmerFirstSuffix = HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1).getLong(2) >>> 2 * (HighCoverageSubKmerFirstSuffixLength);

                            if (subKmerFirstSuffix.compareTo(HighCoverageSubKmerFirstSuffix) > 0) {
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        RowFactory.create(subKmer.getSeq(0), subKmer.getInt(1), subKmer.getLong(2), param.subKmerSize, subKmer.getInt(4))
                                );
                            } else {
                                subKmer = HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1); // re assign
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        RowFactory.create(subKmer.getSeq(0), subKmer.getInt(1), subKmer.getLong(2), param.subKmerSize, subKmer.getInt(4))
                                );
                            }
                        } else {
                            subKmer = HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1);
                            HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                    RowFactory.create(subKmer.getSeq(0), subKmer.getInt(1), subKmer.getLong(2), param.subKmerSize, subKmer.getInt(4))
                            );
                        }
                    } else {
                        HighCoverLastCoverage = subKmer.getInt(3);
                        HighCoverageSubKmer.add(
                                RowFactory.create(subKmer.getSeq(0), subKmer.getInt(1), subKmer.getLong(2), -1, subKmer.getInt(4))
                        );
                    }
                }
            }

            return HighCoverageSubKmer.iterator();
        }

        private boolean subKmerSlotComparator(Seq a, Seq b) {
            for (int i = 0; i < a.length(); i++) {
                if (!a.apply(i).equals(b.apply(i))) {
                    return false;
                }
            }

            return true;
        }
    }

    class DSFilterForkReflectedSubKmerWithErrorCorrection implements MapPartitionsFunction<Row, Row>, Serializable {
        List<Row> HighCoverageSubKmer = new ArrayList<Row>();
        Integer HighCoverLastCoverage = 0;
//        Row HighCoverKmer=null;
//                new Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>("",
        //                       new Tuple4<Integer, Long, Integer, Integer>(0, "", 0, 0));

        public Iterator<Row> call(Iterator<Row> s) {

            while (s.hasNext()) {
                Row subKmer = s.next();
                int reflexivMarker = getReflexivMarker(subKmer.getLong(1));
                int leftMarker = getLeftMarker(subKmer.getLong(1));
                int rightMarker = getRightMarker(subKmer.getLong(1));

                long[] subKmerArray = seq2array(subKmer.getSeq(0));
                long attribute=0;

                int currentSubKmerSize= currentKmerSizeFromBinaryBlockArray(subKmerArray);
                int maxKmerSize = param.kmerListInt[param.kmerListInt.length-1];


                if (HighCoverageSubKmer.size() == 0) {
                    HighCoverLastCoverage = leftMarker;
                    attribute = buildingAlongFromThreeInt(reflexivMarker,-1, rightMarker);
                    HighCoverageSubKmer.add(
                            RowFactory.create(subKmer.getSeq(0), attribute, subKmer.getLong(2))
                    );
                } else {
                    int highestLeftMarker = getLeftMarker(HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1).getLong(1));
                    if (subKmerSlotComparator(subKmer.getSeq(0), HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1).getSeq(0)) == true) {
                        if (leftMarker > HighCoverLastCoverage) {
                            if (HighCoverLastCoverage <= param.minErrorCoverage && leftMarker >= param.minRepeatFold * HighCoverLastCoverage) {
                                HighCoverLastCoverage = leftMarker;
                                attribute = buildingAlongFromThreeInt(reflexivMarker, -1, rightMarker);
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        RowFactory.create(subKmer.getSeq(0), attribute, subKmer.getLong(2))
                                );
                            } else {
                                HighCoverLastCoverage = leftMarker;
                                attribute = buildingAlongFromThreeInt(reflexivMarker, maxKmerSize+3, rightMarker);
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        RowFactory.create(subKmer.getSeq(0), attribute, subKmer.getLong(2))
                                );
                            }
                        } else if (leftMarker == HighCoverLastCoverage) {
                            int subKmerFirstSuffixLength = Long.SIZE / 2 - (Long.numberOfTrailingZeros(subKmer.getLong(2)) / 2 + 1);
                            int HighCoverageSubKmerFirstSuffixLength = Long.SIZE / 2 - ((Long.numberOfTrailingZeros(HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1).getLong(2))) / 2 + 1);
                            Long subKmerFirstSuffix = subKmer.getLong(2) >>> 2 * (32-subKmerFirstSuffixLength);
                            Long HighCoverageSubKmerFirstSuffix = HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1).getLong(2) >>> 2 * (32-HighCoverageSubKmerFirstSuffixLength);

                            if (subKmerFirstSuffix.compareTo(HighCoverageSubKmerFirstSuffix) > 0) {
                                attribute = buildingAlongFromThreeInt(reflexivMarker, maxKmerSize+3, rightMarker);
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        RowFactory.create(subKmer.getSeq(0),
                                                attribute, subKmer.getLong(2))
                                );
                            } else {
                                subKmer = HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1); // re assign
                                reflexivMarker=getReflexivMarker(subKmer.getLong(1));
                               // leftMarker=getLeftMarker(subKmer.getLong(1));
                                rightMarker=getRightMarker(subKmer.getLong(1));
                                currentSubKmerSize=currentKmerSizeFromBinaryBlockArray(subKmerArray);

                                attribute= buildingAlongFromThreeInt(reflexivMarker,maxKmerSize+3, rightMarker);
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        RowFactory.create(subKmer.getSeq(0),
                                                attribute, subKmer.getLong(2))
                                );
                            }
                        } else {
                            if (leftMarker <= param.minErrorCoverage && HighCoverLastCoverage >= param.minRepeatFold * leftMarker) {
                                subKmer = HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1);

                                reflexivMarker=getReflexivMarker(subKmer.getLong(1));
                                rightMarker=getRightMarker(subKmer.getLong(1));
                                attribute= buildingAlongFromThreeInt(reflexivMarker,-1, rightMarker);
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        RowFactory.create(subKmer.getSeq(0),
                                                attribute, subKmer.getLong(2))
                                );
                            } else {
                                subKmer = HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1);

                                reflexivMarker=getReflexivMarker(subKmer.getLong(1));
                                //leftMarker=getLeftMarker(subKmer.getLong(1));
                                rightMarker=getRightMarker(subKmer.getLong(1));
                                currentSubKmerSize=currentKmerSizeFromBinaryBlockArray(subKmerArray);

                                attribute = buildingAlongFromThreeInt(reflexivMarker, maxKmerSize+3, rightMarker);
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        RowFactory.create(subKmer.getSeq(0),
                                                attribute, subKmer.getLong(2))
                                );
                            }
                        }
                    } else {
                        HighCoverLastCoverage = leftMarker;
                        attribute = buildingAlongFromThreeInt(reflexivMarker,-1, rightMarker);

                        HighCoverageSubKmer.add(
                                RowFactory.create(subKmer.getSeq(0),
                                        attribute, subKmer.getLong(2))
                        );
                    }
                }

              //  System.out.println("second leftMarker: " + leftMarker + " new leftMarker: " + getLeftMarker(attribute));
            }

            return HighCoverageSubKmer.iterator();
        }

        private boolean subKmerSlotComparator(Seq a, Seq b) {
            for (int i = 0; i < a.length(); i++) {
                if (!a.apply(i).equals(b.apply(i))) {
                    return false;
                }
            }

            return true;
        }

        private long[] seq2array(Seq a){
            long[] array =new long[a.length()];
            for (int i = 0; i < a.length(); i++) {
                array[i] = (Long) a.apply(i);
            }
            return array;
        }

        private int currentKmerSizeFromBinaryBlockArray(long[] binaryBlocks){
            int kmerSize;
            int blockSize = binaryBlocks.length;
            kmerSize= (blockSize-1) *31;
            final int suffix0s = Long.numberOfTrailingZeros(binaryBlocks[blockSize - 1]); // ATCG...01---
            int lastMers = Long.SIZE/2-suffix0s/2-1;

            kmerSize+=lastMers;
            return kmerSize;

        }

        private int getReflexivMarker(long attribute){
            int reflexivMarker = (int) (attribute >>> 2*(32-1)); // 01-------- -> ---------01 reflexiv marker
            return reflexivMarker;
        }

        private int getLeftMarker(long attribute){
            int leftMarker = (int) (attribute >>> 2*(16)); // 01--xxxx-----xxxx -> 01--xxxx shift out right marker
            int leftMarkerBinaryBits= ~(3 << 30) ; // ---------11 -> 11---------- -> 0011111111111
            leftMarker &= leftMarkerBinaryBits; // remove reflexivMarker

            if (leftMarker>30000){
                leftMarker=30000-leftMarker;
            }

            return leftMarker;
        }

        private int getRightMarker(long attribute){
            int rightMarker = (int) attribute;

            if (rightMarker>30000){
                rightMarker=30000-rightMarker;
            }

            return rightMarker;
        }

        private long buildingAlongFromThreeInt(int ReflexivMarker, int leftCover, int rightCover){
            long info = (long) ReflexivMarker <<2*(32-1);  //move to the left most

            /**
             * shorten the int and change negative to positive to avoid two's complementary
             */
            if (leftCover>=30000){
                leftCover=30000;
            }else if (leftCover<=-30000){
                leftCover=30000-(-30000);
            }else if (leftCover<0){
                leftCover=30000-leftCover;
            }

            if (rightCover>=30000){
                rightCover=30000;
            }else if (rightCover<=-30000){
                rightCover=30000-(-30000);
            }else if (rightCover<0){
                rightCover=30000-rightCover;
            }

            info |= ((long) leftCover << 32) ; // move one integer (32 bits) to the left
            info |= ((long) rightCover); //  01--LeftCover---RightCover

            return info;
        }
    }

    class DSForwardSubKmerExtraction implements MapPartitionsFunction<Row, Row>, Serializable {
        List<Row> TupleList = new ArrayList<Row>();
        Long suffixBinary;
        long[] prefixBinarySlot;
        Row kmerTuple;
        int currentSubKmerSize;
        int currentSubKmerResidue;
        int currentSubKmerBlock;

        public Iterator<Row> call(Iterator<Row> s) {

            while (s.hasNext()) {
                kmerTuple = s.next();
                /**
                 * normal Sub-kmer
                 *        Kmer      ATGCACGTTATG
                 *        Sub-Kmer  ATGCACGTTAT         marked as Integer 1 in Tuple2
                 *        Left      -----------G
                 */
                currentSubKmerSize= currentKmerSizeFromBinaryBlockArray((long[])kmerTuple.get(0))-1; // current sub kmer = kmerTuple -1
                currentSubKmerResidue = (currentSubKmerSize-1)%31 +1;
                currentSubKmerBlock = (currentSubKmerSize-1)/31+1;


                if (currentSubKmerSize == 31) { // currentSubKmerBlock == previousSubKmerBlock -1
                    prefixBinarySlot = new long[currentSubKmerBlock];

                    suffixBinary = ((long[]) kmerTuple.get(0))[currentSubKmerBlock-1]; // last block XC---------- C marker keep it
                    for (int i = 0; i < currentSubKmerBlock; i++) {
                        prefixBinarySlot[i] = ((long[]) kmerTuple.get(0))[i];
                    }
                } else { // currentSubKmerBlock == previousSubKmerBlock
                    prefixBinarySlot = new long[currentSubKmerBlock];

                    suffixBinary = (((long[]) kmerTuple.get(0))[currentSubKmerBlock-1]
                            << (2*currentSubKmerResidue)); // include C marker


                    for (int i = 0; i < currentSubKmerBlock; i++) {
                        prefixBinarySlot[i] = ((long[]) kmerTuple.get(0))[i];
                    }

                    long currentSubKmerResidueBinary = ~0L<< 2*(32-currentSubKmerResidue);  // 1111111111------

                    prefixBinarySlot[currentSubKmerBlock - 1] = ((long[]) kmerTuple.get(0))[currentSubKmerBlock - 1] & currentSubKmerResidueBinary;
                    prefixBinarySlot[currentSubKmerBlock - 1] |= 1L <<2*(32-currentSubKmerResidue-1); // add C marker
                }

                long attribute = buildingAlongFromThreeInt(1, kmerTuple.getInt(1), kmerTuple.getInt(1));

               // System.out.println("Coverage: " + kmerTuple.getInt(1) + " before long: " + ((long[])kmerTuple.get(0))[0] + " after long: " + prefixBinarySlot[0]);

                long reflexivMarker = getReflexivMarker(attribute);
                long leftMarker=getLeftMarker(attribute);
                long rightMarker=getRightMarker(attribute);

               // System.out.println("leftMarker: " + leftMarker + " rightMarker: " + rightMarker + " reflexivMarker: " + reflexivMarker);


             //   String before = BinaryBlocksToString((long[])kmerTuple.get(0));
             //   String prefix = BinaryBlocksToString(prefixBinarySlot);
                long[] suffixBinaryArray = new long[1];
                suffixBinaryArray[0]= suffixBinary;
              //  String suffix = BinaryBlocksToString(suffixBinaryArray);

                // System.out.println("before forward extract: " + before + " prefix: " + prefix + " suffix: " + suffix);

                TupleList.add(
                        RowFactory.create(prefixBinarySlot, attribute, suffixBinary)
                );
            }

            return TupleList.iterator();
        }

        private long buildingAlongFromThreeInt(int ReflexivMarker, int leftCover, int rightCover){
            long info = (long) ReflexivMarker <<2*(32-1);  //move to the left most


            /**
             * shorten the int and change negative to positive to avoid two's complementary
             */
            if (leftCover>=30000){
                leftCover=30000;
            }else if (leftCover<=-30000){
                leftCover=30000-(-30000);
            }else if (leftCover<0){
                leftCover=30000-leftCover;
            }

            if (rightCover>=30000){
                rightCover=30000;
            }else if (rightCover<=-30000){
                rightCover=30000-(-30000);
            }else if (rightCover<0){
                rightCover=30000-rightCover;
            }

            info |= ((long) leftCover << 32) ; // move one integer (32 bits) to the left
            info |= ((long) rightCover); //  01--LeftCover---RightCover

            return info;
        }

        private int currentKmerSizeFromBinaryBlockArray(long[] binaryBlocks){
            int kmerSize;
            int blockSize = binaryBlocks.length;
            kmerSize= (blockSize-1) *31;
            final int suffix0s = Long.numberOfTrailingZeros(binaryBlocks[blockSize - 1]); // ATCG...01---
            int lastMers = Long.SIZE/2-suffix0s/2-1;

            kmerSize+=lastMers;
            return kmerSize;

        }


        private char BinaryToNucleotide(Long twoBits) {
            char nucleotide;
            if (twoBits == 0L) {
                nucleotide = 'A';
            } else if (twoBits == 1L) {
                nucleotide = 'C';
            } else if (twoBits == 2L) {
                nucleotide = 'G';
            } else {
                nucleotide = 'T';
            }
            return nucleotide;
        }
        private long[] seq2array(Seq a){
            long[] array =new long[a.length()];
            for (int i = 0; i < a.length(); i++) {
                array[i] = (Long) a.apply(i);
            }
            return array;
        }


        private String BinaryBlocksToString (long[] binaryBlocks){
            //           String KmerString="";
            int KmerLength = currentKmerSizeFromBinaryBlockArray(binaryBlocks);
            StringBuilder sb= new StringBuilder();
            char currentNucleotide;

            for (int i=0; i< KmerLength; i++){
                Long currentNucleotideBinary = binaryBlocks[i/31] >>> 2 * (32 - (i%31+1));
                currentNucleotideBinary &= 3L;
                currentNucleotide = BinaryToNucleotide(currentNucleotideBinary);
                sb.append(currentNucleotide);
            }

            return sb.toString();
        }

        private int getReflexivMarker(long attribute){
            int reflexivMarker = (int) (attribute >>> 2*(32-1)); // 01-------- -> ---------01 reflexiv marker
            return reflexivMarker;
        }

        private int getLeftMarker(long attribute){
            int leftMarker = (int) (attribute >>> 2*(16)); // 01--xxxx-----xxxx -> 01--xxxx shift out right marker
            int leftMarkerBinaryBits= ~(3 << 30) ; // ---------11 -> 11---------- -> 0011111111111
            leftMarker &= leftMarkerBinaryBits; // remove reflexivMarker

            if (leftMarker>30000){
                leftMarker=30000-leftMarker;
            }

            return leftMarker;
        }

        private int getRightMarker(long attribute){
            int rightMarker = (int) attribute;

            if (rightMarker>30000){
                rightMarker=30000-rightMarker;
            }

            return rightMarker;
        }

    }


    /**
     *
     */


    class DSReflectedSubKmerExtractionFromForward implements MapPartitionsFunction<Row, Row>, Serializable {
        List<Row> TupleList = new ArrayList<Row>();
        Long suffixBinary;
        long[] prefixBinarySlot;
        Row kmerTuple;
     //   int shift = (2 * (param.subKmerSizeResidue - 1));
        Long maxSubKmerResdueBinary;
        Long maxSubKmerBinary = ~((~0L) << 2 * 31);

        int currentSubKmerSize;
        int currentSubKmerResidue;
        int currentSubKmerBlock;

        public Iterator<Row> call(Iterator<Row> s) {

            while (s.hasNext()) {
                kmerTuple = s.next();

                long[] kmerTupleArray = seq2array(kmerTuple.getSeq(0));

            //    String before = BinaryBlocksToString(kmerTupleArray);
                long[] beforeSuffixLong = new long[1];
                beforeSuffixLong[0]=kmerTuple.getLong(2);
             //   String beforeSuffix = BinaryBlocksToString(beforeSuffixLong);

                currentSubKmerSize= currentKmerSizeFromBinaryBlockArray(kmerTupleArray);
                currentSubKmerResidue = (currentSubKmerSize-1)%31 +1;
                currentSubKmerBlock = (currentSubKmerSize-1)/31+1;
                maxSubKmerResdueBinary=  ((~0L) << 2 * (32-currentSubKmerResidue));

                long[] prefixBinarySlot = new long[currentSubKmerBlock];

                /**
                 * reflected Sub-kmer
                 *        Kmer      ATGCACGTTATG
                 *        Sub-Kmer  ATGCACGTTAT         marked as Integer 1 in Tuple2
                 *        Left      -----------G
                 */
                // suffixBinary = 3L << shift;
                suffixBinary = (Long) kmerTuple.getSeq(0).apply(0) >>> 2*(32-1);  // xx--------- -> ----------xx
                suffixBinary <<= 2*(32-1);  // ---------xx -> xx000000000
                //  suffixBinary >>>= shift;
                suffixBinary |= (1L << 2*(32-1-1)); // add C marker in the front 0100 = 4L

                long transmitBit1 = (Long) kmerTuple.getSeq(0).apply(currentSubKmerBlock - 1) >>> 2 * (32 - 1);   // xx-------------
                prefixBinarySlot[currentSubKmerBlock - 1] = ((Long) kmerTuple.getSeq(0).apply(currentSubKmerBlock - 1) & maxSubKmerResdueBinary) << 2;
                //prefixBinarySlot[currentSubKmerBlock - 1] &= maxSubKmerResdueBinary;
                prefixBinarySlot[currentSubKmerBlock - 1] |= kmerTuple.getLong(2)>>> 2*(currentSubKmerResidue-1); // xx01-------- -> ----------xx01

                for (int i = currentSubKmerBlock - 2; i >= 0; i--) {
                    long transmitBit2 = (Long) kmerTuple.getSeq(0).apply(i) >>> 2*(32-1);

                    prefixBinarySlot[i] = (Long) kmerTuple.getSeq(0).apply(i) << 2;
                 //   prefixBinarySlot[i] &= maxSubKmerBinary;
                    prefixBinarySlot[i] |= (transmitBit1 <<1*2); // --------xx - > --------xx--

                    transmitBit1 = transmitBit2;
                }

                long beforeMarker= kmerTuple.getLong(1) >>> 2*31;
                long attribute = onlyChangeReflexivMarker(kmerTuple.getLong(1), 2);
                long afterMarker= attribute >>> 2*31;
                long afterMarker2= getReflexivMarker(attribute);
                long leftMarker= getLeftMarker(attribute);
                long rightMarker =getRightMarker(attribute);
              //  System.out.println("before long: " + kmerTupleArray[0] + " after long: " + prefixBinarySlot[0]);
              //  System.out.println("before Marker: " + beforeMarker + " after Marker: " + afterMarker + " " + afterMarker2);
              //  System.out.println("leftMarker: " + leftMarker + " rightMarker: " + rightMarker);


             //   String after = BinaryBlocksToString(prefixBinarySlot);
                long[] afterSuffixLong = new long[1];
                afterSuffixLong[0]=suffixBinary;
             //   String afterSuffix = BinaryBlocksToString(afterSuffixLong);

               // System.out.println("before: " + before + " " + beforeSuffix + " after: " + afterSuffix + " " + after);

                TupleList.add(
                        RowFactory.create(prefixBinarySlot, attribute, suffixBinary)
                );
            }

            return TupleList.iterator();
        }

        private int currentKmerSizeFromBinaryBlockArray(long[] binaryBlocks){
            int kmerSize;
            int blockSize = binaryBlocks.length;
            kmerSize= (blockSize-1) *31;
            final int suffix0s = Long.numberOfTrailingZeros(binaryBlocks[blockSize - 1]); // ATCG...01---
            int lastMers = Long.SIZE/2-suffix0s/2-1;

            kmerSize+=lastMers;
            return kmerSize;

        }

        private long[] seq2array(Seq a){
            long[] array =new long[a.length()];
            for (int i = 0; i < a.length(); i++) {
                array[i] = (Long) a.apply(i);
            }
            return array;
        }

        private long onlyChangeReflexivMarker(long oldMarker, int reflexivMarker){
            long newMarker = oldMarker & maxSubKmerBinary;
            newMarker |= ((long) reflexivMarker) << 2*(32-1);
            return newMarker;
        }

        private long buildingAlongFromThreeInt(int ReflexivMarker, int leftCover, int rightCover){
            long info = (long) ReflexivMarker <<2*(32-1);  //move to the left most

            /**
             * shorten the int and change negative to positive to avoid two's complementary
             */
            if (leftCover>=30000){
                leftCover=30000;
            }else if (leftCover<=-30000){
                leftCover=30000-(-30000);
            }else if (leftCover<0){
                leftCover=30000-leftCover;
            }

            if (rightCover>=30000){
                rightCover=30000;
            }else if (rightCover<=-30000){
                rightCover=30000-(-30000);
            }else if (rightCover<0){
                rightCover=30000-rightCover;
            }


            info |= ((long) leftCover << 32) ; // move one integer (32 bits) to the left

            info |= ((long) rightCover); //  01--LeftCover---RightCover

            return info;
        }

        private char BinaryToNucleotide(Long twoBits) {
            char nucleotide;
            if (twoBits == 0L) {
                nucleotide = 'A';
            } else if (twoBits == 1L) {
                nucleotide = 'C';
            } else if (twoBits == 2L) {
                nucleotide = 'G';
            } else {
                nucleotide = 'T';
            }
            return nucleotide;
        }

        private String BinaryBlocksToString (long[] binaryBlocks){
            //           String KmerString="";
            int KmerLength = currentKmerSizeFromBinaryBlockArray(binaryBlocks);
            StringBuilder sb= new StringBuilder();
            char currentNucleotide;

            for (int i=0; i< KmerLength; i++){
                Long currentNucleotideBinary = binaryBlocks[i/31] >>> 2 * (32 - (i%31+1));
                currentNucleotideBinary &= 3L;
                currentNucleotide = BinaryToNucleotide(currentNucleotideBinary);
                sb.append(currentNucleotide);
            }

            return sb.toString();
        }

        private int getReflexivMarker(long attribute){
            int reflexivMarker = (int) (attribute >>> 2*(32-1)); // 01-------- -> ---------01 reflexiv marker
            return reflexivMarker;
        }

        private int getLeftMarker(long attribute){
            int leftMarker = (int) (attribute >>> 2*(16)); // 01--xxxx-----xxxx -> 01--xxxx shift out right marker
            int leftMarkerBinaryBits= ~(3 << 30) ; // ---------11 -> 11---------- -> 0011111111111


            leftMarker &= leftMarkerBinaryBits; // remove reflexivMarker



            if (leftMarker>30000){
                leftMarker=30000-leftMarker;
            }

            return leftMarker;
        }

        private int getRightMarker(long attribute){
            int rightMarker = (int) attribute;

            if (rightMarker>30000){
                rightMarker=30000-rightMarker;
            }

            return rightMarker;
        }


    }

    class DSSubKmerToFullKmer implements MapPartitionsFunction<Row, Row>, Serializable {
        /* 0 stands for forward sub-kmer */
        /* 1 stands for reflexiv sub-kmer */
        List<Row> reflexivKmerConcatList = new ArrayList<Row>();
        Row kmerTuple;
        long maxSubKmerBinary = ~((~0L) << 2 * 31);

        public Iterator<Row> call(Iterator<Row> s) throws Exception {
    //        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
    //        System.out.println(timestamp+ "RepeatCheck DSSubKmerToFullKmer: " + param.kmerSize1);

            while (s.hasNext()) {
                kmerTuple = s.next();

                subKmerToFullKmer(kmerTuple);
            }

            return reflexivKmerConcatList.iterator();
        }

        public void subKmerToFullKmer(Row currentSubKmer) throws Exception {
            long[] kmerBinaryBlocks;
            long attribute = onlyChangeReflexivMarker(currentSubKmer.getLong(1), 1); // in this case, all changes to full kmers, we mark as forward k-mer, this will change afterwards
            long[] currentSubKmerArray = seq2array(currentSubKmer.getSeq(0));

            if (getReflexivMarker(currentSubKmer.getLong(1)) == 1) {
                long[] newReflexivLongArray = new long[1];
                newReflexivLongArray[0]= currentSubKmer.getLong(2);
                kmerBinaryBlocks=combineTwoLongBlocks(currentSubKmerArray, newReflexivLongArray);
            //    System.out.println("before combine: " + currentSubKmerArray[0] + " after combine: " + kmerBinaryBlocks[0]);

                reflexivKmerConcatList.add(RowFactory.create(kmerBinaryBlocks,attribute));
    //            System.out.println("final LeftMarker: " + getLeftMarker(attribute));
            } else { /* currentSubKmer._2._1() == 2 */
                long[] newReflexivLongArray = new long[1];
                newReflexivLongArray[0]= currentSubKmer.getLong(2);
                kmerBinaryBlocks=combineTwoLongBlocks(newReflexivLongArray, currentSubKmerArray);
             //   System.out.println("before combine: " + currentSubKmerArray[0] + " after combine: " + kmerBinaryBlocks[0]);
                long newSuffix = onlyChangeReflexivMarker(currentSubKmer.getLong(1), 1);

                reflexivKmerConcatList.add(RowFactory.create(kmerBinaryBlocks,newSuffix));
         //       System.out.println("final LeftMarker suffix: " + getLeftMarker(newSuffix));
            }


        }

        private int getLeftMarker(long attribute){
            int leftMarker = (int) (attribute >>> 2*(16)); // 01--xxxx-----xxxx -> 01--xxxx shift out right marker
            int leftMarkerBinaryBits= ~(3 << 30) ; // ---------11 -> 11---------- -> 0011111111111
            leftMarker &= leftMarkerBinaryBits; // remove reflexivMarker

            if (leftMarker>30000){
                leftMarker=30000-leftMarker;
            }

            return leftMarker;
        }

        private int getReflexivMarker(long attribute){
            int reflexivMarker = (int) (attribute >>> 2*(32-1)); // 01-------- -> ---------01 reflexiv marker
            return reflexivMarker;
        }

        private long[] seq2array(Seq a){
            long[] array =new long[a.length()];
            for (int i = 0; i < a.length(); i++) {
                array[i] = (Long) a.apply(i);
            }
            return array;
        }

        private long[] leftShiftArray(long[] blocks, int shiftingLength) throws Exception {
            int startingBlockIndex = (shiftingLength-1)/31;
            int nucleotideLength = currentKmerSizeFromBinaryBlockArray(blocks);
            int residueLength = Long.SIZE / 2 - (Long.numberOfTrailingZeros(blocks[blocks.length-1])/2+1); // last block length

            long[] newBlock = new long[(nucleotideLength-shiftingLength)/31+1];
            int relativeShiftSize = shiftingLength % 31;

            if (shiftingLength >= nucleotideLength){
                // apparantly, it is possible. meaning the block has nothing left
               // throw new Exception("shifting length longer than the kmer length");
                newBlock[0]|=(1L<<2*31); //add c marker at the end
                return newBlock;
            }

           // if (relativeShiftSize ==0) then only shifting blocks

            int j=0; // new index for shifted blocks
 //           long oldShiftOut=0L; // if only one block, then 0 bits
//            if (blocks.length-(startingBlockIndex+1) >=1) { // more than one block, newBlock.length = blocks.length-startingBlockIndex
//                oldShiftOut = blocks[startingBlockIndex + 1] >>> 2 * (32 - relativeShiftSize);
 //           }
            for (int i=startingBlockIndex; i<blocks.length-1; i++){ // without the last block
                long shiftOut = blocks[i+1] >>> 2*(31-relativeShiftSize); // ooooxxxxxxx -> -------oooo  o=shift out x=needs to be left shifted
                newBlock[j]= blocks[i] << 2*relativeShiftSize; // 00000xxxxx -> xxxxx-----
                newBlock[j] |= shiftOut;
                newBlock[j] &= (~0L<<2); // remove the last two bits, in case of overlength  xxxxxxxxxxx - > xxxxxxxxxxx-  C marker will be added later if necessary

                j++;
            }

            if (residueLength > relativeShiftSize){ // still some nucleotide left in the last block
                newBlock[j]= blocks[blocks.length-1] << 2*relativeShiftSize;
            }else if (residueLength == relativeShiftSize){ // nothing left in the last block, but the new last block needs a C marker in the end
                newBlock[j-1] |= 1L; // j-1 == newBlock.length-1
            } // else the last block has been completely shift into the new last block, including the C marker

            return newBlock;

        }

        private long[] leftShiftOutFromArray(long[] blocks, int shiftingLength) throws Exception{
            int relativeShiftSize = shiftingLength % 31;
            int endingBlockIndex = (shiftingLength-1)/31;
            int nucleotideLength = currentKmerSizeFromBinaryBlockArray(blocks);
            long[] shiftOutBlocks = new long[endingBlockIndex+1];

            if (shiftingLength > nucleotideLength){
               // throw new Exception("shifting length longer than the kmer length");  apparently it is possible, only one block
                return blocks;
            }

            for (int i=0; i<endingBlockIndex; i++){
                shiftOutBlocks[i]=blocks[i];
            }

            if (relativeShiftSize > 0) {
                shiftOutBlocks[endingBlockIndex] = blocks[endingBlockIndex] & (~0L << 2 * (32 - relativeShiftSize));  //   1111111100000000000
                shiftOutBlocks[endingBlockIndex] |= (1L << (2 * (32 - relativeShiftSize - 1)));
            }else{ // relativeShiftSize == 0;
                if (endingBlockIndex+1 == blocks.length) { // a block with C marker
                    shiftOutBlocks[endingBlockIndex] = blocks[endingBlockIndex];
                }else{ // endingBlockIndex < blocks.length -1     means a block without C marker
                    shiftOutBlocks[endingBlockIndex] = blocks[endingBlockIndex];
                    shiftOutBlocks[endingBlockIndex]|=1L;  // adding C marker in the end xxxxxxxxxC
                }

            }

            return shiftOutBlocks;
        }

        private long[] combineTwoLongBlocks(long[] leftBlocks, long[] rightBlocks) throws Exception {
            int leftNucleotideLength = currentKmerSizeFromBinaryBlockArray(leftBlocks);
            int leftRelativeNTLength = (leftNucleotideLength-1) % 31+1;
            int leftVacancy = 31-leftRelativeNTLength;
            int rightNucleotideLength = currentKmerSizeFromBinaryBlockArray(rightBlocks);
            int combinedBlockSize = (leftNucleotideLength+rightNucleotideLength-1)/31+1;
            long[] newBlocks= new long[combinedBlockSize];

            if (rightNucleotideLength==0){
                return leftBlocks;
            }

            if (leftNucleotideLength==0){
                return rightBlocks;
            }

            if (leftVacancy ==0){ // left last block is a perfect block
                for (int i =0; i<leftBlocks.length; i++){
                    newBlocks[i]=leftBlocks[i];
                }

                newBlocks[leftBlocks.length-1] &= (~0L<<2); // remove the last block's C marker

                for (int j=leftBlocks.length;j<combinedBlockSize;j++){
                    newBlocks[j]=rightBlocks[j-leftBlocks.length];
                }
            }else{
           //     String rightBlocksString = BinaryBlocksToString(rightBlocks);
           //     String leftBlocksString = BinaryBlocksToString(leftBlocks);

                long[] shiftOutBlocks = leftShiftOutFromArray(rightBlocks, leftVacancy); // right shift out for the left. here we only expect one block, because leftVacancy is relative to one block
                for (int i =0; i<leftBlocks.length; i++){
                    newBlocks[i]=leftBlocks[i];
                }

                newBlocks[leftBlocks.length-1] &= (~0L<<2*(leftVacancy+1)); // leftVacancy = 32-leftRelativeNTLength-1. This is to remove the C marker
                newBlocks[leftBlocks.length-1] |= (shiftOutBlocks[0]>>> 2*(leftRelativeNTLength));
                if (leftBlocks.length<combinedBlockSize) { // this is not the end block, the last 2 bits (C marker) of shift out needs to be removed  ----------C
                    newBlocks[leftBlocks.length-1] &= (~0L<<2); // remove shift out blocks C marker. apparently, if there is a C marker, this is the last block anyway
                }

                long[] rightBlocksLeftShifted = leftShiftArray(rightBlocks, leftVacancy);

                int k=0; // rightBlocksLeftShifted index
                for (int j=leftBlocks.length;j<combinedBlockSize;j++){ // including the last blocks.
                    newBlocks[j]=rightBlocksLeftShifted[k];
                    k++;
                    long[] rightBlocksLeftShiftedArray= new long[1];
                    rightBlocksLeftShiftedArray[0]=rightBlocksLeftShifted[k-1];
         //           String rightShift= BinaryBlocksToString(rightBlocksLeftShiftedArray);
                  //  System.out.println("rightShift: " + rightShift);
                }

          //      String mergedKmer= BinaryBlocksToString(newBlocks);

                //System.out.println(" left Blocks:" + leftBlocksString + " Right blocks: " + rightBlocksString + " rightLength: " + rightNucleotideLength + " leftNucleotideLength: " + leftNucleotideLength + " leftRelativeNTLength: " + leftRelativeNTLength + " leftVacancy: " + leftVacancy + " rightNucleotideLength: " + rightNucleotideLength + " combinedBlockSize: " + combinedBlockSize + " newBlock: " + mergedKmer);
            }

            return newBlocks;
        }

        private long onlyChangeReflexivMarker(long oldMarker, int reflexivMarker){
            long newMarker = oldMarker & maxSubKmerBinary;
            newMarker |= ((long) reflexivMarker) << 2*(32-1);
            return newMarker;
        }

        private int currentKmerSizeFromBinaryBlockArray(long[] binaryBlocks){
            int kmerSize;
            int blockSize = binaryBlocks.length;
            kmerSize= (blockSize-1) *31;
            final int suffix0s = Long.numberOfTrailingZeros(binaryBlocks[blockSize - 1]); // ATCG...01---
            int lastMers = Long.SIZE/2-suffix0s/2-1;

            kmerSize+=lastMers;
            return kmerSize;

        }

        private char BinaryToNucleotide(Long twoBits) {
            char nucleotide;
            if (twoBits == 0L) {
                nucleotide = 'A';
            } else if (twoBits == 1L) {
                nucleotide = 'C';
            } else if (twoBits == 2L) {
                nucleotide = 'G';
            } else {
                nucleotide = 'T';
            }
            return nucleotide;
        }

        private String BinaryBlocksToString (long[] binaryBlocks){
            //           String KmerString="";
            int KmerLength = currentKmerSizeFromBinaryBlockArray(binaryBlocks);
            StringBuilder sb= new StringBuilder();
            char currentNucleotide;

            for (int i=0; i< KmerLength; i++){
                Long currentNucleotideBinary = binaryBlocks[i/31] >>> 2 * (32 - (i%31+1));
                currentNucleotideBinary &= 3L;
                currentNucleotide = BinaryToNucleotide(currentNucleotideBinary);
                sb.append(currentNucleotide);
            }

            return sb.toString();
        }
    }

    /**
     *
     */


    /**
     * interface class for RDD implementation, used in step 5
     */

    /**
     * interface class for RDD implementation, used in step 4
     */


    class DSKmerReverseComplement implements MapPartitionsFunction<Row, Row>, Serializable {
        /* a capsule for all Kmers and reverseComplementKmers */
        List<Row> kmerList = new ArrayList<Row>();
        long[] reverseComplement;
        long[] forwardKmer;
        Row kmerTuple;
        Long lastTwoBits;
        Seq kmerBinarySeq;

        int currentKmerBlockSize;
        int currentKmerSize;
        int currentKmerResidue;


        public Iterator<Row> call(Iterator<Row> s) {
    //        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
    //        System.out.println(timestamp+ "RepeatCheck DSKmerReverseComplement: " + param.kmerSize1);

            while (s.hasNext()) {
                kmerTuple = s.next();
                kmerBinarySeq = kmerTuple.getSeq(0);
                //reverseComplement=0L;

                currentKmerBlockSize = kmerBinarySeq.length();
                currentKmerSize = currentKmerSizeFromBinaryBlock(kmerBinarySeq);
                currentKmerResidue = currentKmerResidueFromBlock(kmerBinarySeq);

                forwardKmer = new long[currentKmerBlockSize];
                reverseComplement = new long[currentKmerBlockSize];

                for (int i = 0; i < currentKmerSize; i++) {
                    int RCindex = currentKmerSize - i - 1; //  ------------- ------------- ---------**-- RC index goes reverse
                    //  ------------- ------------- -------**----  <--
                   // reverseComplement[i / 31] <<= 2;

                    if (RCindex >= currentKmerSize - currentKmerResidue) {
                        lastTwoBits = (Long) kmerBinarySeq.apply(RCindex / 31) >>> 2 * (32-(RCindex % 31)-1);    //  ------------- ------------- ------|----**
                        lastTwoBits &= 3L;
                        lastTwoBits ^= 3L;
                    } else { // the same
                        lastTwoBits = (Long) kmerBinarySeq.apply(RCindex / 31) >>> 2 * (32 - (RCindex % 31) - 1);
                        lastTwoBits &= 3L;
                        lastTwoBits ^= 3L;
                    }

                    reverseComplement[i / 31] |= lastTwoBits;
                    reverseComplement[i / 31] <<=2; // the order of these two lines are very important

                }
                reverseComplement[(currentKmerSize-1)/31] <<= 2*(32-currentKmerResidue-1); //  ---xxxxxxx -> xxxxxxx--- extra -1 because there are a vacancy from the step above
                reverseComplement[(currentKmerSize-1)/31]|=(1L<<2*(32-currentKmerResidue-1)); // adding ending marker C


                for (int i = 0; i < currentKmerBlockSize; i++) {
                    forwardKmer[i] = (Long) kmerTuple.getSeq(0).apply(i);
                }

                kmerList.add(RowFactory.create(forwardKmer, kmerTuple.getInt(1)));
                kmerList.add(RowFactory.create(reverseComplement, kmerTuple.getInt(1)));

            }

            return kmerList.iterator();
        }

        private int currentKmerResidueFromBlock(Seq binaryBlocks){
            final int suffix0s = Long.numberOfTrailingZeros((Long)binaryBlocks.apply(binaryBlocks.length()-1));
            return Long.SIZE/2 - suffix0s/2 -1;
        }

        private int currentKmerSizeFromBinaryBlock(Seq binaryBlocks){
            int kmerSize;
            int blockSize = binaryBlocks.length();
            kmerSize= (blockSize-1) *31;
            final int suffix0s = Long.numberOfTrailingZeros((Long) binaryBlocks.apply(blockSize - 1)); // ATCG...01---
            int lastMers = Long.SIZE/2-suffix0s/2 -1; // minus last marker

            kmerSize+=lastMers;
            return kmerSize;

        }

        private char BinaryToNucleotide(Long twoBits) {
            char nucleotide;
            if (twoBits == 0L) {
                nucleotide = 'A';
            } else if (twoBits == 1L) {
                nucleotide = 'C';
            } else if (twoBits == 2L) {
                nucleotide = 'G';
            } else {
                nucleotide = 'T';
            }
            return nucleotide;
        }
    }

    class DynamicKmerBinarizer implements MapPartitionsFunction<Row, Row>, Serializable {

        List<Row> kmerList = new ArrayList<Row>();
        Row units;
        String kmer;
        int currentKmerSize;
        int currentKmerBlockSize;
        int cover;
        char nucleotide;
        long nucleotideInt;
        //     Long suffixBinary;
        //     Long[] suffixBinaryArray;

        public Iterator<Row> call(Iterator<Row> s) {
    //        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
    //        System.out.println(timestamp+"RepeatCheck DynamicKmerBinarizer: " + param.kmerSize1);

            while (s.hasNext()) {

                units = s.next();

                kmer = units.getString(0);

                if (kmer.startsWith("(")) {
                    kmer = kmer.substring(1);
                }

                currentKmerSize= kmer.length();
                currentKmerBlockSize = (currentKmerSize-1)/31+1; // each 31 mer is a block

                if (!kmerSizeCheck(kmer, param.kmerListHash)){continue;} // the kmer length does not fit into any of the kmers in the list.

                if (units.getString(1).endsWith(")")) {
                    if (units.getString(1).length() >= 11) {
                        cover = 1000000000;
                    } else {
                        cover = Integer.parseInt(StringUtils.chop(units.getString(1)));
                    }
                } else {
                    if (units.getString(1).length() >= 10) {
                        cover = 1000000000;
                    } else {
                        cover = Integer.parseInt(units.getString(1));
                    }
                }

                long[] nucleotideBinarySlot = new long[currentKmerBlockSize];
                //       Long nucleotideBinary = 0L;

                for (int i = 0; i < currentKmerSize; i++) {
                    nucleotide = kmer.charAt(i);
                    if (nucleotide >= 256) nucleotide = 255;
                    nucleotideInt = nucleotideValue(nucleotide);
                    // forward kmer in bits
                    nucleotideInt <<= 2*(32-1-(i%31)); // shift to the left   [ATCGGATCC-,ATCGGATCC-]
//                    nucleotideBinarySlot[i / 31] <<= 2*((32-i)%32);
                    nucleotideBinarySlot[i / 31] |= nucleotideInt;

                    //   nucleotideBinary <<= 2;
                    //   nucleotideBinary |= nucleotideInt;
                }

                // marking the end of the kmer
                long kmerEndMark = 1L;
                kmerEndMark <<= 2*(32-1-((currentKmerSize-1)%31+1));
                nucleotideBinarySlot[param.kmerListHash.get(currentKmerSize)-1] |= kmerEndMark; // param.kmerListHash.get(currentKmerSize)] == currentKmerBlockSize

                // return
                kmerList.add(
                        RowFactory.create(nucleotideBinarySlot, cover)
                );
            }

            return kmerList.iterator();
        }

        private boolean kmerSizeCheck(String kmer, HashMap<Integer, Integer> kmerList){
            if (kmerList.containsKey(kmer.length())) {
                return true;
            }else {
                return false;
            }
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

    class ReverseComplementKmerBinaryExtractionFromDataset implements MapPartitionsFunction<String, Long>, Serializable {
        long maxKmerBits = ~((~0L) << (2 * param.kmerSize));

        List<Long> kmerList = new ArrayList<Long>();
        int readLength;
        String[] units;
        String read;
        char nucleotide;
        long nucleotideInt;
        long nucleotideIntComplement;

        public Iterator<Long> call(Iterator<String> s) {

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
                            kmerList.add(nucleotideBinary);
                        } else {
                            kmerList.add(nucleotideBinaryReverseComplement);
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
     * interface class for RDD implementation, used in step 3
     *      -----------
     *      ------
     *       ------
     *        ------
     *         ------
     *          ------
     *           ------
     */


    class DSFastqUnitFilter implements FilterFunction<String>, Serializable {
        public boolean call(String s) {
            return s != null;
        }
    }

    /**
     * interface class for RDD implementation, Used in step 1
     */


    class DSFastqFilterWithQual implements MapFunction<String, String>, Serializable {
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
            } else {
                return null;
            }
        }
    }

    /**
     * interface class for RDD implementation, used in step 2
     */


    /**
     *
     * @param param
     */
    public void setParam(DefaultParam param) {
        this.param = param;
    }
}
