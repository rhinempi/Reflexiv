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
public class ReflexivDSDynamicKmerRuduction implements Serializable {
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

    private Hashtable<List<Long>, Integer> SubKmerProbRowToHash(List<Row> s){
        Hashtable<List<Long>, Integer> ProbHash = new Hashtable<List<Long>, Integer>();
        for (int i =0; i<s.size();i++){
            List<Long> Key = new ArrayList<Long>();
            for (int j=0; j<s.get(i).getSeq(0).size(); j++){
                Key.add((Long) s.get(i).getSeq(0).apply(j));
            }
            Integer Value = s.get(i).getInt(1);
            ProbHash.put(Key, Value);
        }

        return ProbHash;
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
        info.readMessage("Start Spark framework: Reflexiv dynamic k-mer reduction: " + param.kmerSize1 + " vs " + param.kmerSize2);
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
        Dataset<Row> LongerReflexivSubKmerDS;
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
        Dataset<Row> LongerReflexivFullKmerDS;
        Dataset<Row> MixedFullKmerDS;
        Dataset<Row> MixedReflexivSubkmerDS;
        StructType FullKmerWithAttributeStruct = new StructType();
        FullKmerWithAttributeStruct = FullKmerWithAttributeStruct.add("k", DataTypes.createArrayType(DataTypes.LongType), false);
        FullKmerWithAttributeStruct = FullKmerWithAttributeStruct.add("reflection", DataTypes.LongType, false);
        ExpressionEncoder<Row> ReflexivFullKmerEncoder= RowEncoder.apply(FullKmerWithAttributeStruct);


        Dataset<Row> DSFullKmerStringShort;
        Dataset<Row> DSFullKmerStringLong;
        StructType ReflexivFullKmerStringStruct = new StructType();
        ReflexivFullKmerStringStruct = ReflexivFullKmerStringStruct.add("k", DataTypes.StringType, false);
        ReflexivFullKmerStringStruct = ReflexivFullKmerStringStruct.add("reflection", DataTypes.StringType, false);
        ExpressionEncoder<Row> ReflexivFullKmerStringEncoder = RowEncoder.apply(ReflexivFullKmerStringStruct);





        /**
         * loading Kmer counts
         */
        KmerCountDS = spark.read().csv(param.inputKmerPath1);
        LongerKmerCountDS = spark.read().csv(param.inputKmerPath2);


        if (param.partitions > 0) {
            KmerCountDS = KmerCountDS.repartition(param.partitions);
        }

        if (param.partitions > 0) {
            LongerKmerCountDS = LongerKmerCountDS.repartition(param.partitions);
        }

        DSSubKmerToFullKmer DSSubKmerToFullLengthKmer = new DSSubKmerToFullKmer();

        DynamicKmerBinarizerFromSorted DSBinarizerSort= new DynamicKmerBinarizerFromSorted();
        ReflexivFullKmerDS = KmerCountDS.mapPartitions(DSBinarizerSort, ReflexivFullKmerEncoder);
        LongerReflexivFullKmerDS = LongerKmerCountDS.mapPartitions(DSBinarizerSort, ReflexivFullKmerEncoder);

        MixedFullKmerDS = LongerReflexivFullKmerDS.union(ReflexivFullKmerDS);

 //       MixedFullKmerDS.cache();

        LeftLongerToShorterComparisonPreparation leftComparison = new LeftLongerToShorterComparisonPreparation();
        MixedReflexivSubkmerDS = MixedFullKmerDS.mapPartitions(leftComparison,ReflexivSubKmerCompressedEncoder);

        MixedReflexivSubkmerDS = MixedReflexivSubkmerDS.sort("k-1");

        LeftLongerKmerVariantAdjustment leftAdjustment= new LeftLongerKmerVariantAdjustment();
        MixedReflexivSubkmerDS=MixedReflexivSubkmerDS.mapPartitions(leftAdjustment, ReflexivSubKmerCompressedEncoder);

        RightLongerToShorterComparisonAndNeutralizationPreparation rightComparison = new RightLongerToShorterComparisonAndNeutralizationPreparation();
        MixedReflexivSubkmerDS = MixedReflexivSubkmerDS.mapPartitions(rightComparison, ReflexivSubKmerCompressedEncoder);

        MixedReflexivSubkmerDS= MixedReflexivSubkmerDS.sort("k-1");

        RightLongerKmerVariantAdjustmentAndNeutralization rightAdjustmentAndNeutralization = new RightLongerKmerVariantAdjustmentAndNeutralization();
        MixedReflexivSubkmerDS = MixedReflexivSubkmerDS.mapPartitions(rightAdjustmentAndNeutralization, ReflexivSubKmerCompressedEncoder);

        MixedFullKmerDS = MixedReflexivSubkmerDS.mapPartitions(DSSubKmerToFullLengthKmer, ReflexivFullKmerEncoder);
        MixedReflexivSubkmerDS.unpersist();



       MixedFullKmerDS = MixedFullKmerDS.sort("k");
/*
        LongerKmerEnlightening LongerKmerEnlightment = new LongerKmerEnlightening();
        MixedFullKmerDS = MixedFullKmerDS.mapPartitions(LongerKmerEnlightment,ReflexivFullKmerEncoder);

        MixedFullKmerDS = MixedFullKmerDS.sort("k");
*/
        ShorterKmerNeutralization SKNeutralizer = new ShorterKmerNeutralization();
        MixedFullKmerDS = MixedFullKmerDS.mapPartitions(SKNeutralizer, ReflexivFullKmerEncoder);

        MixedFullKmerDS.persist(StorageLevel.DISK_ONLY());

      //  if(param.partitions>10) {
      //      MixedFullKmerDS = MixedFullKmerDS.coalesce(param.partitions - 1);
      //      MixedFullKmerDS = MixedFullKmerDS.mapPartitions(SKNeutralizer, ReflexivFullKmerEncoder);
      //  }

    //    MixedFullKmerDS.persist(StorageLevel.DISK_ONLY());
     //   MixedFullKmerDS.show();

        /**
         *
         */

        DSBinaryFullKmerArrayToStringShort FullKmerToStringShort = new DSBinaryFullKmerArrayToStringShort();
        DSBinaryFullKmerArrayToStringLong FullKmerToStringLong = new DSBinaryFullKmerArrayToStringLong();

        DSFullKmerStringShort = MixedFullKmerDS.mapPartitions(FullKmerToStringShort, ReflexivFullKmerStringEncoder);
        DSFullKmerStringLong = MixedFullKmerDS.mapPartitions(FullKmerToStringLong, ReflexivFullKmerStringEncoder);

        if (param.gzip) {
            DSFullKmerStringShort.write().
                    mode(SaveMode.Overwrite).
                    format("csv").
                    option("codec", "org.apache.hadoop.io.compress.GzipCodec").
                    save(param.outputPath + "/Count_" + param.kmerSize1 + "_reduced");
        }else{
            DSFullKmerStringShort.write().
                    mode(SaveMode.Overwrite).
                    format("csv").
                    save(param.outputPath + "/Count_" + param.kmerSize1 + "_reduced");
        }

       // if (param.kmerSize2<100) { // longer than 100 will not be enlightened by shorter k-mer
            if (param.kmerSize2 == param.kmerListInt[param.kmerListInt.length - 1]) {
                if (param.gzip) {
                    DSFullKmerStringLong.write().
                            mode(SaveMode.Overwrite).
                            format("csv").
                            option("codec", "org.apache.hadoop.io.compress.GzipCodec").
                            save(param.outputPath + "/Count_" + param.kmerSize2 + "_reduced");
                } else {
                    DSFullKmerStringLong.write().
                            mode(SaveMode.Overwrite).
                            format("csv").
                            save(param.outputPath + "/Count_" + param.kmerSize2 + "_reduced");
                }
            } else {
                if (param.gzip) {
                    DSFullKmerStringLong.write().
                            mode(SaveMode.Overwrite).
                            format("csv").
                            option("codec", "org.apache.hadoop.io.compress.GzipCodec").
                            save(param.outputPath + "/Count_" + param.kmerSize2 + "_sorted");
                } else {
                    DSFullKmerStringLong.write().
                            mode(SaveMode.Overwrite).
                            format("csv").
                            save(param.outputPath + "/Count_" + param.kmerSize2 + "_sorted");
                }
            }
       // }

        spark.stop();
    }





    class TagContigID implements FlatMapFunction<Tuple2<Tuple2<String, String>, Long>, String>, Serializable {

        public Iterator<String> call(Tuple2<Tuple2<String, String>, Long> s) {


            List<String> contigList = new ArrayList<String>();

            contigList.add(s._1._1 + "-" + s._2 + "\n" + s._1._2);

            return contigList.iterator();
        }
    }


    class DSBinaryFullKmerArrayToStringShort implements MapPartitionsFunction<Row, Row>, Serializable {
        List<Row> reflexivKmerStringList = new ArrayList<Row>();

        public Iterator<Row> call(Iterator<Row> sIterator) {
   //         Timestamp timestamp = new Timestamp(System.currentTimeMillis());
  //          System.out.println(timestamp + "RepeatCheck DSBinaryFullKmerArrayToStringShort: " + param.kmerSize1);

            while (sIterator.hasNext()) {
                Row s = sIterator.next();

                int kmerLength= currentKmerSizeFromBinaryBlockArray(
                        seq2array(s.getSeq(0))
                );

                if( kmerLength== param.kmerSize1) {
                    String kmerString = BinaryBlocksToString(
                            seq2array(s.getSeq(0))
                    );

                    String attributeString = getReflexivMarker(s.getLong(1))+"|"+getLeftMarker(s.getLong(1))+ "|"+getRightMarker(s.getLong(1));
                    reflexivKmerStringList.add(
                            RowFactory.create(kmerString, attributeString
                            )
                    );

                  //  System.out.println("final final leftMarker: " + getLeftMarker(s.getLong(1)));
                } // else not return

            }
            return reflexivKmerStringList.iterator();
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

    }

    class DSBinaryFullKmerArrayToStringLong implements MapPartitionsFunction<Row, Row>, Serializable {
        List<Row> reflexivKmerStringList = new ArrayList<Row>();

        public Iterator<Row> call(Iterator<Row> sIterator) {
     //       Timestamp timestamp = new Timestamp(System.currentTimeMillis());
     //       System.out.println(timestamp + "RepeatCheck DSBinaryFullKmerArrayToStringLong: " + param.kmerSize1);

            while (sIterator.hasNext()) {
                Row s = sIterator.next();

                int kmerLength= currentKmerSizeFromBinaryBlockArray(
                        seq2array(s.getSeq(0))
                );

                if( kmerLength== param.kmerSize2) {

                    String kmerString = BinaryBlocksToString(
                            seq2array(s.getSeq(0))
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

    class LongerKmerToEnglightenKmer implements MapPartitionsFunction<Row, Row>, Serializable{
        List<Row> LongToShortReflexedSubKmer = new ArrayList<Row>();

        // xxxxxxxxxxxxxxx x --> ----xxxxxxxxxxxx xxxx
        // ----xxxxxxxxxxx x --> ----xxxxxxxxxxx  x

        public Iterator<Row> call(Iterator<Row> s) throws Exception{
            while (s.hasNext()) {
                Row subKmer = s.next();

                long[] SubKmerArray = seq2array(subKmer.getSeq(0));
                long[] ReflexivKmerArray = new long[1];
                ReflexivKmerArray[0]=subKmer.getLong(2);
                long[] fullKmer;
                long[] newFullKmer;
                long[] newSubKmer;
                long[] newReflexivKmer;
                int leftShiftLength = param.kmerSize2-param.kmerSize1;
                if (getReflexivMarker(subKmer.getLong(1))==1){
                    fullKmer = combineTwoLongBlocks(SubKmerArray, ReflexivKmerArray);
                }else{
                    fullKmer = combineTwoLongBlocks(ReflexivKmerArray, SubKmerArray);
                }

                newSubKmer= leftShiftArray(fullKmer, leftShiftLength); // llllllrrrrrrrrrrrrr - > rrrrrrrrrrrrrr
                newReflexivKmer = leftShiftOutFromArray(fullKmer, leftShiftLength); // lllllllrrrrrrrrrrr-> lllllll
                newFullKmer = combineTwoLongBlocks(newSubKmer, newReflexivKmer); // -> rrrrrrrrrrrrr_lllllll

                long attribute= onlyChangeReflexivMarker(subKmer.getLong(1), 1); // all enlighten k-mer are concatenated full reflexed k-mers, mark 1 just for now
                LongToShortReflexedSubKmer.add(RowFactory.create(newFullKmer,attribute));

            //    String beforeFullKmer = BinaryBlocksToString(fullKmer);
            //    String newFullKmerString= BinaryBlocksToString(newFullKmer);
                //System.out.println(param.kmerSize2 + " before: " + beforeFullKmer + " after: " + newFullKmerString);

            }

            return LongToShortReflexedSubKmer.iterator();
        }

        private long[] seq2array(Seq a){
            long[] array =new long[a.length()];
            for (int i = 0; i < a.length(); i++) {
                array[i] = (Long) a.apply(i);
            }
            return array;
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

        private long onlyChangeReflexivMarker(long oldMarker, int reflexivMarker){
            long maxSubKmerBinary = ~((~0L) << 2 * 31);
            long newMarker = oldMarker & maxSubKmerBinary;
            newMarker |= ((long) reflexivMarker) << 2*(32-1);
            return newMarker;
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
               // String rightBlocksString = BinaryBlocksToString(rightBlocks);
               // String leftBlocksString = BinaryBlocksToString(leftBlocks);

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
                //    String rightShift= BinaryBlocksToString(rightBlocksLeftShiftedArray);
                    //  System.out.println("rightShift: " + rightShift);
                }

               // String mergedKmer= BinaryBlocksToString(newBlocks);

                //System.out.println(" left Blocks:" + leftBlocksString + " Right blocks: " + rightBlocksString + " rightLength: " + rightNucleotideLength + " leftNucleotideLength: " + leftNucleotideLength + " leftRelativeNTLength: " + leftRelativeNTLength + " leftVacancy: " + leftVacancy + " rightNucleotideLength: " + rightNucleotideLength + " combinedBlockSize: " + combinedBlockSize + " newBlock: " + mergedKmer);
            }

            return newBlocks;
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

        private String BinaryBlocksToString (long[] binaryBlocks){
            String KmerString="";
            int KmerLength = currentKmerSizeFromBinaryBlockArray(binaryBlocks);

            for (int i=0; i< KmerLength; i++){
                Long currentNucleotideBinary = binaryBlocks[i/31] >>> 2 * (32 - (i%31+1));
                currentNucleotideBinary &= 3L;
                char currentNucleotide = BinaryToNucleotide(currentNucleotideBinary);
                KmerString += currentNucleotide;
            }

            return KmerString;
        }

        private long[] leftShiftArray(long[] blocks, int shiftingLength) throws Exception {
            int startingBlockIndex = (shiftingLength)/31;
            int nucleotideLength = currentKmerSizeFromBinaryBlockArray(blocks);
            int residueLength = Long.SIZE / 2 - (Long.numberOfTrailingZeros(blocks[blocks.length-1])/2+1); // last block length

            int remainLength=nucleotideLength-shiftingLength-1;
            if (remainLength <0){
                remainLength=0;
            }
            long[] newBlock = new long[remainLength/31+1];
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
                // throw new Exception("shifting length longer than the kmer length");
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

    class LongerKmerEnlightening implements MapPartitionsFunction<Row, Row>, Serializable{
        List<Row> newFullKmerList = new ArrayList<Row>();
        Row shorterFullKmer;
        List<Row> tempLongerFullKmer = new ArrayList<Row>();

        public Iterator<Row> call(Iterator<Row> s) throws Exception{
            while(s.hasNext()) {
                Row FullKmer = s.next();
             //   System.out.println(param.kmerSize2+"mark");
                long[] FullKmerArray = seq2array(FullKmer.getSeq(0));
                int FullKmerLength = currentKmerSizeFromBinaryBlockArray(FullKmerArray);

                if (FullKmerLength == param.kmerSize1) { // shorter FullKmer size  =  param.kmerSize1 -1
                    if (shorterFullKmer != null) { // already one exists
                        if (tempLongerFullKmer.size() > 0) {
                            if (getRightMarker(shorterFullKmer.getLong(1)) > 0) {
                                for (int i = 0; i < tempLongerFullKmer.size(); i++) {
                                    if (dynamicSubKmerComparator(shorterFullKmer.getSeq(0), tempLongerFullKmer.get(i).getSeq(0)) == true) {
                                        newFullKmerList.add(enlightening(tempLongerFullKmer.get(i), true));
                                    } else {
                                        newFullKmerList.add(enlightening(tempLongerFullKmer.get(i), false));
                                    }
                                }
                            } else { // adding temp to output without changing
                                for (int i = 0; i < tempLongerFullKmer.size(); i++) {
                                    newFullKmerList.add(enlightening(tempLongerFullKmer.get(i), false));
                                }
                            }
                            tempLongerFullKmer = new ArrayList<Row>();
                        }
                    }

                    shorterFullKmer = FullKmer;
                    newFullKmerList.add(FullKmer);
                } else { // it is a longer K-mer
                    if (shorterFullKmer == null) {
                        tempLongerFullKmer.add(FullKmer);
                    } else {
                        if (dynamicSubKmerComparator(shorterFullKmer.getSeq(0), FullKmer.getSeq(0)) == true) {
                            if (getRightMarker(shorterFullKmer.getLong(1)) > 0) {
                                newFullKmerList.add(enlightening(FullKmer, true));
                            } else {
                                newFullKmerList.add(enlightening(FullKmer, false));
                            }
                        } else { // longer Kmer not overlap to shorter k-mer anymore, a new round starts
                            if (getRightMarker(shorterFullKmer.getLong(1)) > 0) {
                                for (int i = 0; i < tempLongerFullKmer.size(); i++) {
                                    if (dynamicSubKmerComparator(shorterFullKmer.getSeq(0), tempLongerFullKmer.get(i).getSeq(0)) == true) {
                                        newFullKmerList.add(enlightening(tempLongerFullKmer.get(i), true));
                                    } else {
                                        newFullKmerList.add(enlightening(tempLongerFullKmer.get(i), false));
                                    }
                                }
                            } else { // adding temp to output without changing
                                for (int i = 0; i < tempLongerFullKmer.size(); i++) {
                                    newFullKmerList.add(enlightening(tempLongerFullKmer.get(i), false));
                                }
                            }

                            tempLongerFullKmer=new ArrayList<Row>();
                            shorterFullKmer = null;
                            tempLongerFullKmer.add(FullKmer);
                        }
                    }
                }
            }

            //release the last temp units
            if (shorterFullKmer==null){
                for (int i=0;i<tempLongerFullKmer.size();i++) {
                    newFullKmerList.add(enlightening(tempLongerFullKmer.get(i), false));
                }
            }else{
                if (getRightMarker(shorterFullKmer.getLong(1)) >0){
                    for (int i=0;i<tempLongerFullKmer.size();i++){
                        if (dynamicSubKmerComparator(shorterFullKmer.getSeq(0), tempLongerFullKmer.get(i).getSeq(0))== true){
                            newFullKmerList.add(enlightening(tempLongerFullKmer.get(i), true));
                        }else{
                            newFullKmerList.add(enlightening(tempLongerFullKmer.get(i), false));
                        }
                    }
                }else{ // adding temp to output without changing
                    for (int i=0;i<tempLongerFullKmer.size();i++) {
                        newFullKmerList.add(enlightening(tempLongerFullKmer.get(i), false));
                    }
                }
            }

        //    System.out.println("after enlightning count: " + newFullKmerList.size());

            return newFullKmerList.iterator();
        }

        private Row enlightening (Row s, boolean enlighten) throws Exception { // all to forward k-mer for the next step neutralization
            Row fullKmer;
            long[] fullKmerArray= seq2array(s.getSeq(0));   // rrrrrrrrrrrr_llllll
            long[] reflexivKmerArray = leftShiftArray(fullKmerArray,param.kmerSize1); // llllll   l for left
            long[] newSubKmer = leftShiftOutFromArray(fullKmerArray,param.kmerSize1); // rrrrrrrrrrrr     r for right
            long[] newFullKmer = combineTwoLongBlocks(reflexivKmerArray, newSubKmer); // llllllrrrrrrrrrrrr
            if (getReflexivMarker(s.getLong(1))==1){ // they are all suppose to be 1
                if (enlighten==true){
                    long attribute = buildingAlongFromThreeInt(1, getLeftMarker(s.getLong(1)), param.kmerSize2+10);
                    fullKmer=RowFactory.create(newFullKmer,attribute);
                }else{
                    fullKmer=RowFactory.create(newFullKmer,s.getLong(1));
                }
             //   System.out.println("Enlightening before: " + BinaryBlocksToString(fullKmerArray) + " after: " + BinaryBlocksToString(newFullKmer));
            }else{
                // System.out.println("Null Enlightening before: " + BinaryBlocksToString(fullKmerArray) + " after: " + BinaryBlocksToString(newFullKmer));
                fullKmer=null;
                // something is wrong
            }
            return fullKmer;
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

        private long[] leftShiftArray(long[] blocks, int shiftingLength) throws Exception {
            int startingBlockIndex = (shiftingLength)/31;
            int nucleotideLength = currentKmerSizeFromBinaryBlockArray(blocks);
            int residueLength = Long.SIZE / 2 - (Long.numberOfTrailingZeros(blocks[blocks.length-1])/2+1); // last block length

            int remainLength=nucleotideLength-shiftingLength-1;
            if (remainLength <0){
                remainLength=0;
            }
            long[] newBlock = new long[remainLength/31+1];
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
                // throw new Exception("shifting length longer than the kmer length");
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
                 //   String rightShift= BinaryBlocksToString(rightBlocksLeftShiftedArray);
                    //  System.out.println("rightShift: " + rightShift);
                }

            //    String mergedKmer= BinaryBlocksToString(newBlocks);

                //System.out.println(" left Blocks:" + leftBlocksString + " Right blocks: " + rightBlocksString + " rightLength: " + rightNucleotideLength + " leftNucleotideLength: " + leftNucleotideLength + " leftRelativeNTLength: " + leftRelativeNTLength + " leftVacancy: " + leftVacancy + " rightNucleotideLength: " + rightNucleotideLength + " combinedBlockSize: " + combinedBlockSize + " newBlock: " + mergedKmer);
            }

            return newBlocks;
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

        private boolean dynamicSubKmerComparator(Seq a, Seq b) throws Exception {
            long[] arrayA = seq2array(a);
            long[] arrayB = seq2array(b);

            int aLength= currentKmerSizeFromBinaryBlockArray(arrayA);
            int bLength= currentKmerSizeFromBinaryBlockArray(arrayB);

            if (aLength>bLength){ // equal should not happen
                long[] shorterVersion = leftShiftOutFromArray(arrayA, bLength);
               // String longer = BinaryBlocksToString(shorterVersion);
               // String shorter = BinaryBlocksToString(arrayB);
                // System.out.println("longer: " + longer + " shorter: " + shorter);
                // if (shorterVersion.length>=2 && arrayB.length >=2) {
                //    System.out.println("longer array: " + shorterVersion[0] + " "  + shorterVersion[1] + " shorter array: " + arrayB[0] + " " + arrayB[1]);
                //}
                if (Arrays.equals(shorterVersion, arrayB)){
                    //  if (shorterVersion.length>=2){
                    //        System.out.println("marker!!!");
                    // }
                    return true;
                }else{
                    return false;
                }
            }else{
                long[] shorterVersion = leftShiftOutFromArray(arrayB, aLength);
                if (Arrays.equals(shorterVersion, arrayA)){
                    return true;
                }else{
                    return false;
                }
            }
        }

        private long[] seq2array(Seq a){
            long[] array =new long[a.length()];
            for (int i = 0; i < a.length(); i++) {
                array[i] = (Long) a.apply(i);
            }
            return array;
        }

        private String BinaryBlocksToString (long[] binaryBlocks){
            String KmerString="";
            int KmerLength = currentKmerSizeFromBinaryBlockArray(binaryBlocks);

            for (int i=0; i< KmerLength; i++){
                Long currentNucleotideBinary = binaryBlocks[i/31] >>> 2 * (32 - (i%31+1));
                currentNucleotideBinary &= 3L;
                char currentNucleotide = BinaryToNucleotide(currentNucleotideBinary);
                KmerString += currentNucleotide;
            }

            return KmerString;
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

    class RightLongerToShorterComparisonAndNeutralizationPreparation implements MapPartitionsFunction<Row, Row>, Serializable{
        List<Row> kmerList= new ArrayList<Row>();

        public Iterator<Row> call(Iterator<Row> s) throws Exception {
            long[] seqBlocks;
            int subKmerLength;
            long[] subKmer;
            long[] extension=new long[1];
            long attribute;
            long[] combinedBlock;

    //        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
     //       System.out.println(timestamp+ "RepeatCheck RightLongerToShorterComparisonAndNeutralizationPreparation: " + param.kmerSize1);

            while (s.hasNext()){
                Row fullKmer=s.next();

                seqBlocks=seq2array(fullKmer.getSeq(0));
                subKmerLength=currentKmerSizeFromBinaryBlockArray(seqBlocks);

                subKmer=reverseBinaryBlocks(seqBlocks);
                extension[0]=fullKmer.getLong(2);

                if (getReflexivMarker(fullKmer.getLong(1))==1){
                    combinedBlock=combineTwoLongBlocks(subKmer, extension);
                }else{
                    combinedBlock=combineTwoLongBlocks(extension, subKmer);
                }

                subKmer = leftShiftArray(combinedBlock, 1);
                extension = leftShiftOutFromArray(combinedBlock, 1);

                attribute=onlyChangeReflexivMarker(fullKmer.getLong(1), 2);

                kmerList.add(RowFactory.create(subKmer, attribute, extension[0]));

            }

            return kmerList.iterator();
        }

        private long[] reverseBinaryBlocks(long[] blocks){
            int length = currentKmerSizeFromBinaryBlockArray(blocks);
            int blockNumber= blocks.length;
            long[] newBlocks= new long[blockNumber];
            int reverseIndex;
            int reverseBlockIndex;
            int relativeReverseIndex;

            int forwardBlockIndex;

            long twoBits;
            for (int i=0; i<length;i++){
                reverseIndex=length-i-1;
                reverseBlockIndex=reverseIndex/31;
                relativeReverseIndex=reverseIndex%31;

                forwardBlockIndex=i/31;

                twoBits=blocks[reverseBlockIndex] >>>2*(31-relativeReverseIndex);
                twoBits&=3L;

                newBlocks[forwardBlockIndex]|=twoBits;
                newBlocks[forwardBlockIndex] <<=2;
            }
            int lastBlockShift=31-(length-1)%31-1;
            newBlocks[newBlocks.length-1] <<=2*lastBlockShift;
            newBlocks[newBlocks.length - 1] |= (1L << 2 * (lastBlockShift));

            return newBlocks;
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

        private long[] leftShiftArray(long[] blocks, int shiftingLength) throws Exception {
            int startingBlockIndex = (shiftingLength)/31;
            int nucleotideLength = currentKmerSizeFromBinaryBlockArray(blocks);
            int residueLength = Long.SIZE / 2 - (Long.numberOfTrailingZeros(blocks[blocks.length-1])/2+1); // last block length

            int remainLength=nucleotideLength-shiftingLength-1;
            if (remainLength <0){
                remainLength=0;
            }
            long[] newBlock = new long[remainLength/31+1];
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
                // throw new Exception("shifting length longer than the kmer length");
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
             //   String rightBlocksString = BinaryBlocksToString(rightBlocks);
             //   String leftBlocksString = BinaryBlocksToString(leftBlocks);

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
                //    String rightShift= BinaryBlocksToString(rightBlocksLeftShiftedArray);
                    //  System.out.println("rightShift: " + rightShift);
                }

              //  String mergedKmer= BinaryBlocksToString(newBlocks);

                //System.out.println(" left Blocks:" + leftBlocksString + " Right blocks: " + rightBlocksString + " rightLength: " + rightNucleotideLength + " leftNucleotideLength: " + leftNucleotideLength + " leftRelativeNTLength: " + leftRelativeNTLength + " leftVacancy: " + leftVacancy + " rightNucleotideLength: " + rightNucleotideLength + " combinedBlockSize: " + combinedBlockSize + " newBlock: " + mergedKmer);
            }

            return newBlocks;
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

        private boolean dynamicSubKmerComparator(Seq a, Seq b) throws Exception {
            long[] arrayA = seq2array(a);
            long[] arrayB = seq2array(b);

            int aLength= currentKmerSizeFromBinaryBlockArray(arrayA);
            int bLength= currentKmerSizeFromBinaryBlockArray(arrayB);

            if (aLength>bLength){ // equal should not happen
                long[] shorterVersion = leftShiftOutFromArray(arrayA, bLength);
            //    String longer = BinaryBlocksToString(shorterVersion);
            //    String shorter = BinaryBlocksToString(arrayB);
                // System.out.println("longer: " + longer + " shorter: " + shorter);
                // if (shorterVersion.length>=2 && arrayB.length >=2) {
                //    System.out.println("longer array: " + shorterVersion[0] + " "  + shorterVersion[1] + " shorter array: " + arrayB[0] + " " + arrayB[1]);
                //}
                if (Arrays.equals(shorterVersion, arrayB)){
                    //  if (shorterVersion.length>=2){
                    //        System.out.println("marker!!!");
                    // }
                    return true;
                }else{
                    return false;
                }
            }else{
                long[] shorterVersion = leftShiftOutFromArray(arrayB, aLength);
                if (Arrays.equals(shorterVersion, arrayA)){
                    return true;
                }else{
                    return false;
                }
            }
        }

        private long[] seq2array(Seq a){
            long[] array =new long[a.length()];
            for (int i = 0; i < a.length(); i++) {
                array[i] = (Long) a.apply(i);
            }
            return array;
        }

        private String BinaryBlocksToString (long[] binaryBlocks){
            String KmerString="";
            int KmerLength = currentKmerSizeFromBinaryBlockArray(binaryBlocks);

            for (int i=0; i< KmerLength; i++){
                Long currentNucleotideBinary = binaryBlocks[i/31] >>> 2 * (32 - (i%31+1));
                currentNucleotideBinary &= 3L;
                char currentNucleotide = BinaryToNucleotide(currentNucleotideBinary);
                KmerString += currentNucleotide;
            }

            return KmerString;
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

        private long onlyChangeReflexivMarker(long oldMarker, int reflexivMarker){
            long maxSubKmerBinary = ~((~0L) << 2 * 31);
            long newMarker = oldMarker & maxSubKmerBinary;
            newMarker |= ((long) reflexivMarker) << 2*(32-1);
            return newMarker;
        }
    }

    class LeftLongerToShorterComparisonPreparation implements MapPartitionsFunction<Row, Row>, Serializable{
        List<Row> kmerList = new ArrayList<Row>();

        public Iterator<Row> call(Iterator<Row> s) throws Exception {
            long[] seqBlocks;
            int kmerLength;
            long[] subKmer;
            long[] extension;
            long attribute;

         //   Timestamp timestamp = new Timestamp(System.currentTimeMillis());
         //   System.out.println(timestamp+ "RepeatCheck LeftLongerToShorterComparisonPreparation: " + param.kmerSize1);

            while (s.hasNext()) {
                Row fullKmer = s.next();
                seqBlocks= seq2array(fullKmer.getSeq(0));
                kmerLength=currentKmerSizeFromBinaryBlockArray(seqBlocks);

                subKmer = leftShiftOutFromArray(seqBlocks, kmerLength-1);
                extension=leftShiftArray(seqBlocks, kmerLength-1);
                subKmer = reverseBinaryBlocks(subKmer);

                attribute=onlyChangeReflexivMarker(fullKmer.getLong(1), 1);

                kmerList.add(RowFactory.create(subKmer, attribute, extension[0]));
            }

            return kmerList.iterator();
        }

        private long[] reverseBinaryBlocks(long[] blocks){
            int length = currentKmerSizeFromBinaryBlockArray(blocks);
            int blockNumber= blocks.length;
            long[] newBlocks= new long[blockNumber];
            int reverseIndex;
            int reverseBlockIndex;
            int relativeReverseIndex;

            int forwardBlockIndex;

            long twoBits;
            for (int i=0; i<length;i++){
                reverseIndex=length-i-1;
                reverseBlockIndex=reverseIndex/31;
                relativeReverseIndex=reverseIndex%31;

                forwardBlockIndex=i/31;

                twoBits=blocks[reverseBlockIndex] >>>2*(31-relativeReverseIndex);
                twoBits&=3L;

                newBlocks[forwardBlockIndex]|=twoBits;
                newBlocks[forwardBlockIndex] <<=2;
            }
            int lastBlockShift=31-(length-1)%31-1;
            newBlocks[newBlocks.length-1] <<=2*lastBlockShift;
            newBlocks[newBlocks.length - 1] |= (1L << 2 * (lastBlockShift));

            return newBlocks;
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

        private long[] leftShiftArray(long[] blocks, int shiftingLength) throws Exception {
            int startingBlockIndex = (shiftingLength)/31;
            int nucleotideLength = currentKmerSizeFromBinaryBlockArray(blocks);
            int residueLength = Long.SIZE / 2 - (Long.numberOfTrailingZeros(blocks[blocks.length-1])/2+1); // last block length

            int remainLength=nucleotideLength-shiftingLength-1;
            if (remainLength <0){
                remainLength=0;
            }
            long[] newBlock = new long[remainLength/31+1];
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
                // throw new Exception("shifting length longer than the kmer length");
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
         //       String rightBlocksString = BinaryBlocksToString(rightBlocks);
          //      String leftBlocksString = BinaryBlocksToString(leftBlocks);

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
             //       String rightShift= BinaryBlocksToString(rightBlocksLeftShiftedArray);
                    //  System.out.println("rightShift: " + rightShift);
                }

          //      String mergedKmer= BinaryBlocksToString(newBlocks);

                //System.out.println(" left Blocks:" + leftBlocksString + " Right blocks: " + rightBlocksString + " rightLength: " + rightNucleotideLength + " leftNucleotideLength: " + leftNucleotideLength + " leftRelativeNTLength: " + leftRelativeNTLength + " leftVacancy: " + leftVacancy + " rightNucleotideLength: " + rightNucleotideLength + " combinedBlockSize: " + combinedBlockSize + " newBlock: " + mergedKmer);
            }

            return newBlocks;
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

        private boolean dynamicSubKmerComparator(Seq a, Seq b) throws Exception {
            long[] arrayA = seq2array(a);
            long[] arrayB = seq2array(b);

            int aLength= currentKmerSizeFromBinaryBlockArray(arrayA);
            int bLength= currentKmerSizeFromBinaryBlockArray(arrayB);

            if (aLength>bLength){ // equal should not happen
                long[] shorterVersion = leftShiftOutFromArray(arrayA, bLength);
            //    String longer = BinaryBlocksToString(shorterVersion);
            //    String shorter = BinaryBlocksToString(arrayB);
                // System.out.println("longer: " + longer + " shorter: " + shorter);
                // if (shorterVersion.length>=2 && arrayB.length >=2) {
                //    System.out.println("longer array: " + shorterVersion[0] + " "  + shorterVersion[1] + " shorter array: " + arrayB[0] + " " + arrayB[1]);
                //}
                if (Arrays.equals(shorterVersion, arrayB)){
                    //  if (shorterVersion.length>=2){
                    //        System.out.println("marker!!!");
                    // }
                    return true;
                }else{
                    return false;
                }
            }else{
                long[] shorterVersion = leftShiftOutFromArray(arrayB, aLength);
                if (Arrays.equals(shorterVersion, arrayA)){
                    return true;
                }else{
                    return false;
                }
            }
        }

        private long[] seq2array(Seq a){
            long[] array =new long[a.length()];
            for (int i = 0; i < a.length(); i++) {
                array[i] = (Long) a.apply(i);
            }
            return array;
        }

        private String BinaryBlocksToString (long[] binaryBlocks){
            String KmerString="";
            int KmerLength = currentKmerSizeFromBinaryBlockArray(binaryBlocks);

            for (int i=0; i< KmerLength; i++){
                Long currentNucleotideBinary = binaryBlocks[i/31] >>> 2 * (32 - (i%31+1));
                currentNucleotideBinary &= 3L;
                char currentNucleotide = BinaryToNucleotide(currentNucleotideBinary);
                KmerString += currentNucleotide;
            }

            return KmerString;
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

        private long onlyChangeReflexivMarker(long oldMarker, int reflexivMarker){
            long maxSubKmerBinary = ~((~0L) << 2 * 31);
            long newMarker = oldMarker & maxSubKmerBinary;
            newMarker |= ((long) reflexivMarker) << 2*(32-1);
            return newMarker;
        }
    }

    class RightLongerKmerVariantAdjustmentAndNeutralization implements MapPartitionsFunction<Row, Row>, Serializable{
        List<Row> kmerList= new ArrayList<Row>();
        Row secondLastKmer;
        Row lastKmer;

        public Iterator<Row> call(Iterator<Row> s) throws Exception {
       //     Timestamp timestamp = new Timestamp(System.currentTimeMillis());
       //     System.out.println(timestamp + "RepeatCheck RightLongerKmerVariantAdjustmentAndNeutralization: " + param.kmerSize1);

            while (s.hasNext()) {
                Row fullKmer = s.next();

                if (secondLastKmer == null) {
                    secondLastKmer=fullKmer;
                } else if (lastKmer == null){
                    lastKmer=fullKmer;
                } else {
                    int currentLength= currentKmerSizeFromBinaryBlockArray(seq2array(fullKmer.getSeq(0)));
                    int lastLength = currentKmerSizeFromBinaryBlockArray(seq2array(lastKmer.getSeq(0)));
                    int secondLastLength= currentKmerSizeFromBinaryBlockArray(seq2array(secondLastKmer.getSeq(0)));

                    // -----
                    // ?
                    // ?
                    if (secondLastLength==param.kmerSize1-1){ //shorter
                        // -----
                        // -----
                        // ?
                        if (lastLength==param.kmerSize1-1){ // another shorter
                            // kmerList.add(secondLastKmer);
                            // kmerList.add(lastKmer);

                            // -----
                            // -----
                            // -----
                            if (currentLength==param.kmerSize1-1) {
                                kmerList.add(secondLastKmer);
                                kmerList.add(lastKmer);
                                secondLastKmer=fullKmer;
                                lastKmer=null;
                            }
                            // -----
                            // -----
                            // --------
                            else{

                                if (dynamicSubKmerComparator(fullKmer.getSeq(0),lastKmer.getSeq(0))){
                                   /*
                                    long attribute=fullKmer.getLong(1);
                                    if (getLeftMarker(lastKmer.getLong(1))<0 && getLeftMarker(fullKmer.getLong(1))>=0){
                                        attribute= buildingAlongFromThreeInt(getReflexivMarker(fullKmer.getLong(1)), -1, getRightMarker(fullKmer.getLong(1)));
                                    }

                                    if (lastKmer.getLong(2) == fullKmer.getLong(2)){
                                        fullKmer=RowFactory.create(fullKmer.getSeq(0),attribute, fullKmer.getLong(2));
                                    }else{
                                        fullKmer=RowFactory.create(fullKmer.getSeq(0),attribute, lastKmer.getLong(2));
                                    }
*/
                                    kmerList.add(secondLastKmer);

                                    secondLastKmer=lastKmer;
                                    lastKmer=fullKmer;
                                }else {

                                    kmerList.add(secondLastKmer);
                                    kmerList.add(lastKmer);

                                    secondLastKmer=fullKmer;
                                    lastKmer=null;
                                }
                            }
                        }
                        // -----
                        // --------
                        // ?
                        else{ // longer
                            // -----
                            // --------
                            // -----
                            if (currentLength==param.kmerSize1-1){
                                if (dynamicSubKmerComparator(lastKmer.getSeq(0), secondLastKmer.getSeq(0))){
                                    long attribute=lastKmer.getLong(1);
                                    if (getLeftMarker(secondLastKmer.getLong(1))<0 && getLeftMarker(lastKmer.getLong(1))>=0){
                                        attribute= buildingAlongFromThreeInt(getReflexivMarker(lastKmer.getLong(1)), -1, getRightMarker(lastKmer.getLong(1)));
                                    }

                                    if (secondLastKmer.getLong(2)== lastKmer.getLong(2)){
                                        lastKmer = RowFactory.create(lastKmer.getSeq(0), attribute, lastKmer.getLong(2));
                                    }else{
                                        lastKmer= RowFactory.create(lastKmer.getSeq(0), attribute, secondLastKmer.getLong(2));
                                    }

                                    // kmerList.add(secondLastKmer);
                                    kmerList.add(lastKmer);

                                    secondLastKmer= fullKmer;
                                    lastKmer=null;
                                }else if(dynamicSubKmerComparator(fullKmer.getSeq(0), lastKmer.getSeq(0))){ // need to be decided together with the next in coming k-mer
                                    kmerList.add(secondLastKmer);

                                    secondLastKmer= lastKmer;
                                    lastKmer=fullKmer;
                                }else{
                                    kmerList.add(secondLastKmer);
                                    kmerList.add(lastKmer);

                                    secondLastKmer=fullKmer;
                                    lastKmer=null;

                                }
                            }
                            // -----
                            // --------
                            // --------
                            else{
                                if (dynamicSubKmerComparator(lastKmer.getSeq(0), secondLastKmer.getSeq(0)) && dynamicSubKmerComparator(fullKmer.getSeq(0), secondLastKmer.getSeq(0))){
                                    /*
                                    long attribute=lastKmer.getLong(1);
                                    if (getRightMarker(secondLastKmer.getLong(1))<0 && getRightMarker(lastKmer.getLong(1))>=0){
                                        attribute= buildingAlongFromThreeInt(getReflexivMarker(lastKmer.getLong(1)), getLeftMarker(lastKmer.getLong(1)), -1);
                                    }

                                    lastKmer=RowFactory.create(lastKmer.getSeq(0), attribute, lastKmer.getLong(2));

                                    if (getRightMarker(secondLastKmer.getLong(1))<0 && getRightMarker(fullKmer.getLong(1))>=0){
                                        attribute= buildingAlongFromThreeInt(getReflexivMarker(fullKmer.getLong(1)), getLeftMarker(fullKmer.getLong(1)), -1);
                                    }

                                    fullKmer = RowFactory.create(fullKmer.getSeq(0),attribute, fullKmer.getLong(2));
*/
                                    if (secondLastKmer.getLong(2) == lastKmer.getLong(2) || secondLastKmer.getLong(2) ==fullKmer.getLong(2)){
                                        kmerList.add(lastKmer);
                                        kmerList.add(fullKmer);
                                    }else {
                                        kmerList.add(secondLastKmer);
                                        kmerList.add(lastKmer);
                                        kmerList.add(fullKmer);
                                    }

                                    secondLastKmer=null;
                                    lastKmer=null;
                                }else if (dynamicSubKmerComparator(lastKmer.getSeq(0), secondLastKmer.getSeq(0))){
                                    long attribute=lastKmer.getLong(1);
                                    if (getLeftMarker(secondLastKmer.getLong(1))<0 && getLeftMarker(lastKmer.getLong(1))>=0){
                                        attribute= buildingAlongFromThreeInt(getReflexivMarker(lastKmer.getLong(1)), -1, getRightMarker(lastKmer.getLong(1)));
                                    }

                                    if (lastKmer.getLong(2) == secondLastKmer.getLong(2)){
                                        lastKmer=RowFactory.create(lastKmer.getSeq(0),attribute, lastKmer.getLong(2));
                                    }else{
                                        lastKmer=RowFactory.create(lastKmer.getSeq(0),attribute, secondLastKmer.getLong(2));
                                    }

                                    // kmerList.add(secondLastKmer);
                                    kmerList.add(lastKmer);
                                    secondLastKmer=fullKmer;
                                    lastKmer=null;
                                }else{ // the two long kmer will be decided together with the next incoming k-mer
                                    kmerList.add(secondLastKmer);

                                    secondLastKmer = lastKmer;
                                    lastKmer= fullKmer;
                                }
                            }
                        }
                    }
                    // --------
                    // ?
                    // ?
                    else{
                        // --------
                        // -----
                        // ?
                        if (lastLength==param.kmerSize1-1){
                            // --------
                            // -----
                            // -----
                            if (currentLength==param.kmerSize1-1){
                                if (dynamicSubKmerComparator(secondLastKmer.getSeq(0),lastKmer.getSeq(0))){
                                    long attribute=secondLastKmer.getLong(1);
                                    if (getLeftMarker(lastKmer.getLong(1))<0 && getLeftMarker(secondLastKmer.getLong(1))>=0){
                                        attribute= buildingAlongFromThreeInt(getReflexivMarker(secondLastKmer.getLong(1)), -1, getRightMarker(secondLastKmer.getLong(1)));
                                    }

                                    if (secondLastKmer.getLong(2) == lastKmer.getLong(2)){
                                        secondLastKmer=RowFactory.create(secondLastKmer.getSeq(0),attribute, secondLastKmer.getLong(2));
                                    }else{
                                        secondLastKmer=RowFactory.create(secondLastKmer.getSeq(0),attribute, lastKmer.getLong(2));
                                    }

                                    kmerList.add(secondLastKmer);
                                  //  kmerList.add(lastKmer);

                                    secondLastKmer=fullKmer;
                                    lastKmer=null;
                                }else{
                                    kmerList.add(secondLastKmer);
                                    kmerList.add(lastKmer);

                                    secondLastKmer=fullKmer;
                                    lastKmer=null;
                                }
                            }
                            // --------
                            // -----
                            // --------
                            else{
                                if (dynamicSubKmerComparator(secondLastKmer.getSeq(0),lastKmer.getSeq(0)) && dynamicSubKmerComparator(fullKmer.getSeq(0),lastKmer.getSeq(0)) ){
                                    if (secondLastKmer.getLong(2) == lastKmer.getLong(2) || lastKmer.getLong(2) ==fullKmer.getLong(2)){
                                        kmerList.add(secondLastKmer);
                                        kmerList.add(fullKmer);
                                    }else {
                                        kmerList.add(secondLastKmer);
                                        kmerList.add(lastKmer);
                                        kmerList.add(fullKmer);
                                    }

                                    secondLastKmer=null;
                                    lastKmer=null;
                                }else if (dynamicSubKmerComparator(secondLastKmer.getSeq(0),lastKmer.getSeq(0))){
                                    long attribute=secondLastKmer.getLong(1);
                                    if (getLeftMarker(lastKmer.getLong(1))<0 && getLeftMarker(secondLastKmer.getLong(1))>=0){
                                        attribute= buildingAlongFromThreeInt(getReflexivMarker(secondLastKmer.getLong(1)), -1, getRightMarker(secondLastKmer.getLong(1)));
                                    }

                                    if (secondLastKmer.getLong(2) == lastKmer.getLong(2)){
                                        secondLastKmer=RowFactory.create(secondLastKmer.getSeq(0),attribute, secondLastKmer.getLong(2));
                                    }else{
                                        secondLastKmer=RowFactory.create(secondLastKmer.getSeq(0),attribute, lastKmer.getLong(2));
                                    }

                                    kmerList.add(secondLastKmer);
                                //    kmerList.add(lastKmer);

                                    secondLastKmer=fullKmer;
                                    lastKmer=null;
                                }else if (dynamicSubKmerComparator(fullKmer.getSeq(0),lastKmer.getSeq(0))){ // the last two need to be decided together with the incoming k-mer

                                    kmerList.add(secondLastKmer);
                                    secondLastKmer= lastKmer;
                                    lastKmer=fullKmer;

                                }else{
                                    kmerList.add(secondLastKmer);
                                    kmerList.add(lastKmer);

                                    secondLastKmer=fullKmer;
                                    lastKmer=null;
                                }
                            }
                        }
                        // --------
                        // --------
                        // ?
                        else{
                            // --------
                            // --------
                            // -----
                            if (currentLength==param.kmerSize1-1){
                                if (dynamicSubKmerComparator(secondLastKmer.getSeq(0),fullKmer.getSeq(0)) && dynamicSubKmerComparator(fullKmer.getSeq(0),lastKmer.getSeq(0)) ){
                                    if (secondLastKmer.getLong(2) == fullKmer.getLong(2) || lastKmer.getLong(2) ==fullKmer.getLong(2)){
                                        kmerList.add(secondLastKmer);
                                        kmerList.add(lastKmer);
                                    }else {
                                        kmerList.add(secondLastKmer);
                                        kmerList.add(lastKmer);
                                        kmerList.add(fullKmer);
                                    }

                                    secondLastKmer=null;
                                    lastKmer=null;
                                }else if (dynamicSubKmerComparator(fullKmer.getSeq(0),lastKmer.getSeq(0))){ // the last two need to be decided together with the incoming kmer
                                    kmerList.add(secondLastKmer);
                                    secondLastKmer=lastKmer;
                                    lastKmer=fullKmer;
                                }else{
                                    kmerList.add(secondLastKmer);
                                    kmerList.add(lastKmer);

                                    secondLastKmer=fullKmer;
                                    lastKmer=null;
                                }
                            }
                            // --------
                            // --------
                            // --------
                            else{ // the last two need to be decided together with the incoming k-mer
                                kmerList.add(secondLastKmer);

                                secondLastKmer=lastKmer;
                                lastKmer = fullKmer;
                            }
                        }
                    }
                }
            }

            if (secondLastKmer!=null){
                if (lastKmer!=null){
                    int secondLastLength = currentKmerSizeFromBinaryBlockArray(seq2array(secondLastKmer.getSeq(0)));
                    int lastLength = currentKmerSizeFromBinaryBlockArray(seq2array(lastKmer.getSeq(0)));
                    // -----
                    // ?
                    if (secondLastLength==param.kmerSize1-1){
                        // -----
                        // -----
                        if (lastLength==param.kmerSize1-1){
                            kmerList.add(secondLastKmer);
                            kmerList.add(lastKmer);
                        }
                        // -----
                        // --------
                        else{
                            if (dynamicSubKmerComparator(secondLastKmer.getSeq(0),lastKmer.getSeq(0))){
                                long attribute=lastKmer.getLong(1);
                                if (getLeftMarker(secondLastKmer.getLong(1))<0 && getLeftMarker(lastKmer.getLong(1))>=0){
                                    attribute= buildingAlongFromThreeInt(getReflexivMarker(lastKmer.getLong(1)), -1, getRightMarker(lastKmer.getLong(1)));
                                }

                                if (secondLastKmer.getLong(2) == lastKmer.getLong(2)){
                                    lastKmer=RowFactory.create(lastKmer.getSeq(0),attribute, lastKmer.getLong(2));
                                }else{
                                    lastKmer=RowFactory.create(lastKmer.getSeq(0),attribute, secondLastKmer.getLong(2));
                                }

                                // kmerList.add(secondLastKmer);
                                kmerList.add(lastKmer);
                            }else{
                                kmerList.add(secondLastKmer);
                                kmerList.add(lastKmer);
                            }
                        }
                    }
                    // --------
                    // ?
                    else{
                        // --------
                        // -----
                        if (lastLength==param.kmerSize1-1){
                            if (dynamicSubKmerComparator(secondLastKmer.getSeq(0),lastKmer.getSeq(0))){
                                long attribute=secondLastKmer.getLong(1);
                                if (getLeftMarker(lastKmer.getLong(1))<0 && getLeftMarker(secondLastKmer.getLong(1))>=0){
                                    attribute= buildingAlongFromThreeInt(getReflexivMarker(secondLastKmer.getLong(1)), -1,  getRightMarker(secondLastKmer.getLong(1)));
                                }

                                if (secondLastKmer.getLong(2) == lastKmer.getLong(2)){
                                    secondLastKmer=RowFactory.create(secondLastKmer.getSeq(0),attribute, secondLastKmer.getLong(2));
                                }else{
                                    secondLastKmer=RowFactory.create(secondLastKmer.getSeq(0),attribute, lastKmer.getLong(2));
                                }

                                kmerList.add(secondLastKmer);
                                // kmerList.add(lastKmer);
                            }
                        }
                        // --------
                        // --------
                        else{
                            kmerList.add(secondLastKmer);
                            kmerList.add(lastKmer);
                        }
                    }
                }else{
                    kmerList.add(secondLastKmer);
                }
            }else if (lastKmer !=null){
                kmerList.add(lastKmer);
            }

            return kmerList.iterator();
        }

        private long[] reverseBinaryBlocks(long[] blocks){
            int length = currentKmerSizeFromBinaryBlockArray(blocks);
            int blockNumber= blocks.length;
            long[] newBlocks= new long[blockNumber];
            int reverseIndex;
            int reverseBlockIndex;
            int relativeReverseIndex;

            int forwardBlockIndex;

            long twoBits;
            for (int i=0; i<length;i++){
                reverseIndex=length-i-1;
                reverseBlockIndex=reverseIndex/31;
                relativeReverseIndex=reverseIndex%31;

                forwardBlockIndex=i/31;

                twoBits=blocks[reverseBlockIndex] >>>2*(31-relativeReverseIndex);
                twoBits&=3L;

                newBlocks[forwardBlockIndex]|=twoBits;
                newBlocks[forwardBlockIndex] <<=2;
            }
            int lastBlockShift=31-(length-1)%31-1;
            newBlocks[newBlocks.length-1] <<=2*lastBlockShift;
            newBlocks[newBlocks.length - 1] |= (1L << 2 * (lastBlockShift));

            return newBlocks;
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

        private long[] leftShiftArray(long[] blocks, int shiftingLength) throws Exception {
            int startingBlockIndex = (shiftingLength)/31;
            int nucleotideLength = currentKmerSizeFromBinaryBlockArray(blocks);
            int residueLength = Long.SIZE / 2 - (Long.numberOfTrailingZeros(blocks[blocks.length-1])/2+1); // last block length

            int remainLength=nucleotideLength-shiftingLength-1;
            if (remainLength <0){
                remainLength=0;
            }
            long[] newBlock = new long[remainLength/31+1];
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
                // throw new Exception("shifting length longer than the kmer length");
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
            //    String rightBlocksString = BinaryBlocksToString(rightBlocks);
            //    String leftBlocksString = BinaryBlocksToString(leftBlocks);

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
             //       String rightShift= BinaryBlocksToString(rightBlocksLeftShiftedArray);
                    //  System.out.println("rightShift: " + rightShift);
                }

             //   String mergedKmer= BinaryBlocksToString(newBlocks);

                //System.out.println(" left Blocks:" + leftBlocksString + " Right blocks: " + rightBlocksString + " rightLength: " + rightNucleotideLength + " leftNucleotideLength: " + leftNucleotideLength + " leftRelativeNTLength: " + leftRelativeNTLength + " leftVacancy: " + leftVacancy + " rightNucleotideLength: " + rightNucleotideLength + " combinedBlockSize: " + combinedBlockSize + " newBlock: " + mergedKmer);
            }

            return newBlocks;
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

        private boolean dynamicSubKmerComparator(Seq a, Seq b) throws Exception {
            long[] arrayA = seq2array(a);
            long[] arrayB = seq2array(b);

            int aLength= currentKmerSizeFromBinaryBlockArray(arrayA);
            int bLength= currentKmerSizeFromBinaryBlockArray(arrayB);

            if (aLength>bLength){ // equal should not happen
                long[] shorterVersion = leftShiftOutFromArray(arrayA, bLength);
        //        String longer = BinaryBlocksToString(shorterVersion);
        //        String shorter = BinaryBlocksToString(arrayB);
                // System.out.println("longer: " + longer + " shorter: " + shorter);
                // if (shorterVersion.length>=2 && arrayB.length >=2) {
                //    System.out.println("longer array: " + shorterVersion[0] + " "  + shorterVersion[1] + " shorter array: " + arrayB[0] + " " + arrayB[1]);
                //}
                if (Arrays.equals(shorterVersion, arrayB)){
                    //  if (shorterVersion.length>=2){
                    //        System.out.println("marker!!!");
                    // }
                    return true;
                }else{
                    return false;
                }
            }else{
                long[] shorterVersion = leftShiftOutFromArray(arrayB, aLength);
                if (Arrays.equals(shorterVersion, arrayA)){
                    return true;
                }else{
                    return false;
                }
            }
        }

        private long[] seq2array(Seq a){
            long[] array =new long[a.length()];
            for (int i = 0; i < a.length(); i++) {
                array[i] = (Long) a.apply(i);
            }
            return array;
        }

        private String BinaryBlocksToString (long[] binaryBlocks){
            String KmerString="";
            int KmerLength = currentKmerSizeFromBinaryBlockArray(binaryBlocks);

            for (int i=0; i< KmerLength; i++){
                Long currentNucleotideBinary = binaryBlocks[i/31] >>> 2 * (32 - (i%31+1));
                currentNucleotideBinary &= 3L;
                char currentNucleotide = BinaryToNucleotide(currentNucleotideBinary);
                KmerString += currentNucleotide;
            }

            return KmerString;
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

        private long onlyChangeReflexivMarker(long oldMarker, int reflexivMarker){
            long maxSubKmerBinary = ~((~0L) << 2 * 31);
            long newMarker = oldMarker & maxSubKmerBinary;
            newMarker |= ((long) reflexivMarker) << 2*(32-1);
            return newMarker;
        }
    }

    class LeftLongerKmerVariantAdjustment implements MapPartitionsFunction<Row, Row>, Serializable{
        List<Row> kmerList = new ArrayList<Row>();
        Row secondLastKmer;
        Row lastKmer;

        public Iterator<Row> call(Iterator<Row> s) throws Exception {
        //    Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        //    System.out.println(timestamp+ "RepeatCheck LeftLongerKmerVariantAdjustment: " + param.kmerSize1);

            while (s.hasNext()) {
                Row fullKmer = s.next();

                if (secondLastKmer == null) {
                    secondLastKmer=fullKmer;
                } else if (lastKmer == null){
                    lastKmer=fullKmer;
                } else {
                    int currentLength= currentKmerSizeFromBinaryBlockArray(seq2array(fullKmer.getSeq(0)));
                    int lastLength = currentKmerSizeFromBinaryBlockArray(seq2array(lastKmer.getSeq(0)));
                    int secondLastLength= currentKmerSizeFromBinaryBlockArray(seq2array(secondLastKmer.getSeq(0)));

                    // -----
                    // ?
                    // ?
                    if (secondLastLength==param.kmerSize1-1){ //shorter
                        // -----
                        // -----
                        // ?
                        if (lastLength==param.kmerSize1-1){ // another shorter
                           // kmerList.add(secondLastKmer);
                           // kmerList.add(lastKmer);

                            // -----
                            // -----
                            // -----
                            if (currentLength==param.kmerSize1-1) {
                                kmerList.add(secondLastKmer);
                                kmerList.add(lastKmer);
                                secondLastKmer=fullKmer;
                                lastKmer=null;
                            }
                            // -----
                            // -----
                            // --------
                            else{

                                if (dynamicSubKmerComparator(fullKmer.getSeq(0),lastKmer.getSeq(0))){
                                    /*
                                    long attribute=fullKmer.getLong(1);
                                    if (getRightMarker(lastKmer.getLong(1))<0 && getRightMarker(fullKmer.getLong(1))>=0){
                                        attribute= buildingAlongFromThreeInt(getReflexivMarker(fullKmer.getLong(1)), getLeftMarker(fullKmer.getLong(1)), -1);
                                    }

                                    if (lastKmer.getLong(2) == fullKmer.getLong(2)){
                                        fullKmer=RowFactory.create(fullKmer.getSeq(0),attribute, fullKmer.getLong(2));
                                    }else{
                                        fullKmer=RowFactory.create(fullKmer.getSeq(0),attribute, lastKmer.getLong(2));
                                    }
*/
                                    kmerList.add(secondLastKmer);

                                    secondLastKmer=lastKmer;
                                    lastKmer=fullKmer;
                                }else {

                                    kmerList.add(secondLastKmer);
                                    kmerList.add(lastKmer);

                                    secondLastKmer=fullKmer;
                                    lastKmer=null;
                                }
                            }
                        }
                        // -----
                        // --------
                        // ?
                        else{ // longer
                            // -----
                            // --------
                            // -----
                            if (currentLength==param.kmerSize1-1){
                                if (dynamicSubKmerComparator(lastKmer.getSeq(0), secondLastKmer.getSeq(0))){
                                    long attribute=lastKmer.getLong(1);
                                    if (getRightMarker(secondLastKmer.getLong(1))<0 && getRightMarker(lastKmer.getLong(1))>=0){
                                        attribute= buildingAlongFromThreeInt(getReflexivMarker(lastKmer.getLong(1)), getLeftMarker(lastKmer.getLong(1)), -1);
                                    }

                                    if (secondLastKmer.getLong(2)== lastKmer.getLong(2)){
                                        lastKmer = RowFactory.create(lastKmer.getSeq(0), attribute, lastKmer.getLong(2));
                                    }else{
                                        lastKmer= RowFactory.create(lastKmer.getSeq(0), attribute, secondLastKmer.getLong(2));
                                    }

                                    kmerList.add(secondLastKmer);
                                    kmerList.add(lastKmer);

                                    secondLastKmer= fullKmer;
                                    lastKmer=null;
                                }else if(dynamicSubKmerComparator(fullKmer.getSeq(0), lastKmer.getSeq(0))){ // need to be decided together with the next in coming k-mer
                                    kmerList.add(secondLastKmer);

                                    secondLastKmer= lastKmer;
                                    lastKmer=fullKmer;
                                }else{
                                    kmerList.add(secondLastKmer);
                                    kmerList.add(lastKmer);

                                    secondLastKmer=fullKmer;
                                    lastKmer=null;

                                }
                            }
                            // -----
                            // --------
                            // --------
                            else{
                                if (dynamicSubKmerComparator(lastKmer.getSeq(0), secondLastKmer.getSeq(0)) && dynamicSubKmerComparator(fullKmer.getSeq(0), secondLastKmer.getSeq(0))){
                                    /*
                                    long attribute=lastKmer.getLong(1);
                                    if (getRightMarker(secondLastKmer.getLong(1))<0 && getRightMarker(lastKmer.getLong(1))>=0){
                                        attribute= buildingAlongFromThreeInt(getReflexivMarker(lastKmer.getLong(1)), getLeftMarker(lastKmer.getLong(1)), -1);
                                    }

                                    lastKmer=RowFactory.create(lastKmer.getSeq(0), attribute, lastKmer.getLong(2));

                                    if (getRightMarker(secondLastKmer.getLong(1))<0 && getRightMarker(fullKmer.getLong(1))>=0){
                                        attribute= buildingAlongFromThreeInt(getReflexivMarker(fullKmer.getLong(1)), getLeftMarker(fullKmer.getLong(1)), -1);
                                    }

                                    fullKmer = RowFactory.create(fullKmer.getSeq(0),attribute, fullKmer.getLong(2));
*/
                                    kmerList.add(secondLastKmer);
                                    kmerList.add(lastKmer);
                                    kmerList.add(fullKmer);

                                    secondLastKmer=null;
                                    lastKmer=null;
                                }else if (dynamicSubKmerComparator(lastKmer.getSeq(0), secondLastKmer.getSeq(0))){
                                    long attribute=lastKmer.getLong(1);
                                    if (getRightMarker(secondLastKmer.getLong(1))<0 && getRightMarker(lastKmer.getLong(1))>=0){
                                        attribute= buildingAlongFromThreeInt(getReflexivMarker(lastKmer.getLong(1)), getLeftMarker(lastKmer.getLong(1)), -1);
                                    }

                                    if (lastKmer.getLong(2) == secondLastKmer.getLong(2)){
                                        lastKmer=RowFactory.create(lastKmer.getSeq(0),attribute, lastKmer.getLong(2));
                                    }else{
                                        lastKmer=RowFactory.create(lastKmer.getSeq(0),attribute, secondLastKmer.getLong(2));
                                    }

                                    kmerList.add(secondLastKmer);
                                    kmerList.add(lastKmer);
                                    secondLastKmer=fullKmer;
                                    lastKmer=null;
                                }else{ // the two long kmer will be decided together with the next incoming k-mer
                                    kmerList.add(secondLastKmer);

                                    secondLastKmer = lastKmer;
                                    lastKmer= fullKmer;
                                }
                            }
                        }
                    }
                    // --------
                    // ?
                    // ?
                    else{
                        // --------
                        // -----
                        // ?
                        if (lastLength==param.kmerSize1-1){
                            // --------
                            // -----
                            // -----
                            if (currentLength==param.kmerSize1-1){
                                if (dynamicSubKmerComparator(secondLastKmer.getSeq(0),lastKmer.getSeq(0))){
                                    long attribute=secondLastKmer.getLong(1);
                                    if (getRightMarker(lastKmer.getLong(1))<0 && getRightMarker(secondLastKmer.getLong(1))>=0){
                                        attribute= buildingAlongFromThreeInt(getReflexivMarker(secondLastKmer.getLong(1)), getLeftMarker(secondLastKmer.getLong(1)), -1);
                                    }

                                    if (secondLastKmer.getLong(2) == lastKmer.getLong(2)){
                                        secondLastKmer=RowFactory.create(secondLastKmer.getSeq(0),attribute, secondLastKmer.getLong(2));
                                    }else{
                                        secondLastKmer=RowFactory.create(secondLastKmer.getSeq(0),attribute, lastKmer.getLong(2));
                                    }

                                    kmerList.add(secondLastKmer);
                                    kmerList.add(lastKmer);

                                    secondLastKmer=fullKmer;
                                    lastKmer=null;
                                }else{
                                    kmerList.add(secondLastKmer);
                                    kmerList.add(lastKmer);

                                    secondLastKmer=fullKmer;
                                    lastKmer=null;
                                }
                            }
                            // --------
                            // -----
                            // --------
                            else{
                                if (dynamicSubKmerComparator(secondLastKmer.getSeq(0),lastKmer.getSeq(0)) && dynamicSubKmerComparator(fullKmer.getSeq(0),lastKmer.getSeq(0)) ){
                                    kmerList.add(secondLastKmer);
                                    kmerList.add(lastKmer);
                                    kmerList.add(fullKmer);

                                    secondLastKmer=null;
                                    lastKmer=null;
                                }else if (dynamicSubKmerComparator(secondLastKmer.getSeq(0),lastKmer.getSeq(0))){
                                    long attribute=secondLastKmer.getLong(1);
                                    if (getRightMarker(lastKmer.getLong(1))<0 && getRightMarker(secondLastKmer.getLong(1))>=0){
                                        attribute= buildingAlongFromThreeInt(getReflexivMarker(secondLastKmer.getLong(1)), getLeftMarker(secondLastKmer.getLong(1)), -1);
                                    }

                                    if (secondLastKmer.getLong(2) == lastKmer.getLong(2)){
                                        secondLastKmer=RowFactory.create(secondLastKmer.getSeq(0),attribute, secondLastKmer.getLong(2));
                                    }else{
                                        secondLastKmer=RowFactory.create(secondLastKmer.getSeq(0),attribute, lastKmer.getLong(2));
                                    }

                                    kmerList.add(secondLastKmer);
                                    kmerList.add(lastKmer);

                                    secondLastKmer=fullKmer;
                                    lastKmer=null;
                                }else if (dynamicSubKmerComparator(fullKmer.getSeq(0),lastKmer.getSeq(0))){ // the last two need to be decided together with the incoming k-mer

                                    kmerList.add(secondLastKmer);
                                    secondLastKmer= lastKmer;
                                    lastKmer=fullKmer;

                                }else{
                                    kmerList.add(secondLastKmer);
                                    kmerList.add(lastKmer);

                                    secondLastKmer=fullKmer;
                                    lastKmer=null;
                                }
                            }
                        }
                        // --------
                        // --------
                        // ?
                        else{
                            // --------
                            // --------
                            // -----
                            if (currentLength==param.kmerSize1-1){
                                if (dynamicSubKmerComparator(secondLastKmer.getSeq(0),fullKmer.getSeq(0)) && dynamicSubKmerComparator(fullKmer.getSeq(0),lastKmer.getSeq(0)) ){
                                    kmerList.add(secondLastKmer);
                                    kmerList.add(lastKmer);
                                    kmerList.add(fullKmer);

                                    secondLastKmer=null;
                                    lastKmer=null;
                                }else if (dynamicSubKmerComparator(fullKmer.getSeq(0),lastKmer.getSeq(0))){ // the last two need to be decided together with the incoming kmer
                                    kmerList.add(secondLastKmer);
                                    secondLastKmer=lastKmer;
                                    lastKmer=fullKmer;
                                }else{
                                    kmerList.add(secondLastKmer);
                                    kmerList.add(lastKmer);

                                    secondLastKmer=fullKmer;
                                    lastKmer=null;
                                }
                            }
                            // --------
                            // --------
                            // --------
                            else{ // the last two need to be decided together with the incoming k-mer
                                kmerList.add(secondLastKmer);

                                secondLastKmer=lastKmer;
                                lastKmer = fullKmer;
                            }
                        }
                    }
                }
            }

            if (secondLastKmer!=null){
                if (lastKmer!=null){
                    int secondLastLength = currentKmerSizeFromBinaryBlockArray(seq2array(secondLastKmer.getSeq(0)));
                    int lastLength = currentKmerSizeFromBinaryBlockArray(seq2array(lastKmer.getSeq(0)));
                    // -----
                    // ?
                    if (secondLastLength==param.kmerSize1-1){
                        // -----
                        // -----
                        if (lastLength==param.kmerSize1-1){
                            kmerList.add(secondLastKmer);
                            kmerList.add(lastKmer);
                        }
                        // -----
                        // --------
                        else{
                            if (dynamicSubKmerComparator(secondLastKmer.getSeq(0),lastKmer.getSeq(0))){
                                long attribute=lastKmer.getLong(1);
                                if (getRightMarker(secondLastKmer.getLong(1))<0 && getRightMarker(lastKmer.getLong(1))>=0){
                                    attribute= buildingAlongFromThreeInt(getReflexivMarker(lastKmer.getLong(1)), getLeftMarker(lastKmer.getLong(1)), -1);
                                }

                                if (secondLastKmer.getLong(2) == lastKmer.getLong(2)){
                                    lastKmer=RowFactory.create(lastKmer.getSeq(0),attribute, lastKmer.getLong(2));
                                }else{
                                    lastKmer=RowFactory.create(lastKmer.getSeq(0),attribute, secondLastKmer.getLong(2));
                                }

                                kmerList.add(secondLastKmer);
                                kmerList.add(lastKmer);
                            }else{
                                kmerList.add(secondLastKmer);
                                kmerList.add(lastKmer);
                            }
                        }
                    }
                    // --------
                    // ?
                    else{
                        // --------
                        // -----
                        if (lastLength==param.kmerSize1-1){
                            if (dynamicSubKmerComparator(secondLastKmer.getSeq(0),lastKmer.getSeq(0))){
                                long attribute=secondLastKmer.getLong(1);
                                if (getRightMarker(lastKmer.getLong(1))<0 && getRightMarker(secondLastKmer.getLong(1))>=0){
                                    attribute= buildingAlongFromThreeInt(getReflexivMarker(secondLastKmer.getLong(1)), getLeftMarker(secondLastKmer.getLong(1)), -1);
                                }

                                if (secondLastKmer.getLong(2) == lastKmer.getLong(2)){
                                    secondLastKmer=RowFactory.create(secondLastKmer.getSeq(0),attribute, secondLastKmer.getLong(2));
                                }else{
                                    secondLastKmer=RowFactory.create(secondLastKmer.getSeq(0),attribute, lastKmer.getLong(2));
                                }

                                kmerList.add(secondLastKmer);
                                kmerList.add(lastKmer);
                            }
                        }
                        // --------
                        // --------
                        else{
                            kmerList.add(secondLastKmer);
                            kmerList.add(lastKmer);
                        }
                    }
                }else{
                    kmerList.add(secondLastKmer);
                }
            }else if (lastKmer !=null){
                kmerList.add(lastKmer);
            }

            return kmerList.iterator();
        }

        private long[] reverseBinaryBlocks(long[] blocks){
            int length = currentKmerSizeFromBinaryBlockArray(blocks);
            int blockNumber= blocks.length;
            long[] newBlocks= new long[blockNumber];
            int reverseIndex;
            int reverseBlockIndex;
            int relativeReverseIndex;

            int forwardBlockIndex;

            long twoBits;
            for (int i=0; i<length;i++){
                reverseIndex=length-i-1;
                reverseBlockIndex=reverseIndex/31;
                relativeReverseIndex=reverseIndex%31;

                forwardBlockIndex=i/31;

                twoBits=blocks[reverseBlockIndex] >>>2*(31-relativeReverseIndex);
                twoBits&=3L;

                newBlocks[forwardBlockIndex]|=twoBits;
                newBlocks[forwardBlockIndex] <<=2;
            }
            int lastBlockShift=31-(length-1)%31-1;
            newBlocks[newBlocks.length-1] <<=2*lastBlockShift;
            newBlocks[newBlocks.length - 1] |= (1L << 2 * (lastBlockShift));

            return newBlocks;
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

        private long[] leftShiftArray(long[] blocks, int shiftingLength) throws Exception {
            int startingBlockIndex = (shiftingLength)/31;
            int nucleotideLength = currentKmerSizeFromBinaryBlockArray(blocks);
            int residueLength = Long.SIZE / 2 - (Long.numberOfTrailingZeros(blocks[blocks.length-1])/2+1); // last block length

            int remainLength=nucleotideLength-shiftingLength-1;
            if (remainLength <0){
                remainLength=0;
            }
            long[] newBlock = new long[remainLength/31+1];
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
                // throw new Exception("shifting length longer than the kmer length");
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
             //   String rightBlocksString = BinaryBlocksToString(rightBlocks);
             //   String leftBlocksString = BinaryBlocksToString(leftBlocks);

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
              //      String rightShift= BinaryBlocksToString(rightBlocksLeftShiftedArray);
                    //  System.out.println("rightShift: " + rightShift);
                }

              //  String mergedKmer= BinaryBlocksToString(newBlocks);

                //System.out.println(" left Blocks:" + leftBlocksString + " Right blocks: " + rightBlocksString + " rightLength: " + rightNucleotideLength + " leftNucleotideLength: " + leftNucleotideLength + " leftRelativeNTLength: " + leftRelativeNTLength + " leftVacancy: " + leftVacancy + " rightNucleotideLength: " + rightNucleotideLength + " combinedBlockSize: " + combinedBlockSize + " newBlock: " + mergedKmer);
            }

            return newBlocks;
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

        private boolean dynamicSubKmerComparator(Seq a, Seq b) throws Exception {
            long[] arrayA = seq2array(a);
            long[] arrayB = seq2array(b);

            int aLength= currentKmerSizeFromBinaryBlockArray(arrayA);
            int bLength= currentKmerSizeFromBinaryBlockArray(arrayB);

            if (aLength>bLength){ // equal should not happen
                long[] shorterVersion = leftShiftOutFromArray(arrayA, bLength);
            //    String longer = BinaryBlocksToString(shorterVersion);
            //    String shorter = BinaryBlocksToString(arrayB);
                // System.out.println("longer: " + longer + " shorter: " + shorter);
                // if (shorterVersion.length>=2 && arrayB.length >=2) {
                //    System.out.println("longer array: " + shorterVersion[0] + " "  + shorterVersion[1] + " shorter array: " + arrayB[0] + " " + arrayB[1]);
                //}
                if (Arrays.equals(shorterVersion, arrayB)){
                    //  if (shorterVersion.length>=2){
                    //        System.out.println("marker!!!");
                    // }
                    return true;
                }else{
                    return false;
                }
            }else{
                long[] shorterVersion = leftShiftOutFromArray(arrayB, aLength);
                if (Arrays.equals(shorterVersion, arrayA)){
                    return true;
                }else{
                    return false;
                }
            }
        }

        private long[] seq2array(Seq a){
            long[] array =new long[a.length()];
            for (int i = 0; i < a.length(); i++) {
                array[i] = (Long) a.apply(i);
            }
            return array;
        }

        private String BinaryBlocksToString (long[] binaryBlocks){
            String KmerString="";
            int KmerLength = currentKmerSizeFromBinaryBlockArray(binaryBlocks);

            for (int i=0; i< KmerLength; i++){
                Long currentNucleotideBinary = binaryBlocks[i/31] >>> 2 * (32 - (i%31+1));
                currentNucleotideBinary &= 3L;
                char currentNucleotide = BinaryToNucleotide(currentNucleotideBinary);
                KmerString += currentNucleotide;
            }

            return KmerString;
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

        private long onlyChangeReflexivMarker(long oldMarker, int reflexivMarker){
            long maxSubKmerBinary = ~((~0L) << 2 * 31);
            long newMarker = oldMarker & maxSubKmerBinary;
            newMarker |= ((long) reflexivMarker) << 2*(32-1);
            return newMarker;
        }
    }

    /**
     *
     */
    class ShorterKmerNeutralization implements MapPartitionsFunction<Row, Row>, Serializable{
        List<Row> LongerFullKmer = new ArrayList<Row>();
        List<Row> newFullKmerList = new ArrayList<Row>();
        Row shorterFullKmer;
        List<Row> tempLongerFullKmer = new ArrayList<Row>();
        boolean neutralizeMarker = false;

        public Iterator<Row> call(Iterator<Row> s) throws Exception {
     //       Timestamp timestamp = new Timestamp(System.currentTimeMillis());
      //      System.out.println(timestamp + "RepeatCheck ShorterKmerNeutralization: " + param.kmerSize1);


            while (s.hasNext()) {
                Row fullKmer = s.next();

  //              String fullKmerString = BinaryBlocksToString(seq2array(FullKmer.getSeq(0)));
               // System.out.println("neutralization kmer: " +  fullKmerString);
/*
                long[] FullKmerArray = seq2array(FullKmer.getSeq(0));
                int FullKmerLength = currentKmerSizeFromBinaryBlockArray(FullKmerArray);
                if (FullKmerLength == param.kmerSize1) { // shorter FullKmer size  =  param.kmerSize1 -1
                    if (shorterFullKmer != null) { // already one exists
                        if (tempLongerFullKmer.size() > 0) {
                            if (getLeftMarker(shorterFullKmer.getLong(1)) > 0) {
                                for (int i = 0; i < tempLongerFullKmer.size(); i++) {
                                    if (dynamicSubKmerComparator(shorterFullKmer.getSeq(0), tempLongerFullKmer.get(i).getSeq(0)) == true) {
                                        neutralizeMarker=true;
                                        newFullKmerList.add(enlighteningLeft(tempLongerFullKmer.get(i), true));
                                    } else {
                                        newFullKmerList.add(enlighteningLeft(tempLongerFullKmer.get(i), false));
                                    }
                                }
                            } else { // adding temp to output without changing
                                for (int i = 0; i < tempLongerFullKmer.size(); i++) {
                                    if (dynamicSubKmerComparator(shorterFullKmer.getSeq(0), tempLongerFullKmer.get(i).getSeq(0)) == true) {
                                        neutralizeMarker=true;
                                    }
                                    newFullKmerList.add(enlighteningLeft(tempLongerFullKmer.get(i), false));
                                }
                            }
                            tempLongerFullKmer=new ArrayList<Row>();
                        }
                    }

                    if(neutralizeMarker==true){
                        neutralizeMarker=false; // reset marker
                    }else{
                        if (shorterFullKmer!=null) {
                            newFullKmerList.add(shorterFullKmer);
                        }
                    }
                    shorterFullKmer = FullKmer;
                  // newFullKmerList.add(FullKmer);
                } else { // it is a longer K-mer
                    if (shorterFullKmer == null) {
                        tempLongerFullKmer.add(FullKmer);
                    } else {
                        if (dynamicSubKmerComparator(shorterFullKmer.getSeq(0), FullKmer.getSeq(0)) == true) {
                            neutralizeMarker=true;
                            if (getLeftMarker(shorterFullKmer.getLong(1)) > 0) {
                                newFullKmerList.add(enlighteningLeft(FullKmer, true));
                            } else {
                                newFullKmerList.add(enlighteningLeft(FullKmer, false));
                            }
                        } else { // longer Kmer not overlap to shorter k-mer anymore, a new round starts
                            if (getLeftMarker(shorterFullKmer.getLong(1)) > 0) {
                                for (int i = 0; i < tempLongerFullKmer.size(); i++) {
                                    if (dynamicSubKmerComparator(shorterFullKmer.getSeq(0), tempLongerFullKmer.get(i).getSeq(0)) == true) {
                                        neutralizeMarker=true;
                                        newFullKmerList.add(enlighteningLeft(tempLongerFullKmer.get(i), true));
                                    } else {
                                        newFullKmerList.add(enlighteningLeft(tempLongerFullKmer.get(i), false));
                                    }
                                }
                            } else { // adding temp to output without changing

                                for (int i = 0; i < tempLongerFullKmer.size(); i++) {
                                    if (dynamicSubKmerComparator(shorterFullKmer.getSeq(0), tempLongerFullKmer.get(i).getSeq(0)) == true) {
                                        neutralizeMarker=true;
                                    }
                                    newFullKmerList.add(enlighteningLeft(tempLongerFullKmer.get(i), false));
                                }
                            }

                            if (neutralizeMarker==true){
                                neutralizeMarker=false;
                            }else{
                                newFullKmerList.add(shorterFullKmer);
                            }

                            tempLongerFullKmer= new ArrayList<Row>();
                            shorterFullKmer = null;
                            tempLongerFullKmer.add(FullKmer);
                        }
                    }
                }
*/

                if (LongerFullKmer.size() == 0) {
                    LongerFullKmer.add(
                            RowFactory.create(fullKmer.getSeq(0), fullKmer.getLong(1))
                    );
                } else {
                    int currentLength= currentKmerSizeFromBinaryBlockArray(seq2array(fullKmer.getSeq(0)));
                    int lastLength = currentKmerSizeFromBinaryBlockArray(seq2array(LongerFullKmer.get(LongerFullKmer.size() - 1).getSeq(0)));
                    if ( currentLength== lastLength ){ // two kmer with equal size
                        LongerFullKmer.add(
                                RowFactory.create(fullKmer.getSeq(0), fullKmer.getLong(1))
                        );
                    } else if (dynamicSubKmerComparator(fullKmer.getSeq(0), LongerFullKmer.get(LongerFullKmer.size() - 1).getSeq(0)) == true) {
                        long[] lastKmer = seq2array(LongerFullKmer.get(LongerFullKmer.size() - 1).getSeq(0));
                        long[] currentKmer =  seq2array(fullKmer.getSeq(0));

                        int lastKmerLength = currentKmerSizeFromBinaryBlockArray(lastKmer);
                        int currentKmerLength = currentKmerSizeFromBinaryBlockArray(currentKmer);

                        if (lastKmerLength >currentKmerLength){
                            continue;
                        }else{
                            LongerFullKmer.remove(LongerFullKmer.size() - 1);
                            LongerFullKmer.add(
                                    RowFactory.create(fullKmer.getSeq(0), fullKmer.getLong(1))
                            );
                        }
                    } else {
                        LongerFullKmer.add(
                                RowFactory.create(fullKmer.getSeq(0), fullKmer.getLong(1))
                        );
                    }
                }

            }
/*
            if (shorterFullKmer==null){
                for (int i=0;i<tempLongerFullKmer.size();i++) {
                    newFullKmerList.add(enlighteningLeft(tempLongerFullKmer.get(i), false));
                }
            }else{
                if (getLeftMarker(shorterFullKmer.getLong(1)) >0){
                    for (int i=0;i<tempLongerFullKmer.size();i++){
                        if (dynamicSubKmerComparator(shorterFullKmer.getSeq(0), tempLongerFullKmer.get(i).getSeq(0))== true){
                            neutralizeMarker=true;
                            newFullKmerList.add(enlighteningLeft(tempLongerFullKmer.get(i), true));
                        }else{
                            newFullKmerList.add(enlighteningLeft(tempLongerFullKmer.get(i), false));
                        }
                    }
                }else{ // adding temp to output without changing

                    for (int i=0;i<tempLongerFullKmer.size();i++) {
                        if (dynamicSubKmerComparator(shorterFullKmer.getSeq(0), tempLongerFullKmer.get(i).getSeq(0))== true){
                            neutralizeMarker=true;
                        }
                        newFullKmerList.add(enlighteningLeft(tempLongerFullKmer.get(i), false));
                    }
                }

                if (neutralizeMarker==true) {
                    neutralizeMarker=false;
                }else{
                    newFullKmerList.add(shorterFullKmer);
                }
            }


            return newFullKmerList.iterator();
*/
            return LongerFullKmer.iterator();
        }

        private Row enlighteningLeft (Row s, boolean enlighten) throws Exception { // all to forward k-mer for the next step neutralization
            Row fullKmer;
            long[] fullKmerArray= seq2array(s.getSeq(0));

            if (getReflexivMarker(s.getLong(1))==1){ // they are all suppose to be 1
                if (enlighten==true){
                    long attribute = buildingAlongFromThreeInt(1, param.kmerListInt[param.kmerListInt.length-1]+3, getRightMarker(s.getLong(1)));
                    fullKmer=RowFactory.create(fullKmerArray,attribute);
                }else{
                    fullKmer=RowFactory.create(fullKmerArray,s.getLong(1));
                }
               // System.out.println("Enlightening before: " + BinaryBlocksToString(fullKmerArray) + " after: " + BinaryBlocksToString(fullKmerArray));
            }else{
              //   System.out.println("Null Enlightening before: " + BinaryBlocksToString(fullKmerArray) + " after: " + BinaryBlocksToString(fullKmerArray));
                fullKmer=null;
                // something is wrong
            }
            return fullKmer;
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

        private boolean dynamicSubKmerComparator(Seq a, Seq b) throws Exception {
            long[] arrayA = seq2array(a);
            long[] arrayB = seq2array(b);

            int aLength= currentKmerSizeFromBinaryBlockArray(arrayA);
            int bLength= currentKmerSizeFromBinaryBlockArray(arrayB);

            if (aLength>bLength){ // equal should not happen
                long[] shorterVersion = leftShiftOutFromArray(arrayA, bLength);
            //    String longer = BinaryBlocksToString(shorterVersion);
            //    String shorter = BinaryBlocksToString(arrayB);
               // System.out.println("longer: " + longer + " shorter: " + shorter);
               // if (shorterVersion.length>=2 && arrayB.length >=2) {
                //    System.out.println("longer array: " + shorterVersion[0] + " "  + shorterVersion[1] + " shorter array: " + arrayB[0] + " " + arrayB[1]);
                //}
                if (Arrays.equals(shorterVersion, arrayB)){
                  //  if (shorterVersion.length>=2){
                //        System.out.println("marker!!!");
                   // }
                    return true;
                }else{
                    return false;
                }
            }else{
                long[] shorterVersion = leftShiftOutFromArray(arrayB, aLength);
                if (Arrays.equals(shorterVersion, arrayA)){
                    return true;
                }else{
                    return false;
                }
            }
        }

        private long[] seq2array(Seq a){
            long[] array =new long[a.length()];
            for (int i = 0; i < a.length(); i++) {
                array[i] = (Long) a.apply(i);
            }
            return array;
        }

        private long[] leftShiftOutFromArray(long[] blocks, int shiftingLength) throws Exception{
            int relativeShiftSize = shiftingLength % 31;
            int endingBlockIndex = (shiftingLength-1)/31;
            int nucleotideLength = currentKmerSizeFromBinaryBlockArray(blocks);
            long[] shiftOutBlocks = new long[endingBlockIndex+1];

            if (shiftingLength > nucleotideLength){
                return blocks;
                // throw new Exception("shifting length longer than the kmer length");
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

        private String BinaryBlocksToString (long[] binaryBlocks){
            String KmerString="";
            int KmerLength = currentKmerSizeFromBinaryBlockArray(binaryBlocks);

            for (int i=0; i< KmerLength; i++){
                Long currentNucleotideBinary = binaryBlocks[i/31] >>> 2 * (32 - (i%31+1));
                currentNucleotideBinary &= 3L;
                char currentNucleotide = BinaryToNucleotide(currentNucleotideBinary);
                KmerString += currentNucleotide;
            }

            return KmerString;
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
                            if (highestLeftMarker <= param.minErrorCoverage && leftMarker >= 2 * highestLeftMarker) {
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
                            if (leftMarker <= param.minErrorCoverage && highestLeftMarker >= 2 * leftMarker) {
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
                            if (HighCoverLastCoverage <= param.minErrorCoverage && leftMarker >= 2 * HighCoverLastCoverage) {
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
                            if (leftMarker <= param.minErrorCoverage && HighCoverLastCoverage >= 2 * leftMarker) {
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
            String KmerString="";
            int KmerLength = currentKmerSizeFromBinaryBlockArray(binaryBlocks);

            for (int i=0; i< KmerLength; i++){
                Long currentNucleotideBinary = binaryBlocks[i/31] >>> 2 * (32 - (i%31+1));
                currentNucleotideBinary &= 3L;
                char currentNucleotide = BinaryToNucleotide(currentNucleotideBinary);
                KmerString += currentNucleotide;
            }

            return KmerString;
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
            String KmerString="";
            int KmerLength = currentKmerSizeFromBinaryBlockArray(binaryBlocks);

            for (int i=0; i< KmerLength; i++){
                Long currentNucleotideBinary = binaryBlocks[i/31] >>> 2 * (32 - (i%31+1));
                currentNucleotideBinary &= 3L;
                char currentNucleotide = BinaryToNucleotide(currentNucleotideBinary);
                KmerString += currentNucleotide;
            }

            return KmerString;
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
      //      Timestamp timestamp = new Timestamp(System.currentTimeMillis());
      //      System.out.println(timestamp+ "RepeatCheck DSSubKmerToFullKmer: " + param.kmerSize1);

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
            String KmerString="";
            int KmerLength = currentKmerSizeFromBinaryBlockArray(binaryBlocks);

            for (int i=0; i< KmerLength; i++){
                Long currentNucleotideBinary = binaryBlocks[i/31] >>> 2 * (32 - (i%31+1));
                currentNucleotideBinary &= 3L;
                char currentNucleotide = BinaryToNucleotide(currentNucleotideBinary);
                KmerString += currentNucleotide;
            }

            return KmerString;
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

    class DynamicKmerBinarizerFromSorted implements MapPartitionsFunction<Row, Row>, Serializable{
        List<Row> kmerList = new ArrayList<Row>();
        Row units;
        String kmer;
        int currentKmerSize;
        int currentKmerBlockSize;
        long attribute;
        char nucleotide;
        long nucleotideInt;
        //     Long suffixBinary;
        //     Long[] suffixBinaryArray;

        public Iterator<Row> call(Iterator<Row> s) {
     //       Timestamp timestamp = new Timestamp(System.currentTimeMillis());
    //        System.out.println(timestamp+"RepeatCheck DynamicKmerBinarizerFromSorted: " + param.kmerSize1);

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
                    String[] attributeStringArray = StringUtils.chop(units.getString(1)).split("\\|");
                    attribute = buildingAlongFromThreeInt(
                            Integer.parseInt(attributeStringArray[0]),Integer.parseInt(attributeStringArray[1]),Integer.parseInt(attributeStringArray[2])
                    );
                    //attribute = Long.parseLong(StringUtils.chop(units.getString(1)));
                } else {
                    String[] attributeStringArray = units.getString(1).split("\\|");
                    attribute = buildingAlongFromThreeInt(
                            Integer.parseInt(attributeStringArray[0]),Integer.parseInt(attributeStringArray[1]),Integer.parseInt(attributeStringArray[2])
                    );
                    //attribute = Long.parseLong(units.getString(1));
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
                        RowFactory.create(nucleotideBinarySlot, attribute)
                );
            }

            return kmerList.iterator();
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
