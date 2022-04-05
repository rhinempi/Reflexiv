package uni.bielefeld.cmg.reflexiv.pipeline;


import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import uni.bielefeld.cmg.reflexiv.util.DefaultParam;
import uni.bielefeld.cmg.reflexiv.util.InfoDumper;

import java.io.Serializable;
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
                .config("spark.sql.shuffle.partitions", shufflePartitions)
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
        info.readMessage("Start Spark framework");
        info.screenDump();

        sc.setCheckpointDir("/tmp/checkpoints");
        String checkpointDir= sc.getCheckpointDir().get();

        Dataset<Row> KmerCountDS;
        Dataset<Row> LongerKmerCountDS;
        Dataset<String> FastqDS;

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
        StructType ReflexivKmerStructCompressed = new StructType();
        ReflexivKmerStructCompressed = ReflexivKmerStruct.add("k-1", DataTypes.createArrayType(DataTypes.LongType), false);
        ReflexivKmerStructCompressed = ReflexivKmerStruct.add("reflection", DataTypes.IntegerType, false);
        ReflexivKmerStructCompressed = ReflexivKmerStruct.add("extension", DataTypes.LongType, false);
        ExpressionEncoder<Row> ReflexivSubKmerEncoderCompressed = RowEncoder.apply(ReflexivKmerStructCompressed);

        Dataset<Row> ReflexivSubKmerStringDS;
        StructType ReflexivKmerStringStruct = new StructType();
        ReflexivKmerStringStruct = ReflexivKmerStringStruct.add("k-1", DataTypes.StringType, false);
        ReflexivKmerStringStruct = ReflexivKmerStringStruct.add("reflection", DataTypes.IntegerType, false);
        ReflexivKmerStringStruct = ReflexivKmerStringStruct.add("extension", DataTypes.StringType, false);
        ReflexivKmerStringStruct = ReflexivKmerStringStruct.add("left", DataTypes.IntegerType, false);
        ReflexivKmerStringStruct = ReflexivKmerStringStruct.add("right", DataTypes.IntegerType, false);
        ExpressionEncoder<Row> ReflexivKmerStringEncoder = RowEncoder.apply(ReflexivKmerStringStruct);

        Dataset<Row> ReflexivLongSubKmerDS;
        Dataset<Row> ReflexivLongFragmentDS;
        Dataset<Row> ExtendableReflexivKmer = null;
        Dataset<Row> ExtendableReflexivKmerPairs = null;
        Dataset<Row> UnExtendableReflexivKmer = null;
        Dataset<Row> UnExtendableReflexivKmerPairs = null;
        StructType ReflexivLongKmerStruct = new StructType();
        ReflexivLongKmerStruct = ReflexivLongKmerStruct.add("k-1", DataTypes.createArrayType(DataTypes.LongType), false);
        ReflexivLongKmerStruct = ReflexivLongKmerStruct.add("reflection", DataTypes.IntegerType, false);
        ReflexivLongKmerStruct = ReflexivLongKmerStruct.add("extension", DataTypes.createArrayType(DataTypes.LongType), false);
        ReflexivLongKmerStruct = ReflexivLongKmerStruct.add("left", DataTypes.IntegerType, false);
        ReflexivLongKmerStruct = ReflexivLongKmerStruct.add("right", DataTypes.IntegerType, false);
        ExpressionEncoder<Row> ReflexivLongKmerEncoder = RowEncoder.apply(ReflexivLongKmerStruct);

        Dataset<Row> ReflexivLongSubKmerStringDS;
        StructType ReflexivLongKmerStringStruct = new StructType();
        ReflexivLongKmerStringStruct = ReflexivLongKmerStringStruct.add("k-1", DataTypes.StringType, false);
        ReflexivLongKmerStringStruct = ReflexivLongKmerStringStruct.add("reflection", DataTypes.IntegerType, false);
        ReflexivLongKmerStringStruct = ReflexivLongKmerStringStruct.add("extension", DataTypes.StringType, false);
        ReflexivLongKmerStringStruct = ReflexivLongKmerStringStruct.add("left", DataTypes.IntegerType, false);
        ReflexivLongKmerStringStruct = ReflexivLongKmerStringStruct.add("right", DataTypes.IntegerType, false);
        ExpressionEncoder<Row> ReflexivLongKmerStringEncoder = RowEncoder.apply(ReflexivLongKmerStringStruct);

        Dataset<Row> ContigProbRows;
        StructType ReflexivSubKmerProbStruct = new StructType();
        ReflexivSubKmerProbStruct = ReflexivSubKmerProbStruct.add("subkmerBinary", DataTypes.createArrayType(DataTypes.LongType), false);
        ReflexivSubKmerProbStruct = ReflexivSubKmerProbStruct.add("marker", DataTypes.IntegerType, false);
        ExpressionEncoder<Row> ReflexivSubKmerProbEncoder = RowEncoder.apply(ReflexivSubKmerProbStruct);

        Dataset<Row> ContigRows;
        StructType ContigLongKmerStringStruct = new StructType();
        ContigLongKmerStringStruct = ContigLongKmerStringStruct.add("ID", DataTypes.StringType, false);
        ContigLongKmerStringStruct = ContigLongKmerStringStruct.add("contig", DataTypes.StringType, false);
        ExpressionEncoder<Row> ContigStringEncoder = RowEncoder.apply(ContigLongKmerStringStruct);

        JavaRDD<Row> ContigRowsRDD;
        JavaPairRDD<Row, Long> ContigsRDDIndex;
        JavaRDD<String> ContigRDD;



        /**
         * loading Kmer counts
         */
        KmerCountDS = spark.read().csv(param.inputKmerPath + "_" + param.kmerSize1);
        LongerKmerCountDS = spark.read().csv(param.inputKmerPath + "_" + param.kmerSize2);

        /*
        if (param.partitions > 0) {
            KmerCountDS = KmerCountDS.repartition(param.partitions);
        }
        */

        /**
         * Transforming kmer string to binary kmer
         */
        DynamicKmerBinarizer DSBinarizer = new DynamicKmerBinarizer();
        KmerBinaryCountDS = KmerCountDS.mapPartitions(DSBinarizer, KmerBinaryCountEncoder);

        LongerKmerBinaryCountDS = LongerKmerCountDS.mapPartitions(DSBinarizer, KmerBinaryCountEncoder);


        /**
         * Filter kmer with lower coverage
         */
        KmerBinaryCountDS = KmerBinaryCountDS.filter(col("count")
                .geq(param.minKmerCoverage)
                .and(col("count")
                        .leq(param.maxKmerCoverage)
                )
        );

        LongerKmerBinaryCountDS = LongerKmerBinaryCountDS.filter(col("count")
                .geq(param.minKmerCoverage)
                .and(col("count")
                        .leq(param.maxKmerCoverage)
                )
        );


        if (param.cache) {
            KmerBinaryCountDS.cache();
            LongerKmerBinaryCountDS.cache();
        }

        /**
         * Extract reverse complementary kmer
         */
        DSKmerReverseComplement DSRCKmer = new DSKmerReverseComplement();
        KmerBinaryCountDS = KmerBinaryCountDS.mapPartitions(DSRCKmer, KmerBinaryCountEncoder);
        LongerKmerBinaryCountDS = LongerKmerBinaryCountDS.mapPartitions(DSRCKmer, KmerBinaryCountEncoder);

//        KmerBinaryCountDS.show();

        /**
         * Extract forward sub kmer
         */


        DSForwardSubKmerExtraction DSextractForwardSubKmer = new DSForwardSubKmerExtraction();
        ReflexivSubKmerDS = KmerBinaryCountDS.mapPartitions(DSextractForwardSubKmer, ReflexivSubKmerEncoderCompressed);

//        ReflexivSubKmerDS.show();

        if (param.bubble == true) {
            ReflexivSubKmerDS = ReflexivSubKmerDS.sort("k-1");
            if (param.minErrorCoverage == 0) {
                DSFilterForkSubKmer DShighCoverageSelector = new DSFilterForkSubKmer();
                ReflexivSubKmerDS = ReflexivSubKmerDS.mapPartitions(DShighCoverageSelector, ReflexivSubKmerEncoder);
            } else {
                DSFilterForkSubKmerWithErrorCorrection DShighCoverageErrorRemovalSelector = new DSFilterForkSubKmerWithErrorCorrection();
                ReflexivSubKmerDS = ReflexivSubKmerDS.mapPartitions(DShighCoverageErrorRemovalSelector, ReflexivSubKmerEncoderCompressed);
            }

            DSReflectedSubKmerExtractionFromForward DSreflectionExtractor = new DSReflectedSubKmerExtractionFromForward();
            ReflexivSubKmerDS = ReflexivSubKmerDS.mapPartitions(DSreflectionExtractor, ReflexivSubKmerEncoder);

            ReflexivSubKmerDS = ReflexivSubKmerDS.sort("k-1");
            if (param.minErrorCoverage == 0) {
                DSFilterForkReflectedSubKmer DShighCoverageReflectedSelector = new DSFilterForkReflectedSubKmer();
                ReflexivSubKmerDS = ReflexivSubKmerDS.mapPartitions(DShighCoverageReflectedSelector, ReflexivSubKmerEncoder);
            } else {
                DSFilterForkReflectedSubKmerWithErrorCorrection DShighCoverageReflectedErrorRemovalSelector = new DSFilterForkReflectedSubKmerWithErrorCorrection();
                ReflexivSubKmerDS = ReflexivSubKmerDS.mapPartitions(DShighCoverageReflectedErrorRemovalSelector, ReflexivSubKmerEncoderCompressed);
            }
        }

        /**
         *
         */

        DSkmerRandomReflection DSrandomizeSubKmer = new DSkmerRandomReflection();
        ReflexivSubKmerDS = ReflexivSubKmerDS.mapPartitions(DSrandomizeSubKmer, ReflexivSubKmerEncoderCompressed);

        /**
         *
         */

        ReflexivLongSubKmerStringDS = ReflexivLongSubKmerDS.mapPartitions(DSArrayStringOutput, ReflexivLongKmerStringEncoder);

        /**
         *
         */
     //   DSKmerToContigLength contigLengthDS = new DSKmerToContigLength();
     //   ContigLengthRows = ReflexivLongSubKmerStringDS.mapPartitions(contigLengthDS, ContigLengthEncoder);


        // DSFormatContigs ContigFormater = new DSFormatContigs();
        // ContigRows= ContigMergedRow.mapPartitions(ContigFormater, ContigStringEncoder);



        DSKmerToContig contigformaterDS = new DSKmerToContig();
        ContigRows = ReflexivLongSubKmerStringDS.mapPartitions(contigformaterDS, ContigStringEncoder);

        /**
         *
         */

        ContigRowsRDD = ContigRows.toJavaRDD();

        ContigRowsRDD.cache();

        ContigsRDDIndex = ContigRowsRDD.zipWithIndex();

        TagRowContigID DSIdLabeling = new TagRowContigID();
        ContigRDD = ContigsRDDIndex.flatMap(DSIdLabeling);

        ContigRDD.cache();
        long filteredCountig = ContigRDD.count();
        System.out.println("mark contig number: " + filteredCountig);

        if (param.gzip) {
            ContigRDD.saveAsTextFile(param.outputPath + "/Assemble_" + param.kmerSize, GzipCodec.class);
        }else{
            ContigRDD.saveAsTextFile(param.outputPath + "/Assemble_" + param.kmerSize);
        }

        spark.stop();
    }



    class TagRowContigID implements FlatMapFunction<Tuple2<Row, Long>, String>, Serializable {

        public Iterator<String> call(Tuple2<Row, Long> s) {


            List<String> contigList = new ArrayList<String>();

            contigList.add(s._1.getString(0) + "-" + s._2 + "\n" + s._1.getString(1));

            return contigList.iterator();
        }
    }

    class TagContigID implements FlatMapFunction<Tuple2<Tuple2<String, String>, Long>, String>, Serializable {

        public Iterator<String> call(Tuple2<Tuple2<String, String>, Long> s) {


            List<String> contigList = new ArrayList<String>();

            contigList.add(s._1._1 + "-" + s._2 + "\n" + s._1._2);

            return contigList.iterator();
        }
    }

    class DSKmerToContig implements MapPartitionsFunction<Row, Row>, Serializable {

        public Iterator<Row> call(Iterator<Row> sIterator) {
            List<Row> contigList = new ArrayList<Row>();

            while (sIterator.hasNext()) {
                Row s = sIterator.next();
                if (s.getInt(1) == 1) {
                    String contig = s.getString(0) + s.getString(2);
                    int length = contig.length();
                    if (length >= param.minContig) {
                        String ID = ">Contig-" + length;
                        String formatedContig = changeLine(contig, length, 100);
                        contigList.add(RowFactory.create(ID, formatedContig));
                    }
                } else { // (randomReflexivMarker == 2) {
                    String contig = s.getString(2) + s.getString(0);
                    int length = contig.length();
                    if (length >= param.minContig) {
                        String ID = ">Contig-" + length;
                        String formatedContig = changeLine(contig, length, 100);
                        contigList.add(RowFactory.create(ID, formatedContig));
                    }
                }
            }

            return contigList.iterator();
        }

        public String changeLine(String oneLine, int lineLength, int limitedLength) {
            String blockLine = "";
            int fold = lineLength / limitedLength;
            int remainder = lineLength % limitedLength;
            if (fold == 0) {
                blockLine = oneLine;
            } else if (fold == 1 && remainder == 0) {
                blockLine = oneLine;
            } else if (fold > 1 && remainder == 0) {
                for (int i = 0; i < fold - 1; i++) {
                    blockLine += oneLine.substring(i * limitedLength, (i + 1) * limitedLength) + "\n";
                }
                blockLine += oneLine.substring((fold - 1) * limitedLength);
            } else {
                for (int i = 0; i < fold; i++) {
                    blockLine += oneLine.substring(i * limitedLength, (i + 1) * limitedLength) + "\n";
                }
                blockLine += oneLine.substring(fold * limitedLength);
            }

            return blockLine;
        }
    }


    class DSLowCoverageSubKmerExtraction implements  MapPartitionsFunction<Row, Row>, Serializable{
        List<Row> subKmerProb = new ArrayList<Row>();
        int randomReflexivMarker =1;
        //long maxSubKmerBinary = ~((~0L) << 2*param.subKmerSize);
        long maxSubKmerBinary = ~((~0L) << 2 * 31);
        long maxBlockBinary = ~((~0L) << 2*31); // a block has 31 nucleotide
        int contigIndex = 0;
        long maxSubKmerResidueBinary = ~((~0L) << 2 * param.subKmerSizeResidue);

        public Iterator<Row> call (Iterator<Row> sIterator){

            while(sIterator.hasNext()){
                Row s = sIterator.next();
                contigIndex++;

                int contigLength = param.subKmerSize + (s.getSeq(2).length()-1) * 31;
                int firstSuffixLength = Long.SIZE/2 - (Long.numberOfLeadingZeros((Long)s.getSeq(2).apply(0))/2 + 1);
                contigLength += firstSuffixLength;

                if (contigLength <61) continue;

                if (s.getInt(3)>=-5 && s.getInt(3)<0){
                    if (s.getInt(1) == 1){
                        Long[] subKmerBinary = new Long[s.getSeq(0).length()];
                        for (int i=0 ; i<s.getSeq(0).length(); i++) {
                            subKmerBinary[i] = (Long) s.getSeq(0).apply(i);
                        }
                        int marker = (contigIndex | (1<<31));  // 1 as right extendable
                        subKmerProb.add(
                                RowFactory.create(subKmerBinary, marker)
                        );
                    }else{
                        randomReflexivMarker =1;
                        Row newS = singleKmerRandomizer(s);
                        Long[] subKmerBinary = new Long[((Long[]) newS.get(0)).length];
                        for (int i=0 ; i<((Long[]) newS.get(0)).length; i++) {
                            subKmerBinary[i] = ((Long[]) newS.get(0))[i];
                        }
                        int marker = (contigIndex| (1<<31));  // 1 as right extendable
                        subKmerProb.add(
                                RowFactory.create(subKmerBinary, marker)
                        );
                    }
                }

                if (s.getInt(4)>=-5 && s.getInt(4)<0){
                    if (s.getInt(1) == 1){
                        randomReflexivMarker =2;
                        Row newS = singleKmerRandomizer(s);
                        Long[] subKmerBinary = new Long[((Long[]) newS.get(0)).length];
                        for (int i=0 ; i<((Long[]) newS.get(0)).length; i++) {
                            subKmerBinary[i] = ((Long[]) newS.get(0))[i];
                        }
                        int marker = contigIndex ; // 0 as left extendable
                        subKmerProb.add(
                                RowFactory.create(subKmerBinary, marker)
                        );
                    }else {
                        Long[] subKmerBinary = new Long[s.getSeq(0).length()];
                        for (int i=0 ; i<s.getSeq(0).length(); i++) {
                            subKmerBinary[i] = (Long) s.getSeq(0).apply(i);
                        }
                        int marker = contigIndex ; // 0 as left extendable
                        subKmerProb.add(
                                RowFactory.create(subKmerBinary, marker)
                        );
                    }
                }

            }
/*
            int maxMarkerBinary =  ~((~0) << 31);
            for (int i=0; i<subKmerProb.size(); i++){
                long subKmerBinary = subKmerProb.get(i).getLong(0);
                int marker = subKmerProb.get(i).getInt(1);

                System.out.print("extracted probs: ");

                for (int j = 31; j>=0; j--){
                    long a = subKmerBinary >>> 2 *j;
                    a &= 3L;
                    char b = BinaryToNucleotide(a);
                    System.out.print(b);
                }

                int direction = marker >>> 31;
                int contigID = marker & maxMarkerBinary;
                System.out.println(" " + direction + " " + contigID);
            }
*/
            return subKmerProb.iterator();
        }

        public Row singleKmerRandomizer(Row currentSubKmer) {
            int blockSize = currentSubKmer.getSeq(2).length();

            Long[] newReflexivLongArray = new Long[blockSize];

            if (currentSubKmer.getInt(1) == 1) {
                int firstSuffixBlockLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros((Long) currentSubKmer.getSeq(2).apply(0)) / 2 + 1);
                long maxSuffixLengthBinary = ~((~0L) << (2 * firstSuffixBlockLength));
                Long[] newReflexivSubKmer = new Long[param.subKmerBinarySlots];
                Long newReflexivLong;

                if (randomReflexivMarker == 2) {
                    if (blockSize > 1) {

                        if (firstSuffixBlockLength >= param.subKmerSizeResidue) {
                            if (blockSize >= param.subKmerBinarySlots) {
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] = ((Long) currentSubKmer.getSeq(2).apply(blockSize - 1) & maxSubKmerResidueBinary);

                                long transit1 = (Long) currentSubKmer.getSeq(2).apply(blockSize - 1) >>> (2 * param.subKmerSizeResidue);
                                long transit2 = 0L;
                                for (int i = param.subKmerBinarySlots - 2; i >= 0; i--) {
                                    int j = blockSize - param.subKmerBinarySlots + i; // index of suffix long array
                                    transit2 = (Long) currentSubKmer.getSeq(2).apply(j) >>> (2 * param.subKmerSizeResidue);

                                    newReflexivSubKmer[i] = transit1;
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(2).apply(j) << 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;

                                    transit1 = transit2;
                                }

                                for (int i = blockSize - param.subKmerBinarySlots; i > 0; i--) {
                                    int j = param.subKmerBinarySlots + i - 1; // index of the new prefix long array
                                    newReflexivLongArray[j] = ((Long) currentSubKmer.getSeq(2).apply(i) >>> 2 * param.subKmerSizeResidue);
                                    newReflexivLongArray[j] |= (Long) currentSubKmer.getSeq(2).apply(i - 1) << 2 * (31 - param.subKmerSizeResidue);
                                    newReflexivLongArray[j] &= maxSubKmerBinary;
                                }

                                //   if (blockSize== param.subKmerBinarySlots){ // in the context of blockSize >= param.subKmerBinarySlots
                                newReflexivLongArray[param.subKmerBinarySlots - 1] = ((Long) currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary) >>> (2 * param.subKmerSizeResidue);
                                newReflexivLongArray[param.subKmerBinarySlots - 1] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) << 2 * (firstSuffixBlockLength - param.subKmerSizeResidue));
                                newReflexivLongArray[param.subKmerBinarySlots - 1] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 2) << 2 * firstSuffixBlockLength);
                                newReflexivLongArray[param.subKmerBinarySlots - 1] &= maxSubKmerBinary;
                                //  }

                                for (int i = param.subKmerBinarySlots - 2; i > 0; i--) {
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * (31 - firstSuffixBlockLength);
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(0).apply(i - 1) << 2 * firstSuffixBlockLength);
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                newReflexivLongArray[0] = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * (31 - firstSuffixBlockLength);
                                newReflexivLongArray[0] |= (1L << 2 * firstSuffixBlockLength); // add C marker in the front
                            } else { // blockSize < param.subKmerSizeResidue
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] = ((Long) currentSubKmer.getSeq(2).apply(blockSize - 1) & maxSubKmerResidueBinary);

                                long transit1 = (Long) currentSubKmer.getSeq(2).apply(blockSize - 1) >>> (2 * param.subKmerSizeResidue);
                                long transit2 = 0L;
                                for (int i = param.subKmerBinarySlots - 2; i >= param.subKmerBinarySlots - blockSize; i--) {
                                    int j = blockSize - param.subKmerBinarySlots + i; // index of suffix long array
                                    transit2 = (Long) currentSubKmer.getSeq(2).apply(j) >>> (2 * param.subKmerSizeResidue);

                                    newReflexivSubKmer[i] = transit1;
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(2).apply(j) << 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;

                                    transit1 = transit2;
                                }

                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize - 1] = ((Long) currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary) >>> (2 * param.subKmerSizeResidue);
                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize - 1] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) << 2 * (firstSuffixBlockLength - param.subKmerSizeResidue));
                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize - 1] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 2) << 2 * firstSuffixBlockLength);
                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize - 1] &= maxSubKmerBinary;

                                for (int i = param.subKmerBinarySlots - blockSize - 2; i >= 0; i--) {
                                    int j = blockSize + i; // index of the subkmer

                                    newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(j) >>> 2 * (31 - firstSuffixBlockLength);
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(0).apply(j - 1) << 2 * firstSuffixBlockLength);
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;
                                }

                                for (int i = blockSize - 1; i > 0; i--) {
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * (31 - firstSuffixBlockLength);
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(0).apply(i - 1) << 2 * firstSuffixBlockLength);
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                newReflexivLongArray[0] = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * (31 - firstSuffixBlockLength);
                                newReflexivLongArray[0] |= (1L << 2 * firstSuffixBlockLength); // add C marker in the front
                            }
                        } else { // firstSuffixBlockLength < param.subKmerSizeResidue
                            if (blockSize >= param.subKmerBinarySlots) {
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] = ((Long) currentSubKmer.getSeq(2).apply(blockSize - 1) & maxSubKmerResidueBinary);

                                long transit1 = (Long) currentSubKmer.getSeq(2).apply(blockSize - 1) >>> (2 * param.subKmerSizeResidue);
                                long transit2 = 0L;
                                for (int i = param.subKmerBinarySlots - 2; i > 0; i--) {
                                    int j = blockSize - param.subKmerBinarySlots + i; // index of suffix long array
                                    transit2 = (Long) currentSubKmer.getSeq(2).apply(j) >>> (2 * param.subKmerSizeResidue);

                                    newReflexivSubKmer[i] = transit1;
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(2).apply(j) << 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;

                                    transit1 = transit2;
                                }

                                newReflexivSubKmer[0] = transit1;
                                if (blockSize > param.subKmerBinarySlots) {
                                    newReflexivSubKmer[0] |= ((Long) currentSubKmer.getSeq(2).apply(blockSize - param.subKmerBinarySlots) << 2 * (31 - param.subKmerSizeResidue));
                                }else { // (blockSize== param.subKmerBinarySlots){
                                    newReflexivSubKmer[0] |= (((Long) currentSubKmer.getSeq(2).apply(blockSize - param.subKmerBinarySlots) & maxSuffixLengthBinary) << 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivSubKmer[0] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots-1) << 2*(31-param.subKmerSizeResidue + firstSuffixBlockLength));
                                }
                                newReflexivSubKmer[0] &= maxSubKmerBinary;

                                //newReflexivSubKmer[0] = transit1;
                                //newReflexivSubKmer[0] |= ((Long) currentSubKmer.getSeq(2).apply(blockSize-param.subKmerBinarySlots) << 2*(31-param.subKmerSizeResidue));


                                for (int i = blockSize - param.subKmerBinarySlots; i > 1; i--) {
                                    int j = param.subKmerBinarySlots + i - 1; // index of the new prefix long array
                                    newReflexivLongArray[j] = ((Long) currentSubKmer.getSeq(2).apply(i) >>> 2 * param.subKmerSizeResidue);
                                    newReflexivLongArray[j] |= (Long) currentSubKmer.getSeq(2).apply(i - 1) << 2 * (31 - param.subKmerSizeResidue);
                                    newReflexivLongArray[j] &= maxSubKmerBinary;
                                }

                                if (param.subKmerBinarySlots < blockSize) {
                                    newReflexivLongArray[param.subKmerBinarySlots] = ((Long) currentSubKmer.getSeq(2).apply(1) >>> 2 * param.subKmerSizeResidue);
                                    newReflexivLongArray[param.subKmerBinarySlots] |= ((Long) currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary)<< 2 * (31 - param.subKmerSizeResidue);
                                    newReflexivLongArray[param.subKmerBinarySlots] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) << 2 * (31 + firstSuffixBlockLength - param.subKmerSizeResidue));
                                    newReflexivLongArray[param.subKmerBinarySlots] &= maxSubKmerBinary;
                                }

                                //   if (blockSize== param.subKmerBinarySlots){ // in the context of blockSize >= param.subKmerBinarySlots
                                //  newReflexivLongArray[param.subKmerBinarySlots-1] = ((Long) currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary) >>> (2 * param.subKmerSizeResidue);
                                newReflexivLongArray[param.subKmerBinarySlots - 1] = ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) >>> 2 * (param.subKmerSizeResidue - firstSuffixBlockLength));
                                newReflexivLongArray[param.subKmerBinarySlots - 1] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 2) << 2 * firstSuffixBlockLength);
                                newReflexivLongArray[param.subKmerBinarySlots - 1] &= maxSubKmerBinary;
                                //  }

                                for (int i = param.subKmerBinarySlots - 2; i > 0; i--) {
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * (31 - firstSuffixBlockLength);
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(0).apply(i - 1) << 2 * firstSuffixBlockLength);
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                newReflexivLongArray[0] = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * (31 - firstSuffixBlockLength);
                                newReflexivLongArray[0] |= (1L << 2 * firstSuffixBlockLength); // add C marker in the front
                            } else { // blockSize < param.subKmerSizeResidue
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] = ((Long) currentSubKmer.getSeq(2).apply(blockSize - 1) & maxSubKmerResidueBinary);

                                long transit1 = (Long) currentSubKmer.getSeq(2).apply(blockSize - 1) >>> (2 * param.subKmerSizeResidue);
                                long transit2 = 0L;
                                for (int i = param.subKmerBinarySlots - 2; i > param.subKmerBinarySlots - blockSize; i--) {
                                    int j = blockSize - param.subKmerBinarySlots + i; // index of suffix long array
                                    transit2 = (Long) currentSubKmer.getSeq(2).apply(j) >>> (2 * param.subKmerSizeResidue);

                                    newReflexivSubKmer[i] = transit1;
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(2).apply(j) << 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;

                                    transit1 = transit2;
                                }

                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize] = (Long) currentSubKmer.getSeq(2).apply(1) >>> 2 * param.subKmerSizeResidue;
                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize] |= ((Long) currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary) << 2 * (31 - param.subKmerSizeResidue);
                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) << 2 * (31 - param.subKmerSizeResidue + firstSuffixBlockLength));
                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize] &= maxSubKmerBinary;

                                // newReflexivSubKmer[param.subKmerBinarySlots-blockSize-1] = ((Long) currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary) >>> (2 * param.subKmerSizeResidue);
                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize - 1] = ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) >>> 2 * (param.subKmerSizeResidue - firstSuffixBlockLength));
                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize - 1] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 2) << 2 * firstSuffixBlockLength);
                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize - 1] &= maxSubKmerBinary;

                                for (int i = param.subKmerBinarySlots - blockSize - 2; i >= 0; i--) {
                                    int j = blockSize + i; // index of the subkmer

                                    newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(j) >>> 2 * (31 - firstSuffixBlockLength);
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(0).apply(j - 1) << 2 * firstSuffixBlockLength);
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;
                                }

                                for (int i = blockSize - 1; i > 0; i--) {
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * (31 - firstSuffixBlockLength);
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(0).apply(i - 1) << 2 * firstSuffixBlockLength);
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                newReflexivLongArray[0] = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * (31 - firstSuffixBlockLength);
                                newReflexivLongArray[0] |= (1L << 2 * firstSuffixBlockLength); // add C marker in the front
                            }
                        }


                    } else { // block size ==1
                        if (firstSuffixBlockLength > param.subKmerSizeResidue) { // firstSuffixBlockLength is longer than the length of the last block (element) of sub kmer long array
                            //long transitBit1 = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) << 2 * (firstSuffixBlockLength - param.subKmerSizeResidue);
                            long transitBit1 = (((Long)currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary) >>> 2 * param.subKmerSizeResidue);
                            long transitBit2 = 0L;

                            newReflexivSubKmer[param.subKmerBinarySlots - 1] = ((Long) currentSubKmer.getSeq(2).apply(0) & maxSubKmerResidueBinary);

                            newReflexivSubKmer[param.subKmerBinarySlots - 2] = ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots-1) << 2* (firstSuffixBlockLength-param.subKmerSizeResidue));
                            newReflexivSubKmer[param.subKmerBinarySlots - 2] |= transitBit1;
                            newReflexivSubKmer[param.subKmerBinarySlots - 2] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots-2) << 2*firstSuffixBlockLength);
                            newReflexivSubKmer[param.subKmerBinarySlots - 2] &= maxSubKmerBinary;
                            transitBit1 = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots-2) >>> 2 * (31 - firstSuffixBlockLength);

                            for (int i = param.subKmerBinarySlots - 3; i >= 0; i--) {
                                transitBit2 = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * (31 - firstSuffixBlockLength);

                                newReflexivSubKmer[i] = ((Long) currentSubKmer.getSeq(0).apply(i) << 2 * firstSuffixBlockLength);
                                newReflexivSubKmer[i] |= transitBit1;
                                newReflexivSubKmer[i] &= maxSubKmerBinary;

                                transitBit1 = transitBit2;
                            }

                            newReflexivLong = transitBit1;
                            newReflexivLong |= (1L << 2 * firstSuffixBlockLength);
                            newReflexivLongArray[0] = newReflexivLong;

                        } else if (firstSuffixBlockLength == param.subKmerSizeResidue) {
                            long transitBit1 = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                            long transitBit2 = 0L;

                            newReflexivSubKmer[param.subKmerBinarySlots - 1] = ((Long) currentSubKmer.getSeq(2).apply(0) & maxSubKmerResidueBinary);

                            for (int i = param.subKmerBinarySlots - 2; i >= 0; i--) {
                                transitBit2 = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * (31 - firstSuffixBlockLength);

                                newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(i) << 2 * firstSuffixBlockLength;
                                newReflexivSubKmer[i] |= transitBit1;
                                newReflexivSubKmer[i] &= maxSubKmerBinary;

                                transitBit1 = transitBit2;
                            }

                            newReflexivLong = transitBit2;
                            newReflexivLong |= (1L << 2 * firstSuffixBlockLength);
                            newReflexivLongArray[0] = newReflexivLong;

                        } else { //firstSuffixBlockLength < param.subKmerSizeResidue
                            long transitBit1 = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) >>> 2 * (param.subKmerSizeResidue - firstSuffixBlockLength);
                            long transitBit2 = 0L;

                            newReflexivSubKmer[param.subKmerBinarySlots - 1] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) << 2 * firstSuffixBlockLength;
                            newReflexivSubKmer[param.subKmerBinarySlots - 1] |= ((Long) currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary);
                            newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;

                            for (int i = param.subKmerBinarySlots - 2; i >= 0; i--) {
                                transitBit2 = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * (31 - firstSuffixBlockLength);

                                newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(i) << 2 * firstSuffixBlockLength;
                                newReflexivSubKmer[i] |= transitBit1;
                                newReflexivSubKmer[i] &= maxSubKmerBinary;

                                transitBit1 = transitBit2;
                            }

                            newReflexivLong = transitBit2;
                            newReflexivLong |= (1L << 2 * firstSuffixBlockLength); // add C marker
                            newReflexivLongArray[0] = newReflexivLong;
                        }
                    }


                    return RowFactory.create(newReflexivSubKmer,
                                    randomReflexivMarker, newReflexivLongArray, currentSubKmer.getInt(3), currentSubKmer.getInt(4)
                            );

                } else {


                    return currentSubKmer;
                }
            } else { /* currentSubKmer.getInt(1) == 2 */
                int firstPrefixLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros((Long) currentSubKmer.getSeq(2).apply(0)) / 2 + 1);
                long maxPrefixLengthBinary = ~((~0L) << (2 * firstPrefixLength));
                Long[] newReflexivSubKmer = new Long[param.subKmerBinarySlots];
                long newReflexivLong = 0L;


                if (randomReflexivMarker == 2) {

                    return currentSubKmer;
                } else { /* randomReflexivMarker == 1 */
                    if (blockSize > 1) {
                        if (firstPrefixLength >= param.subKmerSizeResidue) {
                            if (blockSize >= param.subKmerBinarySlots) {
                                for (int i = 0; i < param.subKmerBinarySlots - 1; i++) {
                                    newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(2).apply(i) << 2 * (31 - firstPrefixLength);
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(2).apply(i + 1) >>> 2 * firstPrefixLength);
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;
                                }

                                newReflexivSubKmer[param.subKmerBinarySlots - 1] = (Long) currentSubKmer.getSeq(2).apply(param.subKmerBinarySlots - 1) >>> 2 * (firstPrefixLength - param.subKmerSizeResidue);
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;

                                newReflexivLongArray[0] = (Long) currentSubKmer.getSeq(2).apply(param.subKmerBinarySlots - 1) << 2 * param.subKmerSizeResidue;
                                if (blockSize > param.subKmerBinarySlots) {
                                    newReflexivLongArray[0] |= ((Long) currentSubKmer.getSeq(2).apply(param.subKmerBinarySlots) >>> 2 * (31 - param.subKmerSizeResidue));
                                } else {
                                    newReflexivLongArray[0] |= ((Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * (31 - param.subKmerSizeResidue));
                                }
                                newReflexivLongArray[0] &= maxPrefixLengthBinary;
                                newReflexivLongArray[0] |= (1L << 2 * firstPrefixLength); // add C marker in the front

                                for (int i = 1; i < blockSize - param.subKmerBinarySlots; i++) {
                                    int j = param.subKmerBinarySlots + i - 1;
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(2).apply(j) << 2 * param.subKmerSizeResidue;
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(2).apply(j + 1) >>> 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                if (blockSize > param.subKmerBinarySlots) {
                                    newReflexivLongArray[blockSize - param.subKmerBinarySlots] = (Long) currentSubKmer.getSeq(2).apply(blockSize - 1) << 2 * param.subKmerSizeResidue;
                                    newReflexivLongArray[blockSize - param.subKmerBinarySlots] |= ((Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivLongArray[blockSize - param.subKmerBinarySlots] &= maxSubKmerBinary;
                                }

                                for (int i = blockSize - param.subKmerBinarySlots + 1; i < blockSize - 1; i++) {
                                    int j = i - blockSize + param.subKmerBinarySlots - 1; // index of subKmer
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(0).apply(j) << 2 * param.subKmerSizeResidue;
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(0).apply(j + 1) >>> 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                newReflexivLongArray[blockSize - 1] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 2) << 2 * param.subKmerSizeResidue;
                                newReflexivLongArray[blockSize - 1] |= (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                                newReflexivLongArray[blockSize - 1] &= maxSubKmerBinary;

                            } else { // blockSize < param.subKmerBinarySlots
                                for (int i = 0; i < blockSize - 1; i++) {
                                    newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(2).apply(i) << 2 * (31 - firstPrefixLength);
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(2).apply(i + 1) >>> 2 * firstPrefixLength);
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;
                                }

                                newReflexivSubKmer[blockSize - 1] = (Long) currentSubKmer.getSeq(2).apply(blockSize - 1) << 2 * (31 - firstPrefixLength);
                                newReflexivSubKmer[blockSize - 1] |= ((Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * firstPrefixLength);
                                newReflexivSubKmer[blockSize - 1] &= maxSubKmerBinary;

                                for (int i = blockSize; i < param.subKmerBinarySlots - 1; i++) {
                                    int j = i - blockSize; // index of subKmer
                                    newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(j) << 2 * (31 - firstPrefixLength);
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(0).apply(j + 1) >>> 2 * firstPrefixLength);
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;
                                }

                                newReflexivSubKmer[param.subKmerBinarySlots - 1] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - blockSize - 1) >>> 2 * (firstPrefixLength - param.subKmerSizeResidue);
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;

                                newReflexivLongArray[0] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - blockSize - 1) << 2 * param.subKmerSizeResidue;
                                newReflexivLongArray[0] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - blockSize) >>> 2 * (31 - param.subKmerSizeResidue));
                                newReflexivLongArray[0] &= maxPrefixLengthBinary;
                                newReflexivLongArray[0] |= (1L << 2 * firstPrefixLength);

                                for (int i = 1; i < blockSize - 1; i++) {
                                    int j = param.subKmerBinarySlots - blockSize - 1 + i;
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(0).apply(j) << 2 * param.subKmerSizeResidue;
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(0).apply(j + 1) >>> 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                newReflexivLongArray[blockSize - 1] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 2) << 2 * param.subKmerSizeResidue;
                                newReflexivLongArray[blockSize - 1] |= (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                                newReflexivLongArray[blockSize - 1] &= maxSubKmerBinary;
                            }
                        } else { // firstPrefixLength < param.subKmerSizeResidue
                            if (blockSize >= param.subKmerBinarySlots) {
                                newReflexivSubKmer[0] =((Long) currentSubKmer.getSeq(2).apply(0) & maxPrefixLengthBinary) << 2 * (31 - firstPrefixLength);
                                newReflexivSubKmer[0] |= ((Long) currentSubKmer.getSeq(2).apply(1) >>> 2 * firstPrefixLength);
                                newReflexivSubKmer[0] &= maxSubKmerBinary;

                                for (int i = 1; i < param.subKmerBinarySlots - 1; i++) {
                                    newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(2).apply(i) << 2 * (31 - firstPrefixLength);
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(2).apply(i + 1) >>> 2 * firstPrefixLength);
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;
                                }

                                newReflexivSubKmer[param.subKmerBinarySlots - 1] = (Long) currentSubKmer.getSeq(2).apply(param.subKmerBinarySlots - 1) << 2 * (param.subKmerSizeResidue - firstPrefixLength);
                                if (blockSize > param.subKmerBinarySlots) {
                                    newReflexivSubKmer[param.subKmerBinarySlots - 1] |= ((Long) currentSubKmer.getSeq(2).apply(param.subKmerBinarySlots) >>> 2 * (31 - param.subKmerSizeResidue + firstPrefixLength));
                                } else { // blockSize == param.subKmerBinarySlots
                                    newReflexivSubKmer[param.subKmerBinarySlots - 1] |= ((Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * (31 - param.subKmerSizeResidue + firstPrefixLength));
                                }
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;


                                if (blockSize > param.subKmerBinarySlots) {
                                    newReflexivLongArray[0] = (Long) currentSubKmer.getSeq(2).apply(param.subKmerBinarySlots) >>> 2 * (31 - param.subKmerSizeResidue);
                                } else { // blockSize == param.subKmerBinarySlots
                                    newReflexivLongArray[0] = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * (31 - param.subKmerSizeResidue);
                                }
                                newReflexivLongArray[0] &= maxPrefixLengthBinary;
                                newReflexivLongArray[0] |= (1L << 2 * firstPrefixLength);

                                for (int i = 1; i < blockSize - param.subKmerBinarySlots; i++) {
                                    int j = param.subKmerBinarySlots + i - 1;
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(2).apply(j) << 2 * param.subKmerSizeResidue;
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(2).apply(j + 1) >>> 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                if (blockSize > param.subKmerBinarySlots) {
                                    newReflexivLongArray[blockSize - param.subKmerBinarySlots] = (Long) currentSubKmer.getSeq(2).apply(blockSize - 1) << 2 * param.subKmerSizeResidue;
                                    newReflexivLongArray[blockSize - param.subKmerBinarySlots] |= ((Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivLongArray[blockSize - param.subKmerBinarySlots] &= maxSubKmerBinary;
                                }

                                for (int i = blockSize - param.subKmerBinarySlots + 1; i < blockSize - 1; i++) {
                                    int j = i - blockSize + param.subKmerBinarySlots - 1; // index of subKmer
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(0).apply(j) << 2 * param.subKmerSizeResidue;
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(0).apply(j + 1) >>> 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                newReflexivLongArray[blockSize - 1] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 2) << 2 * param.subKmerSizeResidue;
                                newReflexivLongArray[blockSize - 1] |= (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                                newReflexivLongArray[blockSize - 1] &= maxSubKmerBinary;


                            } else { // blockSize < param.subKmerBinarySlots
                                newReflexivSubKmer[0] = ( (Long) currentSubKmer.getSeq(2).apply(0) & maxPrefixLengthBinary) << 2 * (31 - firstPrefixLength);
                                newReflexivSubKmer[0] |= ((Long) currentSubKmer.getSeq(2).apply(1) >>> 2 * firstPrefixLength);
                                newReflexivSubKmer[0] &= maxSubKmerBinary;
                                for (int i = 1; i < blockSize - 1; i++) {
                                    newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(2).apply(i) << 2 * (31 - firstPrefixLength);
                                    newReflexivSubKmer[i] |=( (Long) currentSubKmer.getSeq(2).apply(i + 1) >>> 2 * firstPrefixLength);
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;
                                }

                                newReflexivSubKmer[blockSize - 1] = (Long) currentSubKmer.getSeq(2).apply(blockSize - 1) << 2 * (31 - firstPrefixLength);
                                newReflexivSubKmer[blockSize - 1] |= ((Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * firstPrefixLength);
                                newReflexivSubKmer[blockSize - 1] &= maxSubKmerBinary;

                                for (int i = blockSize; i < param.subKmerBinarySlots - 1; i++) {
                                    int j = i - blockSize; // index of subKmer
                                    newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(j) << 2 * (31 - firstPrefixLength);
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(0).apply(j + 1) >>> 2 * firstPrefixLength);
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;
                                }

                                // newReflexivSubKmer[param.subKmerBinarySlots-1] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots-blockSize-1) >>> 2*(firstPrefixLength-param.subKmerSizeResidue);
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - blockSize - 1) << 2 * (param.subKmerSizeResidue - firstPrefixLength);
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - blockSize) >>> 2 * (31 - param.subKmerSizeResidue + firstPrefixLength));
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;

                                // newReflexivLongArray[0] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots-blockSize) << 2*param.subKmerSizeResidue;
                                newReflexivLongArray[0] = ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - blockSize) >>> 2 * (31 - param.subKmerSizeResidue));
                                newReflexivLongArray[0] &= maxPrefixLengthBinary;
                                newReflexivLongArray[0] |= (1L << 2 * firstPrefixLength);

                                for (int i = 1; i < blockSize - 1; i++) {
                                    int j = param.subKmerBinarySlots - blockSize + i -1;
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(0).apply(j) << 2 * param.subKmerSizeResidue;
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(0).apply(j + 1) >>> 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                newReflexivLongArray[blockSize - 1] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 2) << 2 * param.subKmerSizeResidue;
                                newReflexivLongArray[blockSize - 1] |= (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                                newReflexivLongArray[blockSize - 1] &= maxSubKmerBinary;
                            }
                        }

                    } else { /* blockSize == 1*/
                        if (firstPrefixLength > param.subKmerSizeResidue) {
                            long transitBit1 = (Long) currentSubKmer.getSeq(0).apply(0) & maxPrefixLengthBinary;

                            newReflexivSubKmer[0] = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * firstPrefixLength;
                            newReflexivSubKmer[0] |= (((Long) currentSubKmer.getSeq(2).apply(0) & maxPrefixLengthBinary) << 2 * (31 - firstPrefixLength));
                            newReflexivSubKmer[0] &= maxSubKmerBinary;

                            for (int i = 1; i < param.subKmerBinarySlots - 1; i++) {
                                long transitBit2 = (Long) currentSubKmer.getSeq(0).apply(i) & maxPrefixLengthBinary;

                                newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * firstPrefixLength;
                                newReflexivSubKmer[i] |= (transitBit1 << 2 * (31 - firstPrefixLength));
                                newReflexivSubKmer[i] &= maxSubKmerBinary;

                                transitBit1 = transitBit2;
                            }

                            newReflexivSubKmer[param.subKmerBinarySlots - 1] = transitBit1 >>> 2*(firstPrefixLength- param.subKmerSizeResidue);
                            newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;

                            newReflexivLong = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                            newReflexivLong |= (transitBit1 << 2 * param.subKmerSizeResidue);
                            newReflexivLong &= maxPrefixLengthBinary;
                            newReflexivLong |= (1L << 2 * firstPrefixLength); // add C marker in the front

                            newReflexivLongArray[0] = newReflexivLong;
                        } else if (firstPrefixLength == param.subKmerSizeResidue) {
                            long transitBit1 = (Long) currentSubKmer.getSeq(0).apply(0) & maxPrefixLengthBinary;

                            newReflexivSubKmer[0] = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * firstPrefixLength;
                            newReflexivSubKmer[0] |= (((Long) currentSubKmer.getSeq(2).apply(0) & maxPrefixLengthBinary) << 2 * (31 - firstPrefixLength));
                            newReflexivSubKmer[0] &= maxSubKmerBinary;

                            for (int i = 1; i < param.subKmerBinarySlots - 1; i++) {
                                long transitBit2 = (Long) currentSubKmer.getSeq(0).apply(i) & maxPrefixLengthBinary;

                                newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * firstPrefixLength;
                                newReflexivSubKmer[i] |= (transitBit1 << 2 * (31 - firstPrefixLength));
                                newReflexivSubKmer[i] &= maxSubKmerBinary;

                                transitBit1 = transitBit2;
                            }

                            newReflexivSubKmer[param.subKmerBinarySlots - 1] = transitBit1;
                            newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;

                            newReflexivLong = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                            newReflexivLong |= (1L << 2 * firstPrefixLength); // add C marker

                            newReflexivLongArray[0] = newReflexivLong;
                        } else {
                            long transitBit1 = (Long) currentSubKmer.getSeq(0).apply(0) & maxPrefixLengthBinary;

                            newReflexivSubKmer[0] = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * firstPrefixLength;
                            newReflexivSubKmer[0] |= (((Long) currentSubKmer.getSeq(2).apply(0) & maxPrefixLengthBinary) << 2 * (31 - firstPrefixLength));
                            newReflexivSubKmer[0] &= maxSubKmerBinary;

                            for (int i = 1; i < param.subKmerBinarySlots - 1; i++) {
                                long transitBit2 = (Long) currentSubKmer.getSeq(0).apply(i) & maxPrefixLengthBinary;

                                newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * firstPrefixLength;
                                newReflexivSubKmer[i] |= (transitBit1 << 2 * (31 - firstPrefixLength));
                                newReflexivSubKmer[i] &= maxSubKmerBinary;

                                transitBit1 = transitBit2;
                            }

                            newReflexivSubKmer[param.subKmerBinarySlots - 1] = transitBit1 << 2 * (param.subKmerSizeResidue - firstPrefixLength);
                            newReflexivSubKmer[param.subKmerBinarySlots - 1] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) >>> 2 * firstPrefixLength);
                            newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;


                            newReflexivLong = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) & maxPrefixLengthBinary;
                            newReflexivLong |= (1L << 2 * firstPrefixLength); // add C marker
                            newReflexivLongArray[0] = newReflexivLong;
                        }
                    }

                    return
                            RowFactory.create(newReflexivSubKmer,
                                    randomReflexivMarker, newReflexivLongArray, currentSubKmer.getInt(3), currentSubKmer.getInt(4)
                            );
                }

            }
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

    class DSLowCoverageReadDetection implements MapPartitionsFunction<String, Row>, Serializable{
        long maxSubKmerBits= ~((~0L) << (2*param.subKmerSize));
        int maxContigIndexBits = ~((~0) << 31);
        Hashtable<List<Long>, Integer> probTable;

        List<Row> fragmentList = new ArrayList<Row>();
        int readLength;
        String[] units;
        String read;
        char nucleotide;
        long nucleotideInt;
        long nucleotideIntComplement;

        public DSLowCoverageReadDetection(Broadcast<Hashtable<List<Long>, Integer>> s){
            probTable = s.value();
        }

        public Iterator<Row> call(Iterator<String> s){

            while (s.hasNext()) {
                units = s.next().split("\\n");
                read = units[1];
                String RCread = reverseComplement(read);

                readLength = read.length();

                if (readLength - param.subKmerSize<= 1) {
                    continue;
                }

                Long nucleotideBinary = 0L;
                Long nucleotideBinaryReverseComplement = 0L;


                int probedContigID =-1;
                int probedLeftIndex =-1;
                int probedRightIndex = -1;

                for (int i = 0; i < readLength; i++) {
                    nucleotide = read.charAt(i);
                    if (nucleotide >= 256) nucleotide = 255;
                    nucleotideInt = nucleotideValue(nucleotide);
                    // forward kmer in bits
                    nucleotideBinary <<= 2;
                    nucleotideBinary |= nucleotideInt;
                    if (i >= param.subKmerSize) {
                        nucleotideBinary &= maxSubKmerBits;
                    }

                    // reach the first complete K-mer
                    if (i >= param.subKmerSize - 1) {
                        if (probTable.containsKey(nucleotideBinary)){
                            int marker = (probTable.get(nucleotideBinary)& maxContigIndexBits);
                            int direction = ((probTable.get(nucleotideBinary) >>> 31) & 1);
/*
                            System.out.print("find a match: ");

                            for (int j=param.subKmerSize-1; j>=0; j--){
                                long a = nucleotideBinary  >>> 2 *j;
                                a &= 3L;
                                char b = BinaryToNucleotide(a);
                                System.out.print(b);
                            }

                            System.out.println(" " + marker + " " + direction);
*/
                            if (direction == 0) { // left extendable
                                if (probedLeftIndex == -1) {
                                    probedContigID = marker;
                                    probedLeftIndex = i;
                                }else{ // != -1 anthor left extendable, usually not possible
                                    continue;
                                }
                            }else if (direction == 1){ // right extendable
                                if (probedRightIndex == -1){
                                    if (probedContigID == marker ){
                                        continue;
                                    } else{
                                        probedRightIndex =i;
                                    }
                                }else { // != 1 anther right extendable, usually not possible
                                    if (probedContigID == marker ){
                                        continue;
                                    } else{
                                        probedRightIndex =i;
                                    }
                                }
                            }
                        }
                    }
                }

                if (probedLeftIndex >=0 && probedRightIndex >=0 && probedLeftIndex < probedRightIndex){
                    String lowCoverageFragment = read.substring(probedLeftIndex-param.subKmerSize+1, probedRightIndex+1);
                    System.out.println("forward: " + lowCoverageFragment);
                    fragmentList.add(
                            reflexivKmerExtractionFromLowCoverageFragment(lowCoverageFragment)
                    );
                }

                probedLeftIndex =-1;
                probedRightIndex =-1;
                probedContigID =-1;

                for (int i = 0; i < readLength; i++) {
                    nucleotide = RCread.charAt(i);
                    if (nucleotide >= 256) nucleotide = 255;
                    nucleotideInt = nucleotideValue(nucleotide);
                    // forward kmer in bits
                    nucleotideBinaryReverseComplement <<= 2;
                    nucleotideBinaryReverseComplement |= nucleotideInt;
                    if (i >= param.subKmerSize) {
                        nucleotideBinaryReverseComplement &= maxSubKmerBits;
                    }

                    // reach the first complete K-mer
                    if (i >= param.subKmerSize - 1) {
                        if (probTable.containsKey(nucleotideBinaryReverseComplement)){
                            int marker = (probTable.get(nucleotideBinaryReverseComplement) & maxContigIndexBits);
                            int direction = ((probTable.get(nucleotideBinaryReverseComplement) >>> 31) & 1);
/*
                            System.out.print("find a RC match: ");

                            for (int j=param.subKmerSize-1; j>=0; j--){
                                long a = nucleotideBinaryReverseComplement  >>> 2 *j;
                                a &= 3L;
                                char b = BinaryToNucleotide(a);
                                System.out.print(b);
                            }

                            System.out.println(" " + marker + " " + direction);
*/
                            if (direction == 0) { // left extendable
                                if (probedLeftIndex == -1) {
                                    probedContigID = marker ;
                                    probedLeftIndex = i;
                                }else{ // != -1 anthor left extendable, usually not possible
                                    continue;
                                }
                            }else if (direction == 1){ // right extendable
                                if (probedRightIndex == -1){
                                    if (probedContigID == marker ){
                                        continue;
                                    } else{
                                        probedRightIndex =i;
                                    }
                                }else { // != 1 anther right extendable, usually not possible
                                    if (probedContigID == marker ){
                                        continue;
                                    } else{
                                        probedRightIndex =i;
                                    }
                                }
                            }
                        }
                    }
                }

                if (probedLeftIndex >=0 && probedRightIndex >=0 && probedLeftIndex < probedRightIndex){
                    String lowCoverageRCFragment = RCread.substring(probedLeftIndex-param.subKmerSize+1, probedRightIndex+1);
                    System.out.println("RC: " + lowCoverageRCFragment);
                    fragmentList.add(
                            reflexivKmerExtractionFromLowCoverageFragment(lowCoverageRCFragment)
                    );
                }

            }
            return fragmentList.iterator();
        }

        private String reverseComplement(String forward){
            String reverseComplementNucleotides;


            char[] nucleotides = forward.toCharArray();
            int nucleotideNum = nucleotides.length;
            char[] nucleotidesRC = new char[nucleotideNum];

            for (int i=0; i<nucleotideNum; i++){
                nucleotidesRC[nucleotideNum-i-1] = complementary(nucleotides[i]);
            }

            reverseComplementNucleotides = new String(nucleotidesRC);
            return reverseComplementNucleotides;
        }

        private char complementary (char a){
            if (a == 'A' || a == 'a'){
                return 'T';
            }else if (a == 'T' || a == 't' || a == 'U' || a == 'u'){
                return 'A';
            }else if (a == 'C' || a == 'c'){
                return 'G';
            }else if (a == 'G' || a == 'g'){
                return 'C';
            }else {
                return 'N';
            }
        }

        private Row reflexivKmerExtractionFromLowCoverageFragment(String s){
            int remain = s.length() - param.subKmerSize;
            long nucleotideBinary=0L;
            long[] remainNucleotidesArray = new long[remain/31+1];
            int firstBlock = remain % 31;
            long maxFirstBlockBinary = ~((~0L) << (2 * firstBlock));
            char nucleotideNT;
            long nucleotideNTInt;

            for (int i = 0; i < param.subKmerSize; i++){
                nucleotideNT = s.charAt(i);
                if (nucleotideNT >= 256) nucleotideNT = 255;
                nucleotideNTInt = nucleotideValue(nucleotideNT);
                // forward kmer in bits
                nucleotideBinary <<= 2;
                nucleotideBinary |= nucleotideNTInt;
            }

            for (int i = param.subKmerSize; i < s.length(); i++){
                nucleotideNT = s.charAt(i);
                if (nucleotideNT >= 256) nucleotideNT = 255;
                nucleotideNTInt = nucleotideValue(nucleotideNT);

                if (i-param.subKmerSize - firstBlock >=0) {
                    remainNucleotidesArray[((i - param.subKmerSize - firstBlock) / 31)+1] <<= 2;
                    remainNucleotidesArray[((i - param.subKmerSize - firstBlock) / 31)+1] |= nucleotideNTInt;
                }else {
                    remainNucleotidesArray[0] <<=2;
                    remainNucleotidesArray[0] |= nucleotideNTInt;
                }

                if (i-param.subKmerSize == firstBlock-1){
                    remainNucleotidesArray[0] &= maxFirstBlockBinary;
                    remainNucleotidesArray[0] |= (1L << 2*firstBlock); // add C markers in front
                }
            }
/*
            System.out.print("binarized: ");

            for (int i=param.subKmerSize-1; i>=0; i--){
                long a = nucleotideBinary  >>> 2 *i;
                a &= 3L;
                char b = BinaryToNucleotide(a);
                System.out.print(b);
            }

            for (int i=firstBlock-1; i>=0; i--){
                long a = remainNucleotidesArray[0]  >>> 2 *i;
                a &= 3L;
                char b = BinaryToNucleotide(a);
                System.out.print(b);
            }

            for (int i=30; i>=0; i--){
                long a = remainNucleotidesArray[1]  >>> 2 *i;
                a &= 3L;
                char b = BinaryToNucleotide(a);
                System.out.print(b);
            }

            System.out.println();
*/
            return RowFactory.create(nucleotideBinary,
                    1, remainNucleotidesArray, -10000000, -10000000
            );
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
     * interface class for RDD implementation, used in step 5
     */

    class DSBinaryReflexivKmerToString implements MapPartitionsFunction<Row, Row>, Serializable {
        List<Row> reflexivKmerStringList = new ArrayList<Row>();

        public Iterator<Row> call(Iterator<Row> sIterator) {
            while (sIterator.hasNext()) {
                String subKmer = "";
                String subString = "";
                Row s = sIterator.next();
                int currentSuffixLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros(s.getLong(2)) / 2 + 1);
                for (int i = 1; i <= param.subKmerSize; i++) {
                    Long currentNucleotideBinary = s.getLong(0) >>> 2 * (param.subKmerSize - i);
                    currentNucleotideBinary &= 3L;
                    char currentNucleotide = BinaryToNucleotide(currentNucleotideBinary);
                    subKmer += currentNucleotide;
                }

                for (int i = 1; i <= currentSuffixLength; i++) {
                    Long currentNucleotideBinary = s.getLong(2) >>> 2 * (currentSuffixLength - i);
                    currentNucleotideBinary &= 3L;
                    char currentNucleotide = BinaryToNucleotide(currentNucleotideBinary);
                    subString += currentNucleotide;
                }

                reflexivKmerStringList.add(
                        RowFactory.create(
                                subKmer, s.getInt(1), subString, s.getInt(3), s.getInt(4))
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
    class DSBinaryReflexivKmerArrayToString implements MapPartitionsFunction<Row, Row>, Serializable {
        List<Row> reflexivKmerStringList = new ArrayList<Row>();

        public Iterator<Row> call(Iterator<Row> sIterator) {
            while (sIterator.hasNext()) {
                String subKmer = "";
                String subString = "";
                Row s = sIterator.next();

                int firstSuffixBlockLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros((Long) s.getSeq(2).apply(0)) / 2 + 1);

                for (int i = 0; i < param.subKmerSize- param.subKmerSizeResidue; i++) {
                    Long currentNucleotideBinary = (Long) s.getSeq(0).apply(i/31) >>> 2 * (31 - (i%31+1));
                    currentNucleotideBinary &= 3L;
                    char currentNucleotide = BinaryToNucleotide(currentNucleotideBinary);
                    subKmer += currentNucleotide;
                }

                for (int i= param.subKmerSize- param.subKmerSizeResidue; i< param.subKmerSize; i++){
                    Long currentNucleotideBinary = (Long) s.getSeq(0).apply(i/31) >>> 2 * (param.subKmerSizeResidue - (i%31+1));
                    currentNucleotideBinary &= 3L;
                    char currentNucleotide = BinaryToNucleotide(currentNucleotideBinary);
                    subKmer += currentNucleotide;
                }

                for (int i = 0; i < s.getSeq(2).length(); i++) {
                    if (i == 0) {
                        for (int j = 1; j <= firstSuffixBlockLength; j++) { // j=0 including the C marker; for debug
                            Long currentNucleotideBinary = (Long) s.getSeq(2).apply(i) >>> 2 * (firstSuffixBlockLength - j);
                            currentNucleotideBinary &= 3L;
                            char currentNucleotide = BinaryToNucleotide(currentNucleotideBinary);
                            subString += currentNucleotide;
                        }
                    } else {
                        for (int j = 1; j <= 31; j++) {
                            if (s.getSeq(2).apply(i) == null) {
                       //         System.out.println(subKmer + "\t" + subString);
                                continue;
                            }
                            Long currentNucleotideBinary = (Long) s.getSeq(2).apply(i) >>> 2 * (31 - j);
                            currentNucleotideBinary &= 3L;
                            char currentNucleotide = BinaryToNucleotide(currentNucleotideBinary);
                            subString += currentNucleotide;
                        }
                    }
                    //     subString += ">----<";
                }

                int length = subKmer.length() + subString.length();
                System.out.println("Length: " + length + " " + s.getInt(3) + " " + s.getInt(4));

                reflexivKmerStringList.add(
                        RowFactory.create(subKmer,
                                s.getInt(1), subString, s.getInt(3), s.getInt(4)
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

    class DSFilterRepeatLowCoverageFragment implements MapPartitionsFunction<Row, Row>, Serializable {

        /* marker to identify similar SubKmers in the loop sequence */
        private int lineMarker = 1;

        /* 1 stands for forward sub-kmer */
        /* 2 stands for reflexiv sub-kmer */
        // private int randomReflexivMarker = ThreadLocalRandom.current().nextInt(1, 3);
        private int randomReflexivMarker = 2;

        long maxSubKmerResidueBinary = ~((~0L) << 2 * param.subKmerSizeResidue);
        long maxSubKmerBinary = ~((~0L) << 2 * 31);


        //long maxBlockBinary = ~((~0L) << 2*31); // a block has 31 nucleotide


        /* temporary capsule to store identical SubKmer units */
        List<Row> tmpReflexivKmerExtendList = new ArrayList<Row>();

        /* return capsule of extend Tuples for next iteration*/
        List<Row> reflexivKmerConcatList = new ArrayList<Row>();

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
        public Iterator<Row> call(Iterator<Row> sIterator) {

            randomReflexivMarker=1; // in this class, randomReflexivMarker is constantly set to 1

            if (param.scramble ==3){
                randomReflexivMarker =1;
            }

            while (sIterator.hasNext()) {
                Row s = sIterator.next();

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
                        if (subKmerSlotComparator(s.getSeq(0), tmpReflexivKmerExtendList.get(0).getSeq(0))){
                            continue;
                        }else{
                            reflexivKmerConcatList.add(tmpReflexivKmerExtendList.get(0));
                            resetSubKmerGroup(s);
                        }
                    }// end of else condition

                    lineMarker++;
                    // return reflexivKmerConcatList.iterator();
                }
            } // while loop

            if (tmpReflexivKmerExtendList.size() != 0) {
                for (int i = 0; i < tmpReflexivKmerExtendList.size(); i++) {
                    reflexivKmerConcatList.add(tmpReflexivKmerExtendList.get(i));
                }
            }

      //      int kmernumber = reflexivKmerConcatList.size();
       //     System.out.println("Double afterwards extendable number: " + kmernumber);
            return reflexivKmerConcatList.iterator();
        }

        /**
         *
         * @param currentSubKmer
         */
        public void directKmerComparison(Row currentSubKmer) {
            tmpReflexivKmerExtendList.add(currentSubKmer);
        }

        /**
         *
         * @param S
         */
        public void resetSubKmerGroup(Row S) {
            if (lineMarker == 1) {
                lineMarker = 2;
            } else {
                lineMarker = 3; /* reset to new sub-kmer group */
            }
            /* re-reflex all single kmers in the sub-kmer group */
//            if (tmpReflexivKmerExtendList.size() != 0) {
//                for (int i = 0; i < tmpReflexivKmerExtendList.size(); i++) {
            //                   singleKmerRandomizer(tmpReflexivKmerExtendList.get(i));
            //               }
            //          }

            tmpReflexivKmerExtendList = new ArrayList<Row>();
            tmpReflexivKmerExtendList.add(S
                    //   RowFactory.create(S.getLong(0),
                    //                   S.getInt(1), S.get(2), S.getInt(3), S.getInt(4)
                    //   )
            );
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

    class DSReflexivAndForwardKmer implements MapPartitionsFunction<Row, Row>, Serializable {

        /* 1 stands for forward sub-kmer */
        /* 2 stands for reflexiv sub-kmer */
        // private int randomReflexivMarker = ThreadLocalRandom.current().nextInt(1, 3);
        private int randomReflexivMarker = 2;




        long maxSubKmerResidueBinary = ~((~0L) << 2 * param.subKmerSizeResidue);
        long maxSubKmerBinary = ~((~0L) << 2 * 31);


        /* return capsule of extend Tuples for next iteration*/
        List<Row> reflexivKmerConcatList = new ArrayList<Row>();

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
        public Iterator<Row> call(Iterator<Row> sIterator) {

            while (sIterator.hasNext()) {
                Row s = sIterator.next();

                reflexivKmerConcatList.add(s); // forward or reflected kmer
                if (s.getInt(1)==1) {
                    randomReflexivMarker =2;
                }else{
                    randomReflexivMarker =1;
                }
                singleKmerRandomizer(s); // reflected or forward kmer

            } // while loop

            return reflexivKmerConcatList.iterator();
        }

        /**
         *
         * @param currentSubKmer
         */
        public void singleKmerRandomizer(Row currentSubKmer) {
            int blockSize = currentSubKmer.getSeq(2).length();

            Long[] newReflexivLongArray = new Long[blockSize];

            if (currentSubKmer.getInt(1) == 1) {
                int firstSuffixBlockLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros((Long) currentSubKmer.getSeq(2).apply(0)) / 2 + 1);
                long maxSuffixLengthBinary = ~((~0L) << (2 * firstSuffixBlockLength));
                Long[] newReflexivSubKmer = new Long[param.subKmerBinarySlots];
                Long newReflexivLong;

                if (randomReflexivMarker == 2) {
                    if (blockSize > 1) {

                        if (firstSuffixBlockLength >= param.subKmerSizeResidue) {
                            if (blockSize >= param.subKmerBinarySlots) {
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] = ((Long) currentSubKmer.getSeq(2).apply(blockSize - 1) & maxSubKmerResidueBinary);

                                long transit1 = (Long) currentSubKmer.getSeq(2).apply(blockSize - 1) >>> (2 * param.subKmerSizeResidue);
                                long transit2 = 0L;
                                for (int i = param.subKmerBinarySlots - 2; i >= 0; i--) {
                                    int j = blockSize - param.subKmerBinarySlots + i; // index of suffix long array
                                    transit2 = (Long) currentSubKmer.getSeq(2).apply(j) >>> (2 * param.subKmerSizeResidue);

                                    newReflexivSubKmer[i] = transit1;
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(2).apply(j) << 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;

                                    transit1 = transit2;
                                }

                                for (int i = blockSize - param.subKmerBinarySlots; i > 0; i--) {
                                    int j = param.subKmerBinarySlots + i - 1; // index of the new prefix long array
                                    newReflexivLongArray[j] = ((Long) currentSubKmer.getSeq(2).apply(i) >>> 2 * param.subKmerSizeResidue);
                                    newReflexivLongArray[j] |= (Long) currentSubKmer.getSeq(2).apply(i - 1) << 2 * (31 - param.subKmerSizeResidue);
                                    newReflexivLongArray[j] &= maxSubKmerBinary;
                                }

                                //   if (blockSize== param.subKmerBinarySlots){ // in the context of blockSize >= param.subKmerBinarySlots
                                newReflexivLongArray[param.subKmerBinarySlots - 1] = ((Long) currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary) >>> (2 * param.subKmerSizeResidue);
                                newReflexivLongArray[param.subKmerBinarySlots - 1] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) << 2 * (firstSuffixBlockLength - param.subKmerSizeResidue));
                                newReflexivLongArray[param.subKmerBinarySlots - 1] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 2) << 2 * firstSuffixBlockLength);
                                newReflexivLongArray[param.subKmerBinarySlots - 1] &= maxSubKmerBinary;
                                //  }

                                for (int i = param.subKmerBinarySlots - 2; i > 0; i--) {
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * (31 - firstSuffixBlockLength);
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(0).apply(i - 1) << 2 * firstSuffixBlockLength);
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                newReflexivLongArray[0] = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * (31 - firstSuffixBlockLength);
                                newReflexivLongArray[0] |= (1L << 2 * firstSuffixBlockLength); // add C marker in the front
                            } else { // blockSize < param.subKmerSizeResidue
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] = ((Long) currentSubKmer.getSeq(2).apply(blockSize - 1) & maxSubKmerResidueBinary);

                                long transit1 = (Long) currentSubKmer.getSeq(2).apply(blockSize - 1) >>> (2 * param.subKmerSizeResidue);
                                long transit2 = 0L;
                                for (int i = param.subKmerBinarySlots - 2; i >= param.subKmerBinarySlots - blockSize; i--) {
                                    int j = blockSize - param.subKmerBinarySlots + i; // index of suffix long array
                                    transit2 = (Long) currentSubKmer.getSeq(2).apply(j) >>> (2 * param.subKmerSizeResidue);

                                    newReflexivSubKmer[i] = transit1;
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(2).apply(j) << 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;

                                    transit1 = transit2;
                                }

                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize - 1] = ((Long) currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary) >>> (2 * param.subKmerSizeResidue);
                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize - 1] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) << 2 * (firstSuffixBlockLength - param.subKmerSizeResidue));
                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize - 1] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 2) << 2 * firstSuffixBlockLength);
                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize - 1] &= maxSubKmerBinary;

                                for (int i = param.subKmerBinarySlots - blockSize - 2; i >= 0; i--) {
                                    int j = blockSize + i; // index of the subkmer

                                    newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(j) >>> 2 * (31 - firstSuffixBlockLength);
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(0).apply(j - 1) << 2 * firstSuffixBlockLength);
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;
                                }

                                for (int i = blockSize - 1; i > 0; i--) {
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * (31 - firstSuffixBlockLength);
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(0).apply(i - 1) << 2 * firstSuffixBlockLength);
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                newReflexivLongArray[0] = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * (31 - firstSuffixBlockLength);
                                newReflexivLongArray[0] |= (1L << 2 * firstSuffixBlockLength); // add C marker in the front
                            }
                        } else { // firstSuffixBlockLength < param.subKmerSizeResidue
                            if (blockSize >= param.subKmerBinarySlots) {
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] = ((Long) currentSubKmer.getSeq(2).apply(blockSize - 1) & maxSubKmerResidueBinary);

                                long transit1 = (Long) currentSubKmer.getSeq(2).apply(blockSize - 1) >>> (2 * param.subKmerSizeResidue);
                                long transit2 = 0L;
                                for (int i = param.subKmerBinarySlots - 2; i > 0; i--) {
                                    int j = blockSize - param.subKmerBinarySlots + i; // index of suffix long array
                                    transit2 = (Long) currentSubKmer.getSeq(2).apply(j) >>> (2 * param.subKmerSizeResidue);

                                    newReflexivSubKmer[i] = transit1;
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(2).apply(j) << 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;

                                    transit1 = transit2;
                                }

                                newReflexivSubKmer[0] = transit1;
                                if (blockSize > param.subKmerBinarySlots) {
                                    newReflexivSubKmer[0] |= ((Long) currentSubKmer.getSeq(2).apply(blockSize - param.subKmerBinarySlots) << 2 * (31 - param.subKmerSizeResidue));
                                }else { // (blockSize== param.subKmerBinarySlots){
                                    newReflexivSubKmer[0] |= (((Long) currentSubKmer.getSeq(2).apply(blockSize - param.subKmerBinarySlots) & maxSuffixLengthBinary) << 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivSubKmer[0] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots-1) << 2*(31-param.subKmerSizeResidue + firstSuffixBlockLength));
                                }
                                newReflexivSubKmer[0] &= maxSubKmerBinary;

                                //newReflexivSubKmer[0] = transit1;
                                //newReflexivSubKmer[0] |= ((Long) currentSubKmer.getSeq(2).apply(blockSize-param.subKmerBinarySlots) << 2*(31-param.subKmerSizeResidue));


                                for (int i = blockSize - param.subKmerBinarySlots; i > 1; i--) {
                                    int j = param.subKmerBinarySlots + i - 1; // index of the new prefix long array
                                    newReflexivLongArray[j] = ((Long) currentSubKmer.getSeq(2).apply(i) >>> 2 * param.subKmerSizeResidue);
                                    newReflexivLongArray[j] |= (Long) currentSubKmer.getSeq(2).apply(i - 1) << 2 * (31 - param.subKmerSizeResidue);
                                    newReflexivLongArray[j] &= maxSubKmerBinary;
                                }

                                if (param.subKmerBinarySlots < blockSize) {
                                    newReflexivLongArray[param.subKmerBinarySlots] = ((Long) currentSubKmer.getSeq(2).apply(1) >>> 2 * param.subKmerSizeResidue);
                                    newReflexivLongArray[param.subKmerBinarySlots] |= ((Long) currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary)<< 2 * (31 - param.subKmerSizeResidue);
                                    newReflexivLongArray[param.subKmerBinarySlots] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) << 2 * (31 + firstSuffixBlockLength - param.subKmerSizeResidue));
                                    newReflexivLongArray[param.subKmerBinarySlots] &= maxSubKmerBinary;
                                }

                                //   if (blockSize== param.subKmerBinarySlots){ // in the context of blockSize >= param.subKmerBinarySlots
                                //  newReflexivLongArray[param.subKmerBinarySlots-1] = ((Long) currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary) >>> (2 * param.subKmerSizeResidue);
                                newReflexivLongArray[param.subKmerBinarySlots - 1] = ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) >>> 2 * (param.subKmerSizeResidue - firstSuffixBlockLength));
                                newReflexivLongArray[param.subKmerBinarySlots - 1] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 2) << 2 * firstSuffixBlockLength);
                                newReflexivLongArray[param.subKmerBinarySlots - 1] &= maxSubKmerBinary;
                                //  }

                                for (int i = param.subKmerBinarySlots - 2; i > 0; i--) {
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * (31 - firstSuffixBlockLength);
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(0).apply(i - 1) << 2 * firstSuffixBlockLength);
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                newReflexivLongArray[0] = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * (31 - firstSuffixBlockLength);
                                newReflexivLongArray[0] |= (1L << 2 * firstSuffixBlockLength); // add C marker in the front
                            } else { // blockSize < param.subKmerBinarySlots
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] = ((Long) currentSubKmer.getSeq(2).apply(blockSize - 1) & maxSubKmerResidueBinary);

                                long transit1 = (Long) currentSubKmer.getSeq(2).apply(blockSize - 1) >>> (2 * param.subKmerSizeResidue);
                                long transit2 = 0L;
                                for (int i = param.subKmerBinarySlots - 2; i > param.subKmerBinarySlots - blockSize; i--) {
                                    int j = blockSize - param.subKmerBinarySlots + i; // index of suffix long array
                                    transit2 = (Long) currentSubKmer.getSeq(2).apply(j) >>> (2 * param.subKmerSizeResidue);

                                    newReflexivSubKmer[i] = transit1;
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(2).apply(j) << 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;

                                    transit1 = transit2;
                                }

                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize] = (Long) currentSubKmer.getSeq(2).apply(1) >>> 2 * param.subKmerSizeResidue;
                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize] |= ((Long) currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary) << 2 * (31 - param.subKmerSizeResidue);
                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) << 2 * (31 - param.subKmerSizeResidue + firstSuffixBlockLength));
                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize] &= maxSubKmerBinary;

                                // newReflexivSubKmer[param.subKmerBinarySlots-blockSize-1] = ((Long) currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary) >>> (2 * param.subKmerSizeResidue);
                                /* System.out.println("mark  newReflxivSubkmer Length: " + newReflexivSubKmer.length);
                                System.out.println("mark  first newReflexivBlock: " + newReflexivSubKmer[0]);
                                System.out.println("mark  second newReflexivBlock: " + newReflexivSubKmer[1]);
                                System.out.println("mark  third newReflexivBlock: " + newReflexivSubKmer[2]);
                                System.out.println("mark  blockSize: " + blockSize);
                                System.out.println("mark  currentSubKmer: " + currentSubKmer.getSeq(0).size());
                                System.out.println("mark  first currentSubKmer: " + currentSubKmer.getSeq(0).apply(0));
                                System.out.println("mark  second currentSubKmer: " + currentSubKmer.getSeq(0).apply(1));
                                System.out.println("mark  third currentSubKmer: " + currentSubKmer.getSeq(0).apply(2));
                                System.out.println("mark  param.subKmerBinaryslots: " + param.subKmerBinarySlots);
*/
                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize - 1] = ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) >>> 2 * (param.subKmerSizeResidue - firstSuffixBlockLength));
                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize - 1] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 2) << 2 * firstSuffixBlockLength);
                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize - 1] &= maxSubKmerBinary;

                                for (int i = param.subKmerBinarySlots - blockSize - 2; i >= 0; i--) {
                                    int j = blockSize + i; // index of the subkmer

                                    newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(j) >>> 2 * (31 - firstSuffixBlockLength);
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(0).apply(j - 1) << 2 * firstSuffixBlockLength);
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;
                                }

                                for (int i = blockSize - 1; i > 0; i--) {
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * (31 - firstSuffixBlockLength);
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(0).apply(i - 1) << 2 * firstSuffixBlockLength);
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                newReflexivLongArray[0] = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * (31 - firstSuffixBlockLength);
                                newReflexivLongArray[0] |= (1L << 2 * firstSuffixBlockLength); // add C marker in the front
                            }
                        }


                    } else { // block size ==1
                        if (firstSuffixBlockLength > param.subKmerSizeResidue) { // firstSuffixBlockLength is longer than the length of the last block (element) of sub kmer long array
                            //long transitBit1 = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) << 2 * (firstSuffixBlockLength - param.subKmerSizeResidue);
                            long transitBit1 = (((Long)currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary) >>> 2 * param.subKmerSizeResidue);
                            long transitBit2 = 0L;

                            newReflexivSubKmer[param.subKmerBinarySlots - 1] = ((Long) currentSubKmer.getSeq(2).apply(0) & maxSubKmerResidueBinary);

                            newReflexivSubKmer[param.subKmerBinarySlots - 2] = ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots-1) << 2* (firstSuffixBlockLength-param.subKmerSizeResidue));
                            newReflexivSubKmer[param.subKmerBinarySlots - 2] |= transitBit1;
                            newReflexivSubKmer[param.subKmerBinarySlots - 2] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots-2) << 2*firstSuffixBlockLength);
                            newReflexivSubKmer[param.subKmerBinarySlots - 2] &= maxSubKmerBinary;
                            transitBit1 = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots-2) >>> 2 * (31 - firstSuffixBlockLength);

                            for (int i = param.subKmerBinarySlots - 3; i >= 0; i--) {
                                transitBit2 = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * (31 - firstSuffixBlockLength);

                                newReflexivSubKmer[i] = ((Long) currentSubKmer.getSeq(0).apply(i) << 2 * firstSuffixBlockLength);
                                newReflexivSubKmer[i] |= transitBit1;
                                newReflexivSubKmer[i] &= maxSubKmerBinary;

                                transitBit1 = transitBit2;
                            }

                            newReflexivLong = transitBit1;
                            newReflexivLong |= (1L << 2 * firstSuffixBlockLength);
                            newReflexivLongArray[0] = newReflexivLong;

                        } else if (firstSuffixBlockLength == param.subKmerSizeResidue) {
                            long transitBit1 = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                            long transitBit2 = 0L;

                            newReflexivSubKmer[param.subKmerBinarySlots - 1] = ((Long) currentSubKmer.getSeq(2).apply(0) & maxSubKmerResidueBinary);

                            for (int i = param.subKmerBinarySlots - 2; i >= 0; i--) {
                                transitBit2 = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * (31 - firstSuffixBlockLength);

                                newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(i) << 2 * firstSuffixBlockLength;
                                newReflexivSubKmer[i] |= transitBit1;
                                newReflexivSubKmer[i] &= maxSubKmerBinary;

                                transitBit1 = transitBit2;
                            }

                            newReflexivLong = transitBit2;
                            newReflexivLong |= (1L << 2 * firstSuffixBlockLength);
                            newReflexivLongArray[0] = newReflexivLong;

                        } else { //firstSuffixBlockLength < param.subKmerSizeResidue
                            long transitBit1 = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) >>> 2 * (param.subKmerSizeResidue - firstSuffixBlockLength);
                            long transitBit2 = 0L;

                            newReflexivSubKmer[param.subKmerBinarySlots - 1] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) << 2 * firstSuffixBlockLength;
                            newReflexivSubKmer[param.subKmerBinarySlots - 1] |= ((Long) currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary);
                            newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;

                            for (int i = param.subKmerBinarySlots - 2; i >= 0; i--) {
                                transitBit2 = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * (31 - firstSuffixBlockLength);

                                newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(i) << 2 * firstSuffixBlockLength;
                                newReflexivSubKmer[i] |= transitBit1;
                                newReflexivSubKmer[i] &= maxSubKmerBinary;

                                transitBit1 = transitBit2;
                            }

                            newReflexivLong = transitBit2;
                            newReflexivLong |= (1L << 2 * firstSuffixBlockLength); // add C marker
                            newReflexivLongArray[0] = newReflexivLong;
                        }
                    }

/*
                    for (int k = 30; k >= 0; k--) {
                        long a = newReflexivSubKmer[0] >> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    for (int k = 30; k >= 0; k--) {
                        long a = newReflexivSubKmer[1] >>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    if (newReflexivSubKmer.length >=3){
                        for (int k = 30; k >= 0; k--) {
                            long a = newReflexivSubKmer[2] >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    if (newReflexivSubKmer.length >=4){
                        for (int k = 16; k >= 0; k--) {
                            long a = newReflexivSubKmer[3] >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }


                    System.out.print(" ");
                    for (int k = 30; k >= 0; k--) {
                        long a = newReflexivLongArray[0] >>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    if (newReflexivLongArray.length >=2){
                        for (int k = 30; k >= 0; k--) {
                            long a = newReflexivLongArray[1] >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    if (newReflexivLongArray.length >=3){
                        for (int k = 30; k >= 0; k--) {
                            long a = newReflexivLongArray[2] >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    if (newReflexivLongArray.length >=4){
                        for (int k = 30; k >= 0; k--) {
                            long a = newReflexivLongArray[3] >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }


                       System.out.println(" Double random " + randomReflexivMarker);
*/
                    reflexivKmerConcatList.add(
                            RowFactory.create(JavaConverters.asScalaIteratorConverter(Arrays.asList(newReflexivSubKmer).iterator()).asScala().toSeq(),
                                    randomReflexivMarker,  JavaConverters.asScalaIteratorConverter(Arrays.asList(newReflexivLongArray).iterator()).asScala().toSeq(), currentSubKmer.getInt(3), currentSubKmer.getInt(4)
                            )
                    );
                } else {

/*
                    for (int k = 30; k >= 0; k--) {
                        long a = (Long)currentSubKmer.getSeq(0).apply(0)>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    for (int k = 30; k >= 0; k--) {
                        long a = (Long)currentSubKmer.getSeq(0).apply(1)>>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    if (currentSubKmer.getSeq(0).length() >=3){
                        for (int k = 30; k >= 0; k--) {
                            long a = (Long)currentSubKmer.getSeq(0).apply(2)>>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    if (currentSubKmer.getSeq(0).length() >=4){
                        for (int k = 16; k >= 0; k--) {
                            long a = (Long)currentSubKmer.getSeq(0).apply(3)>>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }


                    System.out.print(" ");
                    for (int k = 30; k >= 0; k--) {
                        long a = (Long)currentSubKmer.getSeq(2).apply(0) >>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    if (newReflexivLongArray.length >=2){
                        for (int k = 30; k >= 0; k--) {
                            long a = (Long)currentSubKmer.getSeq(2).apply(1) >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    if (newReflexivLongArray.length >=3){
                        for (int k = 30; k >= 0; k--) {
                            long a = (Long)currentSubKmer.getSeq(2).apply(2) >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    if (newReflexivLongArray.length >=4){
                        for (int k = 30; k >= 0; k--) {
                            long a = (Long)currentSubKmer.getSeq(2).apply(3) >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }


                           System.out.println(" Double random same " + randomReflexivMarker);
                           */
                    reflexivKmerConcatList.add(currentSubKmer);
                }
            } else { /* currentSubKmer.getInt(1) == 2 */
                int firstPrefixLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros((Long) currentSubKmer.getSeq(2).apply(0)) / 2 + 1);
                long maxPrefixLengthBinary = ~((~0L) << (2 * firstPrefixLength));
                Long[] newReflexivSubKmer = new Long[param.subKmerBinarySlots];
                long newReflexivLong = 0L;


                if (randomReflexivMarker == 2) {

/*
                    for (int k = 30; k >= 0; k--) {
                        long a = (Long)currentSubKmer.getSeq(0).apply(0)>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    for (int k = 30; k >= 0; k--) {
                        long a = (Long)currentSubKmer.getSeq(0).apply(1)>>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    if (currentSubKmer.getSeq(0).length() >=3){
                        for (int k = 30; k >= 0; k--) {
                            long a = (Long)currentSubKmer.getSeq(0).apply(2)>>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    if (currentSubKmer.getSeq(0).length() >=4){
                        for (int k = 16; k >= 0; k--) {
                            long a = (Long)currentSubKmer.getSeq(0).apply(3)>>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }


                    System.out.print(" ");
                    for (int k = 30; k >= 0; k--) {
                        long a = (Long)currentSubKmer.getSeq(2).apply(0) >>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    if (newReflexivLongArray.length >=2){
                        for (int k = 30; k >= 0; k--) {
                            long a = (Long)currentSubKmer.getSeq(2).apply(1) >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    if (newReflexivLongArray.length >=3){
                        for (int k = 30; k >= 0; k--) {
                            long a = (Long)currentSubKmer.getSeq(2).apply(2) >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    if (newReflexivLongArray.length >=4){
                        for (int k = 30; k >= 0; k--) {
                            long a = (Long)currentSubKmer.getSeq(2).apply(3) >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    System.out.println(" Double random same " + randomReflexivMarker);
*/
                    reflexivKmerConcatList.add(currentSubKmer);
                } else { /* randomReflexivMarker == 1 */
                    if (blockSize > 1) {
                        if (firstPrefixLength >= param.subKmerSizeResidue) {
                            if (blockSize >= param.subKmerBinarySlots) {
                                for (int i = 0; i < param.subKmerBinarySlots - 1; i++) {
                                    newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(2).apply(i) << 2 * (31 - firstPrefixLength);
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(2).apply(i + 1) >>> 2 * firstPrefixLength);
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;
                                }

                                newReflexivSubKmer[param.subKmerBinarySlots - 1] = (Long) currentSubKmer.getSeq(2).apply(param.subKmerBinarySlots - 1) >>> 2 * (firstPrefixLength - param.subKmerSizeResidue);
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;

                                newReflexivLongArray[0] = (Long) currentSubKmer.getSeq(2).apply(param.subKmerBinarySlots - 1) << 2 * param.subKmerSizeResidue;
                                if (blockSize > param.subKmerBinarySlots) {
                                    newReflexivLongArray[0] |= ((Long) currentSubKmer.getSeq(2).apply(param.subKmerBinarySlots) >>> 2 * (31 - param.subKmerSizeResidue));
                                } else {
                                    newReflexivLongArray[0] |= ((Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * (31 - param.subKmerSizeResidue));
                                }
                                newReflexivLongArray[0] &= maxPrefixLengthBinary;
                                newReflexivLongArray[0] |= (1L << 2 * firstPrefixLength); // add C marker in the front

                                for (int i = 1; i < blockSize - param.subKmerBinarySlots; i++) {
                                    int j = param.subKmerBinarySlots + i - 1;
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(2).apply(j) << 2 * param.subKmerSizeResidue;
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(2).apply(j + 1) >>> 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                if (blockSize > param.subKmerBinarySlots) {
                                    newReflexivLongArray[blockSize - param.subKmerBinarySlots] = (Long) currentSubKmer.getSeq(2).apply(blockSize - 1) << 2 * param.subKmerSizeResidue;
                                    newReflexivLongArray[blockSize - param.subKmerBinarySlots] |= ((Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivLongArray[blockSize - param.subKmerBinarySlots] &= maxSubKmerBinary;
                                }

                                for (int i = blockSize - param.subKmerBinarySlots + 1; i < blockSize - 1; i++) {
                                    int j = i - blockSize + param.subKmerBinarySlots - 1; // index of subKmer
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(0).apply(j) << 2 * param.subKmerSizeResidue;
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(0).apply(j + 1) >>> 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                newReflexivLongArray[blockSize - 1] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 2) << 2 * param.subKmerSizeResidue;
                                newReflexivLongArray[blockSize - 1] |= (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                                newReflexivLongArray[blockSize - 1] &= maxSubKmerBinary;

                            } else { // blockSize < param.subKmerBinarySlots
                                for (int i = 0; i < blockSize - 1; i++) {
                                    newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(2).apply(i) << 2 * (31 - firstPrefixLength);
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(2).apply(i + 1) >>> 2 * firstPrefixLength);
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;
                                }

                                newReflexivSubKmer[blockSize - 1] = (Long) currentSubKmer.getSeq(2).apply(blockSize - 1) << 2 * (31 - firstPrefixLength);
                                newReflexivSubKmer[blockSize - 1] |= ((Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * firstPrefixLength);
                                newReflexivSubKmer[blockSize - 1] &= maxSubKmerBinary;

                                for (int i = blockSize; i < param.subKmerBinarySlots - 1; i++) {
                                    int j = i - blockSize; // index of subKmer
                                    newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(j) << 2 * (31 - firstPrefixLength);
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(0).apply(j + 1) >>> 2 * firstPrefixLength);
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;
                                }

                                newReflexivSubKmer[param.subKmerBinarySlots - 1] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - blockSize - 1) >>> 2 * (firstPrefixLength - param.subKmerSizeResidue);
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;

                                newReflexivLongArray[0] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - blockSize - 1) << 2 * param.subKmerSizeResidue;
                                newReflexivLongArray[0] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - blockSize) >>> 2 * (31 - param.subKmerSizeResidue));
                                newReflexivLongArray[0] &= maxPrefixLengthBinary;
                                newReflexivLongArray[0] |= (1L << 2 * firstPrefixLength);

                                for (int i = 1; i < blockSize - 1; i++) {
                                    int j = param.subKmerBinarySlots - blockSize - 1 + i;
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(0).apply(j) << 2 * param.subKmerSizeResidue;
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(0).apply(j + 1) >>> 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                newReflexivLongArray[blockSize - 1] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 2) << 2 * param.subKmerSizeResidue;
                                newReflexivLongArray[blockSize - 1] |= (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                                newReflexivLongArray[blockSize - 1] &= maxSubKmerBinary;
                            }
                        } else { // firstPrefixLength < param.subKmerSizeResidue
                            if (blockSize >= param.subKmerBinarySlots) {
                                newReflexivSubKmer[0] =((Long) currentSubKmer.getSeq(2).apply(0) & maxPrefixLengthBinary) << 2 * (31 - firstPrefixLength);
                                newReflexivSubKmer[0] |= ((Long) currentSubKmer.getSeq(2).apply(1) >>> 2 * firstPrefixLength);
                                newReflexivSubKmer[0] &= maxSubKmerBinary;

                                for (int i = 1; i < param.subKmerBinarySlots - 1; i++) {
                                    newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(2).apply(i) << 2 * (31 - firstPrefixLength);
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(2).apply(i + 1) >>> 2 * firstPrefixLength);
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;
                                }

                                newReflexivSubKmer[param.subKmerBinarySlots - 1] = (Long) currentSubKmer.getSeq(2).apply(param.subKmerBinarySlots - 1) << 2 * (param.subKmerSizeResidue - firstPrefixLength);
                                if (blockSize > param.subKmerBinarySlots) {
                                    newReflexivSubKmer[param.subKmerBinarySlots - 1] |= ((Long) currentSubKmer.getSeq(2).apply(param.subKmerBinarySlots) >>> 2 * (31 - param.subKmerSizeResidue + firstPrefixLength));
                                } else { // blockSize == param.subKmerBinarySlots
                                    newReflexivSubKmer[param.subKmerBinarySlots - 1] |= ((Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * (31 - param.subKmerSizeResidue + firstPrefixLength));
                                }
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;


                                if (blockSize > param.subKmerBinarySlots) {
                                    newReflexivLongArray[0] = (Long) currentSubKmer.getSeq(2).apply(param.subKmerBinarySlots) >>> 2 * (31 - param.subKmerSizeResidue);
                                } else { // blockSize == param.subKmerBinarySlots
                                    newReflexivLongArray[0] = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * (31 - param.subKmerSizeResidue);
                                }
                                newReflexivLongArray[0] &= maxPrefixLengthBinary;
                                newReflexivLongArray[0] |= (1L << 2 * firstPrefixLength);

                                for (int i = 1; i < blockSize - param.subKmerBinarySlots; i++) {
                                    int j = param.subKmerBinarySlots + i - 1;
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(2).apply(j) << 2 * param.subKmerSizeResidue;
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(2).apply(j + 1) >>> 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                if (blockSize > param.subKmerBinarySlots) {
                                    newReflexivLongArray[blockSize - param.subKmerBinarySlots] = (Long) currentSubKmer.getSeq(2).apply(blockSize - 1) << 2 * param.subKmerSizeResidue;
                                    newReflexivLongArray[blockSize - param.subKmerBinarySlots] |= ((Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivLongArray[blockSize - param.subKmerBinarySlots] &= maxSubKmerBinary;
                                }

                                for (int i = blockSize - param.subKmerBinarySlots + 1; i < blockSize - 1; i++) {
                                    int j = i - blockSize + param.subKmerBinarySlots - 1; // index of subKmer
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(0).apply(j) << 2 * param.subKmerSizeResidue;
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(0).apply(j + 1) >>> 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                newReflexivLongArray[blockSize - 1] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 2) << 2 * param.subKmerSizeResidue;
                                newReflexivLongArray[blockSize - 1] |= (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                                newReflexivLongArray[blockSize - 1] &= maxSubKmerBinary;


                            } else { // blockSize < param.subKmerBinarySlots
                                newReflexivSubKmer[0] = ( (Long) currentSubKmer.getSeq(2).apply(0) & maxPrefixLengthBinary) << 2 * (31 - firstPrefixLength);
                                newReflexivSubKmer[0] |= ((Long) currentSubKmer.getSeq(2).apply(1) >>> 2 * firstPrefixLength);
                                newReflexivSubKmer[0] &= maxSubKmerBinary;
                                for (int i = 1; i < blockSize - 1; i++) {
                                    newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(2).apply(i) << 2 * (31 - firstPrefixLength);
                                    newReflexivSubKmer[i] |=( (Long) currentSubKmer.getSeq(2).apply(i + 1) >>> 2 * firstPrefixLength);
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;
                                }

                                newReflexivSubKmer[blockSize - 1] = (Long) currentSubKmer.getSeq(2).apply(blockSize - 1) << 2 * (31 - firstPrefixLength);
                                newReflexivSubKmer[blockSize - 1] |= ((Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * firstPrefixLength);
                                newReflexivSubKmer[blockSize - 1] &= maxSubKmerBinary;

                                for (int i = blockSize; i < param.subKmerBinarySlots - 1; i++) {
                                    int j = i - blockSize; // index of subKmer
                                    newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(j) << 2 * (31 - firstPrefixLength);
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(0).apply(j + 1) >>> 2 * firstPrefixLength);
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;
                                }

                                // newReflexivSubKmer[param.subKmerBinarySlots-1] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots-blockSize-1) >>> 2*(firstPrefixLength-param.subKmerSizeResidue);
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - blockSize - 1) << 2 * (param.subKmerSizeResidue - firstPrefixLength);
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - blockSize) >>> 2 * (31 - param.subKmerSizeResidue + firstPrefixLength));
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;

                                // newReflexivLongArray[0] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots-blockSize) << 2*param.subKmerSizeResidue;
                                newReflexivLongArray[0] = ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - blockSize) >>> 2 * (31 - param.subKmerSizeResidue));
                                newReflexivLongArray[0] &= maxPrefixLengthBinary;
                                newReflexivLongArray[0] |= (1L << 2 * firstPrefixLength);

                                for (int i = 1; i < blockSize - 1; i++) {
                                    int j = param.subKmerBinarySlots - blockSize + i -1;
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(0).apply(j) << 2 * param.subKmerSizeResidue;
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(0).apply(j + 1) >>> 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                newReflexivLongArray[blockSize - 1] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 2) << 2 * param.subKmerSizeResidue;
                                newReflexivLongArray[blockSize - 1] |= (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                                newReflexivLongArray[blockSize - 1] &= maxSubKmerBinary;
                            }
                        }

                    } else { /* blockSize == 1*/
                        if (firstPrefixLength > param.subKmerSizeResidue) {
                            long transitBit1 = (Long) currentSubKmer.getSeq(0).apply(0) & maxPrefixLengthBinary;

                            newReflexivSubKmer[0] = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * firstPrefixLength;
                            newReflexivSubKmer[0] |= (((Long) currentSubKmer.getSeq(2).apply(0) & maxPrefixLengthBinary) << 2 * (31 - firstPrefixLength));
                            newReflexivSubKmer[0] &= maxSubKmerBinary;

                            for (int i = 1; i < param.subKmerBinarySlots - 1; i++) {
                                long transitBit2 = (Long) currentSubKmer.getSeq(0).apply(i) & maxPrefixLengthBinary;

                                newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * firstPrefixLength;
                                newReflexivSubKmer[i] |= (transitBit1 << 2 * (31 - firstPrefixLength));
                                newReflexivSubKmer[i] &= maxSubKmerBinary;

                                transitBit1 = transitBit2;
                            }

                            newReflexivSubKmer[param.subKmerBinarySlots - 1] = transitBit1 >>> 2*(firstPrefixLength- param.subKmerSizeResidue);
                            newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;

                            newReflexivLong = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                            newReflexivLong |= (transitBit1 << 2 * param.subKmerSizeResidue);
                            newReflexivLong &= maxPrefixLengthBinary;
                            newReflexivLong |= (1L << 2 * firstPrefixLength); // add C marker in the front

                            newReflexivLongArray[0] = newReflexivLong;
                        } else if (firstPrefixLength == param.subKmerSizeResidue) {
                            long transitBit1 = (Long) currentSubKmer.getSeq(0).apply(0) & maxPrefixLengthBinary;

                            newReflexivSubKmer[0] = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * firstPrefixLength;
                            newReflexivSubKmer[0] |= (((Long) currentSubKmer.getSeq(2).apply(0) & maxPrefixLengthBinary) << 2 * (31 - firstPrefixLength));
                            newReflexivSubKmer[0] &= maxSubKmerBinary;

                            for (int i = 1; i < param.subKmerBinarySlots - 1; i++) {
                                long transitBit2 = (Long) currentSubKmer.getSeq(0).apply(i) & maxPrefixLengthBinary;

                                newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * firstPrefixLength;
                                newReflexivSubKmer[i] |= (transitBit1 << 2 * (31 - firstPrefixLength));
                                newReflexivSubKmer[i] &= maxSubKmerBinary;

                                transitBit1 = transitBit2;
                            }

                            newReflexivSubKmer[param.subKmerBinarySlots - 1] = transitBit1;
                            newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;

                            newReflexivLong = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                            newReflexivLong |= (1L << 2 * firstPrefixLength); // add C marker

                            newReflexivLongArray[0] = newReflexivLong;
                        } else {
                            long transitBit1 = (Long) currentSubKmer.getSeq(0).apply(0) & maxPrefixLengthBinary;

                            newReflexivSubKmer[0] = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * firstPrefixLength;
                            newReflexivSubKmer[0] |= (((Long) currentSubKmer.getSeq(2).apply(0) & maxPrefixLengthBinary) << 2 * (31 - firstPrefixLength));
                            newReflexivSubKmer[0] &= maxSubKmerBinary;

                            for (int i = 1; i < param.subKmerBinarySlots - 1; i++) {
                                long transitBit2 = (Long) currentSubKmer.getSeq(0).apply(i) & maxPrefixLengthBinary;

                                newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * firstPrefixLength;
                                newReflexivSubKmer[i] |= (transitBit1 << 2 * (31 - firstPrefixLength));
                                newReflexivSubKmer[i] &= maxSubKmerBinary;

                                transitBit1 = transitBit2;
                            }

                            newReflexivSubKmer[param.subKmerBinarySlots - 1] = transitBit1 << 2 * (param.subKmerSizeResidue - firstPrefixLength);
                            newReflexivSubKmer[param.subKmerBinarySlots - 1] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) >>> 2 * firstPrefixLength);
                            newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;


                            newReflexivLong = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) & maxPrefixLengthBinary;
                            newReflexivLong |= (1L << 2 * firstPrefixLength); // add C marker
                            newReflexivLongArray[0] = newReflexivLong;
                        }
                    }

/*
                    for (int k = 30; k >= 0; k--) {
                        long a = newReflexivSubKmer[0] >> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    for (int k = 30; k >= 0; k--) {
                        long a = newReflexivSubKmer[1] >>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    if (newReflexivSubKmer.length >=3){
                        for (int k = 30; k >= 0; k--) {
                            long a = newReflexivSubKmer[2] >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    if (newReflexivSubKmer.length >=4){
                        for (int k = 16; k >= 0; k--) {
                            long a = newReflexivSubKmer[3] >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    System.out.print(" ");
                    for (int k = 30; k >= 0; k--) {
                        long a = newReflexivLongArray[0] >>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    if (newReflexivLongArray.length >=2){
                        for (int k = 30; k >= 0; k--) {
                            long a = newReflexivLongArray[1] >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    if (newReflexivLongArray.length >=3){
                        for (int k = 30; k >= 0; k--) {
                            long a = newReflexivLongArray[2] >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    if (newReflexivLongArray.length >=4){
                        for (int k = 30; k >= 0; k--) {
                            long a = newReflexivLongArray[3] >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }


                    System.out.println(" Double random " + randomReflexivMarker);
*/
                    reflexivKmerConcatList.add(
                            RowFactory.create(JavaConverters.asScalaIteratorConverter(Arrays.asList(newReflexivSubKmer).iterator()).asScala().toSeq() ,
                                    randomReflexivMarker, JavaConverters.asScalaIteratorConverter(Arrays.asList(newReflexivLongArray).iterator()).asScala().toSeq(), currentSubKmer.getInt(3), currentSubKmer.getInt(4)
                            )
                    );
                }

            }
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

    class DSFilterStillExtendableKmerEnds implements MapPartitionsFunction<Row, Row>, Serializable {

        /* marker to identify similar SubKmers in the loop sequence */
        private int lineMarker = 1;

        /* 1 stands for forward sub-kmer */
        /* 2 stands for reflexiv sub-kmer */
        // private int randomReflexivMarker = ThreadLocalRandom.current().nextInt(1, 3);
        private int randomReflexivMarker = 2;

        long maxSubKmerResidueBinary = ~((~0L) << 2 * param.subKmerSizeResidue);
        long maxSubKmerBinary = ~((~0L) << 2 * 31);


        //long maxBlockBinary = ~((~0L) << 2*31); // a block has 31 nucleotide


        /* temporary capsule to store identical SubKmer units */
        List<Row> tmpReflexivKmerExtendList = new ArrayList<Row>();

        /* return capsule of extend Tuples for next iteration*/
        List<Row> reflexivKmerConcatList = new ArrayList<Row>();

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
        public Iterator<Row> call(Iterator<Row> sIterator) {

            randomReflexivMarker=1; // in this class, randomReflexivMarker is constantly set to 1

            if (param.scramble ==3){
                randomReflexivMarker =1;
            }

            while (sIterator.hasNext()) {
                Row s = sIterator.next();

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
                            if (subKmerSlotComparator(s.getSeq(0), tmpReflexivKmerExtendList.get(i).getSeq(0))) {
                                int tmpReflexivKmerSuffixLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros((Long) tmpReflexivKmerExtendList.get(i).getSeq(2).apply(0)) / 2 + 1);
                                int tmpBlockSize = (tmpReflexivKmerExtendList.get(i).getSeq(2).length() - 1) * 31 + tmpReflexivKmerSuffixLength;
                                int currentReflexivKmerSuffixLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros((Long) s.getSeq(2).apply(0)) / 2 + 1);
                                int currentBlockSize = (s.getSeq(2).length() - 1) * 31 + currentReflexivKmerSuffixLength;

                                if (s.getInt(1) == 1) {
                                    if (tmpReflexivKmerExtendList.get(i).getInt(1) == 2) {
                                        if (tmpBlockSize *31 + tmpReflexivKmerSuffixLength >=currentBlockSize *31 + currentReflexivKmerSuffixLength) {
                                            reflexivKmerConcatList.add(tmpReflexivKmerExtendList.get(i));
                                        }else{
                                            reflexivKmerConcatList.add(s);
                                        }
                                        tmpReflexivKmerExtendList.remove(i);
                                        break;
                                    } else if (tmpReflexivKmerExtendList.get(i).getInt(1) == 1) {
                                        if (tmpBlockSize *31 + tmpReflexivKmerSuffixLength >=currentBlockSize *31 + currentReflexivKmerSuffixLength) {
                                            reflexivKmerConcatList.add(tmpReflexivKmerExtendList.get(i));
                                        }else{
                                            reflexivKmerConcatList.add(s);
                                        }
                                        tmpReflexivKmerExtendList.remove(i);
                                        break;
                                    }
                                } else { /* if (s.getInt(1) == 2) { */
                                    if (tmpReflexivKmerExtendList.get(i).getInt(1) == 2) {
                                        if (tmpBlockSize *31 + tmpReflexivKmerSuffixLength >=currentBlockSize *31 + currentReflexivKmerSuffixLength) {
                                            reflexivKmerConcatList.add(tmpReflexivKmerExtendList.get(i));
                                        }else{
                                            reflexivKmerConcatList.add(s);
                                        }
                                        tmpReflexivKmerExtendList.remove(i);
                                        break;
                                    } else if (tmpReflexivKmerExtendList.get(i).getInt(1) == 1) {
                                        if (tmpBlockSize *31 + tmpReflexivKmerSuffixLength >=currentBlockSize *31 + currentReflexivKmerSuffixLength) {
                                            reflexivKmerConcatList.add(tmpReflexivKmerExtendList.get(i));
                                        }else{
                                            reflexivKmerConcatList.add(s);
                                        }
                                        tmpReflexivKmerExtendList.remove(i);
                                        break;
                                    }
                                }
                            /* return reflexivKmerConcatList.iterator(); */
                            }

                        /* new Sub-kmer group section */
                            else { /* s.getLong(0) != tmpReflexivKmerExtendList.get(i).getLong(0)()*/
                                //  if (lineMarker == 2) { // lineMarker == 2 represents the second line of the partition
                                //     singleKmerRandomizer(tmpReflexivKmerExtendList.get(i));
                                // }
                                //  singleKmerRandomizer(s);
                                reflexivKmerConcatList.add(tmpReflexivKmerExtendList.get(i));
                                resetSubKmerGroup(s);
                                break;
                            }
                        } /* end of the while loop */
                    }// end of else condition

                    lineMarker++;
                    // return reflexivKmerConcatList.iterator();
                }
            } // while loop

            if (tmpReflexivKmerExtendList.size() != 0) {
                for (int i = 0; i < tmpReflexivKmerExtendList.size(); i++) {
                    reflexivKmerConcatList.add(tmpReflexivKmerExtendList.get(i));
                }
            }

            int kmernumber = reflexivKmerConcatList.size();
            System.out.println("Double afterwards extendable number: " + kmernumber);
            return reflexivKmerConcatList.iterator();
        }

        /**
         *
         * @param currentSubKmer
         */
        public void directKmerComparison(Row currentSubKmer) {
            tmpReflexivKmerExtendList.add(currentSubKmer);
        }

        /**
         *
         * @param S
         */
        public void resetSubKmerGroup(Row S) {
            if (lineMarker == 1) {
                lineMarker = 2;
            } else {
                lineMarker = 3; /* reset to new sub-kmer group */
            }
            /* re-reflex all single kmers in the sub-kmer group */
//            if (tmpReflexivKmerExtendList.size() != 0) {
//                for (int i = 0; i < tmpReflexivKmerExtendList.size(); i++) {
            //                   singleKmerRandomizer(tmpReflexivKmerExtendList.get(i));
            //               }
            //          }

            tmpReflexivKmerExtendList = new ArrayList<Row>();
            tmpReflexivKmerExtendList.add(S
                    //   RowFactory.create(S.getLong(0),
                    //                   S.getInt(1), S.get(2), S.getInt(3), S.getInt(4)
                    //   )
            );
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

    class DSFilterStillExtendableKmerFromPairs implements MapPartitionsFunction<Row, Row>, Serializable {

        /* marker to identify similar SubKmers in the loop sequence */
        private int lineMarker = 1;

        /* 1 stands for forward sub-kmer */
        /* 2 stands for reflexiv sub-kmer */
        // private int randomReflexivMarker = ThreadLocalRandom.current().nextInt(1, 3);
        private int randomReflexivMarker = 2;

        long maxSubKmerResidueBinary = ~((~0L) << 2 * param.subKmerSizeResidue);
        long maxSubKmerBinary = ~((~0L) << 2 * 31);


        //long maxBlockBinary = ~((~0L) << 2*31); // a block has 31 nucleotide


        /* temporary capsule to store identical SubKmer units */
        List<Row> tmpReflexivKmerExtendList = new ArrayList<Row>();

        /* return capsule of extend Tuples for next iteration*/
        List<Row> reflexivKmerConcatList = new ArrayList<Row>();

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
        public Iterator<Row> call(Iterator<Row> sIterator) {

            if (param.scramble ==3){
                randomReflexivMarker =1;
            }

            randomReflexivMarker=1; // in this class, randomReflexivMarker is constantly set to 1

            while (sIterator.hasNext()) {
                Row s = sIterator.next();

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
                            if (subKmerSlotComparator(s.getSeq(0), tmpReflexivKmerExtendList.get(i).getSeq(0))) {

                                if (s.getInt(1) == 1) {
                                    if (tmpReflexivKmerExtendList.get(i).getInt(1) == 2) {
                                        reflexivKmerConcatList.add(tmpReflexivKmerExtendList.get(i));
                                        tmpReflexivKmerExtendList.remove(i);
                                        break;
                                    } else if (tmpReflexivKmerExtendList.get(i).getInt(1) == 1) {
                                        reflexivKmerConcatList.add(tmpReflexivKmerExtendList.get(i));
                                        tmpReflexivKmerExtendList.remove(i);
                                        break;
                                    }
                                } else { /* if (s.getInt(1) == 2) { */
                                    if (tmpReflexivKmerExtendList.get(i).getInt(1) == 2) {
                                        reflexivKmerConcatList.add(tmpReflexivKmerExtendList.get(i));
                                        tmpReflexivKmerExtendList.remove(i);
                                        break;
                                    } else if (tmpReflexivKmerExtendList.get(i).getInt(1) == 1) {
                                        reflexivKmerConcatList.add(tmpReflexivKmerExtendList.get(i));
                                        tmpReflexivKmerExtendList.remove(i);
                                        break;
                                    }
                                }
                            /* return reflexivKmerConcatList.iterator(); */
                            }

                        /* new Sub-kmer group section */
                            else { /* s.getLong(0) != tmpReflexivKmerExtendList.get(i).getLong(0)()*/
                                //  if (lineMarker == 2) { // lineMarker == 2 represents the second line of the partition
                                //     singleKmerRandomizer(tmpReflexivKmerExtendList.get(i));
                                // }
                                //  singleKmerRandomizer(s);
                                reflexivKmerConcatList.add(tmpReflexivKmerExtendList.get(i));
                                resetSubKmerGroup(s);
                                break;
                            }
                        } /* end of the while loop */
                    }// end of else condition

                    lineMarker++;
                    // return reflexivKmerConcatList.iterator();
                }
            } // while loop

            if (tmpReflexivKmerExtendList.size() != 0) {
                for (int i = 0; i < tmpReflexivKmerExtendList.size(); i++) {
                    reflexivKmerConcatList.add(tmpReflexivKmerExtendList.get(i));
                }
            }

            int kmernumber = reflexivKmerConcatList.size();
            System.out.println("Double afterwards extendable number: " + kmernumber);
            return reflexivKmerConcatList.iterator();
        }

        /**
         *
         * @param currentSubKmer
         */
        public void directKmerComparison(Row currentSubKmer) {
            tmpReflexivKmerExtendList.add(currentSubKmer);
        }

        /**
         *
         * @param S
         */
        public void resetSubKmerGroup(Row S) {
            if (lineMarker == 1) {
                lineMarker = 2;
            } else {
                lineMarker = 3; /* reset to new sub-kmer group */
            }
            /* re-reflex all single kmers in the sub-kmer group */
//            if (tmpReflexivKmerExtendList.size() != 0) {
//                for (int i = 0; i < tmpReflexivKmerExtendList.size(); i++) {
            //                   singleKmerRandomizer(tmpReflexivKmerExtendList.get(i));
            //               }
            //          }

            tmpReflexivKmerExtendList = new ArrayList<Row>();
            tmpReflexivKmerExtendList.add(S
                    //   RowFactory.create(S.getLong(0),
                    //                   S.getInt(1), S.get(2), S.getInt(3), S.getInt(4)
                    //   )
            );
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

    class DSFilterUnExtendableKmerLeftEnds implements MapPartitionsFunction<Row, Row>, Serializable {

        /* marker to identify similar SubKmers in the loop sequence */
        private int lineMarker = 1;

        /* 1 stands for forward sub-kmer */
        /* 2 stands for reflexiv sub-kmer */
        // private int randomReflexivMarker = ThreadLocalRandom.current().nextInt(1, 3);
        private int randomReflexivMarker = 2;

        long maxSubKmerResidueBinary = ~((~0L) << 2 * param.subKmerSizeResidue);
        long maxSubKmerBinary = ~((~0L) << 2 * 31);


        //long maxBlockBinary = ~((~0L) << 2*31); // a block has 31 nucleotide


        /* temporary capsule to store identical SubKmer units */
        List<Row> tmpReflexivKmerExtendList = new ArrayList<Row>();

        /* return capsule of extend Tuples for next iteration*/
        List<Row> reflexivKmerConcatList = new ArrayList<Row>();

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
        public Iterator<Row> call(Iterator<Row> sIterator) {

            if (param.scramble ==3){
                randomReflexivMarker =1;
            }

            randomReflexivMarker=1; // in this class, randomReflexivMarker is constantly set to 1

            while (sIterator.hasNext()) {
                Row s = sIterator.next();

                if (s.getInt(1) == 1){
                    reflexivKmerConcatList.add(s);
                } else {
                    singleKmerRandomizer(s);
                }
            }

            return reflexivKmerConcatList.iterator();
        }


        public void singleKmerRandomizer(Row currentSubKmer) {
            int blockSize = currentSubKmer.getSeq(2).length();

            Long[] newReflexivLongArray = new Long[blockSize];

            if (currentSubKmer.getInt(1) == 1) {
                int firstSuffixBlockLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros((Long) currentSubKmer.getSeq(2).apply(0)) / 2 + 1);
                long maxSuffixLengthBinary = ~((~0L) << (2 * firstSuffixBlockLength));
                Long[] newReflexivSubKmer = new Long[param.subKmerBinarySlots];
                Long newReflexivLong;

                if (randomReflexivMarker == 2) {
                    if (blockSize > 1) {

                        if (firstSuffixBlockLength >= param.subKmerSizeResidue) {
                            if (blockSize >= param.subKmerBinarySlots) {
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] = ((Long) currentSubKmer.getSeq(2).apply(blockSize - 1) & maxSubKmerResidueBinary);

                                long transit1 = (Long) currentSubKmer.getSeq(2).apply(blockSize - 1) >>> (2 * param.subKmerSizeResidue);
                                long transit2 = 0L;
                                for (int i = param.subKmerBinarySlots - 2; i >= 0; i--) {
                                    int j = blockSize - param.subKmerBinarySlots + i; // index of suffix long array
                                    transit2 = (Long) currentSubKmer.getSeq(2).apply(j) >>> (2 * param.subKmerSizeResidue);

                                    newReflexivSubKmer[i] = transit1;
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(2).apply(j) << 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;

                                    transit1 = transit2;
                                }

                                for (int i = blockSize - param.subKmerBinarySlots; i > 0; i--) {
                                    int j = param.subKmerBinarySlots + i - 1; // index of the new prefix long array
                                    newReflexivLongArray[j] = ((Long) currentSubKmer.getSeq(2).apply(i) >>> 2 * param.subKmerSizeResidue);
                                    newReflexivLongArray[j] |= (Long) currentSubKmer.getSeq(2).apply(i - 1) << 2 * (31 - param.subKmerSizeResidue);
                                    newReflexivLongArray[j] &= maxSubKmerBinary;
                                }

                                //   if (blockSize== param.subKmerBinarySlots){ // in the context of blockSize >= param.subKmerBinarySlots
                                newReflexivLongArray[param.subKmerBinarySlots - 1] = ((Long) currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary) >>> (2 * param.subKmerSizeResidue);
                                newReflexivLongArray[param.subKmerBinarySlots - 1] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) << 2 * (firstSuffixBlockLength - param.subKmerSizeResidue));
                                newReflexivLongArray[param.subKmerBinarySlots - 1] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 2) << 2 * firstSuffixBlockLength);
                                newReflexivLongArray[param.subKmerBinarySlots - 1] &= maxSubKmerBinary;
                                //  }

                                for (int i = param.subKmerBinarySlots - 2; i > 0; i--) {
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * (31 - firstSuffixBlockLength);
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(0).apply(i - 1) << 2 * firstSuffixBlockLength);
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                newReflexivLongArray[0] = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * (31 - firstSuffixBlockLength);
                                newReflexivLongArray[0] |= (1L << 2 * firstSuffixBlockLength); // add C marker in the front
                            } else { // blockSize < param.subKmerSizeResidue
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] = ((Long) currentSubKmer.getSeq(2).apply(blockSize - 1) & maxSubKmerResidueBinary);

                                long transit1 = (Long) currentSubKmer.getSeq(2).apply(blockSize - 1) >>> (2 * param.subKmerSizeResidue);
                                long transit2 = 0L;
                                for (int i = param.subKmerBinarySlots - 2; i >= param.subKmerBinarySlots - blockSize; i--) {
                                    int j = blockSize - param.subKmerBinarySlots + i; // index of suffix long array
                                    transit2 = (Long) currentSubKmer.getSeq(2).apply(j) >>> (2 * param.subKmerSizeResidue);

                                    newReflexivSubKmer[i] = transit1;
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(2).apply(j) << 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;

                                    transit1 = transit2;
                                }

                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize - 1] = ((Long) currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary) >>> (2 * param.subKmerSizeResidue);
                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize - 1] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) << 2 * (firstSuffixBlockLength - param.subKmerSizeResidue));
                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize - 1] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 2) << 2 * firstSuffixBlockLength);
                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize - 1] &= maxSubKmerBinary;

                                for (int i = param.subKmerBinarySlots - blockSize - 2; i >= 0; i--) {
                                    int j = blockSize + i; // index of the subkmer

                                    newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(j) >>> 2 * (31 - firstSuffixBlockLength);
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(0).apply(j - 1) << 2 * firstSuffixBlockLength);
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;
                                }

                                for (int i = blockSize - 1; i > 0; i--) {
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * (31 - firstSuffixBlockLength);
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(0).apply(i - 1) << 2 * firstSuffixBlockLength);
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                newReflexivLongArray[0] = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * (31 - firstSuffixBlockLength);
                                newReflexivLongArray[0] |= (1L << 2 * firstSuffixBlockLength); // add C marker in the front
                            }
                        } else { // firstSuffixBlockLength < param.subKmerSizeResidue
                            if (blockSize >= param.subKmerBinarySlots) {
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] = ((Long) currentSubKmer.getSeq(2).apply(blockSize - 1) & maxSubKmerResidueBinary);

                                long transit1 = (Long) currentSubKmer.getSeq(2).apply(blockSize - 1) >>> (2 * param.subKmerSizeResidue);
                                long transit2 = 0L;
                                for (int i = param.subKmerBinarySlots - 2; i > 0; i--) {
                                    int j = blockSize - param.subKmerBinarySlots + i; // index of suffix long array
                                    transit2 = (Long) currentSubKmer.getSeq(2).apply(j) >>> (2 * param.subKmerSizeResidue);

                                    newReflexivSubKmer[i] = transit1;
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(2).apply(j) << 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;

                                    transit1 = transit2;
                                }

                                newReflexivSubKmer[0] = transit1;
                                if (blockSize > param.subKmerBinarySlots) {
                                    newReflexivSubKmer[0] |= ((Long) currentSubKmer.getSeq(2).apply(blockSize - param.subKmerBinarySlots) << 2 * (31 - param.subKmerSizeResidue));
                                }else { // (blockSize== param.subKmerBinarySlots){
                                    newReflexivSubKmer[0] |= (((Long) currentSubKmer.getSeq(2).apply(blockSize - param.subKmerBinarySlots) & maxSuffixLengthBinary) << 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivSubKmer[0] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots-1) << 2*(31-param.subKmerSizeResidue + firstSuffixBlockLength));
                                }
                                newReflexivSubKmer[0] &= maxSubKmerBinary;

                                //newReflexivSubKmer[0] = transit1;
                                //newReflexivSubKmer[0] |= ((Long) currentSubKmer.getSeq(2).apply(blockSize-param.subKmerBinarySlots) << 2*(31-param.subKmerSizeResidue));


                                for (int i = blockSize - param.subKmerBinarySlots; i > 1; i--) {
                                    int j = param.subKmerBinarySlots + i - 1; // index of the new prefix long array
                                    newReflexivLongArray[j] = ((Long) currentSubKmer.getSeq(2).apply(i) >>> 2 * param.subKmerSizeResidue);
                                    newReflexivLongArray[j] |= (Long) currentSubKmer.getSeq(2).apply(i - 1) << 2 * (31 - param.subKmerSizeResidue);
                                    newReflexivLongArray[j] &= maxSubKmerBinary;
                                }

                                if (param.subKmerBinarySlots < blockSize) {
                                    newReflexivLongArray[param.subKmerBinarySlots] = ((Long) currentSubKmer.getSeq(2).apply(1) >>> 2 * param.subKmerSizeResidue);
                                    newReflexivLongArray[param.subKmerBinarySlots] |= ((Long) currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary)<< 2 * (31 - param.subKmerSizeResidue);
                                    newReflexivLongArray[param.subKmerBinarySlots] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) << 2 * (31 + firstSuffixBlockLength - param.subKmerSizeResidue));
                                    newReflexivLongArray[param.subKmerBinarySlots] &= maxSubKmerBinary;
                                }

                                //   if (blockSize== param.subKmerBinarySlots){ // in the context of blockSize >= param.subKmerBinarySlots
                                //  newReflexivLongArray[param.subKmerBinarySlots-1] = ((Long) currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary) >>> (2 * param.subKmerSizeResidue);
                                newReflexivLongArray[param.subKmerBinarySlots - 1] = ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) >>> 2 * (param.subKmerSizeResidue - firstSuffixBlockLength));
                                newReflexivLongArray[param.subKmerBinarySlots - 1] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 2) << 2 * firstSuffixBlockLength);
                                newReflexivLongArray[param.subKmerBinarySlots - 1] &= maxSubKmerBinary;
                                //  }

                                for (int i = param.subKmerBinarySlots - 2; i > 0; i--) {
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * (31 - firstSuffixBlockLength);
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(0).apply(i - 1) << 2 * firstSuffixBlockLength);
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                newReflexivLongArray[0] = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * (31 - firstSuffixBlockLength);
                                newReflexivLongArray[0] |= (1L << 2 * firstSuffixBlockLength); // add C marker in the front
                            } else { // blockSize < param.subKmerSizeResidue
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] = ((Long) currentSubKmer.getSeq(2).apply(blockSize - 1) & maxSubKmerResidueBinary);

                                long transit1 = (Long) currentSubKmer.getSeq(2).apply(blockSize - 1) >>> (2 * param.subKmerSizeResidue);
                                long transit2 = 0L;
                                for (int i = param.subKmerBinarySlots - 2; i > param.subKmerBinarySlots - blockSize; i--) {
                                    int j = blockSize - param.subKmerBinarySlots + i; // index of suffix long array
                                    transit2 = (Long) currentSubKmer.getSeq(2).apply(j) >>> (2 * param.subKmerSizeResidue);

                                    newReflexivSubKmer[i] = transit1;
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(2).apply(j) << 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;

                                    transit1 = transit2;
                                }

                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize] = (Long) currentSubKmer.getSeq(2).apply(1) >>> 2 * param.subKmerSizeResidue;
                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize] |= ((Long) currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary) << 2 * (31 - param.subKmerSizeResidue);
                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) << 2 * (31 - param.subKmerSizeResidue + firstSuffixBlockLength));
                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize] &= maxSubKmerBinary;

                                // newReflexivSubKmer[param.subKmerBinarySlots-blockSize-1] = ((Long) currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary) >>> (2 * param.subKmerSizeResidue);
                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize - 1] = ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) >>> 2 * (param.subKmerSizeResidue - firstSuffixBlockLength));
                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize - 1] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 2) << 2 * firstSuffixBlockLength);
                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize - 1] &= maxSubKmerBinary;

                                for (int i = param.subKmerBinarySlots - blockSize - 2; i >= 0; i--) {
                                    int j = blockSize + i; // index of the subkmer

                                    newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(j) >>> 2 * (31 - firstSuffixBlockLength);
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(0).apply(j - 1) << 2 * firstSuffixBlockLength);
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;
                                }

                                for (int i = blockSize - 1; i > 0; i--) {
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * (31 - firstSuffixBlockLength);
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(0).apply(i - 1) << 2 * firstSuffixBlockLength);
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                newReflexivLongArray[0] = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * (31 - firstSuffixBlockLength);
                                newReflexivLongArray[0] |= (1L << 2 * firstSuffixBlockLength); // add C marker in the front
                            }
                        }


                    } else { // block size ==1
                        if (firstSuffixBlockLength > param.subKmerSizeResidue) { // firstSuffixBlockLength is longer than the length of the last block (element) of sub kmer long array
                            //long transitBit1 = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) << 2 * (firstSuffixBlockLength - param.subKmerSizeResidue);
                            long transitBit1 = (((Long)currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary) >>> 2 * param.subKmerSizeResidue);
                            long transitBit2 = 0L;

                            newReflexivSubKmer[param.subKmerBinarySlots - 1] = ((Long) currentSubKmer.getSeq(2).apply(0) & maxSubKmerResidueBinary);

                            newReflexivSubKmer[param.subKmerBinarySlots - 2] = ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots-1) << 2* (firstSuffixBlockLength-param.subKmerSizeResidue));
                            newReflexivSubKmer[param.subKmerBinarySlots - 2] |= transitBit1;
                            newReflexivSubKmer[param.subKmerBinarySlots - 2] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots-2) << 2*firstSuffixBlockLength);
                            newReflexivSubKmer[param.subKmerBinarySlots - 2] &= maxSubKmerBinary;
                            transitBit1 = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots-2) >>> 2 * (31 - firstSuffixBlockLength);

                            for (int i = param.subKmerBinarySlots - 3; i >= 0; i--) {
                                transitBit2 = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * (31 - firstSuffixBlockLength);

                                newReflexivSubKmer[i] = ((Long) currentSubKmer.getSeq(0).apply(i) << 2 * firstSuffixBlockLength);
                                newReflexivSubKmer[i] |= transitBit1;
                                newReflexivSubKmer[i] &= maxSubKmerBinary;

                                transitBit1 = transitBit2;
                            }

                            newReflexivLong = transitBit1;
                            newReflexivLong |= (1L << 2 * firstSuffixBlockLength);
                            newReflexivLongArray[0] = newReflexivLong;

                        } else if (firstSuffixBlockLength == param.subKmerSizeResidue) {
                            long transitBit1 = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                            long transitBit2 = 0L;

                            newReflexivSubKmer[param.subKmerBinarySlots - 1] = ((Long) currentSubKmer.getSeq(2).apply(0) & maxSubKmerResidueBinary);

                            for (int i = param.subKmerBinarySlots - 2; i >= 0; i--) {
                                transitBit2 = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * (31 - firstSuffixBlockLength);

                                newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(i) << 2 * firstSuffixBlockLength;
                                newReflexivSubKmer[i] |= transitBit1;
                                newReflexivSubKmer[i] &= maxSubKmerBinary;

                                transitBit1 = transitBit2;
                            }

                            newReflexivLong = transitBit2;
                            newReflexivLong |= (1L << 2 * firstSuffixBlockLength);
                            newReflexivLongArray[0] = newReflexivLong;

                        } else { //firstSuffixBlockLength < param.subKmerSizeResidue
                            long transitBit1 = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) >>> 2 * (param.subKmerSizeResidue - firstSuffixBlockLength);
                            long transitBit2 = 0L;

                            newReflexivSubKmer[param.subKmerBinarySlots - 1] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) << 2 * firstSuffixBlockLength;
                            newReflexivSubKmer[param.subKmerBinarySlots - 1] |= ((Long) currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary);
                            newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;

                            for (int i = param.subKmerBinarySlots - 2; i >= 0; i--) {
                                transitBit2 = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * (31 - firstSuffixBlockLength);

                                newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(i) << 2 * firstSuffixBlockLength;
                                newReflexivSubKmer[i] |= transitBit1;
                                newReflexivSubKmer[i] &= maxSubKmerBinary;

                                transitBit1 = transitBit2;
                            }

                            newReflexivLong = transitBit2;
                            newReflexivLong |= (1L << 2 * firstSuffixBlockLength); // add C marker
                            newReflexivLongArray[0] = newReflexivLong;
                        }
                    }

/*
                    for (int k = 30; k >= 0; k--) {
                        long a = newReflexivSubKmer[0] >> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    for (int k = 30; k >= 0; k--) {
                        long a = newReflexivSubKmer[1] >>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    if (newReflexivSubKmer.length >=3){
                        for (int k = 30; k >= 0; k--) {
                            long a = newReflexivSubKmer[2] >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    if (newReflexivSubKmer.length >=4){
                        for (int k = 16; k >= 0; k--) {
                            long a = newReflexivSubKmer[3] >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }


                    System.out.print(" ");
                    for (int k = 30; k >= 0; k--) {
                        long a = newReflexivLongArray[0] >>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    if (newReflexivLongArray.length >=2){
                        for (int k = 30; k >= 0; k--) {
                            long a = newReflexivLongArray[1] >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    if (newReflexivLongArray.length >=3){
                        for (int k = 30; k >= 0; k--) {
                            long a = newReflexivLongArray[2] >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    if (newReflexivLongArray.length >=4){
                        for (int k = 30; k >= 0; k--) {
                            long a = newReflexivLongArray[3] >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }


                    System.out.println(" Double random left end " + randomReflexivMarker);
*/
                    reflexivKmerConcatList.add(
                            RowFactory.create(JavaConverters.asScalaIteratorConverter(Arrays.asList(newReflexivSubKmer).iterator()).asScala().toSeq(),
                                    randomReflexivMarker,  JavaConverters.asScalaIteratorConverter(Arrays.asList(newReflexivLongArray).iterator()).asScala().toSeq(), currentSubKmer.getInt(3), currentSubKmer.getInt(4)
                            )
                    );
                } else {

/*
                    for (int k = 30; k >= 0; k--) {
                        long a = (Long)currentSubKmer.getSeq(0).apply(0)>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    for (int k = 30; k >= 0; k--) {
                        long a = (Long)currentSubKmer.getSeq(0).apply(1)>>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    if (currentSubKmer.getSeq(0).length() >=3){
                        for (int k = 30; k >= 0; k--) {
                            long a = (Long)currentSubKmer.getSeq(0).apply(2)>>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    if (currentSubKmer.getSeq(0).length() >=4){
                        for (int k = 16; k >= 0; k--) {
                            long a = (Long)currentSubKmer.getSeq(0).apply(3)>>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }


                    System.out.print(" ");
                    for (int k = 30; k >= 0; k--) {
                        long a = (Long)currentSubKmer.getSeq(2).apply(0) >>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    if (newReflexivLongArray.length >=2){
                        for (int k = 30; k >= 0; k--) {
                            long a = (Long)currentSubKmer.getSeq(2).apply(1) >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    if (newReflexivLongArray.length >=3){
                        for (int k = 30; k >= 0; k--) {
                            long a = (Long)currentSubKmer.getSeq(2).apply(2) >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    if (newReflexivLongArray.length >=4){
                        for (int k = 30; k >= 0; k--) {
                            long a = (Long)currentSubKmer.getSeq(2).apply(3) >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }


                    System.out.println(" Double random same left end" + randomReflexivMarker);
                    */
                    reflexivKmerConcatList.add(currentSubKmer);
                }
            } else { /* currentSubKmer.getInt(1) == 2 */
                int firstPrefixLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros((Long) currentSubKmer.getSeq(2).apply(0)) / 2 + 1);
                long maxPrefixLengthBinary = ~((~0L) << (2 * firstPrefixLength));
                Long[] newReflexivSubKmer = new Long[param.subKmerBinarySlots];
                long newReflexivLong = 0L;


                if (randomReflexivMarker == 2) {

/*
                    for (int k = 30; k >= 0; k--) {
                        long a = (Long)currentSubKmer.getSeq(0).apply(0)>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    for (int k = 30; k >= 0; k--) {
                        long a = (Long)currentSubKmer.getSeq(0).apply(1)>>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    if (currentSubKmer.getSeq(0).length() >=3){
                        for (int k = 30; k >= 0; k--) {
                            long a = (Long)currentSubKmer.getSeq(0).apply(2)>>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    if (currentSubKmer.getSeq(0).length() >=4){
                        for (int k = 16; k >= 0; k--) {
                            long a = (Long)currentSubKmer.getSeq(0).apply(3)>>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }


                    System.out.print(" ");
                    for (int k = 30; k >= 0; k--) {
                        long a = (Long)currentSubKmer.getSeq(2).apply(0) >>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    if (newReflexivLongArray.length >=2){
                        for (int k = 30; k >= 0; k--) {
                            long a = (Long)currentSubKmer.getSeq(2).apply(1) >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    if (newReflexivLongArray.length >=3){
                        for (int k = 30; k >= 0; k--) {
                            long a = (Long)currentSubKmer.getSeq(2).apply(2) >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    if (newReflexivLongArray.length >=4){
                        for (int k = 30; k >= 0; k--) {
                            long a = (Long)currentSubKmer.getSeq(2).apply(3) >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    System.out.println(" Double random same left end" + randomReflexivMarker);
*/
                    reflexivKmerConcatList.add(currentSubKmer);
                } else { /* randomReflexivMarker == 1 */
                    if (blockSize > 1) {
                        if (firstPrefixLength >= param.subKmerSizeResidue) {
                            if (blockSize >= param.subKmerBinarySlots) {
                                for (int i = 0; i < param.subKmerBinarySlots - 1; i++) {
                                    newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(2).apply(i) << 2 * (31 - firstPrefixLength);
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(2).apply(i + 1) >>> 2 * firstPrefixLength);
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;
                                }

                                newReflexivSubKmer[param.subKmerBinarySlots - 1] = (Long) currentSubKmer.getSeq(2).apply(param.subKmerBinarySlots - 1) >>> 2 * (firstPrefixLength - param.subKmerSizeResidue);
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;

                                newReflexivLongArray[0] = (Long) currentSubKmer.getSeq(2).apply(param.subKmerBinarySlots - 1) << 2 * param.subKmerSizeResidue;
                                if (blockSize > param.subKmerBinarySlots) {
                                    newReflexivLongArray[0] |= ((Long) currentSubKmer.getSeq(2).apply(param.subKmerBinarySlots) >>> 2 * (31 - param.subKmerSizeResidue));
                                } else {
                                    newReflexivLongArray[0] |= ((Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * (31 - param.subKmerSizeResidue));
                                }
                                newReflexivLongArray[0] &= maxPrefixLengthBinary;
                                newReflexivLongArray[0] |= (1L << 2 * firstPrefixLength); // add C marker in the front

                                for (int i = 1; i < blockSize - param.subKmerBinarySlots; i++) {
                                    int j = param.subKmerBinarySlots + i - 1;
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(2).apply(j) << 2 * param.subKmerSizeResidue;
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(2).apply(j + 1) >>> 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                if (blockSize > param.subKmerBinarySlots) {
                                    newReflexivLongArray[blockSize - param.subKmerBinarySlots] = (Long) currentSubKmer.getSeq(2).apply(blockSize - 1) << 2 * param.subKmerSizeResidue;
                                    newReflexivLongArray[blockSize - param.subKmerBinarySlots] |= ((Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivLongArray[blockSize - param.subKmerBinarySlots] &= maxSubKmerBinary;
                                }

                                for (int i = blockSize - param.subKmerBinarySlots + 1; i < blockSize - 1; i++) {
                                    int j = i - blockSize + param.subKmerBinarySlots - 1; // index of subKmer
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(0).apply(j) << 2 * param.subKmerSizeResidue;
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(0).apply(j + 1) >>> 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                newReflexivLongArray[blockSize - 1] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 2) << 2 * param.subKmerSizeResidue;
                                newReflexivLongArray[blockSize - 1] |= (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                                newReflexivLongArray[blockSize - 1] &= maxSubKmerBinary;

                            } else { // blockSize < param.subKmerBinarySlots
                                for (int i = 0; i < blockSize - 1; i++) {
                                    newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(2).apply(i) << 2 * (31 - firstPrefixLength);
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(2).apply(i + 1) >>> 2 * firstPrefixLength);
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;
                                }

                                newReflexivSubKmer[blockSize - 1] = (Long) currentSubKmer.getSeq(2).apply(blockSize - 1) << 2 * (31 - firstPrefixLength);
                                newReflexivSubKmer[blockSize - 1] |= ((Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * firstPrefixLength);
                                newReflexivSubKmer[blockSize - 1] &= maxSubKmerBinary;

                                for (int i = blockSize; i < param.subKmerBinarySlots - 1; i++) {
                                    int j = i - blockSize; // index of subKmer
                                    newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(j) << 2 * (31 - firstPrefixLength);
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(0).apply(j + 1) >>> 2 * firstPrefixLength);
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;
                                }

                                newReflexivSubKmer[param.subKmerBinarySlots - 1] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - blockSize - 1) >>> 2 * (firstPrefixLength - param.subKmerSizeResidue);
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;

                                newReflexivLongArray[0] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - blockSize - 1) << 2 * param.subKmerSizeResidue;
                                newReflexivLongArray[0] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - blockSize) >>> 2 * (31 - param.subKmerSizeResidue));
                                newReflexivLongArray[0] &= maxPrefixLengthBinary;
                                newReflexivLongArray[0] |= (1L << 2 * firstPrefixLength);

                                for (int i = 1; i < blockSize - 1; i++) {
                                    int j = param.subKmerBinarySlots - blockSize - 1 + i;
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(0).apply(j) << 2 * param.subKmerSizeResidue;
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(0).apply(j + 1) >>> 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                newReflexivLongArray[blockSize - 1] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 2) << 2 * param.subKmerSizeResidue;
                                newReflexivLongArray[blockSize - 1] |= (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                                newReflexivLongArray[blockSize - 1] &= maxSubKmerBinary;
                            }
                        } else { // firstPrefixLength < param.subKmerSizeResidue
                            if (blockSize >= param.subKmerBinarySlots) {
                                newReflexivSubKmer[0] =((Long) currentSubKmer.getSeq(2).apply(0) & maxPrefixLengthBinary) << 2 * (31 - firstPrefixLength);
                                newReflexivSubKmer[0] |= ((Long) currentSubKmer.getSeq(2).apply(1) >>> 2 * firstPrefixLength);
                                newReflexivSubKmer[0] &= maxSubKmerBinary;

                                for (int i = 1; i < param.subKmerBinarySlots - 1; i++) {
                                    newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(2).apply(i) << 2 * (31 - firstPrefixLength);
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(2).apply(i + 1) >>> 2 * firstPrefixLength);
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;
                                }

                                newReflexivSubKmer[param.subKmerBinarySlots - 1] = (Long) currentSubKmer.getSeq(2).apply(param.subKmerBinarySlots - 1) << 2 * (param.subKmerSizeResidue - firstPrefixLength);
                                if (blockSize > param.subKmerBinarySlots) {
                                    newReflexivSubKmer[param.subKmerBinarySlots - 1] |= ((Long) currentSubKmer.getSeq(2).apply(param.subKmerBinarySlots) >>> 2 * (31 - param.subKmerSizeResidue + firstPrefixLength));
                                } else { // blockSize == param.subKmerBinarySlots
                                    newReflexivSubKmer[param.subKmerBinarySlots - 1] |= ((Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * (31 - param.subKmerSizeResidue + firstPrefixLength));
                                }
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;


                                if (blockSize > param.subKmerBinarySlots) {
                                    newReflexivLongArray[0] = (Long) currentSubKmer.getSeq(2).apply(param.subKmerBinarySlots) >>> 2 * (31 - param.subKmerSizeResidue);
                                } else { // blockSize == param.subKmerBinarySlots
                                    newReflexivLongArray[0] = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * (31 - param.subKmerSizeResidue);
                                }
                                newReflexivLongArray[0] &= maxPrefixLengthBinary;
                                newReflexivLongArray[0] |= (1L << 2 * firstPrefixLength);

                                for (int i = 1; i < blockSize - param.subKmerBinarySlots; i++) {
                                    int j = param.subKmerBinarySlots + i - 1;
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(2).apply(j) << 2 * param.subKmerSizeResidue;
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(2).apply(j + 1) >>> 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                if (blockSize > param.subKmerBinarySlots) {
                                    newReflexivLongArray[blockSize - param.subKmerBinarySlots] = (Long) currentSubKmer.getSeq(2).apply(blockSize - 1) << 2 * param.subKmerSizeResidue;
                                    newReflexivLongArray[blockSize - param.subKmerBinarySlots] |= ((Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivLongArray[blockSize - param.subKmerBinarySlots] &= maxSubKmerBinary;
                                }

                                for (int i = blockSize - param.subKmerBinarySlots + 1; i < blockSize - 1; i++) {
                                    int j = i - blockSize + param.subKmerBinarySlots - 1; // index of subKmer
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(0).apply(j) << 2 * param.subKmerSizeResidue;
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(0).apply(j + 1) >>> 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                newReflexivLongArray[blockSize - 1] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 2) << 2 * param.subKmerSizeResidue;
                                newReflexivLongArray[blockSize - 1] |= (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                                newReflexivLongArray[blockSize - 1] &= maxSubKmerBinary;


                            } else { // blockSize < param.subKmerBinarySlots
                                newReflexivSubKmer[0] = ( (Long) currentSubKmer.getSeq(2).apply(0) & maxPrefixLengthBinary) << 2 * (31 - firstPrefixLength);
                                newReflexivSubKmer[0] |= ((Long) currentSubKmer.getSeq(2).apply(1) >>> 2 * firstPrefixLength);
                                newReflexivSubKmer[0] &= maxSubKmerBinary;
                                for (int i = 1; i < blockSize - 1; i++) {
                                    newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(2).apply(i) << 2 * (31 - firstPrefixLength);
                                    newReflexivSubKmer[i] |=( (Long) currentSubKmer.getSeq(2).apply(i + 1) >>> 2 * firstPrefixLength);
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;
                                }

                                newReflexivSubKmer[blockSize - 1] = (Long) currentSubKmer.getSeq(2).apply(blockSize - 1) << 2 * (31 - firstPrefixLength);
                                newReflexivSubKmer[blockSize - 1] |= ((Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * firstPrefixLength);
                                newReflexivSubKmer[blockSize - 1] &= maxSubKmerBinary;

                                for (int i = blockSize; i < param.subKmerBinarySlots - 1; i++) {
                                    int j = i - blockSize; // index of subKmer
                                    newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(j) << 2 * (31 - firstPrefixLength);
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(0).apply(j + 1) >>> 2 * firstPrefixLength);
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;
                                }

                                // newReflexivSubKmer[param.subKmerBinarySlots-1] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots-blockSize-1) >>> 2*(firstPrefixLength-param.subKmerSizeResidue);
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - blockSize - 1) << 2 * (param.subKmerSizeResidue - firstPrefixLength);
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - blockSize) >>> 2 * (31 - param.subKmerSizeResidue + firstPrefixLength));
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;

                                // newReflexivLongArray[0] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots-blockSize) << 2*param.subKmerSizeResidue;
                                newReflexivLongArray[0] = ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - blockSize) >>> 2 * (31 - param.subKmerSizeResidue));
                                newReflexivLongArray[0] &= maxPrefixLengthBinary;
                                newReflexivLongArray[0] |= (1L << 2 * firstPrefixLength);

                                for (int i = 1; i < blockSize - 1; i++) {
                                    int j = param.subKmerBinarySlots - blockSize + i -1;
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(0).apply(j) << 2 * param.subKmerSizeResidue;
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(0).apply(j + 1) >>> 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                newReflexivLongArray[blockSize - 1] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 2) << 2 * param.subKmerSizeResidue;
                                newReflexivLongArray[blockSize - 1] |= (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                                newReflexivLongArray[blockSize - 1] &= maxSubKmerBinary;
                            }
                        }

                    } else { /* blockSize == 1*/
                        if (firstPrefixLength > param.subKmerSizeResidue) {
                            long transitBit1 = (Long) currentSubKmer.getSeq(0).apply(0) & maxPrefixLengthBinary;

                            newReflexivSubKmer[0] = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * firstPrefixLength;
                            newReflexivSubKmer[0] |= (((Long) currentSubKmer.getSeq(2).apply(0) & maxPrefixLengthBinary) << 2 * (31 - firstPrefixLength));
                            newReflexivSubKmer[0] &= maxSubKmerBinary;

                            for (int i = 1; i < param.subKmerBinarySlots - 1; i++) {
                                long transitBit2 = (Long) currentSubKmer.getSeq(0).apply(i) & maxPrefixLengthBinary;

                                newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * firstPrefixLength;
                                newReflexivSubKmer[i] |= (transitBit1 << 2 * (31 - firstPrefixLength));
                                newReflexivSubKmer[i] &= maxSubKmerBinary;

                                transitBit1 = transitBit2;
                            }

                            newReflexivSubKmer[param.subKmerBinarySlots - 1] = transitBit1 >>> 2*(firstPrefixLength- param.subKmerSizeResidue);
                            newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;

                            newReflexivLong = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                            newReflexivLong |= (transitBit1 << 2 * param.subKmerSizeResidue);
                            newReflexivLong &= maxPrefixLengthBinary;
                            newReflexivLong |= (1L << 2 * firstPrefixLength); // add C marker in the front

                            newReflexivLongArray[0] = newReflexivLong;
                        } else if (firstPrefixLength == param.subKmerSizeResidue) {
                            long transitBit1 = (Long) currentSubKmer.getSeq(0).apply(0) & maxPrefixLengthBinary;

                            newReflexivSubKmer[0] = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * firstPrefixLength;
                            newReflexivSubKmer[0] |= (((Long) currentSubKmer.getSeq(2).apply(0) & maxPrefixLengthBinary) << 2 * (31 - firstPrefixLength));
                            newReflexivSubKmer[0] &= maxSubKmerBinary;

                            for (int i = 1; i < param.subKmerBinarySlots - 1; i++) {
                                long transitBit2 = (Long) currentSubKmer.getSeq(0).apply(i) & maxPrefixLengthBinary;

                                newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * firstPrefixLength;
                                newReflexivSubKmer[i] |= (transitBit1 << 2 * (31 - firstPrefixLength));
                                newReflexivSubKmer[i] &= maxSubKmerBinary;

                                transitBit1 = transitBit2;
                            }

                            newReflexivSubKmer[param.subKmerBinarySlots - 1] = transitBit1;
                            newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;

                            newReflexivLong = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                            newReflexivLong |= (1L << 2 * firstPrefixLength); // add C marker

                            newReflexivLongArray[0] = newReflexivLong;
                        } else {
                            long transitBit1 = (Long) currentSubKmer.getSeq(0).apply(0) & maxPrefixLengthBinary;

                            newReflexivSubKmer[0] = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * firstPrefixLength;
                            newReflexivSubKmer[0] |= (((Long) currentSubKmer.getSeq(2).apply(0) & maxPrefixLengthBinary) << 2 * (31 - firstPrefixLength));
                            newReflexivSubKmer[0] &= maxSubKmerBinary;

                            for (int i = 1; i < param.subKmerBinarySlots - 1; i++) {
                                long transitBit2 = (Long) currentSubKmer.getSeq(0).apply(i) & maxPrefixLengthBinary;

                                newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * firstPrefixLength;
                                newReflexivSubKmer[i] |= (transitBit1 << 2 * (31 - firstPrefixLength));
                                newReflexivSubKmer[i] &= maxSubKmerBinary;

                                transitBit1 = transitBit2;
                            }

                            newReflexivSubKmer[param.subKmerBinarySlots - 1] = transitBit1 << 2 * (param.subKmerSizeResidue - firstPrefixLength);
                            newReflexivSubKmer[param.subKmerBinarySlots - 1] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) >>> 2 * firstPrefixLength);
                            newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;


                            newReflexivLong = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) & maxPrefixLengthBinary;
                            newReflexivLong |= (1L << 2 * firstPrefixLength); // add C marker
                            newReflexivLongArray[0] = newReflexivLong;
                        }
                    }
/*
                    System.err.print("start ---- ");

                    for (int k = 30; k >= 0; k--) {
                        long a = newReflexivSubKmer[0] >> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.err.print(b);
                    }

                    for (int k = 30; k >= 0; k--) {
                        long a = newReflexivSubKmer[1] >>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.err.print(b);
                    }

                    if (newReflexivSubKmer.length >=3){
                        for (int k = 30; k >= 0; k--) {
                            long a = newReflexivSubKmer[2] >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.err.print(b);
                        }
                    }

                    if (newReflexivSubKmer.length >=4){
                        for (int k = 16; k >= 0; k--) {
                            long a = newReflexivSubKmer[3] >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.err.print(b);
                        }
                    }

                    System.out.print(" was den fuk left ");
                    for (int k = 30; k >= 0; k--) {
                        long a = newReflexivLongArray[0] >>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.err.print(b);
                    }

                    if (newReflexivLongArray.length >=2){
                        for (int k = 30; k >= 0; k--) {
                            long a = newReflexivLongArray[1] >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.err.print(b);
                        }
                    }

                    if (newReflexivLongArray.length >=3){
                        for (int k = 30; k >= 0; k--) {
                            long a = newReflexivLongArray[2] >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.err.print(b);
                        }
                    }

                    if (newReflexivLongArray.length >=4){
                        for (int k = 30; k >= 0; k--) {
                            long a = newReflexivLongArray[3] >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.err.print(b);
                        }
                    }


                    System.err.println(" Double random left end " + randomReflexivMarker + " " + newReflexivSubKmer.length + " " + newReflexivLongArray.length);
*/
                    reflexivKmerConcatList.add(
                            RowFactory.create(JavaConverters.asScalaIteratorConverter(Arrays.asList(newReflexivSubKmer).iterator()).asScala().toSeq() ,
                                    randomReflexivMarker, JavaConverters.asScalaIteratorConverter(Arrays.asList(newReflexivLongArray).iterator()).asScala().toSeq(), currentSubKmer.getInt(3), currentSubKmer.getInt(4)
                            )
                    );
                }

            }
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

        /**
         *
         * @param currentSubKmer
         */
        public void directKmerComparison(Row currentSubKmer) {
            tmpReflexivKmerExtendList.add(currentSubKmer);
        }

        /**
         *
         * @param S
         */
        public void resetSubKmerGroup(Row S) {
            if (lineMarker == 1) {
                lineMarker = 2;
            } else {
                lineMarker = 3; /* reset to new sub-kmer group */
            }
            /* re-reflex all single kmers in the sub-kmer group */
//            if (tmpReflexivKmerExtendList.size() != 0) {
//                for (int i = 0; i < tmpReflexivKmerExtendList.size(); i++) {
            //                   singleKmerRandomizer(tmpReflexivKmerExtendList.get(i));
            //               }
            //          }

            tmpReflexivKmerExtendList = new ArrayList<Row>();
            tmpReflexivKmerExtendList.add(S
                    //   RowFactory.create(S.getLong(0),
                    //                   S.getInt(1), S.get(2), S.getInt(3), S.getInt(4)
                    //   )
            );
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

    class DSFilterUnExtendableKmerRightEnds implements MapPartitionsFunction<Row, Row>, Serializable {

        /* marker to identify similar SubKmers in the loop sequence */
        private int lineMarker = 1;

        /* 1 stands for forward sub-kmer */
        /* 2 stands for reflexiv sub-kmer */
        // private int randomReflexivMarker = ThreadLocalRandom.current().nextInt(1, 3);
        private int randomReflexivMarker = 2;

        long maxSubKmerResidueBinary = ~((~0L) << 2 * param.subKmerSizeResidue);
        long maxSubKmerBinary = ~((~0L) << 2 * 31);


        //long maxBlockBinary = ~((~0L) << 2*31); // a block has 31 nucleotide


        /* temporary capsule to store identical SubKmer units */
        List<Row> tmpReflexivKmerExtendList = new ArrayList<Row>();

        /* return capsule of extend Tuples for next iteration*/
        List<Row> reflexivKmerConcatList = new ArrayList<Row>();

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
        public Iterator<Row> call(Iterator<Row> sIterator) {

            if (param.scramble ==3){
                randomReflexivMarker =1;
            }

            randomReflexivMarker=2; // in this class, randomReflexivMarker is constantly set to 2

            while (sIterator.hasNext()) {
                Row s = sIterator.next();

                if (s.getInt(1) == 2){
                    reflexivKmerConcatList.add(s);
                } else {
                    singleKmerRandomizer(s);
                }
            }

            return reflexivKmerConcatList.iterator();
        }

        public void singleKmerRandomizer(Row currentSubKmer) {
            int blockSize = currentSubKmer.getSeq(2).length();

            Long[] newReflexivLongArray = new Long[blockSize];

            if (currentSubKmer.getInt(1) == 1) {
                int firstSuffixBlockLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros((Long) currentSubKmer.getSeq(2).apply(0)) / 2 + 1);
                long maxSuffixLengthBinary = ~((~0L) << (2 * firstSuffixBlockLength));
                Long[] newReflexivSubKmer = new Long[param.subKmerBinarySlots];
                Long newReflexivLong;

                if (randomReflexivMarker == 2) {
                    if (blockSize > 1) {

                        if (firstSuffixBlockLength >= param.subKmerSizeResidue) {
                            if (blockSize >= param.subKmerBinarySlots) {
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] = ((Long) currentSubKmer.getSeq(2).apply(blockSize - 1) & maxSubKmerResidueBinary);

                                long transit1 = (Long) currentSubKmer.getSeq(2).apply(blockSize - 1) >>> (2 * param.subKmerSizeResidue);
                                long transit2 = 0L;
                                for (int i = param.subKmerBinarySlots - 2; i >= 0; i--) {
                                    int j = blockSize - param.subKmerBinarySlots + i; // index of suffix long array
                                    transit2 = (Long) currentSubKmer.getSeq(2).apply(j) >>> (2 * param.subKmerSizeResidue);

                                    newReflexivSubKmer[i] = transit1;
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(2).apply(j) << 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;

                                    transit1 = transit2;
                                }

                                for (int i = blockSize - param.subKmerBinarySlots; i > 0; i--) {
                                    int j = param.subKmerBinarySlots + i - 1; // index of the new prefix long array
                                    newReflexivLongArray[j] = ((Long) currentSubKmer.getSeq(2).apply(i) >>> 2 * param.subKmerSizeResidue);
                                    newReflexivLongArray[j] |= (Long) currentSubKmer.getSeq(2).apply(i - 1) << 2 * (31 - param.subKmerSizeResidue);
                                    newReflexivLongArray[j] &= maxSubKmerBinary;
                                }

                                //   if (blockSize== param.subKmerBinarySlots){ // in the context of blockSize >= param.subKmerBinarySlots
                                newReflexivLongArray[param.subKmerBinarySlots - 1] = ((Long) currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary) >>> (2 * param.subKmerSizeResidue);
                                newReflexivLongArray[param.subKmerBinarySlots - 1] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) << 2 * (firstSuffixBlockLength - param.subKmerSizeResidue));
                                newReflexivLongArray[param.subKmerBinarySlots - 1] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 2) << 2 * firstSuffixBlockLength);
                                newReflexivLongArray[param.subKmerBinarySlots - 1] &= maxSubKmerBinary;
                                //  }

                                for (int i = param.subKmerBinarySlots - 2; i > 0; i--) {
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * (31 - firstSuffixBlockLength);
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(0).apply(i - 1) << 2 * firstSuffixBlockLength);
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                newReflexivLongArray[0] = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * (31 - firstSuffixBlockLength);
                                newReflexivLongArray[0] |= (1L << 2 * firstSuffixBlockLength); // add C marker in the front
                            } else { // blockSize < param.subKmerSizeResidue
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] = ((Long) currentSubKmer.getSeq(2).apply(blockSize - 1) & maxSubKmerResidueBinary);

                                long transit1 = (Long) currentSubKmer.getSeq(2).apply(blockSize - 1) >>> (2 * param.subKmerSizeResidue);
                                long transit2 = 0L;
                                for (int i = param.subKmerBinarySlots - 2; i >= param.subKmerBinarySlots - blockSize; i--) {
                                    int j = blockSize - param.subKmerBinarySlots + i; // index of suffix long array
                                    transit2 = (Long) currentSubKmer.getSeq(2).apply(j) >>> (2 * param.subKmerSizeResidue);

                                    newReflexivSubKmer[i] = transit1;
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(2).apply(j) << 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;

                                    transit1 = transit2;
                                }

                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize - 1] = ((Long) currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary) >>> (2 * param.subKmerSizeResidue);
                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize - 1] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) << 2 * (firstSuffixBlockLength - param.subKmerSizeResidue));
                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize - 1] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 2) << 2 * firstSuffixBlockLength);
                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize - 1] &= maxSubKmerBinary;

                                for (int i = param.subKmerBinarySlots - blockSize - 2; i >= 0; i--) {
                                    int j = blockSize + i; // index of the subkmer

                                    newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(j) >>> 2 * (31 - firstSuffixBlockLength);
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(0).apply(j - 1) << 2 * firstSuffixBlockLength);
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;
                                }

                                for (int i = blockSize - 1; i > 0; i--) {
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * (31 - firstSuffixBlockLength);
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(0).apply(i - 1) << 2 * firstSuffixBlockLength);
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                newReflexivLongArray[0] = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * (31 - firstSuffixBlockLength);
                                newReflexivLongArray[0] |= (1L << 2 * firstSuffixBlockLength); // add C marker in the front
                            }
                        } else { // firstSuffixBlockLength < param.subKmerSizeResidue
                            if (blockSize >= param.subKmerBinarySlots) {
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] = ((Long) currentSubKmer.getSeq(2).apply(blockSize - 1) & maxSubKmerResidueBinary);

                                long transit1 = (Long) currentSubKmer.getSeq(2).apply(blockSize - 1) >>> (2 * param.subKmerSizeResidue);
                                long transit2 = 0L;
                                for (int i = param.subKmerBinarySlots - 2; i > 0; i--) {
                                    int j = blockSize - param.subKmerBinarySlots + i; // index of suffix long array
                                    transit2 = (Long) currentSubKmer.getSeq(2).apply(j) >>> (2 * param.subKmerSizeResidue);

                                    newReflexivSubKmer[i] = transit1;
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(2).apply(j) << 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;

                                    transit1 = transit2;
                                }

                                newReflexivSubKmer[0] = transit1;
                                if (blockSize > param.subKmerBinarySlots) {
                                    newReflexivSubKmer[0] |= ((Long) currentSubKmer.getSeq(2).apply(blockSize - param.subKmerBinarySlots) << 2 * (31 - param.subKmerSizeResidue));
                                }else { // (blockSize== param.subKmerBinarySlots){
                                    newReflexivSubKmer[0] |= (((Long) currentSubKmer.getSeq(2).apply(blockSize - param.subKmerBinarySlots) & maxSuffixLengthBinary) << 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivSubKmer[0] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots-1) << 2*(31-param.subKmerSizeResidue + firstSuffixBlockLength));
                                }
                                newReflexivSubKmer[0] &= maxSubKmerBinary;

                                //newReflexivSubKmer[0] = transit1;
                                //newReflexivSubKmer[0] |= ((Long) currentSubKmer.getSeq(2).apply(blockSize-param.subKmerBinarySlots) << 2*(31-param.subKmerSizeResidue));


                                for (int i = blockSize - param.subKmerBinarySlots; i > 1; i--) {
                                    int j = param.subKmerBinarySlots + i - 1; // index of the new prefix long array
                                    newReflexivLongArray[j] = ((Long) currentSubKmer.getSeq(2).apply(i) >>> 2 * param.subKmerSizeResidue);
                                    newReflexivLongArray[j] |= (Long) currentSubKmer.getSeq(2).apply(i - 1) << 2 * (31 - param.subKmerSizeResidue);
                                    newReflexivLongArray[j] &= maxSubKmerBinary;
                                }

                                if (param.subKmerBinarySlots < blockSize) {
                                    newReflexivLongArray[param.subKmerBinarySlots] = ((Long) currentSubKmer.getSeq(2).apply(1) >>> 2 * param.subKmerSizeResidue);
                                    newReflexivLongArray[param.subKmerBinarySlots] |= ((Long) currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary)<< 2 * (31 - param.subKmerSizeResidue);
                                    newReflexivLongArray[param.subKmerBinarySlots] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) << 2 * (31 + firstSuffixBlockLength - param.subKmerSizeResidue));
                                    newReflexivLongArray[param.subKmerBinarySlots] &= maxSubKmerBinary;
                                }

                                //   if (blockSize== param.subKmerBinarySlots){ // in the context of blockSize >= param.subKmerBinarySlots
                                //  newReflexivLongArray[param.subKmerBinarySlots-1] = ((Long) currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary) >>> (2 * param.subKmerSizeResidue);
                                newReflexivLongArray[param.subKmerBinarySlots - 1] = ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) >>> 2 * (param.subKmerSizeResidue - firstSuffixBlockLength));
                                newReflexivLongArray[param.subKmerBinarySlots - 1] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 2) << 2 * firstSuffixBlockLength);
                                newReflexivLongArray[param.subKmerBinarySlots - 1] &= maxSubKmerBinary;
                                //  }

                                for (int i = param.subKmerBinarySlots - 2; i > 0; i--) {
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * (31 - firstSuffixBlockLength);
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(0).apply(i - 1) << 2 * firstSuffixBlockLength);
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                newReflexivLongArray[0] = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * (31 - firstSuffixBlockLength);
                                newReflexivLongArray[0] |= (1L << 2 * firstSuffixBlockLength); // add C marker in the front
                            } else { // blockSize < param.subKmerSizeResidue
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] = ((Long) currentSubKmer.getSeq(2).apply(blockSize - 1) & maxSubKmerResidueBinary);

                                long transit1 = (Long) currentSubKmer.getSeq(2).apply(blockSize - 1) >>> (2 * param.subKmerSizeResidue);
                                long transit2 = 0L;
                                for (int i = param.subKmerBinarySlots - 2; i > param.subKmerBinarySlots - blockSize; i--) {
                                    int j = blockSize - param.subKmerBinarySlots + i; // index of suffix long array
                                    transit2 = (Long) currentSubKmer.getSeq(2).apply(j) >>> (2 * param.subKmerSizeResidue);

                                    newReflexivSubKmer[i] = transit1;
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(2).apply(j) << 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;

                                    transit1 = transit2;
                                }

                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize] = (Long) currentSubKmer.getSeq(2).apply(1) >>> 2 * param.subKmerSizeResidue;
                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize] |= ((Long) currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary) << 2 * (31 - param.subKmerSizeResidue);
                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) << 2 * (31 - param.subKmerSizeResidue + firstSuffixBlockLength));
                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize] &= maxSubKmerBinary;

                                // newReflexivSubKmer[param.subKmerBinarySlots-blockSize-1] = ((Long) currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary) >>> (2 * param.subKmerSizeResidue);
                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize - 1] = ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) >>> 2 * (param.subKmerSizeResidue - firstSuffixBlockLength));
                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize - 1] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 2) << 2 * firstSuffixBlockLength);
                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize - 1] &= maxSubKmerBinary;

                                for (int i = param.subKmerBinarySlots - blockSize - 2; i >= 0; i--) {
                                    int j = blockSize + i; // index of the subkmer

                                    newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(j) >>> 2 * (31 - firstSuffixBlockLength);
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(0).apply(j - 1) << 2 * firstSuffixBlockLength);
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;
                                }

                                for (int i = blockSize - 1; i > 0; i--) {
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * (31 - firstSuffixBlockLength);
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(0).apply(i - 1) << 2 * firstSuffixBlockLength);
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                newReflexivLongArray[0] = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * (31 - firstSuffixBlockLength);
                                newReflexivLongArray[0] |= (1L << 2 * firstSuffixBlockLength); // add C marker in the front
                            }
                        }


                    } else { // block size ==1
                        if (firstSuffixBlockLength > param.subKmerSizeResidue) { // firstSuffixBlockLength is longer than the length of the last block (element) of sub kmer long array
                            //long transitBit1 = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) << 2 * (firstSuffixBlockLength - param.subKmerSizeResidue);
                            long transitBit1 = (((Long)currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary) >>> 2 * param.subKmerSizeResidue);
                            long transitBit2 = 0L;

                            newReflexivSubKmer[param.subKmerBinarySlots - 1] = ((Long) currentSubKmer.getSeq(2).apply(0) & maxSubKmerResidueBinary);

                            newReflexivSubKmer[param.subKmerBinarySlots - 2] = ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots-1) << 2* (firstSuffixBlockLength-param.subKmerSizeResidue));
                            newReflexivSubKmer[param.subKmerBinarySlots - 2] |= transitBit1;
                            newReflexivSubKmer[param.subKmerBinarySlots - 2] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots-2) << 2*firstSuffixBlockLength);
                            newReflexivSubKmer[param.subKmerBinarySlots - 2] &= maxSubKmerBinary;
                            transitBit1 = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots-2) >>> 2 * (31 - firstSuffixBlockLength);

                            for (int i = param.subKmerBinarySlots - 3; i >= 0; i--) {
                                transitBit2 = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * (31 - firstSuffixBlockLength);

                                newReflexivSubKmer[i] = ((Long) currentSubKmer.getSeq(0).apply(i) << 2 * firstSuffixBlockLength);
                                newReflexivSubKmer[i] |= transitBit1;
                                newReflexivSubKmer[i] &= maxSubKmerBinary;

                                transitBit1 = transitBit2;
                            }

                            newReflexivLong = transitBit1;
                            newReflexivLong |= (1L << 2 * firstSuffixBlockLength);
                            newReflexivLongArray[0] = newReflexivLong;

                        } else if (firstSuffixBlockLength == param.subKmerSizeResidue) {
                            long transitBit1 = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                            long transitBit2 = 0L;

                            newReflexivSubKmer[param.subKmerBinarySlots - 1] = ((Long) currentSubKmer.getSeq(2).apply(0) & maxSubKmerResidueBinary);

                            for (int i = param.subKmerBinarySlots - 2; i >= 0; i--) {
                                transitBit2 = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * (31 - firstSuffixBlockLength);

                                newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(i) << 2 * firstSuffixBlockLength;
                                newReflexivSubKmer[i] |= transitBit1;
                                newReflexivSubKmer[i] &= maxSubKmerBinary;

                                transitBit1 = transitBit2;
                            }

                            newReflexivLong = transitBit2;
                            newReflexivLong |= (1L << 2 * firstSuffixBlockLength);
                            newReflexivLongArray[0] = newReflexivLong;

                        } else { //firstSuffixBlockLength < param.subKmerSizeResidue
                            long transitBit1 = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) >>> 2 * (param.subKmerSizeResidue - firstSuffixBlockLength);
                            long transitBit2 = 0L;

                            newReflexivSubKmer[param.subKmerBinarySlots - 1] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) << 2 * firstSuffixBlockLength;
                            newReflexivSubKmer[param.subKmerBinarySlots - 1] |= ((Long) currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary);
                            newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;

                            for (int i = param.subKmerBinarySlots - 2; i >= 0; i--) {
                                transitBit2 = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * (31 - firstSuffixBlockLength);

                                newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(i) << 2 * firstSuffixBlockLength;
                                newReflexivSubKmer[i] |= transitBit1;
                                newReflexivSubKmer[i] &= maxSubKmerBinary;

                                transitBit1 = transitBit2;
                            }

                            newReflexivLong = transitBit2;
                            newReflexivLong |= (1L << 2 * firstSuffixBlockLength); // add C marker
                            newReflexivLongArray[0] = newReflexivLong;
                        }
                    }
/*
                    System.err.print(" start right ---- ");

                    for (int k = 30; k >= 0; k--) {
                        long a = newReflexivSubKmer[0] >> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.err.print(b);
                    }

                    for (int k = 30; k >= 0; k--) {
                        long a = newReflexivSubKmer[1] >>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.err.print(b);
                    }

                    if (newReflexivSubKmer.length >=3){
                        for (int k = 30; k >= 0; k--) {
                            long a = newReflexivSubKmer[2] >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.err.print(b);
                        }
                    }

                    if (newReflexivSubKmer.length >=4){
                        for (int k = 16; k >= 0; k--) {
                            long a = newReflexivSubKmer[3] >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.err.print(b);
                        }
                    }


                    System.err.print(" was den fuk right ");
                    for (int k = 30; k >= 0; k--) {
                        long a = newReflexivLongArray[0] >>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.err.print(b);
                    }

                    if (newReflexivLongArray.length >=2){
                        for (int k = 30; k >= 0; k--) {
                            long a = newReflexivLongArray[1] >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.err.print(b);
                        }
                    }

                    if (newReflexivLongArray.length >=3){
                        for (int k = 30; k >= 0; k--) {
                            long a = newReflexivLongArray[2] >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.err.print(b);
                        }
                    }

                    if (newReflexivLongArray.length >=4){
                        for (int k = 30; k >= 0; k--) {
                            long a = newReflexivLongArray[3] >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.err.print(b);
                        }
                    }


                    System.err.println(" Double random right end " + randomReflexivMarker + " " + newReflexivSubKmer.length + " " + newReflexivLongArray.length);
*/
                    reflexivKmerConcatList.add(
                            RowFactory.create(JavaConverters.asScalaIteratorConverter(Arrays.asList(newReflexivSubKmer).iterator()).asScala().toSeq(),
                                    randomReflexivMarker,  JavaConverters.asScalaIteratorConverter(Arrays.asList(newReflexivLongArray).iterator()).asScala().toSeq(), currentSubKmer.getInt(3), currentSubKmer.getInt(4)
                            )
                    );
                } else {

/*
                    for (int k = 30; k >= 0; k--) {
                        long a = (Long)currentSubKmer.getSeq(0).apply(0)>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    for (int k = 30; k >= 0; k--) {
                        long a = (Long)currentSubKmer.getSeq(0).apply(1)>>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    if (currentSubKmer.getSeq(0).length() >=3){
                        for (int k = 30; k >= 0; k--) {
                            long a = (Long)currentSubKmer.getSeq(0).apply(2)>>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    if (currentSubKmer.getSeq(0).length() >=4){
                        for (int k = 16; k >= 0; k--) {
                            long a = (Long)currentSubKmer.getSeq(0).apply(3)>>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }


                    System.out.print(" ");
                    for (int k = 30; k >= 0; k--) {
                        long a = (Long)currentSubKmer.getSeq(2).apply(0) >>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    if (newReflexivLongArray.length >=2){
                        for (int k = 30; k >= 0; k--) {
                            long a = (Long)currentSubKmer.getSeq(2).apply(1) >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    if (newReflexivLongArray.length >=3){
                        for (int k = 30; k >= 0; k--) {
                            long a = (Long)currentSubKmer.getSeq(2).apply(2) >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    if (newReflexivLongArray.length >=4){
                        for (int k = 30; k >= 0; k--) {
                            long a = (Long)currentSubKmer.getSeq(2).apply(3) >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }


                    System.out.println(" Double random same right end" + randomReflexivMarker);
                */
                    reflexivKmerConcatList.add(currentSubKmer);
                }
            } else { /* currentSubKmer.getInt(1) == 2 */
                int firstPrefixLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros((Long) currentSubKmer.getSeq(2).apply(0)) / 2 + 1);
                long maxPrefixLengthBinary = ~((~0L) << (2 * firstPrefixLength));
                Long[] newReflexivSubKmer = new Long[param.subKmerBinarySlots];
                long newReflexivLong = 0L;


                if (randomReflexivMarker == 2) {
/*

                    for (int k = 30; k >= 0; k--) {
                        long a = (Long)currentSubKmer.getSeq(0).apply(0)>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    for (int k = 30; k >= 0; k--) {
                        long a = (Long)currentSubKmer.getSeq(0).apply(1)>>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    if (currentSubKmer.getSeq(0).length() >=3){
                        for (int k = 30; k >= 0; k--) {
                            long a = (Long)currentSubKmer.getSeq(0).apply(2)>>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    if (currentSubKmer.getSeq(0).length() >=4){
                        for (int k = 16; k >= 0; k--) {
                            long a = (Long)currentSubKmer.getSeq(0).apply(3)>>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }


                    System.out.print(" ");
                    for (int k = 30; k >= 0; k--) {
                        long a = (Long)currentSubKmer.getSeq(2).apply(0) >>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    if (newReflexivLongArray.length >=2){
                        for (int k = 30; k >= 0; k--) {
                            long a = (Long)currentSubKmer.getSeq(2).apply(1) >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    if (newReflexivLongArray.length >=3){
                        for (int k = 30; k >= 0; k--) {
                            long a = (Long)currentSubKmer.getSeq(2).apply(2) >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    if (newReflexivLongArray.length >=4){
                        for (int k = 30; k >= 0; k--) {
                            long a = (Long)currentSubKmer.getSeq(2).apply(3) >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    System.out.println(" Double random same right end" + randomReflexivMarker);
*/
                    reflexivKmerConcatList.add(currentSubKmer);
                } else { /* randomReflexivMarker == 1 */
                    if (blockSize > 1) {
                        if (firstPrefixLength >= param.subKmerSizeResidue) {
                            if (blockSize >= param.subKmerBinarySlots) {
                                for (int i = 0; i < param.subKmerBinarySlots - 1; i++) {
                                    newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(2).apply(i) << 2 * (31 - firstPrefixLength);
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(2).apply(i + 1) >>> 2 * firstPrefixLength);
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;
                                }

                                newReflexivSubKmer[param.subKmerBinarySlots - 1] = (Long) currentSubKmer.getSeq(2).apply(param.subKmerBinarySlots - 1) >>> 2 * (firstPrefixLength - param.subKmerSizeResidue);
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;

                                newReflexivLongArray[0] = (Long) currentSubKmer.getSeq(2).apply(param.subKmerBinarySlots - 1) << 2 * param.subKmerSizeResidue;
                                if (blockSize > param.subKmerBinarySlots) {
                                    newReflexivLongArray[0] |= ((Long) currentSubKmer.getSeq(2).apply(param.subKmerBinarySlots) >>> 2 * (31 - param.subKmerSizeResidue));
                                } else {
                                    newReflexivLongArray[0] |= ((Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * (31 - param.subKmerSizeResidue));
                                }
                                newReflexivLongArray[0] &= maxPrefixLengthBinary;
                                newReflexivLongArray[0] |= (1L << 2 * firstPrefixLength); // add C marker in the front

                                for (int i = 1; i < blockSize - param.subKmerBinarySlots; i++) {
                                    int j = param.subKmerBinarySlots + i - 1;
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(2).apply(j) << 2 * param.subKmerSizeResidue;
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(2).apply(j + 1) >>> 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                if (blockSize > param.subKmerBinarySlots) {
                                    newReflexivLongArray[blockSize - param.subKmerBinarySlots] = (Long) currentSubKmer.getSeq(2).apply(blockSize - 1) << 2 * param.subKmerSizeResidue;
                                    newReflexivLongArray[blockSize - param.subKmerBinarySlots] |= ((Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivLongArray[blockSize - param.subKmerBinarySlots] &= maxSubKmerBinary;
                                }

                                for (int i = blockSize - param.subKmerBinarySlots + 1; i < blockSize - 1; i++) {
                                    int j = i - blockSize + param.subKmerBinarySlots - 1; // index of subKmer
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(0).apply(j) << 2 * param.subKmerSizeResidue;
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(0).apply(j + 1) >>> 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                newReflexivLongArray[blockSize - 1] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 2) << 2 * param.subKmerSizeResidue;
                                newReflexivLongArray[blockSize - 1] |= (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                                newReflexivLongArray[blockSize - 1] &= maxSubKmerBinary;

                            } else { // blockSize < param.subKmerBinarySlots
                                for (int i = 0; i < blockSize - 1; i++) {
                                    newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(2).apply(i) << 2 * (31 - firstPrefixLength);
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(2).apply(i + 1) >>> 2 * firstPrefixLength);
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;
                                }

                                newReflexivSubKmer[blockSize - 1] = (Long) currentSubKmer.getSeq(2).apply(blockSize - 1) << 2 * (31 - firstPrefixLength);
                                newReflexivSubKmer[blockSize - 1] |= ((Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * firstPrefixLength);
                                newReflexivSubKmer[blockSize - 1] &= maxSubKmerBinary;

                                for (int i = blockSize; i < param.subKmerBinarySlots - 1; i++) {
                                    int j = i - blockSize; // index of subKmer
                                    newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(j) << 2 * (31 - firstPrefixLength);
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(0).apply(j + 1) >>> 2 * firstPrefixLength);
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;
                                }

                                newReflexivSubKmer[param.subKmerBinarySlots - 1] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - blockSize - 1) >>> 2 * (firstPrefixLength - param.subKmerSizeResidue);
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;

                                newReflexivLongArray[0] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - blockSize - 1) << 2 * param.subKmerSizeResidue;
                                newReflexivLongArray[0] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - blockSize) >>> 2 * (31 - param.subKmerSizeResidue));
                                newReflexivLongArray[0] &= maxPrefixLengthBinary;
                                newReflexivLongArray[0] |= (1L << 2 * firstPrefixLength);

                                for (int i = 1; i < blockSize - 1; i++) {
                                    int j = param.subKmerBinarySlots - blockSize - 1 + i;
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(0).apply(j) << 2 * param.subKmerSizeResidue;
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(0).apply(j + 1) >>> 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                newReflexivLongArray[blockSize - 1] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 2) << 2 * param.subKmerSizeResidue;
                                newReflexivLongArray[blockSize - 1] |= (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                                newReflexivLongArray[blockSize - 1] &= maxSubKmerBinary;
                            }
                        } else { // firstPrefixLength < param.subKmerSizeResidue
                            if (blockSize >= param.subKmerBinarySlots) {
                                newReflexivSubKmer[0] =((Long) currentSubKmer.getSeq(2).apply(0) & maxPrefixLengthBinary) << 2 * (31 - firstPrefixLength);
                                newReflexivSubKmer[0] |= ((Long) currentSubKmer.getSeq(2).apply(1) >>> 2 * firstPrefixLength);
                                newReflexivSubKmer[0] &= maxSubKmerBinary;

                                for (int i = 1; i < param.subKmerBinarySlots - 1; i++) {
                                    newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(2).apply(i) << 2 * (31 - firstPrefixLength);
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(2).apply(i + 1) >>> 2 * firstPrefixLength);
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;
                                }

                                newReflexivSubKmer[param.subKmerBinarySlots - 1] = (Long) currentSubKmer.getSeq(2).apply(param.subKmerBinarySlots - 1) << 2 * (param.subKmerSizeResidue - firstPrefixLength);
                                if (blockSize > param.subKmerBinarySlots) {
                                    newReflexivSubKmer[param.subKmerBinarySlots - 1] |= ((Long) currentSubKmer.getSeq(2).apply(param.subKmerBinarySlots) >>> 2 * (31 - param.subKmerSizeResidue + firstPrefixLength));
                                } else { // blockSize == param.subKmerBinarySlots
                                    newReflexivSubKmer[param.subKmerBinarySlots - 1] |= ((Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * (31 - param.subKmerSizeResidue + firstPrefixLength));
                                }
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;


                                if (blockSize > param.subKmerBinarySlots) {
                                    newReflexivLongArray[0] = (Long) currentSubKmer.getSeq(2).apply(param.subKmerBinarySlots) >>> 2 * (31 - param.subKmerSizeResidue);
                                } else { // blockSize == param.subKmerBinarySlots
                                    newReflexivLongArray[0] = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * (31 - param.subKmerSizeResidue);
                                }
                                newReflexivLongArray[0] &= maxPrefixLengthBinary;
                                newReflexivLongArray[0] |= (1L << 2 * firstPrefixLength);

                                for (int i = 1; i < blockSize - param.subKmerBinarySlots; i++) {
                                    int j = param.subKmerBinarySlots + i - 1;
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(2).apply(j) << 2 * param.subKmerSizeResidue;
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(2).apply(j + 1) >>> 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                if (blockSize > param.subKmerBinarySlots) {
                                    newReflexivLongArray[blockSize - param.subKmerBinarySlots] = (Long) currentSubKmer.getSeq(2).apply(blockSize - 1) << 2 * param.subKmerSizeResidue;
                                    newReflexivLongArray[blockSize - param.subKmerBinarySlots] |= ((Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivLongArray[blockSize - param.subKmerBinarySlots] &= maxSubKmerBinary;
                                }

                                for (int i = blockSize - param.subKmerBinarySlots + 1; i < blockSize - 1; i++) {
                                    int j = i - blockSize + param.subKmerBinarySlots - 1; // index of subKmer
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(0).apply(j) << 2 * param.subKmerSizeResidue;
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(0).apply(j + 1) >>> 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                newReflexivLongArray[blockSize - 1] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 2) << 2 * param.subKmerSizeResidue;
                                newReflexivLongArray[blockSize - 1] |= (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                                newReflexivLongArray[blockSize - 1] &= maxSubKmerBinary;


                            } else { // blockSize < param.subKmerBinarySlots
                                newReflexivSubKmer[0] = ( (Long) currentSubKmer.getSeq(2).apply(0) & maxPrefixLengthBinary) << 2 * (31 - firstPrefixLength);
                                newReflexivSubKmer[0] |= ((Long) currentSubKmer.getSeq(2).apply(1) >>> 2 * firstPrefixLength);
                                newReflexivSubKmer[0] &= maxSubKmerBinary;
                                for (int i = 1; i < blockSize - 1; i++) {
                                    newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(2).apply(i) << 2 * (31 - firstPrefixLength);
                                    newReflexivSubKmer[i] |=( (Long) currentSubKmer.getSeq(2).apply(i + 1) >>> 2 * firstPrefixLength);
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;
                                }

                                newReflexivSubKmer[blockSize - 1] = (Long) currentSubKmer.getSeq(2).apply(blockSize - 1) << 2 * (31 - firstPrefixLength);
                                newReflexivSubKmer[blockSize - 1] |= ((Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * firstPrefixLength);
                                newReflexivSubKmer[blockSize - 1] &= maxSubKmerBinary;

                                for (int i = blockSize; i < param.subKmerBinarySlots - 1; i++) {
                                    int j = i - blockSize; // index of subKmer
                                    newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(j) << 2 * (31 - firstPrefixLength);
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(0).apply(j + 1) >>> 2 * firstPrefixLength);
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;
                                }

                                // newReflexivSubKmer[param.subKmerBinarySlots-1] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots-blockSize-1) >>> 2*(firstPrefixLength-param.subKmerSizeResidue);
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - blockSize - 1) << 2 * (param.subKmerSizeResidue - firstPrefixLength);
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - blockSize) >>> 2 * (31 - param.subKmerSizeResidue + firstPrefixLength));
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;

                                // newReflexivLongArray[0] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots-blockSize) << 2*param.subKmerSizeResidue;
                                newReflexivLongArray[0] = ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - blockSize) >>> 2 * (31 - param.subKmerSizeResidue));
                                newReflexivLongArray[0] &= maxPrefixLengthBinary;
                                newReflexivLongArray[0] |= (1L << 2 * firstPrefixLength);

                                for (int i = 1; i < blockSize - 1; i++) {
                                    int j = param.subKmerBinarySlots - blockSize + i -1;
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(0).apply(j) << 2 * param.subKmerSizeResidue;
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(0).apply(j + 1) >>> 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                newReflexivLongArray[blockSize - 1] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 2) << 2 * param.subKmerSizeResidue;
                                newReflexivLongArray[blockSize - 1] |= (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                                newReflexivLongArray[blockSize - 1] &= maxSubKmerBinary;
                            }
                        }

                    } else { /* blockSize == 1*/
                        if (firstPrefixLength > param.subKmerSizeResidue) {
                            long transitBit1 = (Long) currentSubKmer.getSeq(0).apply(0) & maxPrefixLengthBinary;

                            newReflexivSubKmer[0] = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * firstPrefixLength;
                            newReflexivSubKmer[0] |= (((Long) currentSubKmer.getSeq(2).apply(0) & maxPrefixLengthBinary) << 2 * (31 - firstPrefixLength));
                            newReflexivSubKmer[0] &= maxSubKmerBinary;

                            for (int i = 1; i < param.subKmerBinarySlots - 1; i++) {
                                long transitBit2 = (Long) currentSubKmer.getSeq(0).apply(i) & maxPrefixLengthBinary;

                                newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * firstPrefixLength;
                                newReflexivSubKmer[i] |= (transitBit1 << 2 * (31 - firstPrefixLength));
                                newReflexivSubKmer[i] &= maxSubKmerBinary;

                                transitBit1 = transitBit2;
                            }

                            newReflexivSubKmer[param.subKmerBinarySlots - 1] = transitBit1 >>> 2*(firstPrefixLength- param.subKmerSizeResidue);
                            newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;

                            newReflexivLong = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                            newReflexivLong |= (transitBit1 << 2 * param.subKmerSizeResidue);
                            newReflexivLong &= maxPrefixLengthBinary;
                            newReflexivLong |= (1L << 2 * firstPrefixLength); // add C marker in the front

                            newReflexivLongArray[0] = newReflexivLong;
                        } else if (firstPrefixLength == param.subKmerSizeResidue) {
                            long transitBit1 = (Long) currentSubKmer.getSeq(0).apply(0) & maxPrefixLengthBinary;

                            newReflexivSubKmer[0] = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * firstPrefixLength;
                            newReflexivSubKmer[0] |= (((Long) currentSubKmer.getSeq(2).apply(0) & maxPrefixLengthBinary) << 2 * (31 - firstPrefixLength));
                            newReflexivSubKmer[0] &= maxSubKmerBinary;

                            for (int i = 1; i < param.subKmerBinarySlots - 1; i++) {
                                long transitBit2 = (Long) currentSubKmer.getSeq(0).apply(i) & maxPrefixLengthBinary;

                                newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * firstPrefixLength;
                                newReflexivSubKmer[i] |= (transitBit1 << 2 * (31 - firstPrefixLength));
                                newReflexivSubKmer[i] &= maxSubKmerBinary;

                                transitBit1 = transitBit2;
                            }

                            newReflexivSubKmer[param.subKmerBinarySlots - 1] = transitBit1;
                            newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;

                            newReflexivLong = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                            newReflexivLong |= (1L << 2 * firstPrefixLength); // add C marker

                            newReflexivLongArray[0] = newReflexivLong;
                        } else {
                            long transitBit1 = (Long) currentSubKmer.getSeq(0).apply(0) & maxPrefixLengthBinary;

                            newReflexivSubKmer[0] = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * firstPrefixLength;
                            newReflexivSubKmer[0] |= (((Long) currentSubKmer.getSeq(2).apply(0) & maxPrefixLengthBinary) << 2 * (31 - firstPrefixLength));
                            newReflexivSubKmer[0] &= maxSubKmerBinary;

                            for (int i = 1; i < param.subKmerBinarySlots - 1; i++) {
                                long transitBit2 = (Long) currentSubKmer.getSeq(0).apply(i) & maxPrefixLengthBinary;

                                newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * firstPrefixLength;
                                newReflexivSubKmer[i] |= (transitBit1 << 2 * (31 - firstPrefixLength));
                                newReflexivSubKmer[i] &= maxSubKmerBinary;

                                transitBit1 = transitBit2;
                            }

                            newReflexivSubKmer[param.subKmerBinarySlots - 1] = transitBit1 << 2 * (param.subKmerSizeResidue - firstPrefixLength);
                            newReflexivSubKmer[param.subKmerBinarySlots - 1] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) >>> 2 * firstPrefixLength);
                            newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;


                            newReflexivLong = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) & maxPrefixLengthBinary;
                            newReflexivLong |= (1L << 2 * firstPrefixLength); // add C marker
                            newReflexivLongArray[0] = newReflexivLong;
                        }
                    }

/*
                    for (int k = 30; k >= 0; k--) {
                        long a = newReflexivSubKmer[0] >> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    for (int k = 30; k >= 0; k--) {
                        long a = newReflexivSubKmer[1] >>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    if (newReflexivSubKmer.length >=3){
                        for (int k = 30; k >= 0; k--) {
                            long a = newReflexivSubKmer[2] >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    if (newReflexivSubKmer.length >=4){
                        for (int k = 16; k >= 0; k--) {
                            long a = newReflexivSubKmer[3] >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    System.out.print(" ");
                    for (int k = 30; k >= 0; k--) {
                        long a = newReflexivLongArray[0] >>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    if (newReflexivLongArray.length >=2){
                        for (int k = 30; k >= 0; k--) {
                            long a = newReflexivLongArray[1] >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    if (newReflexivLongArray.length >=3){
                        for (int k = 30; k >= 0; k--) {
                            long a = newReflexivLongArray[2] >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    if (newReflexivLongArray.length >=4){
                        for (int k = 30; k >= 0; k--) {
                            long a = newReflexivLongArray[3] >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }


                    System.out.println(" Double random right end" + randomReflexivMarker);
*/
                    reflexivKmerConcatList.add(
                            RowFactory.create(JavaConverters.asScalaIteratorConverter(Arrays.asList(newReflexivSubKmer).iterator()).asScala().toSeq() ,
                                    randomReflexivMarker, JavaConverters.asScalaIteratorConverter(Arrays.asList(newReflexivLongArray).iterator()).asScala().toSeq(), currentSubKmer.getInt(3), currentSubKmer.getInt(4)
                            )
                    );
                }

            }
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

        /**
         *
         * @param currentSubKmer
         */
        public void directKmerComparison(Row currentSubKmer) {
            tmpReflexivKmerExtendList.add(currentSubKmer);
        }

        /**
         *
         * @param S
         */
        public void resetSubKmerGroup(Row S) {
            if (lineMarker == 1) {
                lineMarker = 2;
            } else {
                lineMarker = 3; /* reset to new sub-kmer group */
            }
            /* re-reflex all single kmers in the sub-kmer group */
//            if (tmpReflexivKmerExtendList.size() != 0) {
//                for (int i = 0; i < tmpReflexivKmerExtendList.size(); i++) {
            //                   singleKmerRandomizer(tmpReflexivKmerExtendList.get(i));
            //               }
            //          }

            tmpReflexivKmerExtendList = new ArrayList<Row>();
            tmpReflexivKmerExtendList.add(S
                    //   RowFactory.create(S.getLong(0),
                    //                   S.getInt(1), S.get(2), S.getInt(3), S.getInt(4)
                    //   )
            );
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

    class DSFilterExtendableKmerPairs implements MapPartitionsFunction<Row, Row>, Serializable {

        /* marker to identify similar SubKmers in the loop sequence */
        private int lineMarker = 1;

        /* 1 stands for forward sub-kmer */
        /* 2 stands for reflexiv sub-kmer */
        // private int randomReflexivMarker = ThreadLocalRandom.current().nextInt(1, 3);
        private int randomReflexivMarker = 2;

        long maxSubKmerResidueBinary = ~((~0L) << 2 * param.subKmerSizeResidue);
        long maxSubKmerBinary = ~((~0L) << 2 * 31);


        //long maxBlockBinary = ~((~0L) << 2*31); // a block has 31 nucleotide


        /* temporary capsule to store identical SubKmer units */
        List<Row> tmpReflexivKmerExtendList = new ArrayList<Row>();

        /* return capsule of extend Tuples for next iteration*/
        List<Row> reflexivKmerConcatList = new ArrayList<Row>();

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
        public Iterator<Row> call(Iterator<Row> sIterator) {

            if (param.scramble ==3){
                randomReflexivMarker =1;
            }

            randomReflexivMarker=1; // in this class, randomReflexivMarker is constantly set to 1

            while (sIterator.hasNext()) {
                Row s = sIterator.next();

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
                            if (subKmerSlotComparator(s.getSeq(0), tmpReflexivKmerExtendList.get(i).getSeq(0))) {
                                if (s.getInt(1) == 1) {
                                    if (tmpReflexivKmerExtendList.get(i).getInt(1) == 2) {
                                        int tmpReflexivKmerSuffixLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros((Long) tmpReflexivKmerExtendList.get(i).getSeq(2).apply(0)) / 2 + 1);
                                        int tmpBlockSize = (tmpReflexivKmerExtendList.get(i).getSeq(2).length() - 1) * 31 + tmpReflexivKmerSuffixLength;
                                        int currentReflexivKmerSuffixLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros((Long) s.getSeq(2).apply(0)) / 2 + 1);
                                        int currentBlockSize = (s.getSeq(2).length() - 1) * 31 + currentReflexivKmerSuffixLength;

                                        if (s.getInt(3) < 0 && tmpReflexivKmerExtendList.get(i).getInt(4) < 0) {
                                            reflexivKmerConcatList.add(s);
                                            singleKmerRandomizer(tmpReflexivKmerExtendList.get(i));
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else if (s.getInt(3) >= 0 && tmpReflexivKmerExtendList.get(i).getInt(4) >= 0) {
                                            reflexivKmerConcatList.add(s);
                                            singleKmerRandomizer(tmpReflexivKmerExtendList.get(i));
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else if (s.getInt(3) >= 0 && s.getInt(3) - tmpBlockSize >= 0) {
                                            reflexivKmerConcatList.add(s);
                                            singleKmerRandomizer(tmpReflexivKmerExtendList.get(i));
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else if (tmpReflexivKmerExtendList.get(i).getInt(4) >= 0 && tmpReflexivKmerExtendList.get(i).getInt(4) - currentBlockSize >= 0) {
                                            reflexivKmerConcatList.add(s);
                                            singleKmerRandomizer(tmpReflexivKmerExtendList.get(i));
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else {
                                            resetSubKmerGroup(s);
                                            break;
                                        }
                                    } else if (tmpReflexivKmerExtendList.get(i).getInt(1) == 1) {
                                        //directKmerComparison(s);
                                        System.out.println("mark check if this exists");
                                        resetSubKmerGroup(s);
                                        break;
                                    }
                                } else { /* if (s.getInt(1) == 2) { */
                                    if (tmpReflexivKmerExtendList.get(i).getInt(1) == 2) {
                                        System.out.println("mark check if this exists");
                                        resetSubKmerGroup(s);
                                        //directKmerComparison(s);
                                        break;
                                    } else if (tmpReflexivKmerExtendList.get(i).getInt(1) == 1) {
                                        int tmpReflexivKmerSuffixLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros((Long) tmpReflexivKmerExtendList.get(i).getSeq(2).apply(0)) / 2 + 1);
                                        int tmpBlockSize = (tmpReflexivKmerExtendList.get(i).getSeq(2).length() - 1) * 31 + tmpReflexivKmerSuffixLength;
                                        int currentReflexivKmerSuffixLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros((Long) s.getSeq(2).apply(0)) / 2 + 1);
                                        int currentBlockSize = (s.getSeq(2).length() - 1) * 31 + currentReflexivKmerSuffixLength;
                                        if (s.getInt(4) < 0 && tmpReflexivKmerExtendList.get(i).getInt(3) < 0) {
                                            reflexivKmerConcatList.add(tmpReflexivKmerExtendList.get(i));
                                            singleKmerRandomizer(s);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else if (s.getInt(4) >= 0 && tmpReflexivKmerExtendList.get(i).getInt(3) >= 0) {
                                            reflexivKmerConcatList.add(tmpReflexivKmerExtendList.get(i));
                                            singleKmerRandomizer(s);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else if (s.getInt(4) >= 0 && s.getInt(4) - tmpBlockSize >= 0) {
                                            reflexivKmerConcatList.add(tmpReflexivKmerExtendList.get(i));
                                            singleKmerRandomizer(s);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else if (tmpReflexivKmerExtendList.get(i).getInt(3) >= 0 && tmpReflexivKmerExtendList.get(i).getInt(3) - currentBlockSize >= 0) {
                                            reflexivKmerConcatList.add(tmpReflexivKmerExtendList.get(i));
                                            singleKmerRandomizer(s);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else {
                                            resetSubKmerGroup(s);
                                            break;
                                        }
                                    }
                                }
                            /* return reflexivKmerConcatList.iterator(); */
                            }

                        /* new Sub-kmer group section */
                            else { /* s.getLong(0) != tmpReflexivKmerExtendList.get(i).getLong(0)()*/
                                //  if (lineMarker == 2) { // lineMarker == 2 represents the second line of the partition
                                //     singleKmerRandomizer(tmpReflexivKmerExtendList.get(i));
                                // }
                                //  singleKmerRandomizer(s);
                                resetSubKmerGroup(s);
                                break;
                            }
                        } /* end of the while loop */
                    }// end of else condition

                    lineMarker++;
                    // return reflexivKmerConcatList.iterator();
                }
            } // while loop

            if (tmpReflexivKmerExtendList.size() != 0) {
                for (int i = 0; i < tmpReflexivKmerExtendList.size(); i++) {
                    reflexivKmerConcatList.add(tmpReflexivKmerExtendList.get(i));
                }
            }

            int kmernumber = reflexivKmerConcatList.size();
            System.out.println("Double afterwards extendable number: " + kmernumber);
            return reflexivKmerConcatList.iterator();
        }

        public void singleKmerRandomizer(Row currentSubKmer) {
            int blockSize = currentSubKmer.getSeq(2).length();

            Long[] newReflexivLongArray = new Long[blockSize];

            if (currentSubKmer.getInt(1) == 1) {
                int firstSuffixBlockLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros((Long) currentSubKmer.getSeq(2).apply(0)) / 2 + 1);
                long maxSuffixLengthBinary = ~((~0L) << (2 * firstSuffixBlockLength));
                Long[] newReflexivSubKmer = new Long[param.subKmerBinarySlots];
                Long newReflexivLong;

                if (randomReflexivMarker == 2) {
                    if (blockSize > 1) {

                        if (firstSuffixBlockLength >= param.subKmerSizeResidue) {
                            if (blockSize >= param.subKmerBinarySlots) {
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] = ((Long) currentSubKmer.getSeq(2).apply(blockSize - 1) & maxSubKmerResidueBinary);

                                long transit1 = (Long) currentSubKmer.getSeq(2).apply(blockSize - 1) >>> (2 * param.subKmerSizeResidue);
                                long transit2 = 0L;
                                for (int i = param.subKmerBinarySlots - 2; i >= 0; i--) {
                                    int j = blockSize - param.subKmerBinarySlots + i; // index of suffix long array
                                    transit2 = (Long) currentSubKmer.getSeq(2).apply(j) >>> (2 * param.subKmerSizeResidue);

                                    newReflexivSubKmer[i] = transit1;
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(2).apply(j) << 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;

                                    transit1 = transit2;
                                }

                                for (int i = blockSize - param.subKmerBinarySlots; i > 0; i--) {
                                    int j = param.subKmerBinarySlots + i - 1; // index of the new prefix long array
                                    newReflexivLongArray[j] = ((Long) currentSubKmer.getSeq(2).apply(i) >>> 2 * param.subKmerSizeResidue);
                                    newReflexivLongArray[j] |= (Long) currentSubKmer.getSeq(2).apply(i - 1) << 2 * (31 - param.subKmerSizeResidue);
                                    newReflexivLongArray[j] &= maxSubKmerBinary;
                                }

                                //   if (blockSize== param.subKmerBinarySlots){ // in the context of blockSize >= param.subKmerBinarySlots
                                newReflexivLongArray[param.subKmerBinarySlots - 1] = ((Long) currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary) >>> (2 * param.subKmerSizeResidue);
                                newReflexivLongArray[param.subKmerBinarySlots - 1] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) << 2 * (firstSuffixBlockLength - param.subKmerSizeResidue));
                                newReflexivLongArray[param.subKmerBinarySlots - 1] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 2) << 2 * firstSuffixBlockLength);
                                newReflexivLongArray[param.subKmerBinarySlots - 1] &= maxSubKmerBinary;
                                //  }

                                for (int i = param.subKmerBinarySlots - 2; i > 0; i--) {
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * (31 - firstSuffixBlockLength);
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(0).apply(i - 1) << 2 * firstSuffixBlockLength);
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                newReflexivLongArray[0] = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * (31 - firstSuffixBlockLength);
                                newReflexivLongArray[0] |= (1L << 2 * firstSuffixBlockLength); // add C marker in the front
                            } else { // blockSize < param.subKmerSizeResidue
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] = ((Long) currentSubKmer.getSeq(2).apply(blockSize - 1) & maxSubKmerResidueBinary);

                                long transit1 = (Long) currentSubKmer.getSeq(2).apply(blockSize - 1) >>> (2 * param.subKmerSizeResidue);
                                long transit2 = 0L;
                                for (int i = param.subKmerBinarySlots - 2; i >= param.subKmerBinarySlots - blockSize; i--) {
                                    int j = blockSize - param.subKmerBinarySlots + i; // index of suffix long array
                                    transit2 = (Long) currentSubKmer.getSeq(2).apply(j) >>> (2 * param.subKmerSizeResidue);

                                    newReflexivSubKmer[i] = transit1;
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(2).apply(j) << 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;

                                    transit1 = transit2;
                                }

                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize - 1] = ((Long) currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary) >>> (2 * param.subKmerSizeResidue);
                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize - 1] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) << 2 * (firstSuffixBlockLength - param.subKmerSizeResidue));
                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize - 1] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 2) << 2 * firstSuffixBlockLength);
                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize - 1] &= maxSubKmerBinary;

                                for (int i = param.subKmerBinarySlots - blockSize - 2; i >= 0; i--) {
                                    int j = blockSize + i; // index of the subkmer

                                    newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(j) >>> 2 * (31 - firstSuffixBlockLength);
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(0).apply(j - 1) << 2 * firstSuffixBlockLength);
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;
                                }

                                for (int i = blockSize - 1; i > 0; i--) {
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * (31 - firstSuffixBlockLength);
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(0).apply(i - 1) << 2 * firstSuffixBlockLength);
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                newReflexivLongArray[0] = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * (31 - firstSuffixBlockLength);
                                newReflexivLongArray[0] |= (1L << 2 * firstSuffixBlockLength); // add C marker in the front
                            }
                        } else { // firstSuffixBlockLength < param.subKmerSizeResidue
                            if (blockSize >= param.subKmerBinarySlots) {
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] = ((Long) currentSubKmer.getSeq(2).apply(blockSize - 1) & maxSubKmerResidueBinary);

                                long transit1 = (Long) currentSubKmer.getSeq(2).apply(blockSize - 1) >>> (2 * param.subKmerSizeResidue);
                                long transit2 = 0L;
                                for (int i = param.subKmerBinarySlots - 2; i > 0; i--) {
                                    int j = blockSize - param.subKmerBinarySlots + i; // index of suffix long array
                                    transit2 = (Long) currentSubKmer.getSeq(2).apply(j) >>> (2 * param.subKmerSizeResidue);

                                    newReflexivSubKmer[i] = transit1;
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(2).apply(j) << 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;

                                    transit1 = transit2;
                                }

                                newReflexivSubKmer[0] = transit1;
                                if (blockSize > param.subKmerBinarySlots) {
                                    newReflexivSubKmer[0] |= ((Long) currentSubKmer.getSeq(2).apply(blockSize - param.subKmerBinarySlots) << 2 * (31 - param.subKmerSizeResidue));
                                }else { // (blockSize== param.subKmerBinarySlots){
                                    newReflexivSubKmer[0] |= (((Long) currentSubKmer.getSeq(2).apply(blockSize - param.subKmerBinarySlots) & maxSuffixLengthBinary) << 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivSubKmer[0] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots-1) << 2*(31-param.subKmerSizeResidue + firstSuffixBlockLength));
                                }
                                newReflexivSubKmer[0] &= maxSubKmerBinary;

                                //newReflexivSubKmer[0] = transit1;
                                //newReflexivSubKmer[0] |= ((Long) currentSubKmer.getSeq(2).apply(blockSize-param.subKmerBinarySlots) << 2*(31-param.subKmerSizeResidue));


                                for (int i = blockSize - param.subKmerBinarySlots; i > 1; i--) {
                                    int j = param.subKmerBinarySlots + i - 1; // index of the new prefix long array
                                    newReflexivLongArray[j] = ((Long) currentSubKmer.getSeq(2).apply(i) >>> 2 * param.subKmerSizeResidue);
                                    newReflexivLongArray[j] |= (Long) currentSubKmer.getSeq(2).apply(i - 1) << 2 * (31 - param.subKmerSizeResidue);
                                    newReflexivLongArray[j] &= maxSubKmerBinary;
                                }

                                if (param.subKmerBinarySlots < blockSize) {
                                    newReflexivLongArray[param.subKmerBinarySlots] = ((Long) currentSubKmer.getSeq(2).apply(1) >>> 2 * param.subKmerSizeResidue);
                                    newReflexivLongArray[param.subKmerBinarySlots] |= ((Long) currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary)<< 2 * (31 - param.subKmerSizeResidue);
                                    newReflexivLongArray[param.subKmerBinarySlots] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) << 2 * (31 + firstSuffixBlockLength - param.subKmerSizeResidue));
                                    newReflexivLongArray[param.subKmerBinarySlots] &= maxSubKmerBinary;
                                }

                                //   if (blockSize== param.subKmerBinarySlots){ // in the context of blockSize >= param.subKmerBinarySlots
                                //  newReflexivLongArray[param.subKmerBinarySlots-1] = ((Long) currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary) >>> (2 * param.subKmerSizeResidue);
                                newReflexivLongArray[param.subKmerBinarySlots - 1] = ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) >>> 2 * (param.subKmerSizeResidue - firstSuffixBlockLength));
                                newReflexivLongArray[param.subKmerBinarySlots - 1] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 2) << 2 * firstSuffixBlockLength);
                                newReflexivLongArray[param.subKmerBinarySlots - 1] &= maxSubKmerBinary;
                                //  }

                                for (int i = param.subKmerBinarySlots - 2; i > 0; i--) {
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * (31 - firstSuffixBlockLength);
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(0).apply(i - 1) << 2 * firstSuffixBlockLength);
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                newReflexivLongArray[0] = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * (31 - firstSuffixBlockLength);
                                newReflexivLongArray[0] |= (1L << 2 * firstSuffixBlockLength); // add C marker in the front
                            } else { // blockSize < param.subKmerSizeResidue
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] = ((Long) currentSubKmer.getSeq(2).apply(blockSize - 1) & maxSubKmerResidueBinary);

                                long transit1 = (Long) currentSubKmer.getSeq(2).apply(blockSize - 1) >>> (2 * param.subKmerSizeResidue);
                                long transit2 = 0L;
                                for (int i = param.subKmerBinarySlots - 2; i > param.subKmerBinarySlots - blockSize; i--) {
                                    int j = blockSize - param.subKmerBinarySlots + i; // index of suffix long array
                                    transit2 = (Long) currentSubKmer.getSeq(2).apply(j) >>> (2 * param.subKmerSizeResidue);

                                    newReflexivSubKmer[i] = transit1;
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(2).apply(j) << 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;

                                    transit1 = transit2;
                                }

                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize] = (Long) currentSubKmer.getSeq(2).apply(1) >>> 2 * param.subKmerSizeResidue;
                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize] |= ((Long) currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary) << 2 * (31 - param.subKmerSizeResidue);
                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) << 2 * (31 - param.subKmerSizeResidue + firstSuffixBlockLength));
                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize] &= maxSubKmerBinary;

                                // newReflexivSubKmer[param.subKmerBinarySlots-blockSize-1] = ((Long) currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary) >>> (2 * param.subKmerSizeResidue);
                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize - 1] = ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) >>> 2 * (param.subKmerSizeResidue - firstSuffixBlockLength));
                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize - 1] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 2) << 2 * firstSuffixBlockLength);
                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize - 1] &= maxSubKmerBinary;

                                for (int i = param.subKmerBinarySlots - blockSize - 2; i >= 0; i--) {
                                    int j = blockSize + i; // index of the subkmer

                                    newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(j) >>> 2 * (31 - firstSuffixBlockLength);
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(0).apply(j - 1) << 2 * firstSuffixBlockLength);
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;
                                }

                                for (int i = blockSize - 1; i > 0; i--) {
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * (31 - firstSuffixBlockLength);
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(0).apply(i - 1) << 2 * firstSuffixBlockLength);
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                newReflexivLongArray[0] = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * (31 - firstSuffixBlockLength);
                                newReflexivLongArray[0] |= (1L << 2 * firstSuffixBlockLength); // add C marker in the front
                            }
                        }


                    } else { // block size ==1
                        if (firstSuffixBlockLength > param.subKmerSizeResidue) { // firstSuffixBlockLength is longer than the length of the last block (element) of sub kmer long array
                            //long transitBit1 = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) << 2 * (firstSuffixBlockLength - param.subKmerSizeResidue);
                            long transitBit1 = (((Long)currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary) >>> 2 * param.subKmerSizeResidue);
                            long transitBit2 = 0L;

                            newReflexivSubKmer[param.subKmerBinarySlots - 1] = ((Long) currentSubKmer.getSeq(2).apply(0) & maxSubKmerResidueBinary);

                            newReflexivSubKmer[param.subKmerBinarySlots - 2] = ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots-1) << 2* (firstSuffixBlockLength-param.subKmerSizeResidue));
                            newReflexivSubKmer[param.subKmerBinarySlots - 2] |= transitBit1;
                            newReflexivSubKmer[param.subKmerBinarySlots - 2] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots-2) << 2*firstSuffixBlockLength);
                            newReflexivSubKmer[param.subKmerBinarySlots - 2] &= maxSubKmerBinary;
                            transitBit1 = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots-2) >>> 2 * (31 - firstSuffixBlockLength);

                            for (int i = param.subKmerBinarySlots - 3; i >= 0; i--) {
                                transitBit2 = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * (31 - firstSuffixBlockLength);

                                newReflexivSubKmer[i] = ((Long) currentSubKmer.getSeq(0).apply(i) << 2 * firstSuffixBlockLength);
                                newReflexivSubKmer[i] |= transitBit1;
                                newReflexivSubKmer[i] &= maxSubKmerBinary;

                                transitBit1 = transitBit2;
                            }

                            newReflexivLong = transitBit1;
                            newReflexivLong |= (1L << 2 * firstSuffixBlockLength);
                            newReflexivLongArray[0] = newReflexivLong;

                        } else if (firstSuffixBlockLength == param.subKmerSizeResidue) {
                            long transitBit1 = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                            long transitBit2 = 0L;

                            newReflexivSubKmer[param.subKmerBinarySlots - 1] = ((Long) currentSubKmer.getSeq(2).apply(0) & maxSubKmerResidueBinary);

                            for (int i = param.subKmerBinarySlots - 2; i >= 0; i--) {
                                transitBit2 = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * (31 - firstSuffixBlockLength);

                                newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(i) << 2 * firstSuffixBlockLength;
                                newReflexivSubKmer[i] |= transitBit1;
                                newReflexivSubKmer[i] &= maxSubKmerBinary;

                                transitBit1 = transitBit2;
                            }

                            newReflexivLong = transitBit2;
                            newReflexivLong |= (1L << 2 * firstSuffixBlockLength);
                            newReflexivLongArray[0] = newReflexivLong;

                        } else { //firstSuffixBlockLength < param.subKmerSizeResidue
                            long transitBit1 = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) >>> 2 * (param.subKmerSizeResidue - firstSuffixBlockLength);
                            long transitBit2 = 0L;

                            newReflexivSubKmer[param.subKmerBinarySlots - 1] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) << 2 * firstSuffixBlockLength;
                            newReflexivSubKmer[param.subKmerBinarySlots - 1] |= ((Long) currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary);
                            newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;

                            for (int i = param.subKmerBinarySlots - 2; i >= 0; i--) {
                                transitBit2 = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * (31 - firstSuffixBlockLength);

                                newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(i) << 2 * firstSuffixBlockLength;
                                newReflexivSubKmer[i] |= transitBit1;
                                newReflexivSubKmer[i] &= maxSubKmerBinary;

                                transitBit1 = transitBit2;
                            }

                            newReflexivLong = transitBit2;
                            newReflexivLong |= (1L << 2 * firstSuffixBlockLength); // add C marker
                            newReflexivLongArray[0] = newReflexivLong;
                        }
                    }

/*
                    for (int k = 30; k >= 0; k--) {
                        long a = newReflexivSubKmer[0] >> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    for (int k = 30; k >= 0; k--) {
                        long a = newReflexivSubKmer[1] >>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    if (newReflexivSubKmer.length >=3){
                        for (int k = 30; k >= 0; k--) {
                            long a = newReflexivSubKmer[2] >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    if (newReflexivSubKmer.length >=4){
                        for (int k = 16; k >= 0; k--) {
                            long a = newReflexivSubKmer[3] >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }


                    System.out.print(" ");
                    for (int k = 30; k >= 0; k--) {
                        long a = newReflexivLongArray[0] >>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    if (newReflexivLongArray.length >=2){
                        for (int k = 30; k >= 0; k--) {
                            long a = newReflexivLongArray[1] >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    if (newReflexivLongArray.length >=3){
                        for (int k = 30; k >= 0; k--) {
                            long a = newReflexivLongArray[2] >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    if (newReflexivLongArray.length >=4){
                        for (int k = 30; k >= 0; k--) {
                            long a = newReflexivLongArray[3] >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }


                    System.out.println(" Double random extendable pairs" + randomReflexivMarker);
*/
                    reflexivKmerConcatList.add(
                            RowFactory.create(JavaConverters.asScalaIteratorConverter(Arrays.asList(newReflexivSubKmer).iterator()).asScala().toSeq(),
                                    randomReflexivMarker,  JavaConverters.asScalaIteratorConverter(Arrays.asList(newReflexivLongArray).iterator()).asScala().toSeq(), currentSubKmer.getInt(3), currentSubKmer.getInt(4)
                            )
                    );
                } else {

/*
                    for (int k = 30; k >= 0; k--) {
                        long a = (Long)currentSubKmer.getSeq(0).apply(0)>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    for (int k = 30; k >= 0; k--) {
                        long a = (Long)currentSubKmer.getSeq(0).apply(1)>>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    if (currentSubKmer.getSeq(0).length() >=3){
                        for (int k = 30; k >= 0; k--) {
                            long a = (Long)currentSubKmer.getSeq(0).apply(2)>>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    if (currentSubKmer.getSeq(0).length() >=4){
                        for (int k = 16; k >= 0; k--) {
                            long a = (Long)currentSubKmer.getSeq(0).apply(3)>>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }


                    System.out.print(" ");
                    for (int k = 30; k >= 0; k--) {
                        long a = (Long)currentSubKmer.getSeq(2).apply(0) >>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    if (newReflexivLongArray.length >=2){
                        for (int k = 30; k >= 0; k--) {
                            long a = (Long)currentSubKmer.getSeq(2).apply(1) >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    if (newReflexivLongArray.length >=3){
                        for (int k = 30; k >= 0; k--) {
                            long a = (Long)currentSubKmer.getSeq(2).apply(2) >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    if (newReflexivLongArray.length >=4){
                        for (int k = 30; k >= 0; k--) {
                            long a = (Long)currentSubKmer.getSeq(2).apply(3) >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }


                    System.out.println(" Double random same extendable pairs" + randomReflexivMarker);
                    */
                    reflexivKmerConcatList.add(currentSubKmer);
                }
            } else { /* currentSubKmer.getInt(1) == 2 */
                int firstPrefixLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros((Long) currentSubKmer.getSeq(2).apply(0)) / 2 + 1);
                long maxPrefixLengthBinary = ~((~0L) << (2 * firstPrefixLength));
                Long[] newReflexivSubKmer = new Long[param.subKmerBinarySlots];
                long newReflexivLong = 0L;


                if (randomReflexivMarker == 2) {

/*
                    for (int k = 30; k >= 0; k--) {
                        long a = (Long)currentSubKmer.getSeq(0).apply(0)>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    for (int k = 30; k >= 0; k--) {
                        long a = (Long)currentSubKmer.getSeq(0).apply(1)>>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    if (currentSubKmer.getSeq(0).length() >=3){
                        for (int k = 30; k >= 0; k--) {
                            long a = (Long)currentSubKmer.getSeq(0).apply(2)>>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    if (currentSubKmer.getSeq(0).length() >=4){
                        for (int k = 16; k >= 0; k--) {
                            long a = (Long)currentSubKmer.getSeq(0).apply(3)>>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }


                    System.out.print(" ");
                    for (int k = 30; k >= 0; k--) {
                        long a = (Long)currentSubKmer.getSeq(2).apply(0) >>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    if (newReflexivLongArray.length >=2){
                        for (int k = 30; k >= 0; k--) {
                            long a = (Long)currentSubKmer.getSeq(2).apply(1) >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    if (newReflexivLongArray.length >=3){
                        for (int k = 30; k >= 0; k--) {
                            long a = (Long)currentSubKmer.getSeq(2).apply(2) >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    if (newReflexivLongArray.length >=4){
                        for (int k = 30; k >= 0; k--) {
                            long a = (Long)currentSubKmer.getSeq(2).apply(3) >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    System.out.println(" Double random same extendable pairs" + randomReflexivMarker);
*/
                    reflexivKmerConcatList.add(currentSubKmer);
                } else { /* randomReflexivMarker == 1 */
                    if (blockSize > 1) {
                        if (firstPrefixLength >= param.subKmerSizeResidue) {
                            if (blockSize >= param.subKmerBinarySlots) {
                                for (int i = 0; i < param.subKmerBinarySlots - 1; i++) {
                                    newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(2).apply(i) << 2 * (31 - firstPrefixLength);
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(2).apply(i + 1) >>> 2 * firstPrefixLength);
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;
                                }

                                newReflexivSubKmer[param.subKmerBinarySlots - 1] = (Long) currentSubKmer.getSeq(2).apply(param.subKmerBinarySlots - 1) >>> 2 * (firstPrefixLength - param.subKmerSizeResidue);
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;

                                newReflexivLongArray[0] = (Long) currentSubKmer.getSeq(2).apply(param.subKmerBinarySlots - 1) << 2 * param.subKmerSizeResidue;
                                if (blockSize > param.subKmerBinarySlots) {
                                    newReflexivLongArray[0] |= ((Long) currentSubKmer.getSeq(2).apply(param.subKmerBinarySlots) >>> 2 * (31 - param.subKmerSizeResidue));
                                } else {
                                    newReflexivLongArray[0] |= ((Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * (31 - param.subKmerSizeResidue));
                                }
                                newReflexivLongArray[0] &= maxPrefixLengthBinary;
                                newReflexivLongArray[0] |= (1L << 2 * firstPrefixLength); // add C marker in the front

                                for (int i = 1; i < blockSize - param.subKmerBinarySlots; i++) {
                                    int j = param.subKmerBinarySlots + i - 1;
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(2).apply(j) << 2 * param.subKmerSizeResidue;
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(2).apply(j + 1) >>> 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                if (blockSize > param.subKmerBinarySlots) {
                                    newReflexivLongArray[blockSize - param.subKmerBinarySlots] = (Long) currentSubKmer.getSeq(2).apply(blockSize - 1) << 2 * param.subKmerSizeResidue;
                                    newReflexivLongArray[blockSize - param.subKmerBinarySlots] |= ((Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivLongArray[blockSize - param.subKmerBinarySlots] &= maxSubKmerBinary;
                                }

                                for (int i = blockSize - param.subKmerBinarySlots + 1; i < blockSize - 1; i++) {
                                    int j = i - blockSize + param.subKmerBinarySlots - 1; // index of subKmer
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(0).apply(j) << 2 * param.subKmerSizeResidue;
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(0).apply(j + 1) >>> 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                newReflexivLongArray[blockSize - 1] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 2) << 2 * param.subKmerSizeResidue;
                                newReflexivLongArray[blockSize - 1] |= (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                                newReflexivLongArray[blockSize - 1] &= maxSubKmerBinary;

                            } else { // blockSize < param.subKmerBinarySlots
                                for (int i = 0; i < blockSize - 1; i++) {
                                    newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(2).apply(i) << 2 * (31 - firstPrefixLength);
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(2).apply(i + 1) >>> 2 * firstPrefixLength);
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;
                                }

                                newReflexivSubKmer[blockSize - 1] = (Long) currentSubKmer.getSeq(2).apply(blockSize - 1) << 2 * (31 - firstPrefixLength);
                                newReflexivSubKmer[blockSize - 1] |= ((Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * firstPrefixLength);
                                newReflexivSubKmer[blockSize - 1] &= maxSubKmerBinary;

                                for (int i = blockSize; i < param.subKmerBinarySlots - 1; i++) {
                                    int j = i - blockSize; // index of subKmer
                                    newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(j) << 2 * (31 - firstPrefixLength);
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(0).apply(j + 1) >>> 2 * firstPrefixLength);
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;
                                }

                                newReflexivSubKmer[param.subKmerBinarySlots - 1] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - blockSize - 1) >>> 2 * (firstPrefixLength - param.subKmerSizeResidue);
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;

                                newReflexivLongArray[0] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - blockSize - 1) << 2 * param.subKmerSizeResidue;
                                newReflexivLongArray[0] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - blockSize) >>> 2 * (31 - param.subKmerSizeResidue));
                                newReflexivLongArray[0] &= maxPrefixLengthBinary;
                                newReflexivLongArray[0] |= (1L << 2 * firstPrefixLength);

                                for (int i = 1; i < blockSize - 1; i++) {
                                    int j = param.subKmerBinarySlots - blockSize - 1 + i;
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(0).apply(j) << 2 * param.subKmerSizeResidue;
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(0).apply(j + 1) >>> 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                newReflexivLongArray[blockSize - 1] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 2) << 2 * param.subKmerSizeResidue;
                                newReflexivLongArray[blockSize - 1] |= (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                                newReflexivLongArray[blockSize - 1] &= maxSubKmerBinary;
                            }
                        } else { // firstPrefixLength < param.subKmerSizeResidue
                            if (blockSize >= param.subKmerBinarySlots) {
                                newReflexivSubKmer[0] =((Long) currentSubKmer.getSeq(2).apply(0) & maxPrefixLengthBinary) << 2 * (31 - firstPrefixLength);
                                newReflexivSubKmer[0] |= ((Long) currentSubKmer.getSeq(2).apply(1) >>> 2 * firstPrefixLength);
                                newReflexivSubKmer[0] &= maxSubKmerBinary;

                                for (int i = 1; i < param.subKmerBinarySlots - 1; i++) {
                                    newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(2).apply(i) << 2 * (31 - firstPrefixLength);
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(2).apply(i + 1) >>> 2 * firstPrefixLength);
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;
                                }

                                newReflexivSubKmer[param.subKmerBinarySlots - 1] = (Long) currentSubKmer.getSeq(2).apply(param.subKmerBinarySlots - 1) << 2 * (param.subKmerSizeResidue - firstPrefixLength);
                                if (blockSize > param.subKmerBinarySlots) {
                                    newReflexivSubKmer[param.subKmerBinarySlots - 1] |= ((Long) currentSubKmer.getSeq(2).apply(param.subKmerBinarySlots) >>> 2 * (31 - param.subKmerSizeResidue + firstPrefixLength));
                                } else { // blockSize == param.subKmerBinarySlots
                                    newReflexivSubKmer[param.subKmerBinarySlots - 1] |= ((Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * (31 - param.subKmerSizeResidue + firstPrefixLength));
                                }
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;


                                if (blockSize > param.subKmerBinarySlots) {
                                    newReflexivLongArray[0] = (Long) currentSubKmer.getSeq(2).apply(param.subKmerBinarySlots) >>> 2 * (31 - param.subKmerSizeResidue);
                                } else { // blockSize == param.subKmerBinarySlots
                                    newReflexivLongArray[0] = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * (31 - param.subKmerSizeResidue);
                                }
                                newReflexivLongArray[0] &= maxPrefixLengthBinary;
                                newReflexivLongArray[0] |= (1L << 2 * firstPrefixLength);

                                for (int i = 1; i < blockSize - param.subKmerBinarySlots; i++) {
                                    int j = param.subKmerBinarySlots + i - 1;
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(2).apply(j) << 2 * param.subKmerSizeResidue;
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(2).apply(j + 1) >>> 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                if (blockSize > param.subKmerBinarySlots) {
                                    newReflexivLongArray[blockSize - param.subKmerBinarySlots] = (Long) currentSubKmer.getSeq(2).apply(blockSize - 1) << 2 * param.subKmerSizeResidue;
                                    newReflexivLongArray[blockSize - param.subKmerBinarySlots] |= ((Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivLongArray[blockSize - param.subKmerBinarySlots] &= maxSubKmerBinary;
                                }

                                for (int i = blockSize - param.subKmerBinarySlots + 1; i < blockSize - 1; i++) {
                                    int j = i - blockSize + param.subKmerBinarySlots - 1; // index of subKmer
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(0).apply(j) << 2 * param.subKmerSizeResidue;
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(0).apply(j + 1) >>> 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                newReflexivLongArray[blockSize - 1] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 2) << 2 * param.subKmerSizeResidue;
                                newReflexivLongArray[blockSize - 1] |= (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                                newReflexivLongArray[blockSize - 1] &= maxSubKmerBinary;


                            } else { // blockSize < param.subKmerBinarySlots
                                newReflexivSubKmer[0] = ( (Long) currentSubKmer.getSeq(2).apply(0) & maxPrefixLengthBinary) << 2 * (31 - firstPrefixLength);
                                newReflexivSubKmer[0] |= ((Long) currentSubKmer.getSeq(2).apply(1) >>> 2 * firstPrefixLength);
                                newReflexivSubKmer[0] &= maxSubKmerBinary;
                                for (int i = 1; i < blockSize - 1; i++) {
                                    newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(2).apply(i) << 2 * (31 - firstPrefixLength);
                                    newReflexivSubKmer[i] |=( (Long) currentSubKmer.getSeq(2).apply(i + 1) >>> 2 * firstPrefixLength);
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;
                                }

                                newReflexivSubKmer[blockSize - 1] = (Long) currentSubKmer.getSeq(2).apply(blockSize - 1) << 2 * (31 - firstPrefixLength);
                                newReflexivSubKmer[blockSize - 1] |= ((Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * firstPrefixLength);
                                newReflexivSubKmer[blockSize - 1] &= maxSubKmerBinary;

                                for (int i = blockSize; i < param.subKmerBinarySlots - 1; i++) {
                                    int j = i - blockSize; // index of subKmer
                                    newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(j) << 2 * (31 - firstPrefixLength);
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(0).apply(j + 1) >>> 2 * firstPrefixLength);
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;
                                }

                                // newReflexivSubKmer[param.subKmerBinarySlots-1] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots-blockSize-1) >>> 2*(firstPrefixLength-param.subKmerSizeResidue);
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - blockSize - 1) << 2 * (param.subKmerSizeResidue - firstPrefixLength);
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - blockSize) >>> 2 * (31 - param.subKmerSizeResidue + firstPrefixLength));
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;

                                // newReflexivLongArray[0] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots-blockSize) << 2*param.subKmerSizeResidue;
                                newReflexivLongArray[0] = ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - blockSize) >>> 2 * (31 - param.subKmerSizeResidue));
                                newReflexivLongArray[0] &= maxPrefixLengthBinary;
                                newReflexivLongArray[0] |= (1L << 2 * firstPrefixLength);

                                for (int i = 1; i < blockSize - 1; i++) {
                                    int j = param.subKmerBinarySlots - blockSize + i -1;
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(0).apply(j) << 2 * param.subKmerSizeResidue;
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(0).apply(j + 1) >>> 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                newReflexivLongArray[blockSize - 1] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 2) << 2 * param.subKmerSizeResidue;
                                newReflexivLongArray[blockSize - 1] |= (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                                newReflexivLongArray[blockSize - 1] &= maxSubKmerBinary;
                            }
                        }

                    } else { /* blockSize == 1*/
                        if (firstPrefixLength > param.subKmerSizeResidue) {
                            long transitBit1 = (Long) currentSubKmer.getSeq(0).apply(0) & maxPrefixLengthBinary;

                            newReflexivSubKmer[0] = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * firstPrefixLength;
                            newReflexivSubKmer[0] |= (((Long) currentSubKmer.getSeq(2).apply(0) & maxPrefixLengthBinary) << 2 * (31 - firstPrefixLength));
                            newReflexivSubKmer[0] &= maxSubKmerBinary;

                            for (int i = 1; i < param.subKmerBinarySlots - 1; i++) {
                                long transitBit2 = (Long) currentSubKmer.getSeq(0).apply(i) & maxPrefixLengthBinary;

                                newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * firstPrefixLength;
                                newReflexivSubKmer[i] |= (transitBit1 << 2 * (31 - firstPrefixLength));
                                newReflexivSubKmer[i] &= maxSubKmerBinary;

                                transitBit1 = transitBit2;
                            }

                            newReflexivSubKmer[param.subKmerBinarySlots - 1] = transitBit1 >>> 2*(firstPrefixLength- param.subKmerSizeResidue);
                            newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;

                            newReflexivLong = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                            newReflexivLong |= (transitBit1 << 2 * param.subKmerSizeResidue);
                            newReflexivLong &= maxPrefixLengthBinary;
                            newReflexivLong |= (1L << 2 * firstPrefixLength); // add C marker in the front

                            newReflexivLongArray[0] = newReflexivLong;
                        } else if (firstPrefixLength == param.subKmerSizeResidue) {
                            long transitBit1 = (Long) currentSubKmer.getSeq(0).apply(0) & maxPrefixLengthBinary;

                            newReflexivSubKmer[0] = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * firstPrefixLength;
                            newReflexivSubKmer[0] |= (((Long) currentSubKmer.getSeq(2).apply(0) & maxPrefixLengthBinary) << 2 * (31 - firstPrefixLength));
                            newReflexivSubKmer[0] &= maxSubKmerBinary;

                            for (int i = 1; i < param.subKmerBinarySlots - 1; i++) {
                                long transitBit2 = (Long) currentSubKmer.getSeq(0).apply(i) & maxPrefixLengthBinary;

                                newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * firstPrefixLength;
                                newReflexivSubKmer[i] |= (transitBit1 << 2 * (31 - firstPrefixLength));
                                newReflexivSubKmer[i] &= maxSubKmerBinary;

                                transitBit1 = transitBit2;
                            }

                            newReflexivSubKmer[param.subKmerBinarySlots - 1] = transitBit1;
                            newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;

                            newReflexivLong = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                            newReflexivLong |= (1L << 2 * firstPrefixLength); // add C marker

                            newReflexivLongArray[0] = newReflexivLong;
                        } else {
                            long transitBit1 = (Long) currentSubKmer.getSeq(0).apply(0) & maxPrefixLengthBinary;

                            newReflexivSubKmer[0] = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * firstPrefixLength;
                            newReflexivSubKmer[0] |= (((Long) currentSubKmer.getSeq(2).apply(0) & maxPrefixLengthBinary) << 2 * (31 - firstPrefixLength));
                            newReflexivSubKmer[0] &= maxSubKmerBinary;

                            for (int i = 1; i < param.subKmerBinarySlots - 1; i++) {
                                long transitBit2 = (Long) currentSubKmer.getSeq(0).apply(i) & maxPrefixLengthBinary;

                                newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * firstPrefixLength;
                                newReflexivSubKmer[i] |= (transitBit1 << 2 * (31 - firstPrefixLength));
                                newReflexivSubKmer[i] &= maxSubKmerBinary;

                                transitBit1 = transitBit2;
                            }

                            newReflexivSubKmer[param.subKmerBinarySlots - 1] = transitBit1 << 2 * (param.subKmerSizeResidue - firstPrefixLength);
                            newReflexivSubKmer[param.subKmerBinarySlots - 1] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) >>> 2 * firstPrefixLength);
                            newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;


                            newReflexivLong = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) & maxPrefixLengthBinary;
                            newReflexivLong |= (1L << 2 * firstPrefixLength); // add C marker
                            newReflexivLongArray[0] = newReflexivLong;
                        }
                    }

/*
                    for (int k = 30; k >= 0; k--) {
                        long a = newReflexivSubKmer[0] >> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    for (int k = 30; k >= 0; k--) {
                        long a = newReflexivSubKmer[1] >>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    if (newReflexivSubKmer.length >=3){
                        for (int k = 30; k >= 0; k--) {
                            long a = newReflexivSubKmer[2] >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    if (newReflexivSubKmer.length >=4){
                        for (int k = 16; k >= 0; k--) {
                            long a = newReflexivSubKmer[3] >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    System.out.print(" ");
                    for (int k = 30; k >= 0; k--) {
                        long a = newReflexivLongArray[0] >>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    if (newReflexivLongArray.length >=2){
                        for (int k = 30; k >= 0; k--) {
                            long a = newReflexivLongArray[1] >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    if (newReflexivLongArray.length >=3){
                        for (int k = 30; k >= 0; k--) {
                            long a = newReflexivLongArray[2] >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    if (newReflexivLongArray.length >=4){
                        for (int k = 30; k >= 0; k--) {
                            long a = newReflexivLongArray[3] >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }


                    System.out.println(" Double random extendable pairs" + randomReflexivMarker);
*/
                    reflexivKmerConcatList.add(
                            RowFactory.create(JavaConverters.asScalaIteratorConverter(Arrays.asList(newReflexivSubKmer).iterator()).asScala().toSeq() ,
                                    randomReflexivMarker, JavaConverters.asScalaIteratorConverter(Arrays.asList(newReflexivLongArray).iterator()).asScala().toSeq(), currentSubKmer.getInt(3), currentSubKmer.getInt(4)
                            )
                    );
                }

            }
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

        /**
         *
         * @param currentSubKmer
         */
        public void directKmerComparison(Row currentSubKmer) {
            tmpReflexivKmerExtendList.add(currentSubKmer);
        }

        /**
         *
         * @param S
         */
        public void resetSubKmerGroup(Row S) {
            if (lineMarker == 1) {
                lineMarker = 2;
            } else {
                lineMarker = 3; /* reset to new sub-kmer group */
            }
            /* re-reflex all single kmers in the sub-kmer group */
//            if (tmpReflexivKmerExtendList.size() != 0) {
//                for (int i = 0; i < tmpReflexivKmerExtendList.size(); i++) {
            //                   singleKmerRandomizer(tmpReflexivKmerExtendList.get(i));
            //               }
            //          }

            tmpReflexivKmerExtendList = new ArrayList<Row>();
            tmpReflexivKmerExtendList.add(S
                    //   RowFactory.create(S.getLong(0),
                    //                   S.getInt(1), S.get(2), S.getInt(3), S.getInt(4)
                    //   )
            );
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

    class DSFilterUnExtendableKmer implements MapPartitionsFunction<Row, Row>, Serializable {

        /* marker to identify similar SubKmers in the loop sequence */
        private int lineMarker = 1;

        /* 1 stands for forward sub-kmer */
        /* 2 stands for reflexiv sub-kmer */
        // private int randomReflexivMarker = ThreadLocalRandom.current().nextInt(1, 3);
        private int randomReflexivMarker = 2;




        long maxSubKmerResidueBinary = ~((~0L) << 2 * param.subKmerSizeResidue);
        long maxSubKmerBinary = ~((~0L) << 2 * 31);


        //long maxBlockBinary = ~((~0L) << 2*31); // a block has 31 nucleotide


        /* temporary capsule to store identical SubKmer units */
        List<Row> tmpReflexivKmerExtendList = new ArrayList<Row>();

        /* return capsule of extend Tuples for next iteration*/
        List<Row> reflexivKmerConcatList = new ArrayList<Row>();

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
        public Iterator<Row> call(Iterator<Row> sIterator) {

            if (param.scramble ==3){
                randomReflexivMarker =1;
            }

            randomReflexivMarker =1;

            while (sIterator.hasNext()) {
                Row s = sIterator.next();

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
                            if (subKmerSlotComparator(s.getSeq(0), tmpReflexivKmerExtendList.get(i).getSeq(0))) {
                                if (s.getInt(1) == 1) {
                                    if (tmpReflexivKmerExtendList.get(i).getInt(1) == 2) {
                                        int tmpReflexivKmerSuffixLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros((Long) tmpReflexivKmerExtendList.get(i).getSeq(2).apply(0)) / 2 + 1);
                                        int tmpBlockSize = (tmpReflexivKmerExtendList.get(i).getSeq(2).length() - 1) * 31 + tmpReflexivKmerSuffixLength;
                                        int currentReflexivKmerSuffixLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros((Long) s.getSeq(2).apply(0)) / 2 + 1);
                                        int currentBlockSize = (s.getSeq(2).length() - 1) * 31 + currentReflexivKmerSuffixLength;

                                        if (s.getInt(3) < 0 && tmpReflexivKmerExtendList.get(i).getInt(4) < 0) {
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else if (s.getInt(3) >= 0 && tmpReflexivKmerExtendList.get(i).getInt(4) >= 0) {
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else if (s.getInt(3) >= 0 && s.getInt(3) - tmpBlockSize >= 0) {
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else if (tmpReflexivKmerExtendList.get(i).getInt(4) >= 0 && tmpReflexivKmerExtendList.get(i).getInt(4) - currentBlockSize >= 0) {
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else {
                                            singleKmerRandomizer(tmpReflexivKmerExtendList.get(i));
                                            tmpReflexivKmerExtendList.remove(i);
                                            resetSubKmerGroup(s);
                                            break;
                                        }
                                    } else if (tmpReflexivKmerExtendList.get(i).getInt(1) == 1) {
                                        reflexivKmerConcatList.add(tmpReflexivKmerExtendList.get(i));
                                        tmpReflexivKmerExtendList.remove(i);
                                        resetSubKmerGroup(s);
                                        //directKmerComparison(s);
                                        break;
                                    }
                                } else { /* if (s.getInt(1) == 2) { */
                                    if (tmpReflexivKmerExtendList.get(i).getInt(1) == 2) {
                                        //directKmerComparison(s);
                                        singleKmerRandomizer(tmpReflexivKmerExtendList.get(i));
                                        tmpReflexivKmerExtendList.remove(i);
                                        resetSubKmerGroup(s);
                                        break;
                                    } else if (tmpReflexivKmerExtendList.get(i).getInt(1) == 1) {
                                        int tmpReflexivKmerSuffixLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros((Long) tmpReflexivKmerExtendList.get(i).getSeq(2).apply(0)) / 2 + 1);
                                        int tmpBlockSize = (tmpReflexivKmerExtendList.get(i).getSeq(2).length() - 1) * 31 + tmpReflexivKmerSuffixLength;
                                        int currentReflexivKmerSuffixLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros((Long) s.getSeq(2).apply(0)) / 2 + 1);
                                        int currentBlockSize = (s.getSeq(2).length() - 1) * 31 + currentReflexivKmerSuffixLength;
                                        if (s.getInt(4) < 0 && tmpReflexivKmerExtendList.get(i).getInt(3) < 0) {
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else if (s.getInt(4) >= 0 && tmpReflexivKmerExtendList.get(i).getInt(3) >= 0) {
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else if (s.getInt(4) >= 0 && s.getInt(4) - tmpBlockSize >= 0) {
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else if (tmpReflexivKmerExtendList.get(i).getInt(3) >= 0 && tmpReflexivKmerExtendList.get(i).getInt(3) - currentBlockSize >= 0) {
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else {
                                            reflexivKmerConcatList.add(tmpReflexivKmerExtendList.get(i));
                                            tmpReflexivKmerExtendList.remove(i);
                                            resetSubKmerGroup(s);
                                            break;
                                        }
                                    }
                                }
                            /* return reflexivKmerConcatList.iterator(); */
                            }

                        /* new Sub-kmer group section */
                            else { /* s.getLong(0) != tmpReflexivKmerExtendList.get(i).getLong(0)()*/
                                //  if (lineMarker == 2) { // lineMarker == 2 represents the second line of the partition
                                //     singleKmerRandomizer(tmpReflexivKmerExtendList.get(i));
                                // }
                                //  singleKmerRandomizer(s);

                                reflexivKmerConcatList.add(tmpReflexivKmerExtendList.get(i));
                                tmpReflexivKmerExtendList.remove(i);
                                resetSubKmerGroup(s);
                                break;
                            }
                        } /* end of the while loop */
                    }// end of else condition

                    lineMarker++;
                    // return reflexivKmerConcatList.iterator();
                }
            } // while loop

            if (tmpReflexivKmerExtendList.size() != 0) {
                for (int i = 0; i < tmpReflexivKmerExtendList.size(); i++) {
                       reflexivKmerConcatList.add(tmpReflexivKmerExtendList.get(i));
                }
            }

            return reflexivKmerConcatList.iterator();
        }


        public void singleKmerRandomizer(Row currentSubKmer) {
            int blockSize = currentSubKmer.getSeq(2).length();

            Long[] newReflexivLongArray = new Long[blockSize];

            if (currentSubKmer.getInt(1) == 1) {
                int firstSuffixBlockLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros((Long) currentSubKmer.getSeq(2).apply(0)) / 2 + 1);
                long maxSuffixLengthBinary = ~((~0L) << (2 * firstSuffixBlockLength));
                Long[] newReflexivSubKmer = new Long[param.subKmerBinarySlots];
                Long newReflexivLong;

                if (randomReflexivMarker == 2) {
                    if (blockSize > 1) {

                        if (firstSuffixBlockLength >= param.subKmerSizeResidue) {
                            if (blockSize >= param.subKmerBinarySlots) {
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] = ((Long) currentSubKmer.getSeq(2).apply(blockSize - 1) & maxSubKmerResidueBinary);

                                long transit1 = (Long) currentSubKmer.getSeq(2).apply(blockSize - 1) >>> (2 * param.subKmerSizeResidue);
                                long transit2 = 0L;
                                for (int i = param.subKmerBinarySlots - 2; i >= 0; i--) {
                                    int j = blockSize - param.subKmerBinarySlots + i; // index of suffix long array
                                    transit2 = (Long) currentSubKmer.getSeq(2).apply(j) >>> (2 * param.subKmerSizeResidue);

                                    newReflexivSubKmer[i] = transit1;
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(2).apply(j) << 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;

                                    transit1 = transit2;
                                }

                                for (int i = blockSize - param.subKmerBinarySlots; i > 0; i--) {
                                    int j = param.subKmerBinarySlots + i - 1; // index of the new prefix long array
                                    newReflexivLongArray[j] = ((Long) currentSubKmer.getSeq(2).apply(i) >>> 2 * param.subKmerSizeResidue);
                                    newReflexivLongArray[j] |= (Long) currentSubKmer.getSeq(2).apply(i - 1) << 2 * (31 - param.subKmerSizeResidue);
                                    newReflexivLongArray[j] &= maxSubKmerBinary;
                                }

                                //   if (blockSize== param.subKmerBinarySlots){ // in the context of blockSize >= param.subKmerBinarySlots
                                newReflexivLongArray[param.subKmerBinarySlots - 1] = ((Long) currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary) >>> (2 * param.subKmerSizeResidue);
                                newReflexivLongArray[param.subKmerBinarySlots - 1] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) << 2 * (firstSuffixBlockLength - param.subKmerSizeResidue));
                                newReflexivLongArray[param.subKmerBinarySlots - 1] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 2) << 2 * firstSuffixBlockLength);
                                newReflexivLongArray[param.subKmerBinarySlots - 1] &= maxSubKmerBinary;
                                //  }

                                for (int i = param.subKmerBinarySlots - 2; i > 0; i--) {
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * (31 - firstSuffixBlockLength);
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(0).apply(i - 1) << 2 * firstSuffixBlockLength);
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                newReflexivLongArray[0] = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * (31 - firstSuffixBlockLength);
                                newReflexivLongArray[0] |= (1L << 2 * firstSuffixBlockLength); // add C marker in the front
                            } else { // blockSize < param.subKmerSizeResidue
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] = ((Long) currentSubKmer.getSeq(2).apply(blockSize - 1) & maxSubKmerResidueBinary);

                                long transit1 = (Long) currentSubKmer.getSeq(2).apply(blockSize - 1) >>> (2 * param.subKmerSizeResidue);
                                long transit2 = 0L;
                                for (int i = param.subKmerBinarySlots - 2; i >= param.subKmerBinarySlots - blockSize; i--) {
                                    int j = blockSize - param.subKmerBinarySlots + i; // index of suffix long array
                                    transit2 = (Long) currentSubKmer.getSeq(2).apply(j) >>> (2 * param.subKmerSizeResidue);

                                    newReflexivSubKmer[i] = transit1;
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(2).apply(j) << 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;

                                    transit1 = transit2;
                                }

                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize - 1] = ((Long) currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary) >>> (2 * param.subKmerSizeResidue);
                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize - 1] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) << 2 * (firstSuffixBlockLength - param.subKmerSizeResidue));
                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize - 1] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 2) << 2 * firstSuffixBlockLength);
                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize - 1] &= maxSubKmerBinary;

                                for (int i = param.subKmerBinarySlots - blockSize - 2; i >= 0; i--) {
                                    int j = blockSize + i; // index of the subkmer

                                    newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(j) >>> 2 * (31 - firstSuffixBlockLength);
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(0).apply(j - 1) << 2 * firstSuffixBlockLength);
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;
                                }

                                for (int i = blockSize - 1; i > 0; i--) {
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * (31 - firstSuffixBlockLength);
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(0).apply(i - 1) << 2 * firstSuffixBlockLength);
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                newReflexivLongArray[0] = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * (31 - firstSuffixBlockLength);
                                newReflexivLongArray[0] |= (1L << 2 * firstSuffixBlockLength); // add C marker in the front
                            }
                        } else { // firstSuffixBlockLength < param.subKmerSizeResidue
                            if (blockSize >= param.subKmerBinarySlots) {
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] = ((Long) currentSubKmer.getSeq(2).apply(blockSize - 1) & maxSubKmerResidueBinary);

                                long transit1 = (Long) currentSubKmer.getSeq(2).apply(blockSize - 1) >>> (2 * param.subKmerSizeResidue);
                                long transit2 = 0L;
                                for (int i = param.subKmerBinarySlots - 2; i > 0; i--) {
                                    int j = blockSize - param.subKmerBinarySlots + i; // index of suffix long array
                                    transit2 = (Long) currentSubKmer.getSeq(2).apply(j) >>> (2 * param.subKmerSizeResidue);

                                    newReflexivSubKmer[i] = transit1;
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(2).apply(j) << 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;

                                    transit1 = transit2;
                                }

                                newReflexivSubKmer[0] = transit1;
                                if (blockSize > param.subKmerBinarySlots) {
                                    newReflexivSubKmer[0] |= ((Long) currentSubKmer.getSeq(2).apply(blockSize - param.subKmerBinarySlots) << 2 * (31 - param.subKmerSizeResidue));
                                }else { // (blockSize== param.subKmerBinarySlots){
                                    newReflexivSubKmer[0] |= (((Long) currentSubKmer.getSeq(2).apply(blockSize - param.subKmerBinarySlots) & maxSuffixLengthBinary) << 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivSubKmer[0] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots-1) << 2*(31-param.subKmerSizeResidue + firstSuffixBlockLength));
                                }
                                newReflexivSubKmer[0] &= maxSubKmerBinary;

                                //newReflexivSubKmer[0] = transit1;
                                //newReflexivSubKmer[0] |= ((Long) currentSubKmer.getSeq(2).apply(blockSize-param.subKmerBinarySlots) << 2*(31-param.subKmerSizeResidue));


                                for (int i = blockSize - param.subKmerBinarySlots; i > 1; i--) {
                                    int j = param.subKmerBinarySlots + i - 1; // index of the new prefix long array
                                    newReflexivLongArray[j] = ((Long) currentSubKmer.getSeq(2).apply(i) >>> 2 * param.subKmerSizeResidue);
                                    newReflexivLongArray[j] |= (Long) currentSubKmer.getSeq(2).apply(i - 1) << 2 * (31 - param.subKmerSizeResidue);
                                    newReflexivLongArray[j] &= maxSubKmerBinary;
                                }

                                if (param.subKmerBinarySlots < blockSize) {
                                    newReflexivLongArray[param.subKmerBinarySlots] = ((Long) currentSubKmer.getSeq(2).apply(1) >>> 2 * param.subKmerSizeResidue);
                                    newReflexivLongArray[param.subKmerBinarySlots] |= ((Long) currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary)<< 2 * (31 - param.subKmerSizeResidue);
                                    newReflexivLongArray[param.subKmerBinarySlots] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) << 2 * (31 + firstSuffixBlockLength - param.subKmerSizeResidue));
                                    newReflexivLongArray[param.subKmerBinarySlots] &= maxSubKmerBinary;
                                }

                                //   if (blockSize== param.subKmerBinarySlots){ // in the context of blockSize >= param.subKmerBinarySlots
                                //  newReflexivLongArray[param.subKmerBinarySlots-1] = ((Long) currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary) >>> (2 * param.subKmerSizeResidue);
                                newReflexivLongArray[param.subKmerBinarySlots - 1] = ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) >>> 2 * (param.subKmerSizeResidue - firstSuffixBlockLength));
                                newReflexivLongArray[param.subKmerBinarySlots - 1] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 2) << 2 * firstSuffixBlockLength);
                                newReflexivLongArray[param.subKmerBinarySlots - 1] &= maxSubKmerBinary;
                                //  }

                                for (int i = param.subKmerBinarySlots - 2; i > 0; i--) {
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * (31 - firstSuffixBlockLength);
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(0).apply(i - 1) << 2 * firstSuffixBlockLength);
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                newReflexivLongArray[0] = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * (31 - firstSuffixBlockLength);
                                newReflexivLongArray[0] |= (1L << 2 * firstSuffixBlockLength); // add C marker in the front
                            } else { // blockSize < param.subKmerSizeResidue
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] = ((Long) currentSubKmer.getSeq(2).apply(blockSize - 1) & maxSubKmerResidueBinary);

                                long transit1 = (Long) currentSubKmer.getSeq(2).apply(blockSize - 1) >>> (2 * param.subKmerSizeResidue);
                                long transit2 = 0L;
                                for (int i = param.subKmerBinarySlots - 2; i > param.subKmerBinarySlots - blockSize; i--) {
                                    int j = blockSize - param.subKmerBinarySlots + i; // index of suffix long array
                                    transit2 = (Long) currentSubKmer.getSeq(2).apply(j) >>> (2 * param.subKmerSizeResidue);

                                    newReflexivSubKmer[i] = transit1;
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(2).apply(j) << 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;

                                    transit1 = transit2;
                                }

                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize] = (Long) currentSubKmer.getSeq(2).apply(1) >>> 2 * param.subKmerSizeResidue;
                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize] |= ((Long) currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary) << 2 * (31 - param.subKmerSizeResidue);
                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) << 2 * (31 - param.subKmerSizeResidue + firstSuffixBlockLength));
                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize] &= maxSubKmerBinary;

                                // newReflexivSubKmer[param.subKmerBinarySlots-blockSize-1] = ((Long) currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary) >>> (2 * param.subKmerSizeResidue);
                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize - 1] = ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) >>> 2 * (param.subKmerSizeResidue - firstSuffixBlockLength));
                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize - 1] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 2) << 2 * firstSuffixBlockLength);
                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize - 1] &= maxSubKmerBinary;

                                for (int i = param.subKmerBinarySlots - blockSize - 2; i >= 0; i--) {
                                    int j = blockSize + i; // index of the subkmer

                                    newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(j) >>> 2 * (31 - firstSuffixBlockLength);
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(0).apply(j - 1) << 2 * firstSuffixBlockLength);
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;
                                }

                                for (int i = blockSize - 1; i > 0; i--) {
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * (31 - firstSuffixBlockLength);
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(0).apply(i - 1) << 2 * firstSuffixBlockLength);
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                newReflexivLongArray[0] = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * (31 - firstSuffixBlockLength);
                                newReflexivLongArray[0] |= (1L << 2 * firstSuffixBlockLength); // add C marker in the front
                            }
                        }


                    } else { // block size ==1
                        if (firstSuffixBlockLength > param.subKmerSizeResidue) { // firstSuffixBlockLength is longer than the length of the last block (element) of sub kmer long array
                            //long transitBit1 = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) << 2 * (firstSuffixBlockLength - param.subKmerSizeResidue);
                            long transitBit1 = (((Long)currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary) >>> 2 * param.subKmerSizeResidue);
                            long transitBit2 = 0L;

                            newReflexivSubKmer[param.subKmerBinarySlots - 1] = ((Long) currentSubKmer.getSeq(2).apply(0) & maxSubKmerResidueBinary);

                            newReflexivSubKmer[param.subKmerBinarySlots - 2] = ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots-1) << 2* (firstSuffixBlockLength-param.subKmerSizeResidue));
                            newReflexivSubKmer[param.subKmerBinarySlots - 2] |= transitBit1;
                            newReflexivSubKmer[param.subKmerBinarySlots - 2] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots-2) << 2*firstSuffixBlockLength);
                            newReflexivSubKmer[param.subKmerBinarySlots - 2] &= maxSubKmerBinary;
                            transitBit1 = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots-2) >>> 2 * (31 - firstSuffixBlockLength);

                            for (int i = param.subKmerBinarySlots - 3; i >= 0; i--) {
                                transitBit2 = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * (31 - firstSuffixBlockLength);

                                newReflexivSubKmer[i] = ((Long) currentSubKmer.getSeq(0).apply(i) << 2 * firstSuffixBlockLength);
                                newReflexivSubKmer[i] |= transitBit1;
                                newReflexivSubKmer[i] &= maxSubKmerBinary;

                                transitBit1 = transitBit2;
                            }

                            newReflexivLong = transitBit1;
                            newReflexivLong |= (1L << 2 * firstSuffixBlockLength);
                            newReflexivLongArray[0] = newReflexivLong;

                        } else if (firstSuffixBlockLength == param.subKmerSizeResidue) {
                            long transitBit1 = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                            long transitBit2 = 0L;

                            newReflexivSubKmer[param.subKmerBinarySlots - 1] = ((Long) currentSubKmer.getSeq(2).apply(0) & maxSubKmerResidueBinary);

                            for (int i = param.subKmerBinarySlots - 2; i >= 0; i--) {
                                transitBit2 = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * (31 - firstSuffixBlockLength);

                                newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(i) << 2 * firstSuffixBlockLength;
                                newReflexivSubKmer[i] |= transitBit1;
                                newReflexivSubKmer[i] &= maxSubKmerBinary;

                                transitBit1 = transitBit2;
                            }

                            newReflexivLong = transitBit2;
                            newReflexivLong |= (1L << 2 * firstSuffixBlockLength);
                            newReflexivLongArray[0] = newReflexivLong;

                        } else { //firstSuffixBlockLength < param.subKmerSizeResidue
                            long transitBit1 = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) >>> 2 * (param.subKmerSizeResidue - firstSuffixBlockLength);
                            long transitBit2 = 0L;

                            newReflexivSubKmer[param.subKmerBinarySlots - 1] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) << 2 * firstSuffixBlockLength;
                            newReflexivSubKmer[param.subKmerBinarySlots - 1] |= ((Long) currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary);
                            newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;

                            for (int i = param.subKmerBinarySlots - 2; i >= 0; i--) {
                                transitBit2 = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * (31 - firstSuffixBlockLength);

                                newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(i) << 2 * firstSuffixBlockLength;
                                newReflexivSubKmer[i] |= transitBit1;
                                newReflexivSubKmer[i] &= maxSubKmerBinary;

                                transitBit1 = transitBit2;
                            }

                            newReflexivLong = transitBit2;
                            newReflexivLong |= (1L << 2 * firstSuffixBlockLength); // add C marker
                            newReflexivLongArray[0] = newReflexivLong;
                        }
                    }

/*
                    for (int k = 30; k >= 0; k--) {
                        long a = newReflexivSubKmer[0] >> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    for (int k = 30; k >= 0; k--) {
                        long a = newReflexivSubKmer[1] >>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    if (newReflexivSubKmer.length >=3){
                        for (int k = 30; k >= 0; k--) {
                            long a = newReflexivSubKmer[2] >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    if (newReflexivSubKmer.length >=4){
                        for (int k = 16; k >= 0; k--) {
                            long a = newReflexivSubKmer[3] >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }


                    System.out.print(" ");
                    for (int k = 30; k >= 0; k--) {
                        long a = newReflexivLongArray[0] >>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    if (newReflexivLongArray.length >=2){
                        for (int k = 30; k >= 0; k--) {
                            long a = newReflexivLongArray[1] >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    if (newReflexivLongArray.length >=3){
                        for (int k = 30; k >= 0; k--) {
                            long a = newReflexivLongArray[2] >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    if (newReflexivLongArray.length >=4){
                        for (int k = 30; k >= 0; k--) {
                            long a = newReflexivLongArray[3] >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }


                    System.out.println(" Double random unextendable pairs" + randomReflexivMarker);
*/
                    reflexivKmerConcatList.add(
                            RowFactory.create(JavaConverters.asScalaIteratorConverter(Arrays.asList(newReflexivSubKmer).iterator()).asScala().toSeq(),
                                    randomReflexivMarker,  JavaConverters.asScalaIteratorConverter(Arrays.asList(newReflexivLongArray).iterator()).asScala().toSeq(), currentSubKmer.getInt(3), currentSubKmer.getInt(4)
                            )
                    );
                } else {

/*
                    for (int k = 30; k >= 0; k--) {
                        long a = (Long)currentSubKmer.getSeq(0).apply(0)>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    for (int k = 30; k >= 0; k--) {
                        long a = (Long)currentSubKmer.getSeq(0).apply(1)>>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    if (currentSubKmer.getSeq(0).length() >=3){
                        for (int k = 30; k >= 0; k--) {
                            long a = (Long)currentSubKmer.getSeq(0).apply(2)>>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    if (currentSubKmer.getSeq(0).length() >=4){
                        for (int k = 16; k >= 0; k--) {
                            long a = (Long)currentSubKmer.getSeq(0).apply(3)>>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }


                    System.out.print(" ");
                    for (int k = 30; k >= 0; k--) {
                        long a = (Long)currentSubKmer.getSeq(2).apply(0) >>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    if (newReflexivLongArray.length >=2){
                        for (int k = 30; k >= 0; k--) {
                            long a = (Long)currentSubKmer.getSeq(2).apply(1) >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    if (newReflexivLongArray.length >=3){
                        for (int k = 30; k >= 0; k--) {
                            long a = (Long)currentSubKmer.getSeq(2).apply(2) >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    if (newReflexivLongArray.length >=4){
                        for (int k = 30; k >= 0; k--) {
                            long a = (Long)currentSubKmer.getSeq(2).apply(3) >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }


                    System.out.println(" Double random same unextendable pairs" + randomReflexivMarker);
                    */
                    reflexivKmerConcatList.add(currentSubKmer);
                }
            } else { /* currentSubKmer.getInt(1) == 2 */
                int firstPrefixLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros((Long) currentSubKmer.getSeq(2).apply(0)) / 2 + 1);
                long maxPrefixLengthBinary = ~((~0L) << (2 * firstPrefixLength));
                Long[] newReflexivSubKmer = new Long[param.subKmerBinarySlots];
                long newReflexivLong = 0L;


                if (randomReflexivMarker == 2) {

/*
                    for (int k = 30; k >= 0; k--) {
                        long a = (Long)currentSubKmer.getSeq(0).apply(0)>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    for (int k = 30; k >= 0; k--) {
                        long a = (Long)currentSubKmer.getSeq(0).apply(1)>>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    if (currentSubKmer.getSeq(0).length() >=3){
                        for (int k = 30; k >= 0; k--) {
                            long a = (Long)currentSubKmer.getSeq(0).apply(2)>>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    if (currentSubKmer.getSeq(0).length() >=4){
                        for (int k = 16; k >= 0; k--) {
                            long a = (Long)currentSubKmer.getSeq(0).apply(3)>>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }


                    System.out.print(" ");
                    for (int k = 30; k >= 0; k--) {
                        long a = (Long)currentSubKmer.getSeq(2).apply(0) >>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    if (newReflexivLongArray.length >=2){
                        for (int k = 30; k >= 0; k--) {
                            long a = (Long)currentSubKmer.getSeq(2).apply(1) >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    if (newReflexivLongArray.length >=3){
                        for (int k = 30; k >= 0; k--) {
                            long a = (Long)currentSubKmer.getSeq(2).apply(2) >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    if (newReflexivLongArray.length >=4){
                        for (int k = 30; k >= 0; k--) {
                            long a = (Long)currentSubKmer.getSeq(2).apply(3) >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    System.out.println(" Double random same unextendable pairs" + randomReflexivMarker);
*/
                    reflexivKmerConcatList.add(currentSubKmer);
                } else { /* randomReflexivMarker == 1 */
                    if (blockSize > 1) {
                        if (firstPrefixLength >= param.subKmerSizeResidue) {
                            if (blockSize >= param.subKmerBinarySlots) {
                                for (int i = 0; i < param.subKmerBinarySlots - 1; i++) {
                                    newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(2).apply(i) << 2 * (31 - firstPrefixLength);
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(2).apply(i + 1) >>> 2 * firstPrefixLength);
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;
                                }

                                newReflexivSubKmer[param.subKmerBinarySlots - 1] = (Long) currentSubKmer.getSeq(2).apply(param.subKmerBinarySlots - 1) >>> 2 * (firstPrefixLength - param.subKmerSizeResidue);
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;

                                newReflexivLongArray[0] = (Long) currentSubKmer.getSeq(2).apply(param.subKmerBinarySlots - 1) << 2 * param.subKmerSizeResidue;
                                if (blockSize > param.subKmerBinarySlots) {
                                    newReflexivLongArray[0] |= ((Long) currentSubKmer.getSeq(2).apply(param.subKmerBinarySlots) >>> 2 * (31 - param.subKmerSizeResidue));
                                } else {
                                    newReflexivLongArray[0] |= ((Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * (31 - param.subKmerSizeResidue));
                                }
                                newReflexivLongArray[0] &= maxPrefixLengthBinary;
                                newReflexivLongArray[0] |= (1L << 2 * firstPrefixLength); // add C marker in the front

                                for (int i = 1; i < blockSize - param.subKmerBinarySlots; i++) {
                                    int j = param.subKmerBinarySlots + i - 1;
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(2).apply(j) << 2 * param.subKmerSizeResidue;
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(2).apply(j + 1) >>> 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                if (blockSize > param.subKmerBinarySlots) {
                                    newReflexivLongArray[blockSize - param.subKmerBinarySlots] = (Long) currentSubKmer.getSeq(2).apply(blockSize - 1) << 2 * param.subKmerSizeResidue;
                                    newReflexivLongArray[blockSize - param.subKmerBinarySlots] |= ((Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivLongArray[blockSize - param.subKmerBinarySlots] &= maxSubKmerBinary;
                                }

                                for (int i = blockSize - param.subKmerBinarySlots + 1; i < blockSize - 1; i++) {
                                    int j = i - blockSize + param.subKmerBinarySlots - 1; // index of subKmer
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(0).apply(j) << 2 * param.subKmerSizeResidue;
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(0).apply(j + 1) >>> 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                newReflexivLongArray[blockSize - 1] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 2) << 2 * param.subKmerSizeResidue;
                                newReflexivLongArray[blockSize - 1] |= (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                                newReflexivLongArray[blockSize - 1] &= maxSubKmerBinary;

                            } else { // blockSize < param.subKmerBinarySlots
                                for (int i = 0; i < blockSize - 1; i++) {
                                    newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(2).apply(i) << 2 * (31 - firstPrefixLength);
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(2).apply(i + 1) >>> 2 * firstPrefixLength);
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;
                                }

                                newReflexivSubKmer[blockSize - 1] = (Long) currentSubKmer.getSeq(2).apply(blockSize - 1) << 2 * (31 - firstPrefixLength);
                                newReflexivSubKmer[blockSize - 1] |= ((Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * firstPrefixLength);
                                newReflexivSubKmer[blockSize - 1] &= maxSubKmerBinary;

                                for (int i = blockSize; i < param.subKmerBinarySlots - 1; i++) {
                                    int j = i - blockSize; // index of subKmer
                                    newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(j) << 2 * (31 - firstPrefixLength);
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(0).apply(j + 1) >>> 2 * firstPrefixLength);
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;
                                }

                                newReflexivSubKmer[param.subKmerBinarySlots - 1] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - blockSize - 1) >>> 2 * (firstPrefixLength - param.subKmerSizeResidue);
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;

                                newReflexivLongArray[0] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - blockSize - 1) << 2 * param.subKmerSizeResidue;
                                newReflexivLongArray[0] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - blockSize) >>> 2 * (31 - param.subKmerSizeResidue));
                                newReflexivLongArray[0] &= maxPrefixLengthBinary;
                                newReflexivLongArray[0] |= (1L << 2 * firstPrefixLength);

                                for (int i = 1; i < blockSize - 1; i++) {
                                    int j = param.subKmerBinarySlots - blockSize - 1 + i;
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(0).apply(j) << 2 * param.subKmerSizeResidue;
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(0).apply(j + 1) >>> 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                newReflexivLongArray[blockSize - 1] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 2) << 2 * param.subKmerSizeResidue;
                                newReflexivLongArray[blockSize - 1] |= (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                                newReflexivLongArray[blockSize - 1] &= maxSubKmerBinary;
                            }
                        } else { // firstPrefixLength < param.subKmerSizeResidue
                            if (blockSize >= param.subKmerBinarySlots) {
                                newReflexivSubKmer[0] =((Long) currentSubKmer.getSeq(2).apply(0) & maxPrefixLengthBinary) << 2 * (31 - firstPrefixLength);
                                newReflexivSubKmer[0] |= ((Long) currentSubKmer.getSeq(2).apply(1) >>> 2 * firstPrefixLength);
                                newReflexivSubKmer[0] &= maxSubKmerBinary;

                                for (int i = 1; i < param.subKmerBinarySlots - 1; i++) {
                                    newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(2).apply(i) << 2 * (31 - firstPrefixLength);
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(2).apply(i + 1) >>> 2 * firstPrefixLength);
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;
                                }

                                newReflexivSubKmer[param.subKmerBinarySlots - 1] = (Long) currentSubKmer.getSeq(2).apply(param.subKmerBinarySlots - 1) << 2 * (param.subKmerSizeResidue - firstPrefixLength);
                                if (blockSize > param.subKmerBinarySlots) {
                                    newReflexivSubKmer[param.subKmerBinarySlots - 1] |= ((Long) currentSubKmer.getSeq(2).apply(param.subKmerBinarySlots) >>> 2 * (31 - param.subKmerSizeResidue + firstPrefixLength));
                                } else { // blockSize == param.subKmerBinarySlots
                                    newReflexivSubKmer[param.subKmerBinarySlots - 1] |= ((Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * (31 - param.subKmerSizeResidue + firstPrefixLength));
                                }
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;


                                if (blockSize > param.subKmerBinarySlots) {
                                    newReflexivLongArray[0] = (Long) currentSubKmer.getSeq(2).apply(param.subKmerBinarySlots) >>> 2 * (31 - param.subKmerSizeResidue);
                                } else { // blockSize == param.subKmerBinarySlots
                                    newReflexivLongArray[0] = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * (31 - param.subKmerSizeResidue);
                                }
                                newReflexivLongArray[0] &= maxPrefixLengthBinary;
                                newReflexivLongArray[0] |= (1L << 2 * firstPrefixLength);

                                for (int i = 1; i < blockSize - param.subKmerBinarySlots; i++) {
                                    int j = param.subKmerBinarySlots + i - 1;
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(2).apply(j) << 2 * param.subKmerSizeResidue;
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(2).apply(j + 1) >>> 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                if (blockSize > param.subKmerBinarySlots) {
                                    newReflexivLongArray[blockSize - param.subKmerBinarySlots] = (Long) currentSubKmer.getSeq(2).apply(blockSize - 1) << 2 * param.subKmerSizeResidue;
                                    newReflexivLongArray[blockSize - param.subKmerBinarySlots] |= ((Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivLongArray[blockSize - param.subKmerBinarySlots] &= maxSubKmerBinary;
                                }

                                for (int i = blockSize - param.subKmerBinarySlots + 1; i < blockSize - 1; i++) {
                                    int j = i - blockSize + param.subKmerBinarySlots - 1; // index of subKmer
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(0).apply(j) << 2 * param.subKmerSizeResidue;
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(0).apply(j + 1) >>> 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                newReflexivLongArray[blockSize - 1] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 2) << 2 * param.subKmerSizeResidue;
                                newReflexivLongArray[blockSize - 1] |= (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                                newReflexivLongArray[blockSize - 1] &= maxSubKmerBinary;


                            } else { // blockSize < param.subKmerBinarySlots
                                newReflexivSubKmer[0] = ( (Long) currentSubKmer.getSeq(2).apply(0) & maxPrefixLengthBinary) << 2 * (31 - firstPrefixLength);
                                newReflexivSubKmer[0] |= ((Long) currentSubKmer.getSeq(2).apply(1) >>> 2 * firstPrefixLength);
                                newReflexivSubKmer[0] &= maxSubKmerBinary;
                                for (int i = 1; i < blockSize - 1; i++) {
                                    newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(2).apply(i) << 2 * (31 - firstPrefixLength);
                                    newReflexivSubKmer[i] |=( (Long) currentSubKmer.getSeq(2).apply(i + 1) >>> 2 * firstPrefixLength);
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;
                                }

                                newReflexivSubKmer[blockSize - 1] = (Long) currentSubKmer.getSeq(2).apply(blockSize - 1) << 2 * (31 - firstPrefixLength);
                                newReflexivSubKmer[blockSize - 1] |= ((Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * firstPrefixLength);
                                newReflexivSubKmer[blockSize - 1] &= maxSubKmerBinary;

                                for (int i = blockSize; i < param.subKmerBinarySlots - 1; i++) {
                                    int j = i - blockSize; // index of subKmer
                                    newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(j) << 2 * (31 - firstPrefixLength);
                                    newReflexivSubKmer[i] |= ((Long) currentSubKmer.getSeq(0).apply(j + 1) >>> 2 * firstPrefixLength);
                                    newReflexivSubKmer[i] &= maxSubKmerBinary;
                                }

                                // newReflexivSubKmer[param.subKmerBinarySlots-1] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots-blockSize-1) >>> 2*(firstPrefixLength-param.subKmerSizeResidue);
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - blockSize - 1) << 2 * (param.subKmerSizeResidue - firstPrefixLength);
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - blockSize) >>> 2 * (31 - param.subKmerSizeResidue + firstPrefixLength));
                                newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;

                                // newReflexivLongArray[0] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots-blockSize) << 2*param.subKmerSizeResidue;
                                newReflexivLongArray[0] = ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - blockSize) >>> 2 * (31 - param.subKmerSizeResidue));
                                newReflexivLongArray[0] &= maxPrefixLengthBinary;
                                newReflexivLongArray[0] |= (1L << 2 * firstPrefixLength);

                                for (int i = 1; i < blockSize - 1; i++) {
                                    int j = param.subKmerBinarySlots - blockSize + i -1;
                                    newReflexivLongArray[i] = (Long) currentSubKmer.getSeq(0).apply(j) << 2 * param.subKmerSizeResidue;
                                    newReflexivLongArray[i] |= ((Long) currentSubKmer.getSeq(0).apply(j + 1) >>> 2 * (31 - param.subKmerSizeResidue));
                                    newReflexivLongArray[i] &= maxSubKmerBinary;
                                }

                                newReflexivLongArray[blockSize - 1] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 2) << 2 * param.subKmerSizeResidue;
                                newReflexivLongArray[blockSize - 1] |= (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                                newReflexivLongArray[blockSize - 1] &= maxSubKmerBinary;
                            }
                        }

                    } else { /* blockSize == 1*/
                        if (firstPrefixLength > param.subKmerSizeResidue) {
                            long transitBit1 = (Long) currentSubKmer.getSeq(0).apply(0) & maxPrefixLengthBinary;

                            newReflexivSubKmer[0] = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * firstPrefixLength;
                            newReflexivSubKmer[0] |= (((Long) currentSubKmer.getSeq(2).apply(0) & maxPrefixLengthBinary) << 2 * (31 - firstPrefixLength));
                            newReflexivSubKmer[0] &= maxSubKmerBinary;

                            for (int i = 1; i < param.subKmerBinarySlots - 1; i++) {
                                long transitBit2 = (Long) currentSubKmer.getSeq(0).apply(i) & maxPrefixLengthBinary;

                                newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * firstPrefixLength;
                                newReflexivSubKmer[i] |= (transitBit1 << 2 * (31 - firstPrefixLength));
                                newReflexivSubKmer[i] &= maxSubKmerBinary;

                                transitBit1 = transitBit2;
                            }

                            newReflexivSubKmer[param.subKmerBinarySlots - 1] = transitBit1 >>> 2*(firstPrefixLength- param.subKmerSizeResidue);
                            newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;

                            newReflexivLong = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                            newReflexivLong |= (transitBit1 << 2 * param.subKmerSizeResidue);
                            newReflexivLong &= maxPrefixLengthBinary;
                            newReflexivLong |= (1L << 2 * firstPrefixLength); // add C marker in the front

                            newReflexivLongArray[0] = newReflexivLong;
                        } else if (firstPrefixLength == param.subKmerSizeResidue) {
                            long transitBit1 = (Long) currentSubKmer.getSeq(0).apply(0) & maxPrefixLengthBinary;

                            newReflexivSubKmer[0] = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * firstPrefixLength;
                            newReflexivSubKmer[0] |= (((Long) currentSubKmer.getSeq(2).apply(0) & maxPrefixLengthBinary) << 2 * (31 - firstPrefixLength));
                            newReflexivSubKmer[0] &= maxSubKmerBinary;

                            for (int i = 1; i < param.subKmerBinarySlots - 1; i++) {
                                long transitBit2 = (Long) currentSubKmer.getSeq(0).apply(i) & maxPrefixLengthBinary;

                                newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * firstPrefixLength;
                                newReflexivSubKmer[i] |= (transitBit1 << 2 * (31 - firstPrefixLength));
                                newReflexivSubKmer[i] &= maxSubKmerBinary;

                                transitBit1 = transitBit2;
                            }

                            newReflexivSubKmer[param.subKmerBinarySlots - 1] = transitBit1;
                            newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;

                            newReflexivLong = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                            newReflexivLong |= (1L << 2 * firstPrefixLength); // add C marker

                            newReflexivLongArray[0] = newReflexivLong;
                        } else {
                            long transitBit1 = (Long) currentSubKmer.getSeq(0).apply(0) & maxPrefixLengthBinary;

                            newReflexivSubKmer[0] = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * firstPrefixLength;
                            newReflexivSubKmer[0] |= (((Long) currentSubKmer.getSeq(2).apply(0) & maxPrefixLengthBinary) << 2 * (31 - firstPrefixLength));
                            newReflexivSubKmer[0] &= maxSubKmerBinary;

                            for (int i = 1; i < param.subKmerBinarySlots - 1; i++) {
                                long transitBit2 = (Long) currentSubKmer.getSeq(0).apply(i) & maxPrefixLengthBinary;

                                newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * firstPrefixLength;
                                newReflexivSubKmer[i] |= (transitBit1 << 2 * (31 - firstPrefixLength));
                                newReflexivSubKmer[i] &= maxSubKmerBinary;

                                transitBit1 = transitBit2;
                            }

                            newReflexivSubKmer[param.subKmerBinarySlots - 1] = transitBit1 << 2 * (param.subKmerSizeResidue - firstPrefixLength);
                            newReflexivSubKmer[param.subKmerBinarySlots - 1] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) >>> 2 * firstPrefixLength);
                            newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;


                            newReflexivLong = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) & maxPrefixLengthBinary;
                            newReflexivLong |= (1L << 2 * firstPrefixLength); // add C marker
                            newReflexivLongArray[0] = newReflexivLong;
                        }
                    }

/*
                    for (int k = 30; k >= 0; k--) {
                        long a = newReflexivSubKmer[0] >> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    for (int k = 30; k >= 0; k--) {
                        long a = newReflexivSubKmer[1] >>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    if (newReflexivSubKmer.length >=3){
                        for (int k = 30; k >= 0; k--) {
                            long a = newReflexivSubKmer[2] >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    if (newReflexivSubKmer.length >=4){
                        for (int k = 16; k >= 0; k--) {
                            long a = newReflexivSubKmer[3] >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    System.out.print(" ");
                    for (int k = 30; k >= 0; k--) {
                        long a = newReflexivLongArray[0] >>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    if (newReflexivLongArray.length >=2){
                        for (int k = 30; k >= 0; k--) {
                            long a = newReflexivLongArray[1] >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    if (newReflexivLongArray.length >=3){
                        for (int k = 30; k >= 0; k--) {
                            long a = newReflexivLongArray[2] >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    if (newReflexivLongArray.length >=4){
                        for (int k = 30; k >= 0; k--) {
                            long a = newReflexivLongArray[3] >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }


                    System.out.println(" Double random unextendable pairs" + randomReflexivMarker);
*/
                    reflexivKmerConcatList.add(
                            RowFactory.create(JavaConverters.asScalaIteratorConverter(Arrays.asList(newReflexivSubKmer).iterator()).asScala().toSeq() ,
                                    randomReflexivMarker, JavaConverters.asScalaIteratorConverter(Arrays.asList(newReflexivLongArray).iterator()).asScala().toSeq(), currentSubKmer.getInt(3), currentSubKmer.getInt(4)
                            )
                    );
                }

            }
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


        /**
         *
         * @param currentSubKmer
         */
        public void directKmerComparison(Row currentSubKmer) {
            tmpReflexivKmerExtendList.add(currentSubKmer);
        }

        /**
         *
         * @param S
         */
        public void resetSubKmerGroup(Row S) {
            if (lineMarker == 1) {
                lineMarker = 2;
            } else {
                lineMarker = 3; /* reset to new sub-kmer group */
            }
            /* re-reflex all single kmers in the sub-kmer group */
//            if (tmpReflexivKmerExtendList.size() != 0) {
//                for (int i = 0; i < tmpReflexivKmerExtendList.size(); i++) {
            //                   singleKmerRandomizer(tmpReflexivKmerExtendList.get(i));
            //               }
            //          }

            tmpReflexivKmerExtendList = new ArrayList<Row>();
            tmpReflexivKmerExtendList.add(S
                    //   RowFactory.create(S.getLong(0),
                    //                   S.getInt(1), S.get(2), S.getInt(3), S.getInt(4)
                    //   )
            );
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

    class DSExtendReflexivKmerToArrayLoop implements MapPartitionsFunction<Row, Row>, Serializable {

        /* marker to identify similar SubKmers in the loop sequence */
        private int lineMarker = 1;

        /* 1 stands for forward sub-kmer */
        /* 2 stands for reflexiv sub-kmer */
        // private int randomReflexivMarker = ThreadLocalRandom.current().nextInt(1, 3);
        private int randomReflexivMarker = 2;




        long maxSubKmerResidueBinary = ~((~0L) << 2 * param.subKmerSizeResidue);
        long maxSubKmerBinary = ~((~0L) << 2 * 31);


        //long maxBlockBinary = ~((~0L) << 2*31); // a block has 31 nucleotide


        /* temporary capsule to store identical SubKmer units */
        List<Row> tmpReflexivKmerExtendList = new ArrayList<Row>();

        /* return capsule of extend Tuples for next iteration*/
        List<Row> reflexivKmerConcatList = new ArrayList<Row>();

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
        public Iterator<Row> call(Iterator<Row> sIterator) throws Exception {

            if (param.scramble ==3){
                randomReflexivMarker =1;
            }

            while (sIterator.hasNext()) {
                Row s = sIterator.next();

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
                            if (subKmerSlotComparator(s.getSeq(0), tmpReflexivKmerExtendList.get(i).getSeq(0)) || dynamicSubKmerComparator(s.getSeq(0), tmpReflexivKmerExtendList.get(i).getSeq(0))) {
                                if (s.getInt(1) == 1) {
                                    if (tmpReflexivKmerExtendList.get(i).getInt(1) == 2) {
                                        int tmpReflexivKmerSuffixLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros((Long) tmpReflexivKmerExtendList.get(i).getSeq(2).apply(0)) / 2 + 1);
                                        int tmpBlockSize = (tmpReflexivKmerExtendList.get(i).getSeq(2).length() - 1) * 31 + tmpReflexivKmerSuffixLength;
                                        int currentReflexivKmerSuffixLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros((Long) s.getSeq(2).apply(0)) / 2 + 1);
                                        int currentBlockSize = (s.getSeq(2).length() - 1) * 31 + currentReflexivKmerSuffixLength;

                                        if (s.getInt(3) < 0 && tmpReflexivKmerExtendList.get(i).getInt(4) < 0) {
                                            reflexivExtend(s, tmpReflexivKmerExtendList.get(i), -1);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else if (s.getInt(3) >= 0 && tmpReflexivKmerExtendList.get(i).getInt(4) >= 0) {
                                            reflexivExtend(s, tmpReflexivKmerExtendList.get(i), -1);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else if (s.getInt(3) >= 0 && s.getInt(3) - tmpBlockSize >= 0) {
                                            reflexivExtend(s, tmpReflexivKmerExtendList.get(i), s.getInt(3) - tmpBlockSize);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else if (tmpReflexivKmerExtendList.get(i).getInt(4) >= 0 && tmpReflexivKmerExtendList.get(i).getInt(4) - currentBlockSize >= 0) {
                                            reflexivExtend(s, tmpReflexivKmerExtendList.get(i), tmpReflexivKmerExtendList.get(i).getInt(4) - currentBlockSize);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else {
                                            singleKmerRandomizer(s);
                                            break;
                                        }
                                    } else if (tmpReflexivKmerExtendList.get(i).getInt(1) == 1) {
                                        singleKmerRandomizer(s);
                                        //directKmerComparison(s);
                                        break;
                                    }
                                } else { /* if (s.getInt(1) == 2) { */
                                    if (tmpReflexivKmerExtendList.get(i).getInt(1) == 2) {
                                        singleKmerRandomizer(s);
                                        //directKmerComparison(s);
                                        break;
                                    } else if (tmpReflexivKmerExtendList.get(i).getInt(1) == 1) {
                                        int tmpReflexivKmerSuffixLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros((Long) tmpReflexivKmerExtendList.get(i).getSeq(2).apply(0)) / 2 + 1);
                                        int tmpBlockSize = (tmpReflexivKmerExtendList.get(i).getSeq(2).length() - 1) * 31 + tmpReflexivKmerSuffixLength;
                                        int currentReflexivKmerSuffixLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros((Long) s.getSeq(2).apply(0)) / 2 + 1);
                                        int currentBlockSize = (s.getSeq(2).length() - 1) * 31 + currentReflexivKmerSuffixLength;
                                        if (s.getInt(4) < 0 && tmpReflexivKmerExtendList.get(i).getInt(3) < 0) {
                                            reflexivExtend(tmpReflexivKmerExtendList.get(i), s, -1);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else if (s.getInt(4) >= 0 && tmpReflexivKmerExtendList.get(i).getInt(3) >= 0) {
                                            reflexivExtend(tmpReflexivKmerExtendList.get(i), s, -1);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else if (s.getInt(4) >= 0 && s.getInt(4) - tmpBlockSize >= 0) {
                                            reflexivExtend(tmpReflexivKmerExtendList.get(i), s, s.getInt(4) - tmpBlockSize);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else if (tmpReflexivKmerExtendList.get(i).getInt(3) >= 0 && tmpReflexivKmerExtendList.get(i).getInt(3) - currentBlockSize >= 0) {
                                            reflexivExtend(tmpReflexivKmerExtendList.get(i), s, tmpReflexivKmerExtendList.get(i).getInt(3) - currentBlockSize);
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

                            //else if (dynamicSubKmerComparator(s.getSeq(0), tmpReflexivKmerExtendList.get(i).getSeq(0))){

                           // }

                        /* new Sub-kmer group section */
                            else { /* s.getLong(0) != tmpReflexivKmerExtendList.get(i).getLong(0)()*/
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
        public void singleKmerRandomizer(Row currentSubKmer) throws Exception {

            if (getReflexivMarker(currentSubKmer.getLong(1)) == 1) {
                /**
                 * 00000000000000110010111010010   Long.SIZE
                 * --------------C-G-G-G-T-C-A-G   Long.SIZE - (Long.numberOfLeadingZeros / 2 + 1)
                 * --------------^-Length marker
                 */
                int currentSuffixLength =  currentKmerSizeFromBinaryBlockArray(currentSubKmer.<long[]>getAs(2));  // Long.SIZE / 2 - (Long.numberOfTrailingZeros(currentSubKmer.getLong(2)) / 2 + 1); // xx01-------
                long[] newReflexivSubKmer; //= new long[param.subKmerBinarySlots];
                // long newReflexivLong;

                if (randomReflexivMarker == 2) {

                    long[] currentSuffixArray = currentSubKmer.getAs(2);
                    long[] combinedKmerArray = combineTwoLongBlocks((long[])currentSubKmer.get(0), currentSuffixArray);

                    newReflexivSubKmer = leftShiftArray(combinedKmerArray, currentSuffixLength);

                    long[] newReflexivLongArray = leftShiftOutFromArray(combinedKmerArray, currentSuffixLength);

                    long attribute = onlyChangeReflexivMarker(currentSubKmer.getLong(1), randomReflexivMarker);

                    reflexivKmerConcatList.add(
                            RowFactory.create(newReflexivSubKmer, attribute, newReflexivLongArray)
                    );

                } else {
                    reflexivKmerConcatList.add(currentSubKmer);
                }
            } else { /* currentSubKmer._2._1() == 2 */
                long[] newReflexivSubKmer; // = new long[param.subKmerBinarySlots];

                int currentSubKmerSize= currentKmerSizeFromBinaryBlockArray((long[])currentSubKmer.get(0));

                if (randomReflexivMarker == 2) {
                    reflexivKmerConcatList.add(currentSubKmer);
                } else { /* randomReflexivMarker == 1 */

                    long[] currentPrefixArray = currentSubKmer.getAs(2);
                    long[] combinedKmerArray = combineTwoLongBlocks(currentPrefixArray, (long[])currentSubKmer.get(0));

                    newReflexivSubKmer= leftShiftOutFromArray(combinedKmerArray, currentSubKmerSize);
                    long[] newReflexivLongArray= leftShiftArray(combinedKmerArray, currentSubKmerSize);

                    long attribute = onlyChangeReflexivMarker(currentSubKmer.getLong(1), randomReflexivMarker);


                    reflexivKmerConcatList.add(
                            RowFactory.create(newReflexivSubKmer, attribute, newReflexivLongArray)
                    );
                }

            }

            /* an action of randomization */

            if (randomReflexivMarker == 1) {
                randomReflexivMarker = 2;
            } else { /* randomReflexivMarker == 2 */
                randomReflexivMarker = 1;
            }
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

        private boolean dynamicSubKmerComparator(Seq a, Seq b) throws Exception {
            long[] arrayA = ArrayUtils.toPrimitive((Long[]) JavaConverters.seqAsJavaList(a).toArray());
            long[] arrayB = ArrayUtils.toPrimitive((Long[]) JavaConverters.seqAsJavaList(b).toArray());

            int aLength= currentKmerSizeFromBinaryBlockArray(arrayA);
            int bLength= currentKmerSizeFromBinaryBlockArray(arrayB);

            if (aLength>=bLength){ // equal should not happen
                long[] shorterVersion = leftShiftOutFromArray(arrayA, bLength);
                if (Arrays.equals(shorterVersion, arrayB)){
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

        /**
         *
         * @param currentSubKmer
         */
        public void directKmerComparison(Row currentSubKmer) {
            tmpReflexivKmerExtendList.add(currentSubKmer);
        }

        /**
         *
         * @param forwardSubKmer
         * @param reflexedSubKmer
         */

        public void reflexivExtend(Row forwardSubKmer, Row reflexedSubKmer, int bubbleDistance) throws Exception {

             /* forward   ATCGATCG, 1, ------ */
             /* reflexed  ------, 2, ATCGATCG */

            int forwardSuffixLength = Long.SIZE / 2 - (Long.numberOfTrailingZeros(forwardSubKmer.getLong(2)) / 2 + 1);
            int forwardSubKmerLength = currentKmerSizeFromBinaryBlockArray((long[])forwardSubKmer.get(0));

            int reflexedPrefixLength = Long.SIZE / 2 - (Long.numberOfTrailingZeros(reflexedSubKmer.getLong(2)) / 2 + 1);
            int reflexedSubKmerLength = currentKmerSizeFromBinaryBlockArray((long[])reflexedSubKmer.get(0));

            int newSubKmerLength;
            long[] longerSubKmer;
            if (forwardSubKmerLength >= reflexedSubKmerLength){ // In reality, it is always forwardSubKmer longer than or equal to reflexedSubKmer
                newSubKmerLength=forwardSubKmerLength;
                longerSubKmer= (long[])forwardSubKmer.get(0);
            }else{
                newSubKmerLength=reflexedSubKmerLength;
                longerSubKmer=(long[])reflexedSubKmer.get(0);
            }

            long[] reflexedPrefixArray = reflexedSubKmer.getAs(2);
            long[] forwardSuffixArray = forwardSubKmer.getAs(2);


            if (randomReflexivMarker == 2) {

                long[] newReflexivSubKmer = combineTwoLongBlocks(longerSubKmer, forwardSuffixArray); // xxxxx xxxxx xxx-- + xxx--- = xxxxx xxxxx xxxxx x----
                long[] newReflexivLongArray= leftShiftOutFromArray(newReflexivSubKmer, forwardSuffixLength); // xxx--  | ---xx xxxxx xxxxx x----

                newReflexivSubKmer = leftShiftArray(newReflexivSubKmer, forwardSuffixLength); // xxxxx xxxxx xxx---
                newReflexivLongArray = combineTwoLongBlocks(reflexedPrefixArray, newReflexivLongArray); // xx--- + xxx--

                if (bubbleDistance < 0) {
                    long attribute = buildingAlongFromThreeInt(randomReflexivMarker, getLeftMarker(reflexedSubKmer.getLong(1)), getRightMarker(forwardSubKmer.getLong(1)));
                    reflexivKmerConcatList.add(
                            RowFactory.create(newReflexivSubKmer,
                                    attribute, newReflexivLongArray
                            )
                    );
                } else {
                    if (getLeftMarker(forwardSubKmer.getLong(1)) > 0) {
                        long attribute = buildingAlongFromThreeInt(randomReflexivMarker, bubbleDistance, getRightMarker(forwardSubKmer.getLong(1)));
                        reflexivKmerConcatList.add(
                                RowFactory.create(newReflexivSubKmer,
                                        attribute, newReflexivLongArray
                                )
                        );
                    } else {
                        long attribute = buildingAlongFromThreeInt(randomReflexivMarker, getLeftMarker(reflexedSubKmer.getLong(1)), bubbleDistance);
                        reflexivKmerConcatList.add(
                                RowFactory.create(newReflexivSubKmer,
                                        attribute, newReflexivLongArray
                                )
                        );
                    }
                }

                randomReflexivMarker = 1; /* an action of randomization */
            } else { /* randomReflexivMarker == 1 */

                long[] newForwardSubKmer = combineTwoLongBlocks(reflexedPrefixArray, longerSubKmer); // xx--- + xxxxx xxxxx xx--- = xxxxx xxxxx xxxx-
                long[] newForwardLongArray = leftShiftArray(newForwardSubKmer, newSubKmerLength);  // xxxxx xxxxx xxxx-  -> xx--

                newForwardSubKmer = leftShiftOutFromArray(newForwardSubKmer, newSubKmerLength); // xxxxx xxxxx xxxx- -> xxxxx xxxxx xx---|xx-
                newForwardLongArray = combineTwoLongBlocks(newForwardLongArray, forwardSuffixArray); // xx-- + xxx-- -> xxxxx

                if (bubbleDistance < 0) {
                    long attribute = buildingAlongFromThreeInt(randomReflexivMarker, getLeftMarker(reflexedSubKmer.getLong(1)), getRightMarker(forwardSubKmer.getLong(1)));
                    reflexivKmerConcatList.add(
                            RowFactory.create(newForwardSubKmer,
                                    attribute, newForwardLongArray
                            )
                    );
                } else {

                    if (getLeftMarker(forwardSubKmer.getLong(1)) > 0) {
                        long attribute = buildingAlongFromThreeInt(randomReflexivMarker,bubbleDistance,getRightMarker(forwardSubKmer.getLong(1)));
                        reflexivKmerConcatList.add(
                                RowFactory.create(newForwardSubKmer,
                                        attribute, newForwardLongArray
                                )
                        );
                    } else { // reflexedSubKmer.getInt(4) >0
                        long attribute = buildingAlongFromThreeInt(randomReflexivMarker,getLeftMarker(reflexedSubKmer.getLong(1)), bubbleDistance);
                        reflexivKmerConcatList.add(
                                RowFactory.create(newForwardSubKmer,
                                        attribute, newForwardLongArray
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
        public void resetSubKmerGroup(Row S) {
            if (lineMarker == 1) {
                lineMarker = 2;
            } else {
                lineMarker = 3; /* reset to new sub-kmer group */
            }
            /* re-reflex all single kmers in the sub-kmer group */
//            if (tmpReflexivKmerExtendList.size() != 0) {
//                for (int i = 0; i < tmpReflexivKmerExtendList.size(); i++) {
            //                   singleKmerRandomizer(tmpReflexivKmerExtendList.get(i));
            //               }
            //          }

            tmpReflexivKmerExtendList = new ArrayList<Row>();
            tmpReflexivKmerExtendList.add(S
                    //   RowFactory.create(S.getLong(0),
                    //                   S.getInt(1), S.get(2), S.getInt(3), S.getInt(4)
                    //   )
            );
        }

        private long[] leftShiftArray(long[] blocks, int shiftingLength) throws Exception {
            int startingBlockIndex = (shiftingLength-1)/31;
            int nucleotideLength = currentKmerSizeFromBinaryBlockArray(blocks);
            int residueLength = Long.SIZE / 2 - (Long.numberOfTrailingZeros(blocks[blocks.length-1])/2+1); // last block length

            long[] newBlock = new long[(nucleotideLength-shiftingLength)/31+1];
            int relativeShiftSize = shiftingLength % 31;

            if (shiftingLength > nucleotideLength){
                throw new Exception("shifting length longer than the kmer length");
            }

            // if (relativeShiftSize ==0) then only shifting blocks

            int j=0; // new index for shifted blocks
            //           long oldShiftOut=0L; // if only one block, then 0 bits
//            if (blocks.length-(startingBlockIndex+1) >=1) { // more than one block, newBlock.length = blocks.length-startingBlockIndex
//                oldShiftOut = blocks[startingBlockIndex + 1] >>> 2 * (32 - relativeShiftSize);
            //           }
            for (int i=startingBlockIndex; i<blocks.length-1; i++){ // without the last block
                long shiftOut = blocks[i+1] >>> 2*(32-relativeShiftSize); // ooooxxxxxxx -> -------oooo  o=shift out x=needs to be left shifted
                newBlock[j]= blocks[i] << 2*relativeShiftSize; // 00000xxxxx -> xxxxx-----
                newBlock[j] |= shiftOut;
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
                throw new Exception("shifting length longer than the kmer length");
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



            if (leftVacancy ==0){ // left last block is a perfect block
                for (int i =0; i<leftBlocks.length; i++){
                    newBlocks[i]=leftBlocks[i];
                }

                newBlocks[leftBlocks.length-1] &= (~0L<<2); // remove the last block's C marker

                for (int j=leftBlocks.length;j<combinedBlockSize;j++){
                    newBlocks[j]=rightBlocks[j];
                }
            }else{
                long[] shiftOutBlocks = leftShiftOutFromArray(rightBlocks, leftVacancy); // right shift out for the left. here we only expect one block, because leftVacancy is relative to one block
                for (int i =0; i<leftBlocks.length; i++){
                    newBlocks[i]=leftBlocks[i];
                }

                newBlocks[leftBlocks.length-1] &= (~0L<<2*(leftVacancy+1)); // leftVacancy = 32-leftRelativeNTLength-1. This is to remove the C marker
                newBlocks[leftBlocks.length-1] |= (shiftOutBlocks[0]>>> 2*(leftRelativeNTLength));
                newBlocks[leftBlocks.length-1] &= (~0L<<2); // remove shift out blockls C marker

                long[] rightBlocksLeftShifted = leftShiftArray(rightBlocks, leftVacancy);

                int k=0; // rightBlocksLeftShifted index
                for (int j=leftBlocks.length;j<combinedBlockSize;j++){ // including the last blocks.
                    newBlocks[j]=rightBlocksLeftShifted[k];
                    k++;
                }

            }
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

        private int getReflexivMarker(long attribute){
            int reflexivMarker = (int) (attribute >>> 2*(32-1)); // 01-------- -> ---------01 reflexiv marker
            return reflexivMarker;
        }

        private int getLeftMarker(long attribute){
            int leftMarker = (int) (attribute >>> 2*(16)); // 01--xxxx-----xxxx -> 01--xxxx shift out right marker
            int leftMarkerBinaryBits= ~(3 << 31) ; // ---------11 -> 11---------- -> 0011111111111
            leftMarker &= leftMarkerBinaryBits; // remove reflexivMarker
            return leftMarker;
        }

        private int getRightMarker(long attribute){
            int rightMarker = (int) attribute;
            return rightMarker;
        }

        private long buildingAlongFromThreeInt(int ReflexivMarker, int leftCover, int rightCover){
            long info = (long) ReflexivMarker <<2*(32-1);  //move to the left most
            info |= ((long) leftCover << 32) ; // move one integer (32 bits) to the left
            info |= ((long) rightCover); //  01--LeftCover---RightCover

            return info;
        }

        /**
         *
         */
        public void tmpKmerRandomizer() throws Exception {
            if (tmpReflexivKmerExtendList.size() != 0) {
                for (int i = 0; i < tmpReflexivKmerExtendList.size(); i++) {
                    singleKmerRandomizer(tmpReflexivKmerExtendList.get(i));
                }
            }
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
     *
     */


    class DSExtendReflexivKmerToArrayFirstTime implements MapPartitionsFunction<Row, Row>, Serializable {

        /* marker to identify similar SubKmers in the loop sequence */
        private int lineMarker = 1;

        /* 1 stands for forward sub-kmer */
        /* 2 stands for reflexiv sub-kmer */
        //       private int randomReflexivMarker = ThreadLocalRandom.current().nextInt(1, 3);
        private int randomReflexivMarker = 2;

        long maxSubKmerResidueBinary = ~((~0L) << 2 * param.subKmerSizeResidue);
        long maxSubKmerBinary = ~((~0L) << 2 * 31);


        /* temporary capsule to store identical SubKmer units */
        List<Row> tmpReflexivKmerExtendList = new ArrayList<Row>();

        /* return capsule of extend Tuples for next iteration*/
        List<Row> reflexivKmerConcatList = new ArrayList<Row>();

        /**
         *
         * @param sIterator is the input data structure Tuple2<SubKmer, Tuple2<Marker, TheRestSequence>>
         *          s._1 represents sub kmer sequence
         *          s._2.getLong(0) represents sub kmer marker: 1, for forward sub kmer;
         *                                              2, for reverse (reflexiv) sub kmer;
         *          s._2._2 represents the rest sequence.
         *          s._2._2 represents the coverage of the K-mer
         * @return a list of extended Tuples for next iteration
         */
        public Iterator<Row> call(Iterator<Row> sIterator) throws Exception {

            while (sIterator.hasNext()) {
                Row s = sIterator.next();


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
                    //      reflexivKmerConcatList = new ArrayList<Row>();

                    if (tmpReflexivKmerExtendList.size() == 0) {
                        directKmerComparison(s);
                    } else { /* tmpReflexivKmerExtendList.size() != 0 */
                        for (int i = 0; i < tmpReflexivKmerExtendList.size(); i++) { // the tmpReflexivKmerExtendList is changing dynamically
                            if (subKmerSlotComparator(s.getSeq(0), tmpReflexivKmerExtendList.get(i).getSeq(0))|| dynamicSubKmerComparator(s.getSeq(0), tmpReflexivKmerExtendList.get(i).getSeq(0))) {
                                if (getReflexivMarker(s.getInt(1)) == 1) {
                                    if (getReflexivMarker(tmpReflexivKmerExtendList.get(i).getLong(1)) == 2) {
                                        int tmpReflexivKmerSuffixLength = Long.SIZE / 2 - (Long.numberOfTrailingZeros(tmpReflexivKmerExtendList.get(i).getLong(2)) / 2 + 1);
                                        int currentReflexivKmerSuffixLength = Long.SIZE / 2 - (Long.numberOfTrailingZeros(s.getLong(2)) / 2 + 1);
                                        if (s.getInt(3) < 0 && tmpReflexivKmerExtendList.get(i).getInt(4) < 0) {
                                            reflexivExtend(s, tmpReflexivKmerExtendList.get(i), -1);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else if (s.getInt(3) >= 0 && tmpReflexivKmerExtendList.get(i).getInt(4) >= 0) {
                                            reflexivExtend(s, tmpReflexivKmerExtendList.get(i), -1);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else if (s.getInt(3) >= 0 && s.getInt(3) - tmpReflexivKmerSuffixLength >= 0) {
                                            reflexivExtend(s, tmpReflexivKmerExtendList.get(i), s.getInt(3) - tmpReflexivKmerSuffixLength);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else if (tmpReflexivKmerExtendList.get(i).getInt(4) >= 0 && tmpReflexivKmerExtendList.get(i).getInt(4) - currentReflexivKmerSuffixLength >= 0) {
                                            reflexivExtend(s, tmpReflexivKmerExtendList.get(i), tmpReflexivKmerExtendList.get(i).getInt(4) - currentReflexivKmerSuffixLength);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else {
                                            singleKmerRandomizer(s);
                                            break;
                                        }
                                    } else if (tmpReflexivKmerExtendList.get(i).getInt(1) == 1) {
                                        singleKmerRandomizer(s);
                                        //directKmerComparison(s);
                                        break;
                                    }
                                } else { /* if (s.getInt(1) == 2) { */
                                    if (tmpReflexivKmerExtendList.get(i).getInt(1) == 2) {
                                        singleKmerRandomizer(s);
                                        //directKmerComparison(s);
                                        break;
                                    } else if (tmpReflexivKmerExtendList.get(i).getInt(1) == 1) {
                                        int tmpReflexivKmerSuffixLength = Long.SIZE / 2 - (Long.numberOfTrailingZeros(tmpReflexivKmerExtendList.get(i).getLong(2)) / 2 + 1);
                                        int currentReflexivKmerSuffixLength = Long.SIZE / 2 - (Long.numberOfTrailingZeros(s.getLong(2)) / 2 + 1);
                                        if (s.getInt(4) < 0 && tmpReflexivKmerExtendList.get(i).getInt(3) < 0) {
                                            reflexivExtend(tmpReflexivKmerExtendList.get(i), s, -1);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else if (s.getInt(4) >= 0 && tmpReflexivKmerExtendList.get(i).getInt(3) >= 0) {
                                            reflexivExtend(tmpReflexivKmerExtendList.get(i), s, -1);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else if (s.getInt(4) >= 0 && s.getInt(4) - tmpReflexivKmerSuffixLength >= 0) {
                                            reflexivExtend(tmpReflexivKmerExtendList.get(i), s, s.getInt(4) - tmpReflexivKmerSuffixLength);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else if (tmpReflexivKmerExtendList.get(i).getInt(3) >= 0 && tmpReflexivKmerExtendList.get(i).getInt(3) - currentReflexivKmerSuffixLength >= 0) {
                                            reflexivExtend(tmpReflexivKmerExtendList.get(i), s, tmpReflexivKmerExtendList.get(i).getInt(3) - currentReflexivKmerSuffixLength);
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

                            //else if (dynamicSubKmerComparator(s.getSeq(0), tmpReflexivKmerExtendList.get(i).getSeq(0)) ){

                            //}

                        /* new Sub-kmer group section */
                            else { /* s.getLong(0) != tmpReflexivKmerExtendList.get(i).getLong(0)()*/
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

        private boolean dynamicSubKmerComparator(Seq a, Seq b) throws Exception {
            long[] arrayA = ArrayUtils.toPrimitive((Long[]) JavaConverters.seqAsJavaList(a).toArray());
            long[] arrayB = ArrayUtils.toPrimitive((Long[]) JavaConverters.seqAsJavaList(b).toArray());

            int aLength= currentKmerSizeFromBinaryBlockArray(arrayA);
            int bLength= currentKmerSizeFromBinaryBlockArray(arrayB);

            if (aLength>=bLength){ // equal should not happen
                long[] shorterVersion = leftShiftOutFromArray(arrayA, bLength);
                if (Arrays.equals(shorterVersion, arrayB)){
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


        private boolean subKmerSlotComparator(Seq a, Seq b) {
            for (int i = 0; i < a.length(); i++) {
                if (!a.apply(i).equals(b.apply(i))) {
                    return false;
                }
            }

            return true;
        }


        /**
         *
         * @param currentSubKmer
         */
        public void singleKmerRandomizer(Row currentSubKmer) throws Exception {

            if (getReflexivMarker(currentSubKmer.getLong(1)) == 1) {
                /**
                 * 00000000000000110010111010010   Long.SIZE
                 * --------------C-G-G-G-T-C-A-G   Long.SIZE - (Long.numberOfLeadingZeros / 2 + 1)
                 * --------------^-Length marker
                 */
                int currentSuffixLength = Long.SIZE / 2 - (Long.numberOfTrailingZeros(currentSubKmer.getLong(2)) / 2 + 1); // xx01-------
                long[] newReflexivSubKmer; //= new long[param.subKmerBinarySlots];
               // long newReflexivLong;

                if (randomReflexivMarker == 2) {

                    long[] currentSuffixArray = new long[1];
                    currentSuffixArray[0]=currentSubKmer.getLong(2);
                    long[] combinedKmerArray = combineTwoLongBlocks((long[])currentSubKmer.get(0), currentSuffixArray);

                    newReflexivSubKmer = leftShiftArray(combinedKmerArray, currentSuffixLength);

                    long[] newReflexivLongArray = leftShiftOutFromArray(combinedKmerArray, currentSuffixLength);

                    long attribute = onlyChangeReflexivMarker(currentSubKmer.getLong(1), randomReflexivMarker);

                    reflexivKmerConcatList.add(
                            RowFactory.create(newReflexivSubKmer, attribute, newReflexivLongArray)
                    );

                } else {
                    reflexivKmerConcatList.add(currentSubKmer);
                }
            } else { /* currentSubKmer._2._1() == 2 */
                int currentPrefixLength = Long.SIZE / 2 - (Long.numberOfTrailingZeros(currentSubKmer.getLong(2)) / 2 + 1);
                long[] newReflexivSubKmer; // = new long[param.subKmerBinarySlots];
                Long newReflexivLong;
                int currentSubKmerSize= currentKmerSizeFromBinaryBlockArray((long[])currentSubKmer.get(0));

                if (randomReflexivMarker == 2) {
                    reflexivKmerConcatList.add(currentSubKmer);
                } else { /* randomReflexivMarker == 1 */

                    long[] currentPrefixArray = new long[1];
                    currentPrefixArray[0]= currentSubKmer.getLong(2);
                    long[] combinedKmerArray = combineTwoLongBlocks(currentPrefixArray, (long[])currentSubKmer.get(0));

                    newReflexivSubKmer= leftShiftOutFromArray(combinedKmerArray, currentSubKmerSize);
                    long[] newReflexivLongArray= leftShiftArray(combinedKmerArray, currentSubKmerSize);

                    long attribute = onlyChangeReflexivMarker(currentSubKmer.getLong(1), randomReflexivMarker);


                    reflexivKmerConcatList.add(
                            RowFactory.create(newReflexivSubKmer, attribute, newReflexivLongArray)
                    );
                }

            }

            /* an action of randomization */

            if (randomReflexivMarker == 1) {
                randomReflexivMarker = 2;
            } else { /* randomReflexivMarker == 2 */
                randomReflexivMarker = 1;
            }
        }

        /**
         *
         * @param currentSubKmer
         */
        public void directKmerComparison(Row currentSubKmer) {
            tmpReflexivKmerExtendList.add(currentSubKmer);
        }

        /**
         *
         * @param forwardSubKmer
         * @param reflexedSubKmer
         */

        public void reflexivExtend(Row forwardSubKmer, Row reflexedSubKmer, int bubbleDistance) throws Exception {

             /* forward   ATCGATCG, 1, ------ */
             /* reflexed  ------, 2, ATCGATCG */

            int forwardSuffixLength = Long.SIZE / 2 - (Long.numberOfTrailingZeros(forwardSubKmer.getLong(2)) / 2 + 1);
            int forwardSubKmerLength = currentKmerSizeFromBinaryBlockArray((long[])forwardSubKmer.get(0));

            int reflexedPrefixLength = Long.SIZE / 2 - (Long.numberOfTrailingZeros(reflexedSubKmer.getLong(2)) / 2 + 1);
            int reflexedSubKmerLength = currentKmerSizeFromBinaryBlockArray((long[])reflexedSubKmer.get(0));

            int newSubKmerLength;
            long[] longerSubKmer;
            if (forwardSubKmerLength >= reflexedSubKmerLength){ // In reality, it is always forwardSubKmer longer than or equal to reflexedSubKmer
                newSubKmerLength=forwardSubKmerLength;
                longerSubKmer= (long[])forwardSubKmer.get(0);
            }else{
                newSubKmerLength=reflexedSubKmerLength;
                longerSubKmer=(long[])reflexedSubKmer.get(0);
            }

            long[] reflexedPrefixArray = new long[1];
            reflexedPrefixArray[0] = reflexedSubKmer.getLong(2);

            long[] forwardSuffixArray = new long[1];
            forwardSuffixArray[0] = forwardSubKmer.getLong(2);


            if (randomReflexivMarker == 2) {

                long[] newReflexivSubKmer = combineTwoLongBlocks(longerSubKmer, forwardSuffixArray); // xxxxx xxxxx xxx-- + xxx--- = xxxxx xxxxx xxxxx x----
                long[] newReflexivLongArray= leftShiftOutFromArray(newReflexivSubKmer, forwardSuffixLength); // xxx--  | ---xx xxxxx xxxxx x----

                newReflexivSubKmer = leftShiftArray(newReflexivSubKmer, forwardSuffixLength); // xxxxx xxxxx xxx---
                newReflexivLongArray = combineTwoLongBlocks(reflexedPrefixArray, newReflexivLongArray); // xx--- + xxx--

                if (bubbleDistance < 0) {
                    long attribute = buildingAlongFromThreeInt(randomReflexivMarker, getLeftMarker(reflexedSubKmer.getLong(1)), getRightMarker(forwardSubKmer.getLong(1)));
                    reflexivKmerConcatList.add(
                            RowFactory.create(newReflexivSubKmer,
                                    attribute, newReflexivLongArray
                            )
                    );
                } else {
                    if (getLeftMarker(forwardSubKmer.getLong(1)) > 0) {
                        long attribute = buildingAlongFromThreeInt(randomReflexivMarker, bubbleDistance, getRightMarker(forwardSubKmer.getLong(1)));
                        reflexivKmerConcatList.add(
                                RowFactory.create(newReflexivSubKmer,
                                        attribute, newReflexivLongArray
                                )
                        );
                    } else {
                        long attribute = buildingAlongFromThreeInt(randomReflexivMarker, getLeftMarker(reflexedSubKmer.getLong(1)), bubbleDistance);
                        reflexivKmerConcatList.add(
                                RowFactory.create(newReflexivSubKmer,
                                        attribute, newReflexivLongArray
                                )
                        );
                    }
                }

                randomReflexivMarker = 1; /* an action of randomization */
            } else { /* randomReflexivMarker == 1 */

                long[] newForwardSubKmer = combineTwoLongBlocks(reflexedPrefixArray, longerSubKmer); // xx--- + xxxxx xxxxx xx--- = xxxxx xxxxx xxxx-
                long[] newForwardLongArray = leftShiftArray(newForwardSubKmer, newSubKmerLength);  // xxxxx xxxxx xxxx-  -> xx--

                newForwardSubKmer = leftShiftOutFromArray(newForwardSubKmer, newSubKmerLength); // xxxxx xxxxx xxxx- -> xxxxx xxxxx xx---|xx-
                newForwardLongArray = combineTwoLongBlocks(newForwardLongArray, forwardSuffixArray); // xx-- + xxx-- -> xxxxx

                if (bubbleDistance < 0) {
                    long attribute = buildingAlongFromThreeInt(randomReflexivMarker, getLeftMarker(reflexedSubKmer.getLong(1)), getRightMarker(forwardSubKmer.getLong(1)));
                    reflexivKmerConcatList.add(
                            RowFactory.create(newForwardSubKmer,
                                    attribute, newForwardLongArray
                            )
                    );
                } else {

                    if (getLeftMarker(forwardSubKmer.getLong(1)) > 0) {
                        long attribute = buildingAlongFromThreeInt(randomReflexivMarker,bubbleDistance,getRightMarker(forwardSubKmer.getLong(1)));
                        reflexivKmerConcatList.add(
                                RowFactory.create(newForwardSubKmer,
                                        attribute, newForwardLongArray
                                )
                        );
                    } else { // reflexedSubKmer.getInt(4) >0
                        long attribute = buildingAlongFromThreeInt(randomReflexivMarker,getLeftMarker(reflexedSubKmer.getLong(1)), bubbleDistance);
                        reflexivKmerConcatList.add(
                                RowFactory.create(newForwardSubKmer,
                                        attribute, newForwardLongArray
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
        public void resetSubKmerGroup(Row S) {
            if (lineMarker == 1) {
                lineMarker = 2;
            } else {
                lineMarker = 3; /* reset to new sub-kmer group */
            }
            /* re-reflex all single kmers in the sub-kmer group */
//            if (tmpReflexivKmerExtendList.size() != 0) {
//                for (int i = 0; i < tmpReflexivKmerExtendList.size(); i++) {
            //                   singleKmerRandomizer(tmpReflexivKmerExtendList.get(i));
            //               }
            //          }

            tmpReflexivKmerExtendList = new ArrayList<Row>();


            tmpReflexivKmerExtendList.add(
                    RowFactory.create(S.getSeq(0),
                            S.getLong(1), S.getSeq(2)
                    )
            );
        }

        /**
         *
         */
        public void tmpKmerRandomizer() throws Exception {
            if (tmpReflexivKmerExtendList.size() != 0) {
                for (int i = 0; i < tmpReflexivKmerExtendList.size(); i++) {
                    singleKmerRandomizer(tmpReflexivKmerExtendList.get(i));
                }
            }
        }

        private long[] leftShiftArray(long[] blocks, int shiftingLength) throws Exception {
            int startingBlockIndex = (shiftingLength-1)/31;
            int nucleotideLength = currentKmerSizeFromBinaryBlockArray(blocks);
            int residueLength = Long.SIZE / 2 - (Long.numberOfTrailingZeros(blocks[blocks.length-1])/2+1); // last block length

            long[] newBlock = new long[(nucleotideLength-shiftingLength)/31+1];
            int relativeShiftSize = shiftingLength % 31;

            if (shiftingLength > nucleotideLength){
                throw new Exception("shifting length longer than the kmer length");
            }

            // if (relativeShiftSize ==0) then only shifting blocks

            int j=0; // new index for shifted blocks
            //           long oldShiftOut=0L; // if only one block, then 0 bits
//            if (blocks.length-(startingBlockIndex+1) >=1) { // more than one block, newBlock.length = blocks.length-startingBlockIndex
//                oldShiftOut = blocks[startingBlockIndex + 1] >>> 2 * (32 - relativeShiftSize);
            //           }
            for (int i=startingBlockIndex; i<blocks.length-1; i++){ // without the last block
                long shiftOut = blocks[i+1] >>> 2*(32-relativeShiftSize); // ooooxxxxxxx -> -------oooo  o=shift out x=needs to be left shifted
                newBlock[j]= blocks[i] << 2*relativeShiftSize; // 00000xxxxx -> xxxxx-----
                newBlock[j] |= shiftOut;
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
                throw new Exception("shifting length longer than the kmer length");
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



            if (leftVacancy ==0){ // left last block is a perfect block
                for (int i =0; i<leftBlocks.length; i++){
                    newBlocks[i]=leftBlocks[i];
                }

                newBlocks[leftBlocks.length-1] &= (~0L<<2); // remove the last block's C marker

                for (int j=leftBlocks.length;j<combinedBlockSize;j++){
                    newBlocks[j]=rightBlocks[j];
                }
            }else{
                long[] shiftOutBlocks = leftShiftOutFromArray(rightBlocks, leftVacancy); // right shift out for the left. here we only expect one block, because leftVacancy is relative to one block
                for (int i =0; i<leftBlocks.length; i++){
                    newBlocks[i]=leftBlocks[i];
                }

                newBlocks[leftBlocks.length-1] &= (~0L<<2*(leftVacancy+1)); // leftVacancy = 32-leftRelativeNTLength-1. This is to remove the C marker
                newBlocks[leftBlocks.length-1] |= (shiftOutBlocks[0]>>> 2*(leftRelativeNTLength));
                newBlocks[leftBlocks.length-1] &= (~0L<<2); // remove shift out blockls C marker

                long[] rightBlocksLeftShifted = leftShiftArray(rightBlocks, leftVacancy);

                int k=0; // rightBlocksLeftShifted index
                for (int j=leftBlocks.length;j<combinedBlockSize;j++){ // including the last blocks.
                    newBlocks[j]=rightBlocksLeftShifted[k];
                    k++;
                }

            }
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

        private int getReflexivMarker(long attribute){
            int reflexivMarker = (int) (attribute >>> 2*(32-1)); // 01-------- -> ---------01 reflexiv marker
            return reflexivMarker;
        }

        private int getLeftMarker(long attribute){
            int leftMarker = (int) (attribute >>> 2*(16)); // 01--xxxx-----xxxx -> 01--xxxx shift out right marker
            int leftMarkerBinaryBits= ~(3 << 31) ; // ---------11 -> 11---------- -> 0011111111111
            leftMarker &= leftMarkerBinaryBits; // remove reflexivMarker
            return leftMarker;
        }

        private int getRightMarker(long attribute){
            int rightMarker = (int) attribute;
            return rightMarker;
        }

        private long buildingAlongFromThreeInt(int ReflexivMarker, int leftCover, int rightCover){
            long info = (long) ReflexivMarker <<2*(32-1);  //move to the left most
            info |= ((long) leftCover << 32) ; // move one integer (32 bits) to the left
            info |= ((long) rightCover); //  01--LeftCover---RightCover

            return info;
        }

    }

    /**
     *
     */


    class DSExtendReflexivKmer implements MapPartitionsFunction<Row, Row>, Serializable {

        /* marker to identify similar SubKmers in the loop sequence */
        private int lineMarker = 1;

        /* 1 stands for forward sub-kmer */
        /* 2 stands for reflexiv sub-kmer */
        //     private int randomReflexivMarker = ThreadLocalRandom.current().nextInt(1, 3);
        private int randomReflexivMarker = 2;

        long maxSubKmerResidueBinary = ~((~0L) << 2 * param.subKmerSizeResidue);
        long maxSubKmerBinary = ~((~0L) << 2 * 31);


        /* temporary capsule to store identical SubKmer units */
        List<Row> tmpReflexivKmerExtendList = new ArrayList<Row>();

        /* return capsule of extend Tuples for next iteration*/
        List<Row> reflexivKmerConcatList = new ArrayList<Row>();

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
        public Iterator<Row> call(Iterator<Row> sIterator) throws Exception {

            while (sIterator.hasNext()) {
                Row s = sIterator.next();

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
                    //      reflexivKmerConcatList = new ArrayList<Row>();

                    if (tmpReflexivKmerExtendList.size() == 0) {
                        directKmerComparison(s);
                    } else { /* tmpReflexivKmerExtendList.size() != 0 */
                        for (int i = 0; i < tmpReflexivKmerExtendList.size(); i++) { // the tmpReflexivKmerExtendList is changing dynamically
                            if (subKmerSlotComparator(s.getSeq(0), tmpReflexivKmerExtendList.get(i).getSeq(0)) || dynamicSubKmerComparator(s.getSeq(0), tmpReflexivKmerExtendList.get(i).getSeq(0))) {
                                if (getReflexivMarker(s.getLong(1)) == 1) {
                                    if (getReflexivMarker(tmpReflexivKmerExtendList.get(i).getLong(1)) == 2) {
                                        int tmpReflexivKmerSuffixLength = Long.SIZE / 2 - (Long.numberOfTrailingZeros(tmpReflexivKmerExtendList.get(i).getLong(2)) / 2 + 1);
                                        int currentReflexivKmerSuffixLength = Long.SIZE / 2 - (Long.numberOfTrailingZeros(s.getLong(2)) / 2 + 1);
                                        if (s.getInt(3) < 0 && tmpReflexivKmerExtendList.get(i).getInt(4) < 0) {
                                            reflexivExtend(s, tmpReflexivKmerExtendList.get(i), -1);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else if (s.getInt(3) >= 0 && tmpReflexivKmerExtendList.get(i).getInt(4) >= 0) {
                                            reflexivExtend(s, tmpReflexivKmerExtendList.get(i), -1);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else if (s.getInt(3) >= 0 && s.getInt(3) - tmpReflexivKmerSuffixLength >= 0) {
                                            reflexivExtend(s, tmpReflexivKmerExtendList.get(i), s.getInt(3) - tmpReflexivKmerSuffixLength);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else if (tmpReflexivKmerExtendList.get(i).getInt(4) >= 0 && tmpReflexivKmerExtendList.get(i).getInt(4) - currentReflexivKmerSuffixLength >= 0) {
                                            reflexivExtend(s, tmpReflexivKmerExtendList.get(i), tmpReflexivKmerExtendList.get(i).getInt(4) - currentReflexivKmerSuffixLength);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else {
                                            singleKmerRandomizer(s);
                                            break;
                                        }
                                    } else if (getReflexivMarker(tmpReflexivKmerExtendList.get(i).getLong(1)) == 1) {
                                        singleKmerRandomizer(s);
                                        //directKmerComparison(s);
                                        break;
                                    }
                                } else { /* if (s.getInt(1) == 2) { */
                                    if (getReflexivMarker(tmpReflexivKmerExtendList.get(i).getLong(1)) == 2) {
                                        singleKmerRandomizer(s);
                                        //directKmerComparison(s);
                                        break;
                                    } else if (getReflexivMarker(tmpReflexivKmerExtendList.get(i).getLong(1)) == 1) {
                                        int tmpReflexivKmerSuffixLength = Long.SIZE / 2 - (Long.numberOfTrailingZeros(tmpReflexivKmerExtendList.get(i).getLong(2)) / 2 + 1);
                                        int currentReflexivKmerSuffixLength = Long.SIZE / 2 - (Long.numberOfTrailingZeros(s.getLong(2)) / 2 + 1);
                                        if (s.getInt(4) < 0 && tmpReflexivKmerExtendList.get(i).getInt(3) < 0) {
                                            reflexivExtend(tmpReflexivKmerExtendList.get(i), s, -1);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else if (s.getInt(4) >= 0 && tmpReflexivKmerExtendList.get(i).getInt(3) >= 0) {
                                            reflexivExtend(tmpReflexivKmerExtendList.get(i), s, -1);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else if (s.getInt(4) >= 0 && s.getInt(4) - tmpReflexivKmerSuffixLength >= 0) {
                                            reflexivExtend(tmpReflexivKmerExtendList.get(i), s, s.getInt(4) - tmpReflexivKmerSuffixLength);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else if (tmpReflexivKmerExtendList.get(i).getInt(3) >= 0 && tmpReflexivKmerExtendList.get(i).getInt(3) - currentReflexivKmerSuffixLength >= 0) {
                                            reflexivExtend(tmpReflexivKmerExtendList.get(i), s, tmpReflexivKmerExtendList.get(i).getInt(3) - currentReflexivKmerSuffixLength);
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
                            /**
                             *  comparing k-mers with different length but identical prefix  xxxxxxc--     c is the marker
                             *                                                               xxxxxxatg gttcatgc--
                             *
                             *  same process as k-mers with equal length
                             */

                           // else if (dynamicSubKmerComparator(s.getSeq(0), tmpReflexivKmerExtendList.get(i).getSeq(0))){
                           // }


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

        private boolean dynamicSubKmerComparator(Seq a, Seq b) throws Exception {
            long[] arrayA = ArrayUtils.toPrimitive((Long[]) JavaConverters.seqAsJavaList(a).toArray());
            long[] arrayB = ArrayUtils.toPrimitive((Long[]) JavaConverters.seqAsJavaList(b).toArray());

            int aLength= currentKmerSizeFromBinaryBlockArray(arrayA);
            int bLength= currentKmerSizeFromBinaryBlockArray(arrayB);

            if (aLength>=bLength){ // equal should not happen
                long[] shorterVersion = leftShiftOutFromArray(arrayA, bLength);
                if (Arrays.equals(shorterVersion, arrayB)){
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


        private boolean subKmerSlotComparator(Seq a, Seq b) {
            for (int i = 0; i < a.length(); i++) {
                if (!a.apply(i).equals(b.apply(i))) {
                    return false;
                }
            }

            return true;
        }


        public void singleKmerRandomizer(Row currentSubKmer) throws Exception {

            if (getReflexivMarker(currentSubKmer.getLong(1)) == 1) {
                /**
                 * 00000000000000110010111010010   Long.SIZE
                 * --------------C-G-G-G-T-C-A-G   Long.SIZE - (Long.numberOfLeadingZeros / 2 + 1)
                 * --------------^-Length marker
                 */
                int currentSuffixLength = Long.SIZE / 2 - (Long.numberOfTrailingZeros(currentSubKmer.getLong(2)) / 2 + 1); // xx01-------
                long[] newReflexivSubKmer; //= new long[param.subKmerBinarySlots];
                long newReflexivLong;

                if (randomReflexivMarker == 2) {

                    newReflexivSubKmer = leftShiftArray((long[])currentSubKmer.get(0), currentSuffixLength);
                    long[] newReflexivLongArray = leftShiftOutFromArray((long[])currentSubKmer.get(0), currentSuffixLength);
                    newReflexivLong = newReflexivLongArray[0];
                    newReflexivSubKmer = combineTwoLongBlocks(newReflexivSubKmer, newReflexivLongArray);

                    long attribute = onlyChangeReflexivMarker(currentSubKmer.getLong(1), randomReflexivMarker);

                    reflexivKmerConcatList.add(
                            RowFactory.create(newReflexivSubKmer, attribute, newReflexivLong)
                    );

                } else {
                    reflexivKmerConcatList.add(currentSubKmer);
                }
            } else { /* currentSubKmer._2._1() == 2 */
                int currentPrefixLength = Long.SIZE / 2 - (Long.numberOfTrailingZeros(currentSubKmer.getLong(2)) / 2 + 1);
                long[] newReflexivSubKmer; // = new long[param.subKmerBinarySlots];
                Long newReflexivLong;
                int currentSubKmerSize= currentKmerSizeFromBinaryBlockArray((long[])currentSubKmer.get(0));

                if (randomReflexivMarker == 2) {
                    reflexivKmerConcatList.add(currentSubKmer);
                } else { /* randomReflexivMarker == 1 */

                    int rightShiftRemain = currentSubKmerSize-currentPrefixLength;
                    long[] rightShiftRemainBlocks = leftShiftOutFromArray((long[])currentSubKmer.get(0), rightShiftRemain);
                    long[] prefixArray = new long[1];
                    prefixArray[0]=currentSubKmer.getLong(2);
                    newReflexivSubKmer = combineTwoLongBlocks(prefixArray, rightShiftRemainBlocks);

                    newReflexivLong = (leftShiftArray((long[])currentSubKmer.get(0), rightShiftRemain))[0];

                    long attribute = onlyChangeReflexivMarker(currentSubKmer.getLong(1), randomReflexivMarker);


                    reflexivKmerConcatList.add(
                            RowFactory.create(newReflexivSubKmer, attribute, newReflexivLong)
                    );
                }

            }

            /* an action of randomization */

            if (randomReflexivMarker == 1) {
                randomReflexivMarker = 2;
            } else { /* randomReflexivMarker == 2 */
                randomReflexivMarker = 1;
            }
        }
        /**
         *
         * @param currentSubKmer
         */


        /**
         *
         * @param currentSubKmer
         */
        public void directKmerComparison(Row currentSubKmer) {
            tmpReflexivKmerExtendList.add(currentSubKmer);
        }

        /**
         *
         * @param forwardSubKmer
         * @param reflexedSubKmer
         */
        public void reflexivExtend(Row forwardSubKmer, Row reflexedSubKmer, int bubbleDistance) throws Exception {

             /* forward   ATCGATCG, 1, ------ */
             /* reflexed  ------, 2, ATCGATCG */

            int forwardSuffixLength = Long.SIZE / 2 - (Long.numberOfTrailingZeros(forwardSubKmer.getLong(2)) / 2 + 1);
            int forwardSubKmerLength = currentKmerSizeFromBinaryBlockArray((long[])forwardSubKmer.get(0));

            int reflexedPrefixLength = Long.SIZE / 2 - (Long.numberOfTrailingZeros(reflexedSubKmer.getLong(2)) / 2 + 1);
            int reflexedSubKmerLength = currentKmerSizeFromBinaryBlockArray((long[])reflexedSubKmer.get(0));

            int newSubKmerLength;
            long[] longerSubKmer;
            if (forwardSubKmerLength >= reflexedSubKmerLength){ // In reality, it is always forwardSubKmer longer than or equal to reflexedSubKmer
                newSubKmerLength=forwardSubKmerLength;
                longerSubKmer= (long[])forwardSubKmer.get(0);
            }else{
                newSubKmerLength=reflexedSubKmerLength;
                longerSubKmer=(long[])reflexedSubKmer.get(0);
            }

            long[] reflexedPrefixArray = new long[1];
            reflexedPrefixArray[0] = reflexedSubKmer.getLong(2);

            long[] forwardSuffixArray = new long[1];
            forwardSuffixArray[0] = forwardSubKmer.getLong(2);

            if (randomReflexivMarker == 2) {

                long[] newReflexivSubKmer = combineTwoLongBlocks(longerSubKmer, forwardSuffixArray); // xxxxx xxxxx xxx-- + xxx--- = xxxxx xxxxx xxxxx x----
                long[] newReflexivLongArray= leftShiftOutFromArray(newReflexivSubKmer, forwardSuffixLength); // xxx--  | ---xx xxxxx xxxxx x----

                newReflexivSubKmer = leftShiftArray(newReflexivSubKmer, forwardSuffixLength); // xxxxx xxxxx xxx---
                newReflexivLongArray = combineTwoLongBlocks(reflexedPrefixArray, newReflexivLongArray); // xx--- + xxx--

                if (bubbleDistance < 0) {
                    long attribute = buildingAlongFromThreeInt(randomReflexivMarker, getLeftMarker(reflexedSubKmer.getLong(1)), getRightMarker(forwardSubKmer.getLong(1)));
                    reflexivKmerConcatList.add(
                            RowFactory.create(newReflexivSubKmer, attribute, newReflexivLongArray[0])
                    );
                } else {
                    if (getLeftMarker(forwardSubKmer.getLong(1)) > 0) {
                        long attribute = buildingAlongFromThreeInt(randomReflexivMarker, bubbleDistance, getRightMarker(forwardSubKmer.getLong(1)));
                        reflexivKmerConcatList.add(
                                RowFactory.create(newReflexivSubKmer, attribute, newReflexivLongArray[0])
                        );
                    } else {
                        long attribute = buildingAlongFromThreeInt(randomReflexivMarker, getLeftMarker(reflexedSubKmer.getLong(1)), bubbleDistance);
                        reflexivKmerConcatList.add(
                                RowFactory.create(newReflexivSubKmer, attribute, newReflexivLongArray[0])
                        );
                    }
                }

                randomReflexivMarker = 1; /* an action of randomization */
            } else { /* randomReflexivMarker == 1 */

                long[] newForwardSubKmer = combineTwoLongBlocks(reflexedPrefixArray, longerSubKmer); // xx--- + xxxxx xxxxx xx--- = xxxxx xxxxx xxxx-
                long[] newForwardLongArray = leftShiftArray(newForwardSubKmer, newSubKmerLength);  // xxxxx xxxxx xxxx-  -> xx--

                newForwardSubKmer = leftShiftOutFromArray(newForwardSubKmer, newSubKmerLength); // xxxxx xxxxx xxxx- -> xxxxx xxxxx xx---|xx-
                newForwardLongArray = combineTwoLongBlocks(newForwardLongArray, forwardSuffixArray); // xx-- + xxx-- -> xxxxx


                if (bubbleDistance < 0) {
                    long attribute = buildingAlongFromThreeInt(randomReflexivMarker, getLeftMarker(reflexedSubKmer.getLong(1)), getRightMarker(forwardSubKmer.getLong(1)));
                    reflexivKmerConcatList.add(
                            RowFactory.create(newForwardSubKmer, attribute, newForwardLongArray[0])
                    );
                } else {
                    if (getLeftMarker(forwardSubKmer.getLong(1)) > 0) {
                        long attribute = buildingAlongFromThreeInt(randomReflexivMarker, bubbleDistance, getRightMarker(forwardSubKmer.getLong(1)));
                        reflexivKmerConcatList.add(
                                RowFactory.create(newForwardSubKmer,
                                        attribute, newForwardLongArray[0]
                                )
                        );
                    } else { // reflexedSubKmer.getInt(4) >0
                        long attribute = buildingAlongFromThreeInt(randomReflexivMarker, getLeftMarker(reflexedSubKmer.getLong(1)), bubbleDistance);
                        reflexivKmerConcatList.add(
                                RowFactory.create(newForwardSubKmer,
                                        attribute, newForwardLongArray[0]
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
        public void resetSubKmerGroup(Row S) {
            if (lineMarker == 1) {
                lineMarker = 2;
            } else {
                lineMarker = 3; /* reset to new sub-kmer group */
            }
            /* re-reflex all single kmers in the sub-kmer group */
//            if (tmpReflexivKmerExtendList.size() != 0) {
//                for (int i = 0; i < tmpReflexivKmerExtendList.size(); i++) {
            //                   singleKmerRandomizer(tmpReflexivKmerExtendList.get(i));
            //               }
            //          }

            tmpReflexivKmerExtendList = new ArrayList<Row>();
            tmpReflexivKmerExtendList.add(
                    RowFactory.create(S.getSeq(0),
                            S.getLong(1), S.getLong(2)
                    )
            );
        }

        private long[] leftShiftArray(long[] blocks, int shiftingLength) throws Exception {
            int startingBlockIndex = (shiftingLength-1)/31;
            int nucleotideLength = currentKmerSizeFromBinaryBlockArray(blocks);
            int residueLength = Long.SIZE / 2 - (Long.numberOfTrailingZeros(blocks[blocks.length-1])/2+1); // last block length

            long[] newBlock = new long[(nucleotideLength-shiftingLength)/31+1];
            int relativeShiftSize = shiftingLength % 31;

            if (shiftingLength > nucleotideLength){
                throw new Exception("shifting length longer than the kmer length");
            }

            // if (relativeShiftSize ==0) then only shifting blocks

            int j=0; // new index for shifted blocks
            //           long oldShiftOut=0L; // if only one block, then 0 bits
//            if (blocks.length-(startingBlockIndex+1) >=1) { // more than one block, newBlock.length = blocks.length-startingBlockIndex
//                oldShiftOut = blocks[startingBlockIndex + 1] >>> 2 * (32 - relativeShiftSize);
            //           }
            for (int i=startingBlockIndex; i<blocks.length-1; i++){ // without the last block
                long shiftOut = blocks[i+1] >>> 2*(32-relativeShiftSize); // ooooxxxxxxx -> -------oooo  o=shift out x=needs to be left shifted
                newBlock[j]= blocks[i] << 2*relativeShiftSize; // 00000xxxxx -> xxxxx-----
                newBlock[j] |= shiftOut;
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
                throw new Exception("shifting length longer than the kmer length");
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



            if (leftVacancy ==0){ // left last block is a perfect block
                for (int i =0; i<leftBlocks.length; i++){
                    newBlocks[i]=leftBlocks[i];
                }

                newBlocks[leftBlocks.length-1] &= (~0L<<2); // remove the last block's C marker

                for (int j=leftBlocks.length;j<combinedBlockSize;j++){
                    newBlocks[j]=rightBlocks[j];
                }
            }else{
                long[] shiftOutBlocks = leftShiftOutFromArray(rightBlocks, leftVacancy); // right shift out for the left. here we only expect one block, because leftVacancy is relative to one block
                for (int i =0; i<leftBlocks.length; i++){
                    newBlocks[i]=leftBlocks[i];
                }

                newBlocks[leftBlocks.length-1] &= (~0L<<2*(leftVacancy+1)); // leftVacancy = 32-leftRelativeNTLength-1. This is to remove the C marker
                newBlocks[leftBlocks.length-1] |= (shiftOutBlocks[0]>>> 2*(leftRelativeNTLength));
                newBlocks[leftBlocks.length-1] &= (~0L<<2); // remove shift out blockls C marker

                long[] rightBlocksLeftShifted = leftShiftArray(rightBlocks, leftVacancy);

                int k=0; // rightBlocksLeftShifted index
                for (int j=leftBlocks.length;j<combinedBlockSize;j++){ // including the last blocks.
                    newBlocks[j]=rightBlocksLeftShifted[k];
                    k++;
                }

            }
        }

        private long onlyChangeReflexivMarker(long oldMarker, int reflexivMarker){
            long newMarker = oldMarker & maxSubKmerBinary;
            newMarker |= ((long) reflexivMarker) << 2*(32-1);
            return newMarker;
        }

        /**
         *
         */
        public void tmpKmerRandomizer() throws Exception {
            if (tmpReflexivKmerExtendList.size() != 0) {
                for (int i = 0; i < tmpReflexivKmerExtendList.size(); i++) {
                    singleKmerRandomizer(tmpReflexivKmerExtendList.get(i));
                }
            }
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
            int leftMarkerBinaryBits= ~(3 << 31) ; // ---------11 -> 11---------- -> 0011111111111
            leftMarker &= leftMarkerBinaryBits; // remove reflexivMarker
            return leftMarker;
        }

        private int getRightMarker(long attribute){
            int rightMarker = (int) attribute;
            return rightMarker;
        }

        private long buildingAlongFromThreeInt(int ReflexivMarker, int leftCover, int rightCover){
            long info = (long) ReflexivMarker <<2*(32-1);  //move to the left most
            info |= ((long) leftCover << 32) ; // move one integer (32 bits) to the left
            info |= ((long) rightCover); //  01--LeftCover---RightCover

            return info;
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

                int currentSubKmerSize= currentKmerSizeFromBinaryBlockArray((long[]) subKmer.get(0));

                if (HighCoverageSubKmer.size() == 0) {
                    long attribute = buildingAlongFromThreeInt(reflexivMarker,leftMarker, -1-rightMarker);
                    HighCoverageSubKmer.add(
                            RowFactory.create(subKmer.getSeq(0), attribute, subKmer.getLong(2))
                    );
                } else {
                    int highestLeftMarker = getLeftMarker(HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1).getLong(1));
                    if (subKmerSlotComparator(subKmer.getSeq(0), HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1).getSeq(0)) == true) {
                        if (leftMarker > highestLeftMarker) {
                            if (highestLeftMarker <= param.minErrorCoverage && leftMarker >= 2 * highestLeftMarker) {
                                long attribute = buildingAlongFromThreeInt(reflexivMarker, leftMarker, -1-leftMarker);
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        RowFactory.create(subKmer.getSeq(0), attribute, subKmer.getLong(2))
                                );
                            } else {
                                long attribute = buildingAlongFromThreeInt(reflexivMarker, leftMarker, currentSubKmerSize+10);
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        RowFactory.create(subKmer.getSeq(0), attribute, subKmer.getLong(2))
                                );
                            }
                        } else if (leftMarker == highestLeftMarker) {
                            if (subKmer.getLong(2) > HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1).getLong(2)) {
                                long attribute = buildingAlongFromThreeInt(reflexivMarker, leftMarker, currentSubKmerSize+10);
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        RowFactory.create(subKmer.getSeq(0), attribute, subKmer.getLong(2))
                                );
                            } else {
                                subKmer = HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1);
                                reflexivMarker=getReflexivMarker(subKmer.getLong(1));
                                leftMarker=getLeftMarker(subKmer.getLong(1));
                              //  rightMarker=getRightMarker(subKmer.getLong(1));
                                currentSubKmerSize=currentKmerSizeFromBinaryBlockArray((long[])subKmer.get(0));
                                long attribute = buildingAlongFromThreeInt(reflexivMarker,leftMarker,currentSubKmerSize+10);
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
                                long attribute = buildingAlongFromThreeInt(reflexivMarker,leftMarker,-1-rightMarker);
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        RowFactory.create(subKmer.getSeq(0), attribute, subKmer.getLong(2))
                                );
                            } else {
                                subKmer = HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1);
                                reflexivMarker=getReflexivMarker(subKmer.getLong(1));
                                leftMarker=getLeftMarker(subKmer.getLong(1));
                              //  rightMarker=getRightMarker(subKmer.getLong(1));
                                currentSubKmerSize=currentKmerSizeFromBinaryBlockArray((long[])subKmer.get(0));
                                long attribute = buildingAlongFromThreeInt(reflexivMarker,leftMarker,currentSubKmerSize+10);
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        RowFactory.create(subKmer.getSeq(0), attribute, subKmer.getLong(2))
                                );
                            }
                        }
                    } else {
                        long attribute = buildingAlongFromThreeInt(reflexivMarker, leftMarker, -1-leftMarker);
                        HighCoverageSubKmer.add(
                                RowFactory.create(subKmer.getSeq(0), attribute, subKmer.getLong(2))
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
            int leftMarkerBinaryBits= ~(3 << 31) ; // ---------11 -> 11---------- -> 0011111111111
            leftMarker &= leftMarkerBinaryBits; // remove reflexivMarker
            return leftMarker;
        }

        private int getRightMarker(long attribute){
            int rightMarker = (int) attribute;
            return rightMarker;
        }

        private long buildingAlongFromThreeInt(int ReflexivMarker, int leftCover, int rightCover){
            long info = (long) ReflexivMarker <<2*(32-1);  //move to the left most
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

                int currentSubKmerSize= currentKmerSizeFromBinaryBlockArray((long[]) subKmer.get(0));


                if (HighCoverageSubKmer.size() == 0) {
                    HighCoverLastCoverage = leftMarker;
                    long attribute = buildingAlongFromThreeInt(reflexivMarker,-1-leftMarker, rightMarker);
                    HighCoverageSubKmer.add(
                            RowFactory.create(subKmer.getSeq(0), attribute, subKmer.getLong(2))
                    );
                } else {
                    int highestLeftMarker = getLeftMarker(HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1).getLong(1));
                    if (subKmerSlotComparator(subKmer.getSeq(0), HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1).getSeq(0)) == true) {
                        if (leftMarker > HighCoverLastCoverage) {
                            if (HighCoverLastCoverage <= param.minErrorCoverage && leftMarker >= 2 * HighCoverLastCoverage) {
                                HighCoverLastCoverage = leftMarker;
                                long attribute = buildingAlongFromThreeInt(reflexivMarker, -1-leftMarker, rightMarker);
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        RowFactory.create(subKmer.getSeq(0), attribute, subKmer.getLong(2))
                                );
                            } else {
                                HighCoverLastCoverage = leftMarker;
                                long attribute = buildingAlongFromThreeInt(reflexivMarker, currentSubKmerSize+10, rightMarker);
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
                                long attribute = buildingAlongFromThreeInt(reflexivMarker, currentSubKmerSize+10, rightMarker);
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        RowFactory.create(subKmer.getSeq(0),
                                                attribute, subKmer.getLong(2))
                                );
                            } else {
                                subKmer = HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1); // re assign
                                reflexivMarker=getReflexivMarker(subKmer.getLong(1));
                               // leftMarker=getLeftMarker(subKmer.getLong(1));
                                rightMarker=getRightMarker(subKmer.getLong(1));
                                currentSubKmerSize=currentKmerSizeFromBinaryBlockArray((long[])subKmer.get(0));

                                long attribute= buildingAlongFromThreeInt(reflexivMarker,currentSubKmerSize+10, rightMarker);
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        RowFactory.create(subKmer.getSeq(0),
                                                attribute, subKmer.getLong(2))
                                );
                            }
                        } else {
                            if (leftMarker <= param.minErrorCoverage && HighCoverLastCoverage >= 2 * leftMarker) {
                                subKmer = HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1);

                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        RowFactory.create(subKmer.getSeq(0),
                                                subKmer.getLong(1), subKmer.getLong(2))
                                );
                            } else {
                                subKmer = HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1);

                                reflexivMarker=getReflexivMarker(subKmer.getLong(1));
                                //leftMarker=getLeftMarker(subKmer.getLong(1));
                                rightMarker=getRightMarker(subKmer.getLong(1));
                                currentSubKmerSize=currentKmerSizeFromBinaryBlockArray((long[])subKmer.get(0));

                                long attribute = buildingAlongFromThreeInt(reflexivMarker, currentSubKmerSize+10, rightMarker);
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        RowFactory.create(subKmer.getSeq(0),
                                                attribute, subKmer.getLong(2))
                                );
                            }
                        }
                    } else {
                        HighCoverLastCoverage = leftMarker;
                        long attribute = buildingAlongFromThreeInt(reflexivMarker,-1-leftMarker, rightMarker);

                        HighCoverageSubKmer.add(
                                RowFactory.create(subKmer.getSeq(0),
                                        attribute, subKmer.getLong(2))
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
            int leftMarkerBinaryBits= ~(3 << 31) ; // ---------11 -> 11---------- -> 0011111111111
            leftMarker &= leftMarkerBinaryBits; // remove reflexivMarker
            return leftMarker;
        }

        private int getRightMarker(long attribute){
            int rightMarker = (int) attribute;
            return rightMarker;
        }

        private long buildingAlongFromThreeInt(int ReflexivMarker, int leftCover, int rightCover){
            long info = (long) ReflexivMarker <<2*(32-1);  //move to the left most
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
                currentSubKmerSize= currentKmerSizeFromBinaryBlockArray((long[])kmerTuple.get(0));
                currentSubKmerResidue = (currentSubKmerSize-1)%31 +1;
                currentSubKmerBlock = (currentSubKmerSize-1)/31+1;


                if (currentSubKmerResidue == 31) { // currentSubKmerBlock == previousSubKmerBlock -1
                    prefixBinarySlot = new long[currentSubKmerBlock];

                    suffixBinary = ((long[]) kmerTuple.get(0))[currentSubKmerBlock]; // last block XC---------- C marker keep it
                    for (int i = 0; i < currentSubKmerBlock-1; i++) {
                        prefixBinarySlot[i] = ((long[]) kmerTuple.get(0))[i];
                    }
                } else { // currentSubKmerBlock == previousSubKmerBlock
                    prefixBinarySlot = new long[currentSubKmerSize];

                    suffixBinary = (((long[]) kmerTuple.get(0))[currentSubKmerBlock-1]
                            >>> (2*(32-currentSubKmerResidue)))
                            & 3L;
                    for (int i = 0; i < param.subKmerBinarySlots - 1; i++) {
                        prefixBinarySlot[i] = ((long[]) kmerTuple.get(0))[i];
                    }
                    prefixBinarySlot[currentSubKmerBlock - 1] = ((long[]) kmerTuple.get(0))[currentSubKmerBlock - 1] >>> 2;
                }

                long attribute = buildingAlongFromThreeInt(1, kmerTuple.getInt(1), kmerTuple.getInt(1));

                TupleList.add(
                        RowFactory.create(prefixBinarySlot, attribute, suffixBinary)
                );
            }

            return TupleList.iterator();
        }

        private long buildingAlongFromThreeInt(int ReflexivMarker, int leftCover, int rightCover){
            long info = (long) ReflexivMarker <<2*(32-1);  //move to the left most
            info |= ((long) leftCover << 32) ; // move one integer (32 bits) to the left
            info |= ((long) rightCover); //  01--LeftCover---RightCover

            return info;
        }

        private int currentKmerSizeFromBinaryBlockArray(long[] binaryBlocks){
            int kmerSize;
            int blockSize = binaryBlocks.length;
            kmerSize= (blockSize-1) *31;
            final int suffix0s = Long.numberOfTrailingZeros(binaryBlocks[blockSize - 1]); // ATCG...01---
            int lastMers = Long.SIZE/2-suffix0s/2;

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


    /**
     *
     */


    class DSReflectedSubKmerExtractionFromForward implements MapPartitionsFunction<Row, Row>, Serializable {
        List<Row> TupleList = new ArrayList<Row>();
        Long suffixBinary;
        long[] prefixBinarySlot;
        Row kmerTuple;
     //   int shift = (2 * (param.subKmerSizeResidue - 1));
        Long maxSubKmerResdueBinary = ~((~0L) << 2 * param.subKmerSizeResidue);
        Long maxSubKmerBinary = ~((~0L) << 2 * 31);

        int currentSubKmerSize;
        int currentSubKmerResidue;
        int currentSubKmerBlock;

        public Iterator<Row> call(Iterator<Row> s) {

            while (s.hasNext()) {
                kmerTuple = s.next();

                currentSubKmerSize= currentKmerSizeFromBinaryBlockArray((long[])kmerTuple.get(0));
                currentSubKmerResidue = (currentSubKmerSize-1)%31 +1;
                currentSubKmerBlock = (currentSubKmerSize-1)/31+1;

                long[] prefixBinarySlot = new long[currentSubKmerBlock];

                /**
                 * reflected Sub-kmer
                 *        Kmer      ATGCACGTTATG
                 *        Sub-Kmer  ATGCACGTTAT         marked as Integer 1 in Tuple2
                 *        Left      -----------G
                 */
                // suffixBinary = 3L << shift;
                suffixBinary = (Long) kmerTuple.getSeq(0).apply(0) >>> 2*(31-1);
                //  suffixBinary >>>= shift;
                suffixBinary |= 4L; // add C marker in the front 0100 = 4L

                long transmitBit1 = (Long) kmerTuple.getSeq(0).apply(currentSubKmerBlock - 1) >>> 2 * (32 - 1);   // xx-------------
                prefixBinarySlot[currentSubKmerBlock - 1] = (Long) kmerTuple.getSeq(0).apply(currentSubKmerBlock - 1) << 2;
                //prefixBinarySlot[currentSubKmerBlock - 1] &= maxSubKmerResdueBinary;
                prefixBinarySlot[currentSubKmerBlock - 1] |= kmerTuple.getLong(2)>>> 2*(32-currentSubKmerResidue-1); // xx01-------- -> ----------xx01

                for (int i = currentSubKmerBlock - 2; i >= 0; i--) {
                    long transmitBit2 = (Long) kmerTuple.getSeq(0).apply(i) >>> 2*(32-1);

                    prefixBinarySlot[i] = (Long) kmerTuple.getSeq(0).apply(i) << 2;
                 //   prefixBinarySlot[i] &= maxSubKmerBinary;
                    prefixBinarySlot[i] |= transmitBit1;

                    transmitBit1 = transmitBit2;
                }



                long attribute = kmerTuple.getLong(1) & (3L << 2*32); // switch reflexive marker from 1 to 2.     11----------

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

        private long buildingAlongFromThreeInt(int ReflexivMarker, int leftCover, int rightCover){
            long info = (long) ReflexivMarker <<2*(32-1);  //move to the left most
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
    }

    class DSkmerRandomReflection implements MapPartitionsFunction<Row, Row>, Serializable {
        /* 0 stands for forward sub-kmer */
        /* 1 stands for reflexiv sub-kmer */
        private int randomReflexivMarker = 2;

        List<Row> reflexivKmerConcatList = new ArrayList<Row>();
        Row kmerTuple;
        long maxSubKmerResidueBinary = ~((~0L) << 2 * param.subKmerSizeResidue);
        long maxSubKmerBinary = ~((~0L) << 2 * 31);

        public Iterator<Row> call(Iterator<Row> s) throws Exception {
            while (s.hasNext()) {
                kmerTuple = s.next();

                singleKmerRandomizer(kmerTuple);
            }

            return reflexivKmerConcatList.iterator();
        }

        public void singleKmerRandomizer(Row currentSubKmer) throws Exception {

            if (currentSubKmer.getInt(1) == 1) {
                /**
                 * 00000000000000110010111010010   Long.SIZE
                 * --------------C-G-G-G-T-C-A-G   Long.SIZE - (Long.numberOfLeadingZeros / 2 + 1)
                 * --------------^-Length marker
                 */
                int currentSuffixLength = Long.SIZE / 2 - (Long.numberOfTrailingZeros(currentSubKmer.getLong(2)) / 2 + 1); // xx01-------
                long[] newReflexivSubKmer; //= new long[param.subKmerBinarySlots];
                long newReflexivLong;

                if (randomReflexivMarker == 2) {

                    newReflexivSubKmer = leftShiftArray((long[])currentSubKmer.get(0), currentSuffixLength);
                    long[] newReflexivLongArray = leftShiftOutFromArray((long[])currentSubKmer.get(0), currentSuffixLength);
                    newReflexivLong = newReflexivLongArray[0];
                    newReflexivSubKmer = combineTwoLongBlocks(newReflexivSubKmer, newReflexivLongArray);

                    long attribute = onlyChangeReflexivMarker(currentSubKmer.getLong(1), randomReflexivMarker);

                    reflexivKmerConcatList.add(
                            RowFactory.create(newReflexivSubKmer, attribute, newReflexivLong)
                    );

                } else {
                    reflexivKmerConcatList.add(currentSubKmer);
                }
            } else { /* currentSubKmer._2._1() == 2 */
                int currentPrefixLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros(currentSubKmer.getLong(2)) / 2 + 1);
                long[] newReflexivSubKmer; // = new long[param.subKmerBinarySlots];
                Long newReflexivLong;
                int currentSubKmerSize= currentKmerSizeFromBinaryBlockArray((long[])currentSubKmer.get(0));

                if (randomReflexivMarker == 2) {
                    reflexivKmerConcatList.add(currentSubKmer);
                } else { /* randomReflexivMarker == 1 */

                    int rightShiftRemain = currentSubKmerSize-currentPrefixLength;
                    long[] rightShiftRemainBlocks = leftShiftOutFromArray((long[])currentSubKmer.get(0), rightShiftRemain);
                    long[] prefixArray = new long[1];
                    prefixArray[0]=currentSubKmer.getLong(2);
                    newReflexivSubKmer = combineTwoLongBlocks(prefixArray, rightShiftRemainBlocks);

                    newReflexivLong = (leftShiftArray((long[])currentSubKmer.get(0), rightShiftRemain))[0];

                    long attribute = onlyChangeReflexivMarker(currentSubKmer.getLong(1), randomReflexivMarker);


                    reflexivKmerConcatList.add(
                            RowFactory.create(newReflexivSubKmer, attribute, newReflexivLong)
                    );
                }

            }

            /* an action of randomization */

            if (randomReflexivMarker == 1) {
                randomReflexivMarker = 2;
            } else { /* randomReflexivMarker == 2 */
                randomReflexivMarker = 1;
            }
        }

        private long[] leftShiftArray(long[] blocks, int shiftingLength) throws Exception {
            int startingBlockIndex = (shiftingLength-1)/31;
            int nucleotideLength = currentKmerSizeFromBinaryBlockArray(blocks);
            int residueLength = Long.SIZE / 2 - (Long.numberOfTrailingZeros(blocks[blocks.length-1])/2+1); // last block length

            long[] newBlock = new long[(nucleotideLength-shiftingLength)/31+1];
            int relativeShiftSize = shiftingLength % 31;

            if (shiftingLength > nucleotideLength){
                throw new Exception("shifting length longer than the kmer length");
            }

           // if (relativeShiftSize ==0) then only shifting blocks

            int j=0; // new index for shifted blocks
 //           long oldShiftOut=0L; // if only one block, then 0 bits
//            if (blocks.length-(startingBlockIndex+1) >=1) { // more than one block, newBlock.length = blocks.length-startingBlockIndex
//                oldShiftOut = blocks[startingBlockIndex + 1] >>> 2 * (32 - relativeShiftSize);
 //           }
            for (int i=startingBlockIndex; i<blocks.length-1; i++){ // without the last block
                long shiftOut = blocks[i+1] >>> 2*(32-relativeShiftSize); // ooooxxxxxxx -> -------oooo  o=shift out x=needs to be left shifted
                newBlock[j]= blocks[i] << 2*relativeShiftSize; // 00000xxxxx -> xxxxx-----
                newBlock[j] |= shiftOut;
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
                throw new Exception("shifting length longer than the kmer length");
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



            if (leftVacancy ==0){ // left last block is a perfect block
                for (int i =0; i<leftBlocks.length; i++){
                    newBlocks[i]=leftBlocks[i];
                }

                newBlocks[leftBlocks.length-1] &= (~0L<<2); // remove the last block's C marker

                for (int j=leftBlocks.length;j<combinedBlockSize;j++){
                    newBlocks[j]=rightBlocks[j];
                }
            }else{
                long[] shiftOutBlocks = leftShiftOutFromArray(rightBlocks, leftVacancy); // right shift out for the left. here we only expect one block, because leftVacancy is relative to one block
                for (int i =0; i<leftBlocks.length; i++){
                    newBlocks[i]=leftBlocks[i];
                }

                newBlocks[leftBlocks.length-1] &= (~0L<<2*(leftVacancy+1)); // leftVacancy = 32-leftRelativeNTLength-1. This is to remove the C marker
                newBlocks[leftBlocks.length-1] |= (shiftOutBlocks[0]>>> 2*(leftRelativeNTLength));
                newBlocks[leftBlocks.length-1] &= (~0L<<2); // remove shift out blockls C marker

                long[] rightBlocksLeftShifted = leftShiftArray(rightBlocks, leftVacancy);

                int k=0; // rightBlocksLeftShifted index
                for (int j=leftBlocks.length;j<combinedBlockSize;j++){ // including the last blocks.
                    newBlocks[j]=rightBlocksLeftShifted[k];
                    k++;
                }

            }
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
                    } else {
                        lastTwoBits = (Long) kmerBinarySeq.apply(RCindex / 31) >>> 2 * (32 - (RCindex % 31) - 1);
                        lastTwoBits &= 3L;
                        lastTwoBits ^= 3L;
                    }
                    reverseComplement[i / 31] |= lastTwoBits;
                    reverseComplement[i / 31] <<=2;
                }
                reverseComplement[(currentKmerSize-1)/31]|=(1L<<2*(32-currentKmerResidue-1)); // adding ending marker C


                for (int i = 0; i < param.kmerBinarySlotsAssemble; i++) {
                    forwardKmer[i] = (Long) kmerTuple.getSeq(0).apply(i);
                }

                kmerList.add(RowFactory.create(forwardKmer, kmerTuple.getInt(1)));
                kmerList.add(RowFactory.create(reverseComplement, kmerTuple.getInt(1)));

            }

            return kmerList.iterator();
        }

        private int currentKmerResidueFromBlock(Seq binaryBlocks){
            final int suffix0s = Long.numberOfTrailingZeros((Long)binaryBlocks.apply(binaryBlocks.length()-1));
            return Long.SIZE/2 - suffix0s/2;
        }

        private int currentKmerSizeFromBinaryBlock(Seq binaryBlocks){
            int kmerSize;
            int blockSize = binaryBlocks.length();
            kmerSize= (blockSize-1) *31;
            final int suffix0s = Long.numberOfTrailingZeros((Long) binaryBlocks.apply(blockSize - 1)); // ATCG...01---
            int lastMers = Long.SIZE/2-suffix0s/2;

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

    class DSKmerReverseComplementLong implements MapPartitionsFunction<Row, Row>, Serializable {
        /* a capsule for all Kmers and reverseComplementKmers */
        List<Row> kmerList = new ArrayList<Row>();
        Long reverseComplement;
        Row kmerTuple;
        Long lastTwoBits;
        Long kmerBinary;


        public Iterator<Row> call(Iterator<Row> s) {


            while (s.hasNext()) {
                kmerTuple = s.next();
                kmerBinary = kmerTuple.getLong(0);
                reverseComplement = 0L;
                for (int i = 0; i < param.kmerSize; i++) {
                    reverseComplement <<= 2;

                    lastTwoBits = kmerBinary & 3L ^ 3L;
                    kmerBinary >>>= 2;
                    reverseComplement |= lastTwoBits;
                }

                kmerList.add(RowFactory.create(kmerTuple.getLong(0), (int) kmerTuple.getLong(1)));
                kmerList.add(RowFactory.create(reverseComplement, (int) kmerTuple.getLong(1)));
            }

            return kmerList.iterator();
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
                nucleotideBinarySlot[param.kmerListHash.get(currentKmerSize)] |= kmerEndMark; // param.kmerListHash.get(currentKmerSize)] == currentKmerBlockSize

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

    class ReverseComplementKmerBinaryExtractionFromDataset64 implements MapPartitionsFunction<String, Row>, Serializable{
        long maxKmerBits= ~((~0L) << (2*param.kmerSizeResidue));

        List<Row> kmerList = new ArrayList<Row>();
        int readLength;
        String[] units;
        String read;
        char nucleotide;
        long nucleotideInt;
        long nucleotideIntComplement;

        public Iterator<Row> call(Iterator<String> s){

            while (s.hasNext()) {
                units = s.next().split("\\n");
                read = units[1];
                readLength = read.length();


                if (readLength - param.kmerSize - param.endClip +1 <= 0 || param.frontClip > readLength) {
                    continue;
                }

                Long nucleotideBinary = 0L;
                Long nucleotideBinaryReverseComplement = 0L;
                long[] nucleotideBinarySlot = new long[param.kmerBinarySlots];
                long[] nucleotideBinaryReverseComplementSlot = new long[param.kmerBinarySlots];

                for (int i = param.frontClip; i < readLength - param.endClip; i++) {
                    nucleotide = read.charAt(i);
                    if (nucleotide >= 256) nucleotide = 255;
                    nucleotideInt = nucleotideValue(nucleotide);

                    // forward kmer in bits
                    if (i - param.frontClip <= param.kmerSize-1) {
                        nucleotideBinary <<= 2;
                        nucleotideBinary |= nucleotideInt;

                        if ((i - param.frontClip+1) % 32 == 0) { // each 32 nucleotides fill a slot
                            nucleotideBinarySlot[(i - param.frontClip+1) / 32 - 1] = nucleotideBinary;
                            nucleotideBinary = 0L;
                        }

                        if (i - param.frontClip == param.kmerSize-1) { // start completing the first kmer
                            nucleotideBinary &= maxKmerBits;
                            nucleotideBinarySlot[(i - param.frontClip+1) / 32] = nucleotideBinary; // (i-param.frontClip+1)/32 == nucleotideBinarySlot.length -1
                            nucleotideBinary = 0L;

                            // reverse complement

                        }
                    }else{
                        // the last block, which is shorter than 32 mer
                        Long transitBit1 = nucleotideBinarySlot[param.kmerBinarySlots-1] >>> 2*(param.kmerSizeResidue-1) ;  // 0000**----------  -> 000000000000**
                        // for the next block
                        Long transitBit2; // for the next block

                        // update the last block of kmer binary array
                        nucleotideBinarySlot[param.kmerBinarySlots-1] <<= 2;    // 0000-------------  -> 00------------00
                        nucleotideBinarySlot[param.kmerBinarySlots-1] |= nucleotideInt;  // 00------------00  -> 00------------**
                        nucleotideBinarySlot[param.kmerBinarySlots-1] &= maxKmerBits; // 00------------**  -> 0000----------**

                        // the rest
                        for (int j = param.kmerBinarySlots-2; j >=0; j--) {
                            transitBit2 = nucleotideBinarySlot[j] >>> (2*31);   // **---------------  -> 0000000000000**
                            nucleotideBinarySlot[j] <<=2;    // ---------------  -> --------------00
                            nucleotideBinarySlot[j] |= transitBit1;  // -------------00 -> -------------**
                            transitBit1= transitBit2;
                        }
                    }

                    // reverse kmer binarizationalitivities :) non English native speaking people making fun of English
                    nucleotideIntComplement = nucleotideInt ^ 3;  // 3 is binary 11; complement: 11(T) to 00(A), 10(G) to 01(C)

                    if (i - param.frontClip <= param.kmerSize -1){
                        if (i-param.frontClip < param.kmerSizeResidue-1){
                            nucleotideIntComplement <<=2 * (i-param.frontClip);   //
                            nucleotideBinaryReverseComplement |= nucleotideIntComplement;
                        }else if (i-param.frontClip == param.kmerSizeResidue-1){
                            nucleotideIntComplement <<=2 * (i-param.frontClip);
                            nucleotideBinaryReverseComplement |= nucleotideIntComplement;
                            nucleotideBinaryReverseComplementSlot[param.kmerBinarySlots-1] = nucleotideBinaryReverseComplement; // param.kmerBinarySlot-1 = nucleotideBinaryReverseComplementSlot.length -1
                            nucleotideBinaryReverseComplement =0L;

                            /**
                             * param.kmerSizeResidue is the last block length;
                             * i-param.frontClip is the index of the nucleotide on the sequence;
                             * +1 change index to length
                             */
                        }else if ((i- param.frontClip-param.kmerSizeResidue +1) % 32 ==0){  //

                            nucleotideIntComplement <<= 2 * ((i - param.frontClip-param.kmerSizeResidue) % 32); // length (i- param.frontClip-param.kmerSizeResidue +1) -1 shift
                            nucleotideBinaryReverseComplement |= nucleotideIntComplement;

                            // filling the blocks in a reversed order
                            nucleotideBinaryReverseComplementSlot[param.kmerBinarySlots - ((i- param.frontClip-param.kmerSizeResidue +1)/32) -1]= nucleotideBinaryReverseComplement;
                            nucleotideBinaryReverseComplement=0L;
                        } else{
                            nucleotideIntComplement <<= 2 * ((i - param.frontClip-param.kmerSizeResidue) % 32); // length (i- param.frontClip-param.kmerSizeResidue +1) -1 shift
                            nucleotideBinaryReverseComplement |= nucleotideIntComplement;
                        }
                    }else {
                        // the first transition bit from the first block
                        long transitBit1 = nucleotideBinaryReverseComplementSlot[0] << 2*31;
                        long transitBit2;

                        nucleotideBinaryReverseComplementSlot[0] >>>= 2;
                        nucleotideIntComplement <<= 2*31;
                        nucleotideBinaryReverseComplementSlot[0] |= nucleotideIntComplement;

                        for (int j=1; j<param.kmerBinarySlots-1; j++){
                            transitBit2 = nucleotideBinaryReverseComplementSlot[j] << 2*31;
                            nucleotideBinaryReverseComplementSlot[j] >>>= 2;
                            // transitBit1 <<= 2*31;
                            nucleotideBinaryReverseComplementSlot[j] |= transitBit1;
                            transitBit1 = transitBit2;
                        }

                        nucleotideBinaryReverseComplementSlot[param.kmerBinarySlots-1] >>>= 2;
                        transitBit1 >>>= 2*(31-param.kmerSizeResidue+1);
                        nucleotideBinaryReverseComplementSlot[param.kmerBinarySlots-1] |= transitBit1;
                    }

                    /*
                    if (i - param.frontClip >= param.kmerSize) {
                        nucleotideBinaryReverseComplement >>>= 2;
                        nucleotideIntComplement <<= 2 * (param.kmerSize - 1);
                    } else {
                        nucleotideIntComplement <<= 2 * (i - param.frontClip);
                    }
                    nucleotideBinaryReverseComplement |= nucleotideIntComplement;
*/
                    // reach the first complete K-mer
                    if (i - param.frontClip >= param.kmerSize - 1) {




                        if (compareLongArrayBlocks(nucleotideBinarySlot, nucleotideBinaryReverseComplementSlot) == true) {
                            // System.out.println(nucleotideBinarySlot[0] + " forward " + nucleotideBinarySlot[1] + " rc " + nucleotideBinaryReverseComplementSlot[0]);

                            long[] nucleotideBinarySlotPreRow = new long[param.kmerBinarySlots];
                            for (int j=0; j<nucleotideBinarySlot.length; j++){
                                nucleotideBinarySlotPreRow[j] = nucleotideBinarySlot[j];
                            }
                            kmerList.add(RowFactory.create(nucleotideBinarySlotPreRow, 1));  // the number does not matter, as the count is based on units
                        } else {
                            //  System.out.println(nucleotideBinaryReverseComplementSlot[0] + " RC " + nucleotideBinaryReverseComplementSlot[1] + " forward " + nucleotideBinarySlot[0]);

                            long[] nucleotideBinaryReverseComplementSlotPreRow = new long[param.kmerBinarySlots];
                            for (int j=0; j<nucleotideBinarySlot.length; j++){
                                nucleotideBinaryReverseComplementSlotPreRow[j] = nucleotideBinaryReverseComplementSlot[j];
                            }
                            kmerList.add(RowFactory.create(nucleotideBinaryReverseComplementSlotPreRow, 1));
                        }
                    }
                }
            }

            return kmerList.iterator();
        }

        private boolean compareLongArrayBlocks(long[] forward, long[] reverse){
            for (int i=0; i<forward.length; i++){

                // binary comparison from left to right, because of signed long
                if (i<forward.length-1) {
                    for (int j = 0; j < 32; j++) {
                        long shiftedBinary1 = forward[i] >>> (2 * (31 - j));
                        shiftedBinary1 &= 3L;
                        long shiftedBinary2 = reverse[i] >>> (2 * (31 - j));
                        shiftedBinary2 &= 3L;

                        if (shiftedBinary1 < shiftedBinary2) {
                            return true;
                        } else if (shiftedBinary1 > shiftedBinary2) {
                            return false;
                        }
                    }
                }else{
                    for (int j = 0; j < param.kmerSizeResidue; j++) {
                        long shiftedBinary1 = forward[i] >>> (2 * (param.kmerSizeResidue -1 - j));
                        shiftedBinary1 &= 3L;
                        long shiftedBinary2 = reverse[i] >>> (2 * (param.kmerSizeResidue -1 - j));
                        shiftedBinary2 &= 3L;

                        if (shiftedBinary1 < shiftedBinary2) {
                            return true;
                        } else if (shiftedBinary1 > shiftedBinary2) {
                            return false;
                        }
                    }
                }
            }

            // should not happen
            return true;
        }

        // for testing, remove afterwards
        private char BinaryToNucleotide (Long twoBits){
            char nucleotide;
            if (twoBits == 0L){
                nucleotide = 'A';
            }else if (twoBits == 1L){
                nucleotide = 'C';
            }else if (twoBits == 2L){
                nucleotide = 'G';
            }else{
                nucleotide = 'T';
            }
            return nucleotide;
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

        private boolean compareLongArray (Long[] a, Long[] b){

            return true;
        }

        private Long[] shiftLongArrayBinary (Long[] previousKmer){
            return previousKmer;
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
