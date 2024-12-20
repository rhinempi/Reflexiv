package uni.bielefeld.cmg.reflexiv.pipeline;


import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import uni.bielefeld.cmg.reflexiv.util.DefaultParam;
import uni.bielefeld.cmg.reflexiv.util.InfoDumper;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.first;

import java.io.*;
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
 * Returns an object for running the Reflexiv main pipeline.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
 */
public class ReflexivDSMain implements Serializable{
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

    private SparkSession setSparkSessionConfiguration(int shufflePartitions){
        SparkSession spark = SparkSession
                .builder()
                .appName("Reflexiv")
                .config("spark.kryo.registrator", "uni.bielefeld.cmg.reflexiv.serializer.SparkKryoRegistrator")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.sql.shuffle.partitions", shufflePartitions)
                .getOrCreate();

        return spark;
    }

    private Hashtable<Long, Integer> SubKmerProbRowToHash(Row[] s){
        Hashtable<Long, Integer> ProbHash = new Hashtable<Long, Integer>();
        for (int i =0; i<s.length;i++){
            Long Key = s[i].getLong(0);
            Integer Value = s[i].getInt(1);
            ProbHash.put(Key, Value);
        }

        return ProbHash;
    }

    /**
     *
     */
    public void assembly(){
        SparkSession spark = setSparkSessionConfiguration(param.shufflePartition);


        info.readMessage("Initiating Spark context ...");
        info.screenDump();
        info.readMessage("Start Spark framework");
        info.screenDump();

        Dataset<String> FastqDS;
        Dataset<Long> KmerBinaryDS;

        Dataset<Row> KmerBinaryCountLongDS;
        Dataset<Row> KmerBinaryCountDS;
        StructType kmerCountTupleStruct = new StructType();
        kmerCountTupleStruct= kmerCountTupleStruct.add("kmer", DataTypes.LongType, false);
        kmerCountTupleStruct= kmerCountTupleStruct.add("count", DataTypes.IntegerType, false);
        ExpressionEncoder<Row> KmerBinaryCountEncoder = RowEncoder.apply(kmerCountTupleStruct);

        Dataset<Row> ReflexivSubKmerDS;
        StructType ReflexivKmerStruct = new StructType();
        ReflexivKmerStruct= ReflexivKmerStruct.add("k-1", DataTypes.LongType, false);
        ReflexivKmerStruct= ReflexivKmerStruct.add("reflection", DataTypes.IntegerType, false);
        ReflexivKmerStruct= ReflexivKmerStruct.add("extension", DataTypes.LongType, false);
        ReflexivKmerStruct= ReflexivKmerStruct.add("left", DataTypes.IntegerType, false);
        ReflexivKmerStruct= ReflexivKmerStruct.add("right", DataTypes.IntegerType, false);
        ExpressionEncoder<Row> ReflexivSubKmerEncoder = RowEncoder.apply(ReflexivKmerStruct);

        Dataset<Row> ReflexivSubKmerStringDS;
        StructType ReflexivKmerStringStruct = new StructType();
        ReflexivKmerStringStruct= ReflexivKmerStringStruct.add("k-1", DataTypes.StringType, false);
        ReflexivKmerStringStruct= ReflexivKmerStringStruct.add("reflection", DataTypes.IntegerType, false);
        ReflexivKmerStringStruct= ReflexivKmerStringStruct.add("extension", DataTypes.StringType, false);
        ReflexivKmerStringStruct= ReflexivKmerStringStruct.add("left", DataTypes.IntegerType, false);
        ReflexivKmerStringStruct= ReflexivKmerStringStruct.add("right", DataTypes.IntegerType, false);
        ExpressionEncoder<Row> ReflexivKmerStringEncoder = RowEncoder.apply(ReflexivKmerStringStruct);

        Dataset<Row> ReflexivLongSubKmerDS;
        StructType ReflexivLongKmerStruct = new StructType();
        ReflexivLongKmerStruct= ReflexivLongKmerStruct.add("k-1", DataTypes.LongType, false);
        ReflexivLongKmerStruct= ReflexivLongKmerStruct.add("reflection", DataTypes.IntegerType, false);
        ReflexivLongKmerStruct= ReflexivLongKmerStruct.add("extension", DataTypes.createArrayType(DataTypes.LongType), false);
        ReflexivLongKmerStruct= ReflexivLongKmerStruct.add("left", DataTypes.IntegerType, false);
        ReflexivLongKmerStruct= ReflexivLongKmerStruct.add("right", DataTypes.IntegerType, false);
        ExpressionEncoder<Row> ReflexivLongKmerEncoder = RowEncoder.apply(ReflexivLongKmerStruct);

        Dataset<Row> ReflexivLongSubKmerStringDS;
        StructType ReflexivLongKmerStringStruct = new StructType();
        ReflexivLongKmerStringStruct= ReflexivLongKmerStringStruct.add("k-1", DataTypes.StringType, false);
        ReflexivLongKmerStringStruct= ReflexivLongKmerStringStruct.add("reflection", DataTypes.IntegerType, false);
        ReflexivLongKmerStringStruct= ReflexivLongKmerStringStruct.add("extension", DataTypes.StringType, false);
        ReflexivLongKmerStringStruct= ReflexivLongKmerStringStruct.add("left", DataTypes.IntegerType, false);
        ReflexivLongKmerStringStruct= ReflexivLongKmerStringStruct.add("right", DataTypes.IntegerType, false);
        ExpressionEncoder<Row> ReflexivLongKmerStringEncoder = RowEncoder.apply(ReflexivLongKmerStringStruct);

        Dataset<Row> ContigRows;
        StructType ContigLongKmerStringStruct = new StructType();
        ContigLongKmerStringStruct= ContigLongKmerStringStruct.add("ID", DataTypes.StringType, false);
        ContigLongKmerStringStruct= ContigLongKmerStringStruct.add("contig", DataTypes.StringType, false);
        ExpressionEncoder<Row> ContigStringEncoder = RowEncoder.apply(ContigLongKmerStringStruct);

        JavaRDD<Row> ContigRowsRDD;
        JavaPairRDD<Row, Long> ContigsRDDIndex;
        JavaRDD<String> ContigRDD;

        FastqDS = spark.read().text(param.inputFqPath).as(Encoders.STRING());

        DSFastqFilterWithQual DSFastqFilter = new DSFastqFilterWithQual();
        FastqDS = FastqDS.map(DSFastqFilter, Encoders.STRING());

        DSFastqUnitFilter FilterDSUnit = new DSFastqUnitFilter();

        FastqDS = FastqDS.filter(FilterDSUnit);

        if (param.partitions > 0) {
            FastqDS = FastqDS.repartition(param.partitions);
        }
        if (param.cache) {
            FastqDS.cache();
        }

        ReverseComplementKmerBinaryExtractionFromDataset DSExtractRCKmerBinaryFromFastq = new ReverseComplementKmerBinaryExtractionFromDataset();
        KmerBinaryDS = FastqDS.mapPartitions(DSExtractRCKmerBinaryFromFastq, Encoders.LONG());

        KmerBinaryCountLongDS = KmerBinaryDS.groupBy("value")
                .count()
                .toDF("kmer","count");

        KmerBinaryCountLongDS = KmerBinaryCountLongDS.filter(col("count")
                .geq(param.minKmerCoverage)
                .and(col("count")
                        .leq(param.maxKmerCoverage)
                )
        );

        /**
         * Extract reverse complementary kmer
         */
        DSKmerReverseComplementLong DSRCKmer = new DSKmerReverseComplementLong();
        KmerBinaryCountDS = KmerBinaryCountLongDS.mapPartitions(DSRCKmer, KmerBinaryCountEncoder);

        /**
         * Extract forward sub kmer
         */

        DSForwardSubKmerExtraction DSextractForwardSubKmer = new DSForwardSubKmerExtraction();
        ReflexivSubKmerDS = KmerBinaryCountDS.mapPartitions(DSextractForwardSubKmer, ReflexivSubKmerEncoder);

        if (param.bubble == true) {
            ReflexivSubKmerDS = ReflexivSubKmerDS.sort("k-1");
            if (param.minErrorCoverage == 0) {
                DSFilterForkSubKmer DShighCoverageSelector = new DSFilterForkSubKmer();
                ReflexivSubKmerDS = ReflexivSubKmerDS.mapPartitions(DShighCoverageSelector, ReflexivSubKmerEncoder);
            }else {
                DSFilterForkSubKmerWithErrorCorrection DShighCoverageErrorRemovalSelector = new DSFilterForkSubKmerWithErrorCorrection();
                ReflexivSubKmerDS = ReflexivSubKmerDS.mapPartitions(DShighCoverageErrorRemovalSelector, ReflexivSubKmerEncoder);
            }

            DSReflectedSubKmerExtractionFromForward DSreflectionExtractor = new DSReflectedSubKmerExtractionFromForward();
            ReflexivSubKmerDS = ReflexivSubKmerDS.mapPartitions(DSreflectionExtractor, ReflexivSubKmerEncoder);

            ReflexivSubKmerDS = ReflexivSubKmerDS.sort("k-1");
            if (param.minErrorCoverage == 0) {
                DSFilterForkReflectedSubKmer DShighCoverageReflectedSelector = new DSFilterForkReflectedSubKmer();
                ReflexivSubKmerDS = ReflexivSubKmerDS.mapPartitions(DShighCoverageReflectedSelector, ReflexivSubKmerEncoder);
            }else{
                DSFilterForkReflectedSubKmerWithErrorCorrection DShighCoverageReflectedErrorRemovalSelector =new DSFilterForkReflectedSubKmerWithErrorCorrection();
                ReflexivSubKmerDS = ReflexivSubKmerDS.mapPartitions(DShighCoverageReflectedErrorRemovalSelector, ReflexivSubKmerEncoder);
            }

        }

        /**
         *
         */
        DSkmerRandomReflection DSrandomizeSubKmer = new DSkmerRandomReflection();
        ReflexivSubKmerDS = ReflexivSubKmerDS.mapPartitions(DSrandomizeSubKmer, ReflexivSubKmerEncoder);

        ReflexivSubKmerDS = ReflexivSubKmerDS.sort("k-1");

        DSBinaryReflexivKmerToString StringOutputDS = new DSBinaryReflexivKmerToString();

        DSExtendReflexivKmer DSKmerExtention = new DSExtendReflexivKmer();
        ReflexivSubKmerDS = ReflexivSubKmerDS.mapPartitions(DSKmerExtention, ReflexivSubKmerEncoder);


        int iterations = 0;
        for (int i =1; i<4; i++){
            iterations++;
            ReflexivSubKmerDS = ReflexivSubKmerDS.sort("k-1");
            ReflexivSubKmerDS = ReflexivSubKmerDS.mapPartitions(DSKmerExtention, ReflexivSubKmerEncoder);
        }

        ReflexivSubKmerDS = ReflexivSubKmerDS.sort("k-1");
 //       ReflexivSubKmerDS.cache();

        iterations++;

        /**
         * Extract Long sub kmer
         */


        DSExtendReflexivKmerToArrayFirstTime DSKmerExtentionToArrayFirst = new DSExtendReflexivKmerToArrayFirstTime();
        ReflexivLongSubKmerDS = ReflexivSubKmerDS.mapPartitions(DSKmerExtentionToArrayFirst, ReflexivLongKmerEncoder);
        ReflexivLongSubKmerDS.cache();

        DSExtendReflexivKmerToArrayLoop DSKmerExtenstionArrayToArray = new DSExtendReflexivKmerToArrayLoop();

        DSBinaryReflexivKmerArrayToString DSArrayStringOutput = new DSBinaryReflexivKmerArrayToString();

  //      ReflexivSubKmerDS.unpersist();
        int partitionNumber = ReflexivLongSubKmerDS.toJavaRDD().getNumPartitions();
        long contigNumber = 0;
        while (iterations <= param.maximumIteration) {
            iterations++;
            if (iterations >= param.minimumIteration){
                if (iterations % 3 == 0) {

                    /**
                     *  problem ------------------------------------------v
                     */
                    ReflexivLongSubKmerDS.cache();
                    long currentContigNumber = ReflexivLongSubKmerDS.count();
                    if (contigNumber == currentContigNumber) {
                        break;
                    } else {
                        contigNumber = currentContigNumber;
                    }

                    if (partitionNumber >= 16) {
                        if (currentContigNumber / partitionNumber <= 20) {
                            partitionNumber = partitionNumber / 4 + 1;
                            ReflexivLongSubKmerDS = ReflexivLongSubKmerDS.coalesce(partitionNumber);
                        }
                    }
                }
            }

            ReflexivLongSubKmerDS = ReflexivLongSubKmerDS.sort("k-1");

            ReflexivLongSubKmerDS = ReflexivLongSubKmerDS.mapPartitions(DSKmerExtenstionArrayToArray, ReflexivLongKmerEncoder);

        }

        /**
         *
         */
        ReflexivLongSubKmerStringDS = ReflexivLongSubKmerDS.mapPartitions(DSArrayStringOutput, ReflexivLongKmerStringEncoder);

        /**
         *
         */

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

        ContigRDD.saveAsTextFile(param.outputPath);

        spark.stop();
    }

    /**
     *
     */
    public void assemblyFromKmer(){
        SparkSession spark = setSparkSessionConfiguration(param.shufflePartition);
        SparkConf conf = setSparkConfiguration();
        SparkContext sc = spark.sparkContext();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc);

        spark.sparkContext().setCheckpointDir("/tmp");

        info.readMessage("Initiating Spark context ...");
        info.screenDump();
        info.readMessage("Start Spark framework");
        info.screenDump();

        Dataset<Row> KmerCountDS;
        Dataset<String> FastqDS;

        Dataset<Row> KmerBinaryCountDS;
        StructType kmerCountTupleStruct = new StructType();
        kmerCountTupleStruct= kmerCountTupleStruct.add("kmer", DataTypes.LongType, false);
        kmerCountTupleStruct= kmerCountTupleStruct.add("count", DataTypes.IntegerType, false);
        ExpressionEncoder<Row> KmerBinaryCountEncoder = RowEncoder.apply(kmerCountTupleStruct);

        Dataset<Row> ReflexivSubKmerDS;
        StructType ReflexivKmerStruct = new StructType();
        ReflexivKmerStruct= ReflexivKmerStruct.add("k-1", DataTypes.LongType, false);
        ReflexivKmerStruct= ReflexivKmerStruct.add("reflection", DataTypes.IntegerType, false);
        ReflexivKmerStruct= ReflexivKmerStruct.add("extension", DataTypes.LongType, false);
        ReflexivKmerStruct= ReflexivKmerStruct.add("left", DataTypes.IntegerType, false);
        ReflexivKmerStruct= ReflexivKmerStruct.add("right", DataTypes.IntegerType, false);
        ExpressionEncoder<Row> ReflexivSubKmerEncoder = RowEncoder.apply(ReflexivKmerStruct);

        Dataset<Row> ReflexivSubKmerStringDS;
        StructType ReflexivKmerStringStruct = new StructType();
        ReflexivKmerStringStruct= ReflexivKmerStringStruct.add("k-1", DataTypes.StringType, false);
        ReflexivKmerStringStruct= ReflexivKmerStringStruct.add("reflection", DataTypes.IntegerType, false);
        ReflexivKmerStringStruct= ReflexivKmerStringStruct.add("extension", DataTypes.StringType, false);
        ReflexivKmerStringStruct= ReflexivKmerStringStruct.add("left", DataTypes.IntegerType, false);
        ReflexivKmerStringStruct= ReflexivKmerStringStruct.add("right", DataTypes.IntegerType, false);
        ExpressionEncoder<Row> ReflexivKmerStringEncoder = RowEncoder.apply(ReflexivKmerStringStruct);

        Dataset<Row> ReflexivLongSubKmerDS;
        Dataset<Row> ReflexivLongFragmentDS;
        StructType ReflexivLongKmerStruct = new StructType();
        ReflexivLongKmerStruct= ReflexivLongKmerStruct.add("k-1", DataTypes.LongType, false);
        ReflexivLongKmerStruct= ReflexivLongKmerStruct.add("reflection", DataTypes.IntegerType, false);
        ReflexivLongKmerStruct= ReflexivLongKmerStruct.add("extension", DataTypes.createArrayType(DataTypes.LongType), false);
        ReflexivLongKmerStruct= ReflexivLongKmerStruct.add("left", DataTypes.IntegerType, false);
        ReflexivLongKmerStruct= ReflexivLongKmerStruct.add("right", DataTypes.IntegerType, false);
        ExpressionEncoder<Row> ReflexivLongKmerEncoder = RowEncoder.apply(ReflexivLongKmerStruct);

        Dataset<Row> ReflexivLongSubKmerStringDS;
        StructType ReflexivLongKmerStringStruct = new StructType();
        ReflexivLongKmerStringStruct= ReflexivLongKmerStringStruct.add("k-1", DataTypes.StringType, false);
        ReflexivLongKmerStringStruct= ReflexivLongKmerStringStruct.add("reflection", DataTypes.IntegerType, false);
        ReflexivLongKmerStringStruct= ReflexivLongKmerStringStruct.add("extension", DataTypes.StringType, false);
        ReflexivLongKmerStringStruct= ReflexivLongKmerStringStruct.add("left", DataTypes.IntegerType, false);
        ReflexivLongKmerStringStruct= ReflexivLongKmerStringStruct.add("right", DataTypes.IntegerType, false);
        ExpressionEncoder<Row> ReflexivLongKmerStringEncoder = RowEncoder.apply(ReflexivLongKmerStringStruct);

        Dataset<Row> ContigProbRows;
        StructType ReflexivSubKmerProbStruct = new StructType();
        ReflexivSubKmerProbStruct = ReflexivSubKmerProbStruct.add("subkmerBinary", DataTypes.LongType, false);
        ReflexivSubKmerProbStruct = ReflexivSubKmerProbStruct.add("marker", DataTypes.IntegerType, false);
        ExpressionEncoder<Row> ReflexivSubKmerProbEncoder = RowEncoder.apply(ReflexivSubKmerProbStruct);


        Dataset<Row> ContigRows;
        StructType ContigLongKmerStringStruct = new StructType();
        ContigLongKmerStringStruct= ContigLongKmerStringStruct.add("ID", DataTypes.StringType, false);
        ContigLongKmerStringStruct= ContigLongKmerStringStruct.add("contig", DataTypes.StringType, false);
        ExpressionEncoder<Row> ContigStringEncoder = RowEncoder.apply(ContigLongKmerStringStruct);

        JavaRDD<Row> ContigRowsRDD;
        JavaPairRDD<Row, Long> ContigsRDDIndex;
        JavaRDD<String> ContigRDD;

        /**
         * loading Kmer counts
         */
        KmerCountDS = spark.read().csv(param.inputKmerPath);

        if (param.partitions > 0) {
            KmerCountDS = KmerCountDS.repartition(param.partitions);
        }


        /**
         * Transforming kmer string to binary kmer
         */
        KmerBinarizer DSBinarizer = new KmerBinarizer();
        KmerBinaryCountDS = KmerCountDS.mapPartitions(DSBinarizer, KmerBinaryCountEncoder);

        /**
         * Filter kmer with lower coverage
         */
        KmerBinaryCountDS = KmerBinaryCountDS.filter(col("count")
                .geq(param.minKmerCoverage)
                .and(col("count")
                        .leq(param.maxKmerCoverage)
                )
        );


        if (param.cache) {
            KmerBinaryCountDS.cache();
        }

        /**
         * Extract reverse complementary kmer
         */
        DSKmerReverseComplement DSRCKmer = new DSKmerReverseComplement();
        KmerBinaryCountDS = KmerBinaryCountDS.mapPartitions(DSRCKmer, KmerBinaryCountEncoder);

        /**
         * Extract forward sub kmer
         */


        DSForwardSubKmerExtraction DSextractForwardSubKmer = new DSForwardSubKmerExtraction();
        ReflexivSubKmerDS = KmerBinaryCountDS.mapPartitions(DSextractForwardSubKmer, ReflexivSubKmerEncoder);

        if (param.bubble == true) {
            ReflexivSubKmerDS = ReflexivSubKmerDS.sort("k-1");
            if (param.minErrorCoverage == 0) {
                DSFilterForkSubKmer DShighCoverageSelector = new DSFilterForkSubKmer();
                ReflexivSubKmerDS = ReflexivSubKmerDS.mapPartitions(DShighCoverageSelector, ReflexivSubKmerEncoder);
            }else {
                DSFilterForkSubKmerWithErrorCorrection DShighCoverageErrorRemovalSelector = new DSFilterForkSubKmerWithErrorCorrection();
                ReflexivSubKmerDS = ReflexivSubKmerDS.mapPartitions(DShighCoverageErrorRemovalSelector, ReflexivSubKmerEncoder);
            }

            DSReflectedSubKmerExtractionFromForward DSreflectionExtractor = new DSReflectedSubKmerExtractionFromForward();
            ReflexivSubKmerDS = ReflexivSubKmerDS.mapPartitions(DSreflectionExtractor, ReflexivSubKmerEncoder);

            ReflexivSubKmerDS = ReflexivSubKmerDS.sort("k-1");
            if (param.minErrorCoverage == 0) {
                DSFilterForkReflectedSubKmer DShighCoverageReflectedSelector = new DSFilterForkReflectedSubKmer();
                ReflexivSubKmerDS = ReflexivSubKmerDS.mapPartitions(DShighCoverageReflectedSelector, ReflexivSubKmerEncoder);
            }else{
                DSFilterForkReflectedSubKmerWithErrorCorrection DShighCoverageReflectedErrorRemovalSelector =new DSFilterForkReflectedSubKmerWithErrorCorrection();
                ReflexivSubKmerDS = ReflexivSubKmerDS.mapPartitions(DShighCoverageReflectedErrorRemovalSelector, ReflexivSubKmerEncoder);
            }

        }

        /**
         *
         */
        DSkmerRandomReflection DSrandomizeSubKmer = new DSkmerRandomReflection();
        ReflexivSubKmerDS = ReflexivSubKmerDS.mapPartitions(DSrandomizeSubKmer, ReflexivSubKmerEncoder);

        ReflexivSubKmerDS = ReflexivSubKmerDS.sort("k-1");

        DSBinaryReflexivKmerToString StringOutputDS = new DSBinaryReflexivKmerToString();


        DSExtendReflexivKmer DSKmerExtention = new DSExtendReflexivKmer();
        ReflexivSubKmerDS = ReflexivSubKmerDS.mapPartitions(DSKmerExtention, ReflexivSubKmerEncoder);


        int iterations = 0;
        for (int i =1; i<4; i++){
            iterations++;
            ReflexivSubKmerDS = ReflexivSubKmerDS.sort("k-1");
            ReflexivSubKmerDS = ReflexivSubKmerDS.mapPartitions(DSKmerExtention, ReflexivSubKmerEncoder);
        }

        ReflexivSubKmerDS = ReflexivSubKmerDS.sort("k-1");
 //       ReflexivSubKmerDS.cache();

        iterations++;

        /**
         * Extract Long sub kmer
         */


        DSExtendReflexivKmerToArrayFirstTime DSKmerExtentionToArrayFirst = new DSExtendReflexivKmerToArrayFirstTime();
        ReflexivLongSubKmerDS = ReflexivSubKmerDS.mapPartitions(DSKmerExtentionToArrayFirst, ReflexivLongKmerEncoder);

        DSExtendReflexivKmerToArrayLoop DSKmerExtenstionArrayToArray = new DSExtendReflexivKmerToArrayLoop();

        DSBinaryReflexivKmerArrayToString DSArrayStringOutput = new DSBinaryReflexivKmerArrayToString();

 //       ReflexivSubKmerDS.unpersist();
        ReflexivLongSubKmerDS.cache();
        int partitionNumber = ReflexivLongSubKmerDS.toJavaRDD().getNumPartitions();
        long contigNumber = 0;
        while (iterations <= param.maximumIteration) {
            iterations++;
            if (iterations >= param.minimumIteration){
                if (iterations % 3 == 0) {

                    /**
                     *  problem ------------------------------------------v
                     */
                    ReflexivLongSubKmerDS.cache();
                    long currentContigNumber = ReflexivLongSubKmerDS.count();
            System.out.println("At iteration: " + iterations + ", the contig number is: " + currentContigNumber);
                    if (contigNumber == currentContigNumber) {
                        break;
                    } else {
                        contigNumber = currentContigNumber;
                    }

                    if (partitionNumber >= 16) {
                        if (currentContigNumber / partitionNumber <= 20) {
                            partitionNumber = partitionNumber / 4 + 1;
                            ReflexivLongSubKmerDS = ReflexivLongSubKmerDS.coalesce(partitionNumber);
                        }
                    }
                }
            }

            ReflexivLongSubKmerDS = ReflexivLongSubKmerDS.sort("k-1");


            ReflexivLongSubKmerDS = ReflexivLongSubKmerDS.mapPartitions(DSKmerExtenstionArrayToArray, ReflexivLongKmerEncoder);

        }

        ReflexivLongSubKmerDS = ReflexivLongSubKmerDS.sort("k-1");

        if (param.stitch==true) {

            ReflexivLongSubKmerDS = ReflexivLongSubKmerDS.localCheckpoint();

            DSLowCoverageSubKmerExtraction lowCoverageSubKmerExtractor = new DSLowCoverageSubKmerExtraction();

            ContigProbRows = ReflexivLongSubKmerDS.mapPartitions(lowCoverageSubKmerExtractor, ReflexivSubKmerProbEncoder);

            Row[] ProbSubKmerArray = (Row[]) ContigProbRows.collect();

            Hashtable<Long, Integer> ProbSubKmerTable = SubKmerProbRowToHash(ProbSubKmerArray);
            final Broadcast<Hashtable<Long, Integer>> ProbSubKmerArrayBroadCast = jsc.broadcast(ProbSubKmerTable);
            DSLowCoverageReadDetection LowCoverageReadDetector = new DSLowCoverageReadDetection(ProbSubKmerArrayBroadCast);


            FastqDS = spark.read().text(param.inputFqPath).as(Encoders.STRING());

            DSFastqFilterWithQual DSFastqFilter = new DSFastqFilterWithQual();
            FastqDS = FastqDS.map(DSFastqFilter, Encoders.STRING());

            DSFastqUnitFilter FilterDSUnit = new DSFastqUnitFilter();

            FastqDS = FastqDS.filter(FilterDSUnit);

            if (param.partitions > 0) {
                FastqDS = FastqDS.repartition(param.partitions);
            }
            if (param.cache) {
                FastqDS.cache();
            }

            ReflexivLongFragmentDS = FastqDS.mapPartitions(LowCoverageReadDetector, ReflexivLongKmerEncoder);
            ReflexivLongFragmentDS = ReflexivLongFragmentDS.cache(); // remove after

            ReflexivLongFragmentDS = ReflexivLongFragmentDS.sort("k-1");
            long beforeFilter = ReflexivLongFragmentDS.count();
            System.out.println("mark before low cover filter: " + beforeFilter);

            DSFilterRepeatLowCoverageFragment repeatLowCoverageFragmentFilter = new DSFilterRepeatLowCoverageFragment();
            ReflexivLongFragmentDS = ReflexivLongFragmentDS.mapPartitions(repeatLowCoverageFragmentFilter, ReflexivLongKmerEncoder);

            ReflexivLongFragmentDS = ReflexivLongFragmentDS.sort("k-1");
            long middleFilter = ReflexivLongFragmentDS.count();
            System.out.println("mark middle low cover filter: " + middleFilter);

            ReflexivLongFragmentDS = ReflexivLongFragmentDS.mapPartitions(repeatLowCoverageFragmentFilter, ReflexivLongKmerEncoder);
            long afterFilter = ReflexivLongFragmentDS.count();
            System.out.println("mark after low cover filter: " + afterFilter);

            ReflexivLongSubKmerDS = ReflexivLongSubKmerDS.union(ReflexivLongFragmentDS);
            ReflexivLongSubKmerDS = ReflexivLongSubKmerDS.sort("k-1");
            ReflexivLongSubKmerDS = ReflexivLongSubKmerDS.mapPartitions(DSKmerExtenstionArrayToArray, ReflexivLongKmerEncoder);

            iterations = 6;

            while (iterations <= param.maximumIteration) {
                iterations++;
                if (iterations >= param.minimumIteration){
                    if (iterations % 3 == 0) {

                        /**
                         *  problem ------------------------------------------v
                         */

                        ReflexivLongSubKmerDS.cache();
                        long currentContigNumber = ReflexivLongSubKmerDS.count();
                        if (contigNumber == currentContigNumber) {
                            break;
                        } else {
                            contigNumber = currentContigNumber;
                        }

                        if (partitionNumber >= 16) {
                            if (currentContigNumber / partitionNumber <= 20) {
                                partitionNumber = partitionNumber / 4 + 1;
                                ReflexivLongSubKmerDS = ReflexivLongSubKmerDS.coalesce(partitionNumber);
                            }
                        }
                    }
                }

                ReflexivLongSubKmerDS = ReflexivLongSubKmerDS.sort("k-1");
                ReflexivLongSubKmerDS = ReflexivLongSubKmerDS.mapPartitions(DSKmerExtenstionArrayToArray, ReflexivLongKmerEncoder);
            }

            ReflexivLongSubKmerDS = ReflexivLongSubKmerDS.sort("k-1");

        }

        /**
         *
         */
        ReflexivLongSubKmerStringDS = ReflexivLongSubKmerDS.mapPartitions(DSArrayStringOutput, ReflexivLongKmerStringEncoder);

        /**
         *
         */
       // DSKmerToContigLength contigLengthDS = new DSKmerToContigLength();
       // ContigLengthRows = ReflexivLongSubKmerStringDS.mapPartitions(contigLengthDS, ContigLengthEncoder);


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

    class DSKmerToContig implements MapPartitionsFunction<Row, Row>, Serializable{

        public Iterator<Row> call (Iterator<Row> sIterator){
            List<Row> contigList = new ArrayList<Row>();

            while (sIterator.hasNext()) {
                Row s = sIterator.next();

                if (s.getInt(3) <= -10000000 && s.getInt(4) <= -10000000) continue;

                if (s.getInt(1) == 1) {
                    String contig = s.getString(0) + s.getString(2);
                    int length = contig.length();
                    if (length >= param.minContig) {
                        String ID = ">Contig-" + length + "-(" + s.getInt(3) + "," + s.getInt(4) + ")";
                        String formatedContig = changeLine(contig, length, 100);
                        contigList.add(RowFactory.create(ID, formatedContig));
                    }
                } else { // (randomReflexivMarker == 2) {
                    String contig = s.getString(2) + s.getString(0);
                    int length = contig.length();
                    if (length >= param.minContig) {
                        String ID = ">Contig-" + length + "-(" + s.getInt(3) + "," + s.getInt(4) + ")";
                        String formatedContig = changeLine(contig, length, 100);
                        contigList.add(RowFactory.create(ID, formatedContig));
                    }
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
     * interface class for RDD implementation, used in step 5
     */

    class DSBinaryReflexivKmerToString implements MapPartitionsFunction<Row, Row>, Serializable{
        List<Row> reflexivKmerStringList = new ArrayList<Row>();

        public Iterator<Row> call(Iterator<Row> sIterator){
            while (sIterator.hasNext()){
                String subKmer = "";
                String subString ="";
                Row s = sIterator.next();
                int currentSuffixLength = Long.SIZE/2 - (Long.numberOfLeadingZeros(s.getLong(2))/2 + 1);
                for (int i=1; i<=param.subKmerSize;i++){
                    Long currentNucleotideBinary = s.getLong(0) >>> 2*(param.subKmerSize - i);
                    currentNucleotideBinary &= 3L;
                    char currentNucleotide =  BinaryToNucleotide(currentNucleotideBinary);
                    subKmer += currentNucleotide;
                }

                for (int i=1; i<=currentSuffixLength; i++){
                    Long currentNucleotideBinary = s.getLong(2) >>> 2*(currentSuffixLength - i);
                    currentNucleotideBinary &= 3L;
                    char currentNucleotide =  BinaryToNucleotide(currentNucleotideBinary);
                    subString += currentNucleotide;
                }

                reflexivKmerStringList.add (
                        RowFactory.create(
                                subKmer, s.getInt(1), subString, s.getInt(3), s.getInt(4))
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
    class DSBinaryReflexivKmerArrayToString implements MapPartitionsFunction<Row, Row>, Serializable{
        List<Row> reflexivKmerStringList = new ArrayList<Row>();

        public Iterator<Row> call(Iterator<Row> sIterator) {
            while (sIterator.hasNext()) {
                String subKmer = "";
                String subString = "";
                Row s = sIterator.next();

                int firstSuffixBlockLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros((Long)s.getSeq(2).apply(0)) / 2 + 1);

                for (int i = 1; i <= param.subKmerSize; i++) {
                    Long currentNucleotideBinary = s.getLong(0) >>> 2 * (param.subKmerSize - i);
                    currentNucleotideBinary &= 3L;
                    char currentNucleotide = BinaryToNucleotide(currentNucleotideBinary);
                    subKmer += currentNucleotide;
                }

                for (int i = 0; i < s.getSeq(2).length(); i++) {
                    if (i == 0) {
                        for (int j = 1; j <= firstSuffixBlockLength; j++) { // j=0 including the C marker; for debug
                            Long currentNucleotideBinary = (Long)s.getSeq(2).apply(i) >>> 2 * (firstSuffixBlockLength - j);
                            currentNucleotideBinary &= 3L;
                            char currentNucleotide = BinaryToNucleotide(currentNucleotideBinary);
                            subString += currentNucleotide;
                        }
                    } else {
                        for (int j = 1; j <= 31; j++) {
                            if (s.getSeq(2).apply(i) == null){
                                continue;
                            }
                            Long currentNucleotideBinary = (Long)s.getSeq(2).apply(i) >>> 2 * (31 - j);
                            currentNucleotideBinary &= 3L;
                            char currentNucleotide = BinaryToNucleotide(currentNucleotideBinary);
                            subString += currentNucleotide;
                        }
                    }
                    //     subString += ">----<";
                }

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

        long maxBlockBinary = ~((~0L) << 2*31); // a block has 31 nucleotide


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
                        if (subKmerSlotComparator(s.getLong(0), tmpReflexivKmerExtendList.get(0).getLong(0))){
                            continue;
                        }else{
                            singleKmerRandomizer(tmpReflexivKmerExtendList.get(0));
                            resetSubKmerGroup(s);
                        }
                    }// end of else condition

                    lineMarker++;
                    // return reflexivKmerConcatList.iterator();
                }
            } // while loop

            if (tmpReflexivKmerExtendList.size() != 0) {
                for (int i = 0; i < tmpReflexivKmerExtendList.size(); i++) {
                    singleKmerRandomizer(tmpReflexivKmerExtendList.get(i));
                }
            }

            //      int kmernumber = reflexivKmerConcatList.size();
            //     System.out.println("Double afterwards extendable number: " + kmernumber);
            return reflexivKmerConcatList.iterator();
        }

        public void singleKmerRandomizer(Row currentSubKmer){
            int blockSize = currentSubKmer.getSeq(2).length();
            Long[] newReflexivLongArray= new Long[blockSize];

            if (currentSubKmer.getInt(1) == 1){
                int firstSuffixBlockLength = Long.SIZE/2 - (Long.numberOfLeadingZeros((Long)currentSubKmer.getSeq(2).apply(0))/2 + 1);
                long maxSuffixLengthBinary = ~((~0L) << (2*firstSuffixBlockLength));
                Long newReflexivSubKmer;
                Long newReflexivLong;

                if (randomReflexivMarker == 2) {
                    if ( blockSize > 1) {
                        newReflexivSubKmer = (Long)currentSubKmer.getSeq(2).apply(blockSize-1) & maxSubKmerBinary;

                        // 3rd block and so on
                        for (int i=blockSize-1; i>1; i--){
                            newReflexivLong=(Long)currentSubKmer.getSeq(2).apply(i) >>> 2*param.subKmerSize;
                            newReflexivLong|= ((Long)currentSubKmer.getSeq(2).apply(i-1) << 2*(31-param.subKmerSize));
                            newReflexivLong&= maxBlockBinary;
                            newReflexivLongArray[i] = newReflexivLong;
                        }

                        // 2nd block
                        newReflexivLong=(Long)currentSubKmer.getSeq(2).apply(1) >>> 2*param.subKmerSize;
                        newReflexivLong|= ((Long)currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary) << 2*(31-param.subKmerSize);
                        if (firstSuffixBlockLength < param.subKmerSize){
                            newReflexivLong |= (currentSubKmer.getLong(0) << 2*(31 - param.subKmerSize + firstSuffixBlockLength));
                        }
                        newReflexivLong&= maxBlockBinary;
                        newReflexivLongArray[1] = newReflexivLong;

                        // 1st block
                        if (firstSuffixBlockLength < param.subKmerSize){
                            newReflexivLong = currentSubKmer.getLong(0) >>> 2*(param.subKmerSize - firstSuffixBlockLength);
                            newReflexivLong |= (1L << 2 * firstSuffixBlockLength);
                        }else {
                            newReflexivLong = (Long)currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary; //remove C marker
                            newReflexivLong >>>= 2 * param.subKmerSize;
                            newReflexivLong |= (currentSubKmer.getLong(0) << 2 * (firstSuffixBlockLength - param.subKmerSize));
                            newReflexivLong |= (1L << 2 * firstSuffixBlockLength); // add C marker
                        }
                        newReflexivLongArray[0] = newReflexivLong;
                    }else{
                        if (firstSuffixBlockLength >= param.subKmerSize){
                            newReflexivSubKmer = (Long)currentSubKmer.getSeq(2).apply(0) & maxSubKmerBinary;

                            newReflexivLong = (Long)currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary;
                            newReflexivLong >>>= 2*param.subKmerSize;
                            newReflexivLong |= (currentSubKmer.getLong(0) << 2*(firstSuffixBlockLength-param.subKmerSize));
                            newReflexivLong |= (1L << (2 * firstSuffixBlockLength)); // add C marker in the front
                        }else {
                            newReflexivSubKmer = currentSubKmer.getLong(0) << (firstSuffixBlockLength * 2);
                            newReflexivSubKmer &= maxSubKmerBinary;
                            newReflexivSubKmer |= ((Long)currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary);

                            newReflexivLong = currentSubKmer.getLong(0) >>> (2 * (param.subKmerSize - firstSuffixBlockLength));
                            newReflexivLong |= (1L << (2 * firstSuffixBlockLength)); // add C marker in the front
                        }

                        newReflexivLongArray[0]=newReflexivLong;
                    }

                    reflexivKmerConcatList.add(
                            RowFactory.create(newReflexivSubKmer,
                                    randomReflexivMarker, newReflexivLongArray, currentSubKmer.getInt(3), currentSubKmer.getInt(4)
                            )
                    );
                }else{

                    reflexivKmerConcatList.add(currentSubKmer);
                }
            }else{ /* currentSubKmer.getInt(1) == 2 */
                int firstPrefixLength = Long.SIZE/2 - (Long.numberOfLeadingZeros((Long)currentSubKmer.getSeq(2).apply(0))/2 + 1);
                long maxPrefixLengthBinary = ~((~0L) << (2*firstPrefixLength));

                Long newReflexivSubKmer;
                Long newReflexivLong;

                if (randomReflexivMarker == 2) {

                    reflexivKmerConcatList.add(currentSubKmer);
                }else{ /* randomReflexivMarker == 1 */
                    if (blockSize > 1){
                        // the subKmer
                        if (firstPrefixLength >= param.subKmerSize) {
                            newReflexivSubKmer = ((Long)currentSubKmer.getSeq(2).apply(0) & maxPrefixLengthBinary) >>> 2* (firstPrefixLength-param.subKmerSize); // also removed C marker
                        }else{
                            newReflexivSubKmer = (Long)currentSubKmer.getSeq(2).apply(0) & maxPrefixLengthBinary; // remove C marker
                            newReflexivSubKmer <<= 2*(param.subKmerSize-firstPrefixLength);
                            newReflexivSubKmer |= ((Long)currentSubKmer.getSeq(2).apply(1) >>> 2*(31- param.subKmerSize + firstPrefixLength));
                        }

                        // the last block
                        newReflexivLong = (Long)currentSubKmer.getSeq(2).apply(blockSize - 1) << 2 * param.subKmerSize;
                        newReflexivLong |= currentSubKmer.getLong(0);
                        newReflexivLong &= maxBlockBinary;
                        newReflexivLongArray[blockSize - 1] = newReflexivLong;

                        // 2nd and so on
                        for (int i=blockSize-2; i>=1;i--){
                            newReflexivLong = (Long)currentSubKmer.getSeq(2).apply(i) << 2*param.subKmerSize;
                            newReflexivLong |= ((Long)currentSubKmer.getSeq(2).apply(i+1) >>> 2*(31- param.subKmerSize));
                            newReflexivLong &=maxBlockBinary;
                            newReflexivLongArray[i] = newReflexivLong;
                        }

                        // 1st
                        newReflexivLong = (Long)currentSubKmer.getSeq(2).apply(1) >>> 2*(31-param.subKmerSize);
                        if (firstPrefixLength >= param.subKmerSize) {
                            newReflexivLong |= (((Long)currentSubKmer.getSeq(2).apply(0) & maxPrefixLengthBinary) << 2 * param.subKmerSize);
                        }
                        newReflexivLong &= maxPrefixLengthBinary;
                        newReflexivLong |= (1L << 2*firstPrefixLength); // add C marker
                        newReflexivLongArray[0] = newReflexivLong;

                    }else{ /* blockSize = 1)*/
                        if (firstPrefixLength >= param.subKmerSize) {
                            newReflexivSubKmer = ((Long)currentSubKmer.getSeq(2).apply(0) & maxPrefixLengthBinary) >>> 2*(firstPrefixLength - param.subKmerSize);
                            newReflexivSubKmer &= maxSubKmerBinary; // remove header, including C marker

                            newReflexivLong = ((Long)currentSubKmer.getSeq(2).apply(0) & maxPrefixLengthBinary) << 2*param.subKmerSize;
                            newReflexivLong |= currentSubKmer.getLong(0);
                            newReflexivLong &= maxPrefixLengthBinary; // remove header, including C marker
                            newReflexivLong |= (1L << 2*firstPrefixLength); // add C marker
                        }else {
                            newReflexivSubKmer = ((Long)currentSubKmer.getSeq(2).apply(0) & maxPrefixLengthBinary) << (2 * (param.subKmerSize - firstPrefixLength));
                            newReflexivSubKmer |= (currentSubKmer.getLong(0) >>> (2 * firstPrefixLength));

                            newReflexivLong = currentSubKmer.getLong(0) & maxPrefixLengthBinary;
                            newReflexivLong |= (1L << 2 * firstPrefixLength); // add C marker in the front
                        }

                        newReflexivLongArray[0]=newReflexivLong;
                    }

                    reflexivKmerConcatList.add(
                            RowFactory.create(newReflexivSubKmer,
                                    randomReflexivMarker, newReflexivLongArray, currentSubKmer.getInt(3), currentSubKmer.getInt(4)
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

        private boolean subKmerSlotComparator(Long a, Long b) {

            if (!a.equals(b)) {
                return false;
            }else{

                return true;
            }
        }
    }

    class DSLowCoverageSubKmerExtraction implements  MapPartitionsFunction<Row, Row>, Serializable{
        List<Row> subKmerProb = new ArrayList<Row>();
        int randomReflexivMarker =1;
        long maxSubKmerBinary = ~((~0L) << 2*param.subKmerSize);
        long maxBlockBinary = ~((~0L) << 2*31); // a block has 31 nucleotide
        int contigIndex = 0;

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
                        long subKmerBinary = s.getLong(0);
                        int marker = (contigIndex | (1<<31));  // 1 as right extendable
                        subKmerProb.add(
                                RowFactory.create(subKmerBinary, marker)
                        );
                    }else{
                        randomReflexivMarker =1;
                        Row newS = singleKmerRandomizer(s);
                        long subKmerBinary = newS.getLong(0);
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
                        long subKmerBinary  = newS.getLong(0);
                        int marker = contigIndex ; // 0 as left extendable
                        subKmerProb.add(
                                RowFactory.create(subKmerBinary, marker)
                        );
                    }else {
                        long subKmerBinary = s.getLong(0);
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

        public Row singleKmerRandomizer(Row currentSubKmer){
            int blockSize = currentSubKmer.getSeq(2).length();
            Long[] newReflexivLongArray= new Long[blockSize];

            if (currentSubKmer.getInt(1) == 1){
                int firstSuffixBlockLength = Long.SIZE/2 - (Long.numberOfLeadingZeros((Long)currentSubKmer.getSeq(2).apply(0))/2 + 1);
                long maxSuffixLengthBinary = ~((~0L) << (2*firstSuffixBlockLength));
                Long newReflexivSubKmer;
                Long newReflexivLong;

                if (randomReflexivMarker == 2) {
                    if ( blockSize > 1) {
                        newReflexivSubKmer = (Long)currentSubKmer.getSeq(2).apply(blockSize-1) & maxSubKmerBinary;

                        // 3rd block and so on
                        for (int i=blockSize-1; i>1; i--){
                            newReflexivLong=(Long)currentSubKmer.getSeq(2).apply(i) >>> 2*param.subKmerSize;
                            newReflexivLong|= ((Long)currentSubKmer.getSeq(2).apply(i-1) << 2*(31-param.subKmerSize));
                            newReflexivLong&= maxBlockBinary;
                            newReflexivLongArray[i] = newReflexivLong;
                        }

                        // 2nd block
                        newReflexivLong=(Long)currentSubKmer.getSeq(2).apply(1) >>> 2*param.subKmerSize;
                        newReflexivLong|= ((Long)currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary) << 2*(31-param.subKmerSize);
                        if (firstSuffixBlockLength < param.subKmerSize){
                            newReflexivLong |= (currentSubKmer.getLong(0) << 2*(31 - param.subKmerSize + firstSuffixBlockLength));
                        }
                        newReflexivLong&= maxBlockBinary;
                        newReflexivLongArray[1] = newReflexivLong;

                        // 1st block
                        if (firstSuffixBlockLength < param.subKmerSize){
                            newReflexivLong = currentSubKmer.getLong(0) >>> 2*(param.subKmerSize - firstSuffixBlockLength);
                            newReflexivLong |= (1L << 2 * firstSuffixBlockLength);
                        }else {
                            newReflexivLong = (Long)currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary; //remove C marker
                            newReflexivLong >>>= 2 * param.subKmerSize;
                            newReflexivLong |= (currentSubKmer.getLong(0) << 2 * (firstSuffixBlockLength - param.subKmerSize));
                            newReflexivLong |= (1L << 2 * firstSuffixBlockLength); // add C marker
                        }
                        newReflexivLongArray[0] = newReflexivLong;
                    }else{
                        if (firstSuffixBlockLength >= param.subKmerSize){
                            newReflexivSubKmer = (Long)currentSubKmer.getSeq(2).apply(0) & maxSubKmerBinary;

                            newReflexivLong = (Long)currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary;
                            newReflexivLong >>>= 2*param.subKmerSize;
                            newReflexivLong |= (currentSubKmer.getLong(0) << 2*(firstSuffixBlockLength-param.subKmerSize));
                            newReflexivLong |= (1L << (2 * firstSuffixBlockLength)); // add C marker in the front
                        }else {
                            newReflexivSubKmer = currentSubKmer.getLong(0) << (firstSuffixBlockLength * 2);
                            newReflexivSubKmer &= maxSubKmerBinary;
                            newReflexivSubKmer |= ((Long)currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary);

                            newReflexivLong = currentSubKmer.getLong(0) >>> (2 * (param.subKmerSize - firstSuffixBlockLength));
                            newReflexivLong |= (1L << (2 * firstSuffixBlockLength)); // add C marker in the front
                        }

                        newReflexivLongArray[0]=newReflexivLong;
                    }


                    return        RowFactory.create(newReflexivSubKmer,
                                    randomReflexivMarker, newReflexivLongArray, currentSubKmer.getInt(3), currentSubKmer.getInt(4)
                            );
                }else{

                    return currentSubKmer;
                }
            }else{ /* currentSubKmer.getInt(1) == 2 */
                int firstPrefixLength = Long.SIZE/2 - (Long.numberOfLeadingZeros((Long)currentSubKmer.getSeq(2).apply(0))/2 + 1);
                long maxPrefixLengthBinary = ~((~0L) << (2*firstPrefixLength));

                Long newReflexivSubKmer;
                Long newReflexivLong;

                if (randomReflexivMarker == 2) {

                    return currentSubKmer;
                }else{ /* randomReflexivMarker == 1 */
                    if (blockSize > 1){
                        // the subKmer
                        if (firstPrefixLength >= param.subKmerSize) {
                            newReflexivSubKmer = ((Long)currentSubKmer.getSeq(2).apply(0) & maxPrefixLengthBinary) >>> 2* (firstPrefixLength-param.subKmerSize); // also removed C marker
                        }else{
                            newReflexivSubKmer = (Long)currentSubKmer.getSeq(2).apply(0) & maxPrefixLengthBinary; // remove C marker
                            newReflexivSubKmer <<= 2*(param.subKmerSize-firstPrefixLength);
                            newReflexivSubKmer |= ((Long)currentSubKmer.getSeq(2).apply(1) >>> 2*(31- param.subKmerSize + firstPrefixLength));
                        }

                        // the last block
                        newReflexivLong = (Long)currentSubKmer.getSeq(2).apply(blockSize - 1) << 2 * param.subKmerSize;
                        newReflexivLong |= currentSubKmer.getLong(0);
                        newReflexivLong &= maxBlockBinary;
                        newReflexivLongArray[blockSize - 1] = newReflexivLong;

                        // 2nd and so on
                        for (int i=blockSize-2; i>=1;i--){
                            newReflexivLong = (Long)currentSubKmer.getSeq(2).apply(i) << 2*param.subKmerSize;
                            newReflexivLong |= ((Long)currentSubKmer.getSeq(2).apply(i+1) >>> 2*(31- param.subKmerSize));
                            newReflexivLong &=maxBlockBinary;
                            newReflexivLongArray[i] = newReflexivLong;
                        }

                        // 1st
                        newReflexivLong = (Long)currentSubKmer.getSeq(2).apply(1) >>> 2*(31-param.subKmerSize);
                        if (firstPrefixLength >= param.subKmerSize) {
                            newReflexivLong |= (((Long)currentSubKmer.getSeq(2).apply(0) & maxPrefixLengthBinary) << 2 * param.subKmerSize);
                        }
                        newReflexivLong &= maxPrefixLengthBinary;
                        newReflexivLong |= (1L << 2*firstPrefixLength); // add C marker
                        newReflexivLongArray[0] = newReflexivLong;

                    }else{ /* blockSize = 1)*/
                        if (firstPrefixLength >= param.subKmerSize) {
                            newReflexivSubKmer = ((Long)currentSubKmer.getSeq(2).apply(0) & maxPrefixLengthBinary) >>> 2*(firstPrefixLength - param.subKmerSize);
                            newReflexivSubKmer &= maxSubKmerBinary; // remove header, including C marker

                            newReflexivLong = ((Long)currentSubKmer.getSeq(2).apply(0) & maxPrefixLengthBinary) << 2*param.subKmerSize;
                            newReflexivLong |= currentSubKmer.getLong(0);
                            newReflexivLong &= maxPrefixLengthBinary; // remove header, including C marker
                            newReflexivLong |= (1L << 2*firstPrefixLength); // add C marker
                        }else {
                            newReflexivSubKmer = ((Long)currentSubKmer.getSeq(2).apply(0) & maxPrefixLengthBinary) << (2 * (param.subKmerSize - firstPrefixLength));
                            newReflexivSubKmer |= (currentSubKmer.getLong(0) >>> (2 * firstPrefixLength));

                            newReflexivLong = currentSubKmer.getLong(0) & maxPrefixLengthBinary;
                            newReflexivLong |= (1L << 2 * firstPrefixLength); // add C marker in the front
                        }

                        newReflexivLongArray[0]=newReflexivLong;
                    }

                    return RowFactory.create(newReflexivSubKmer,
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
        Hashtable<Long, Integer> probTable;

        List<Row> fragmentList = new ArrayList<Row>();
        int readLength;
        String[] units;
        String read;
        char nucleotide;
        long nucleotideInt;
        long nucleotideIntComplement;

        public DSLowCoverageReadDetection(Broadcast<Hashtable<Long, Integer>> s){
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
          //          System.out.println("forward: " + lowCoverageFragment);
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
//                    System.out.println("RC: " + lowCoverageRCFragment);
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


    class DSExtendReflexivKmerToArrayLoop implements MapPartitionsFunction<Row, Row>, Serializable{

        /* marker to identify similar SubKmers in the loop sequence */
        private int lineMarker=1;

        /* 1 stands for forward sub-kmer */
        /* 2 stands for reflexiv sub-kmer */
        // private int randomReflexivMarker = ThreadLocalRandom.current().nextInt(1, 3);
        private int randomReflexivMarker = 2;

        long maxSubKmerBinary = ~((~0L) << 2*param.subKmerSize);
        long maxBlockBinary = ~((~0L) << 2*31); // a block has 31 nucleotide


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
        public Iterator<Row> call (Iterator<Row> sIterator) {

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
                            if (s.getLong(0) == tmpReflexivKmerExtendList.get(i).getLong(0)) {
                                if (s.getInt(1) == 1) {
                                    if (tmpReflexivKmerExtendList.get(i).getInt(1) == 2) {
                                        int tmpReflexivKmerSuffixLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros((Long)tmpReflexivKmerExtendList.get(i).getSeq(2).apply(0)) / 2 + 1);
                                        int tmpBlockSize = (tmpReflexivKmerExtendList.get(i).getSeq(2).length() - 1) * 31 + tmpReflexivKmerSuffixLength;
                                        int currentReflexivKmerSuffixLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros((Long)s.getSeq(2).apply(0)) / 2 + 1);
                                        int currentBlockSize = ( s.getSeq(2).length() - 1) * 31 + currentReflexivKmerSuffixLength;

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
                                        int tmpReflexivKmerSuffixLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros((Long)tmpReflexivKmerExtendList.get(i).getSeq(2).apply(0)) / 2 + 1);
                                        int tmpBlockSize = (tmpReflexivKmerExtendList.get(i).getSeq(2).length() - 1) * 31 + tmpReflexivKmerSuffixLength;
                                        int currentReflexivKmerSuffixLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros((Long)s.getSeq(2).apply(0)) / 2 + 1);
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
        public void singleKmerRandomizer(Row currentSubKmer){
            int blockSize = currentSubKmer.getSeq(2).length();
            Long[] newReflexivLongArray= new Long[blockSize];

            if (currentSubKmer.getInt(1) == 1){
                int firstSuffixBlockLength = Long.SIZE/2 - (Long.numberOfLeadingZeros((Long)currentSubKmer.getSeq(2).apply(0))/2 + 1);
                long maxSuffixLengthBinary = ~((~0L) << (2*firstSuffixBlockLength));
                Long newReflexivSubKmer;
                Long newReflexivLong;

                if (randomReflexivMarker == 2) {
                    if ( blockSize > 1) {
                        newReflexivSubKmer = (Long)currentSubKmer.getSeq(2).apply(blockSize-1) & maxSubKmerBinary;

                        // 3rd block and so on
                        for (int i=blockSize-1; i>1; i--){
                            newReflexivLong=(Long)currentSubKmer.getSeq(2).apply(i) >>> 2*param.subKmerSize;
                            newReflexivLong|= ((Long)currentSubKmer.getSeq(2).apply(i-1) << 2*(31-param.subKmerSize));
                            newReflexivLong&= maxBlockBinary;
                            newReflexivLongArray[i] = newReflexivLong;
                        }

                        // 2nd block
                        newReflexivLong=(Long)currentSubKmer.getSeq(2).apply(1) >>> 2*param.subKmerSize;
                        newReflexivLong|= ((Long)currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary) << 2*(31-param.subKmerSize);
                        if (firstSuffixBlockLength < param.subKmerSize){
                            newReflexivLong |= (currentSubKmer.getLong(0) << 2*(31 - param.subKmerSize + firstSuffixBlockLength));
                        }
                        newReflexivLong&= maxBlockBinary;
                        newReflexivLongArray[1] = newReflexivLong;

                        // 1st block
                        if (firstSuffixBlockLength < param.subKmerSize){
                            newReflexivLong = currentSubKmer.getLong(0) >>> 2*(param.subKmerSize - firstSuffixBlockLength);
                            newReflexivLong |= (1L << 2 * firstSuffixBlockLength);
                        }else {
                            newReflexivLong = (Long)currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary; //remove C marker
                            newReflexivLong >>>= 2 * param.subKmerSize;
                            newReflexivLong |= (currentSubKmer.getLong(0) << 2 * (firstSuffixBlockLength - param.subKmerSize));
                            newReflexivLong |= (1L << 2 * firstSuffixBlockLength); // add C marker
                        }
                        newReflexivLongArray[0] = newReflexivLong;
                    }else{
                        if (firstSuffixBlockLength >= param.subKmerSize){
                            newReflexivSubKmer = (Long)currentSubKmer.getSeq(2).apply(0) & maxSubKmerBinary;

                            newReflexivLong = (Long)currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary;
                            newReflexivLong >>>= 2*param.subKmerSize;
                            newReflexivLong |= (currentSubKmer.getLong(0) << 2*(firstSuffixBlockLength-param.subKmerSize));
                            newReflexivLong |= (1L << (2 * firstSuffixBlockLength)); // add C marker in the front
                        }else {
                            newReflexivSubKmer = currentSubKmer.getLong(0) << (firstSuffixBlockLength * 2);
                            newReflexivSubKmer &= maxSubKmerBinary;
                            newReflexivSubKmer |= ((Long)currentSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary);

                            newReflexivLong = currentSubKmer.getLong(0) >>> (2 * (param.subKmerSize - firstSuffixBlockLength));
                            newReflexivLong |= (1L << (2 * firstSuffixBlockLength)); // add C marker in the front
                        }

                        newReflexivLongArray[0]=newReflexivLong;
                    }

                    reflexivKmerConcatList.add(
                            RowFactory.create(newReflexivSubKmer,
                                            randomReflexivMarker, newReflexivLongArray, currentSubKmer.getInt(3), currentSubKmer.getInt(4)
                            )
                    );
                }else{

                    reflexivKmerConcatList.add(currentSubKmer);
                }
            }else{ /* currentSubKmer.getInt(1) == 2 */
                int firstPrefixLength = Long.SIZE/2 - (Long.numberOfLeadingZeros((Long)currentSubKmer.getSeq(2).apply(0))/2 + 1);
                long maxPrefixLengthBinary = ~((~0L) << (2*firstPrefixLength));

                Long newReflexivSubKmer;
                Long newReflexivLong;

                if (randomReflexivMarker == 2) {

                    reflexivKmerConcatList.add(currentSubKmer);
                }else{ /* randomReflexivMarker == 1 */
                    if (blockSize > 1){
                        // the subKmer
                        if (firstPrefixLength >= param.subKmerSize) {
                            newReflexivSubKmer = ((Long)currentSubKmer.getSeq(2).apply(0) & maxPrefixLengthBinary) >>> 2* (firstPrefixLength-param.subKmerSize); // also removed C marker
                        }else{
                            newReflexivSubKmer = (Long)currentSubKmer.getSeq(2).apply(0) & maxPrefixLengthBinary; // remove C marker
                            newReflexivSubKmer <<= 2*(param.subKmerSize-firstPrefixLength);
                            newReflexivSubKmer |= ((Long)currentSubKmer.getSeq(2).apply(1) >>> 2*(31- param.subKmerSize + firstPrefixLength));
                        }

                        // the last block
                        newReflexivLong = (Long)currentSubKmer.getSeq(2).apply(blockSize - 1) << 2 * param.subKmerSize;
                        newReflexivLong |= currentSubKmer.getLong(0);
                        newReflexivLong &= maxBlockBinary;
                        newReflexivLongArray[blockSize - 1] = newReflexivLong;

                        // 2nd and so on
                        for (int i=blockSize-2; i>=1;i--){
                            newReflexivLong = (Long)currentSubKmer.getSeq(2).apply(i) << 2*param.subKmerSize;
                            newReflexivLong |= ((Long)currentSubKmer.getSeq(2).apply(i+1) >>> 2*(31- param.subKmerSize));
                            newReflexivLong &=maxBlockBinary;
                            newReflexivLongArray[i] = newReflexivLong;
                        }

                        // 1st
                        newReflexivLong = (Long)currentSubKmer.getSeq(2).apply(1) >>> 2*(31-param.subKmerSize);
                        if (firstPrefixLength >= param.subKmerSize) {
                            newReflexivLong |= (((Long)currentSubKmer.getSeq(2).apply(0) & maxPrefixLengthBinary) << 2 * param.subKmerSize);
                        }
                        newReflexivLong &= maxPrefixLengthBinary;
                        newReflexivLong |= (1L << 2*firstPrefixLength); // add C marker
                        newReflexivLongArray[0] = newReflexivLong;

                    }else{ /* blockSize = 1)*/
                        if (firstPrefixLength >= param.subKmerSize) {
                            newReflexivSubKmer = ((Long)currentSubKmer.getSeq(2).apply(0) & maxPrefixLengthBinary) >>> 2*(firstPrefixLength - param.subKmerSize);
                            newReflexivSubKmer &= maxSubKmerBinary; // remove header, including C marker

                            newReflexivLong = ((Long)currentSubKmer.getSeq(2).apply(0) & maxPrefixLengthBinary) << 2*param.subKmerSize;
                            newReflexivLong |= currentSubKmer.getLong(0);
                            newReflexivLong &= maxPrefixLengthBinary; // remove header, including C marker
                            newReflexivLong |= (1L << 2*firstPrefixLength); // add C marker
                        }else {
                            newReflexivSubKmer = ((Long)currentSubKmer.getSeq(2).apply(0) & maxPrefixLengthBinary) << (2 * (param.subKmerSize - firstPrefixLength));
                            newReflexivSubKmer |= (currentSubKmer.getLong(0) >>> (2 * firstPrefixLength));

                            newReflexivLong = currentSubKmer.getLong(0) & maxPrefixLengthBinary;
                            newReflexivLong |= (1L << 2 * firstPrefixLength); // add C marker in the front
                        }

                        newReflexivLongArray[0]=newReflexivLong;
                    }

                    reflexivKmerConcatList.add(
                            RowFactory.create(newReflexivSubKmer,
                                            randomReflexivMarker, newReflexivLongArray, currentSubKmer.getInt(3), currentSubKmer.getInt(4)
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
        public void directKmerComparison(Row currentSubKmer){
            tmpReflexivKmerExtendList.add(currentSubKmer);
        }

        /**
         *
         * @param forwardSubKmer
         * @param reflexedSubKmer
         */
        public void reflexivExtend(Row forwardSubKmer, Row reflexedSubKmer, int bubbleDistance) {

             /* forward   ATCGATCG, 1, ------ */
             /* reflexed  ------, 2, ATCGATCG */

            int forwardFirstSuffixLength = Long.SIZE/2 - (Long.numberOfLeadingZeros((Long)forwardSubKmer.getSeq(2).apply(0))/2 + 1);
            int forwardBlockSize = forwardSubKmer.getSeq(2).length();
            int forwardKmerLength= (forwardSubKmer.getSeq(2).length() - 1)*31 + forwardFirstSuffixLength;
            int reflexedFirstPrefixLength = Long.SIZE/2 - (Long.numberOfLeadingZeros((Long)reflexedSubKmer.getSeq(2).apply(0))/2 + 1);
            int reflexedBlockSize = reflexedSubKmer.getSeq(2).length();
            int reflexedKmerLength=(reflexedSubKmer.getSeq(2).length() -1)*31 + reflexedFirstPrefixLength;
            int concatenateLength = forwardKmerLength + reflexedKmerLength;
            int concatBlockSize = concatenateLength/31;
            if (concatenateLength%31 !=0 ){
                concatBlockSize++;
            }
/*
            if (concatBlockSize ==0) {
                System.out.println("concateBlockSize: " + concatBlockSize);
            }
*/
            long maxSuffixLengthBinary = ~((~0L) << 2*forwardFirstSuffixLength);
            long maxPrefixLengthBinary = ~((~0L) << 2*reflexedFirstPrefixLength);


            if (randomReflexivMarker == 2) {
                Long newReflexivSubKmer;
                Long newReflexivLong;
                Long[] newReflexivLongArray = new Long[concatBlockSize];

                if (forwardBlockSize > 1) {
                    newReflexivSubKmer = (Long)forwardSubKmer.getSeq(2).apply(forwardBlockSize - 1) & maxSubKmerBinary;
                } else {
                    newReflexivSubKmer = forwardSubKmer.getLong(0) << (2 * forwardFirstSuffixLength);
                    newReflexivSubKmer |= ((Long)forwardSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary);
                    newReflexivSubKmer &= maxSubKmerBinary;
                }

                // 3rd of forward and so on
                int j = concatBlockSize; // extended array index. Initiating with one more as -1 in the loop
                for (int i = forwardBlockSize - 1; i > 1; i--) {
                    j--;
                    newReflexivLong = (Long)forwardSubKmer.getSeq(2).apply(i) >>> 2 * param.subKmerSize;
                    newReflexivLong |= ((Long)forwardSubKmer.getSeq(2).apply(i - 1) << 2 * (31 - param.subKmerSize));
                    newReflexivLong &= maxBlockBinary;
                    newReflexivLongArray[j] = newReflexivLong;
                }

                // 2nd of forward
                if (forwardBlockSize >1) {
                    newReflexivLong = (Long)forwardSubKmer.getSeq(2).apply(1) >>> 2 * param.subKmerSize;
                    newReflexivLong |= (((Long)forwardSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary) << 2 * (31 - param.subKmerSize)); // remove C marker
                    if (forwardFirstSuffixLength < param.subKmerSize) {
                        newReflexivLong |= (forwardSubKmer.getLong(0) << 2 * (31 - param.subKmerSize + forwardFirstSuffixLength));
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
                    newReflexivLong = forwardSubKmer.getLong(0) >>> 2 * (param.subKmerSize - forwardFirstSuffixLength);
                } else {
                    newReflexivLong = ((Long)forwardSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary) >>> 2 * param.subKmerSize;
                    newReflexivLong |= (forwardSubKmer.getLong(0) << 2 * (forwardFirstSuffixLength - param.subKmerSize));
                }

                if (forwardFirstSuffixLength < 31) {  // well, current version forwardFirstSuffixLength will not be larger than 31
                    if (reflexedBlockSize > 1) {
                        newReflexivLong |= ((Long)reflexedSubKmer.getSeq(2).apply(reflexedBlockSize - 1) << 2 * forwardFirstSuffixLength);
                        newReflexivLong &= maxBlockBinary;
                        newReflexivLongArray[concatBlockSize - forwardBlockSize] = newReflexivLong;
                    } else if (reflexedFirstPrefixLength > (31 - forwardFirstSuffixLength) && reflexedBlockSize == 1) {
                        newReflexivLong |= ((Long)reflexedSubKmer.getSeq(2).apply(reflexedBlockSize - 1) << 2 * forwardFirstSuffixLength);
                        newReflexivLong &= maxBlockBinary;
                        newReflexivLongArray[concatBlockSize - forwardBlockSize] = newReflexivLong;
                    } else { //reflexedFirstPrefixLength <= (31-forwardFirstSuffixLength)
                        newReflexivLong |= ((Long)reflexedSubKmer.getSeq(2).apply(reflexedBlockSize - 1) << 2 * forwardFirstSuffixLength);
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
                        newReflexivLong = (Long)reflexedSubKmer.getSeq(2).apply(i) >>> 2 * (31 - forwardFirstSuffixLength);
                        newReflexivLong |= ((Long)reflexedSubKmer.getSeq(2).apply(i - 1) << 2 * forwardFirstSuffixLength);
                        newReflexivLong &= maxBlockBinary;
                    } else { // forwardFirstSuffixLength == 31
                        newReflexivLong = (Long)reflexedSubKmer.getSeq(2).apply(i);
                    }
                    newReflexivLongArray[k] = newReflexivLong;
                }

                // 2nd of reflected or the 1st if reflexedFirstPrefixLength < (31-forwardFirstSuffixLength)
                if (reflexedBlockSize > 1) {
                    if (forwardFirstSuffixLength < 31) {
                        newReflexivLong = (Long)reflexedSubKmer.getSeq(2).apply(1) >>> 2 * (31 - forwardFirstSuffixLength);
                        newReflexivLong |= (((Long)reflexedSubKmer.getSeq(2).apply(0) & maxPrefixLengthBinary) << 2 * forwardFirstSuffixLength);
                        newReflexivLong &= maxBlockBinary;

                        //reflexedFirstPrefixLength + forwardFirstSuffixLength <= 31
                        if (reflexedFirstPrefixLength <= (31 - forwardFirstSuffixLength)) {
                            newReflexivLong |= (1L << 2 * (forwardFirstSuffixLength + reflexedFirstPrefixLength)); // add C marker
                            newReflexivLongArray[0] = newReflexivLong;
                        } else {
                            newReflexivLongArray[1] = newReflexivLong;
                            newReflexivLong = (Long)reflexedSubKmer.getSeq(2).apply(0) >>> 2 * (31 - forwardFirstSuffixLength); // keep the header
                            newReflexivLongArray[0] = newReflexivLong;
                        }
                    } else { // forwardFirstSuffixLength == 31
                        newReflexivLong = (Long)reflexedSubKmer.getSeq(2).apply(1);
                        newReflexivLongArray[1] = newReflexivLong;
                        newReflexivLongArray[0] = (Long)reflexedSubKmer.getSeq(2).apply(0); // include the C maker
                    }
                } else { // reflexedBlockSize ==1
                    if (forwardFirstSuffixLength < 31) {
                        if (reflexedFirstPrefixLength <= (31 - forwardFirstSuffixLength)) {
                            // the first element is already included above
                        } else {
                            newReflexivLong = (Long)reflexedSubKmer.getSeq(2).apply(0) >>> 2 * (31 - forwardFirstSuffixLength);
                            newReflexivLongArray[0] = newReflexivLong;
                        }
                    } else { // forwardFirstSuffixLength ==31
                        newReflexivLongArray[0] = (Long)reflexedSubKmer.getSeq(2).apply(0);
                    }
                }


                if (bubbleDistance < 0) {
                    reflexivKmerConcatList.add(
                            RowFactory.create(newReflexivSubKmer,
                                            randomReflexivMarker, newReflexivLongArray, reflexedSubKmer.getInt(3), forwardSubKmer.getInt(4)
                            )
                    );
                } else {
                    if (forwardSubKmer.getInt(3) > 0) {
                        reflexivKmerConcatList.add(
                                RowFactory.create(newReflexivSubKmer,
                                                randomReflexivMarker, newReflexivLongArray, bubbleDistance, forwardSubKmer.getInt(4)
                                )
                        );
                    } else {
                        reflexivKmerConcatList.add(
                                RowFactory.create(newReflexivSubKmer,
                                                randomReflexivMarker, newReflexivLongArray, reflexedSubKmer.getInt(3), bubbleDistance
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
                    newForwardSubKmer = ((Long)reflexedSubKmer.getSeq(2).apply(0) & maxPrefixLengthBinary) >>> 2 * (reflexedFirstPrefixLength - param.subKmerSize);
                } else {
                    newForwardSubKmer = ((Long)reflexedSubKmer.getSeq(2).apply(0) & maxPrefixLengthBinary) << 2 * (param.subKmerSize - reflexedFirstPrefixLength);
                    if (reflexedBlockSize > 1) {
                        newForwardSubKmer |= (Long)reflexedSubKmer.getSeq(2).apply(1) >>> 2 * (31 - param.subKmerSize + reflexedFirstPrefixLength);
                    } else {//if (reflexedBlockSize == 1) {
                        newForwardSubKmer |= reflexedSubKmer.getLong(0) >>> 2 * reflexedFirstPrefixLength;
                    }
                }


                // 2nd and so on
                int j = concatBlockSize; // the concatenated array index. With one more as -1 in the loop
                for (int i = forwardBlockSize - 1; i >= 1; i--) {
                    j--;
                    newForwardLongArray[j] = (Long)forwardSubKmer.getSeq(2).apply(i);
                }

                // 1st
                if (forwardFirstSuffixLength + param.subKmerSize < 31) { // forwardFirstSuffixLength < 31
                    newForwardLong = ((Long)forwardSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary);
                    newForwardLong |= (forwardSubKmer.getLong(0) << 2 * (forwardFirstSuffixLength));
                    /**
                     *                    forward        |--------|  |-||------------||------------|
                     *                    reflected      |--------|  |--||------------|
                     *                  |-||------------|
                     *                  |--------||------------||------------||------------|
                     */

                    if (reflexedBlockSize == 1 && reflexedFirstPrefixLength >=param.subKmerSize) {
                        Long maxFirstBlockRestBinary = ~((~0L) << 2 * (reflexedFirstPrefixLength-param.subKmerSize));
                        Long maxSecondBlockRestBinary = ~((~0L) << 2 * (reflexedFirstPrefixLength+forwardFirstSuffixLength));
                        newForwardLong |= (((Long)reflexedSubKmer.getSeq(2).apply(0) & maxFirstBlockRestBinary) << 2 * (param.subKmerSize + forwardFirstSuffixLength));
                        if (forwardFirstSuffixLength + reflexedFirstPrefixLength >31) {
                            newForwardLong &= maxBlockBinary;
                            newForwardLongArray[1] = newForwardLong;

                            newForwardLong = (((Long)reflexedSubKmer.getSeq(2).apply(0) & maxFirstBlockRestBinary) << 2*param.subKmerSize);
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
                        newForwardLong |= ((Long)reflexedSubKmer.getSeq(2).apply(reflexedBlockSize - 1) << 2 * (param.subKmerSize + forwardFirstSuffixLength));
                        newForwardLong &= maxBlockBinary;
                        newForwardLongArray[concatBlockSize - forwardBlockSize] = newForwardLong;
                    }

                    // reflected 3rd or 2nd and so on
                    int k = concatBlockSize - forwardBlockSize; // concatenate array index
                    for (int i = reflexedBlockSize - 1; i > 1; i--) {
                        k--;
                        newForwardLong = (Long)reflexedSubKmer.getSeq(2).apply(i) >>> 2 * (31 - param.subKmerSize - forwardFirstSuffixLength);
                        newForwardLong |= (Long)reflexedSubKmer.getSeq(2).apply(i - 1) << 2 * (param.subKmerSize + forwardFirstSuffixLength);
                        newForwardLong &= maxBlockBinary;
                        newForwardLongArray[k] = newForwardLong;
                    }

                    // reflected 2nd or 1st
                    if (reflexedBlockSize >1) {
                        if (reflexedFirstPrefixLength >= param.subKmerSize && reflexedFirstPrefixLength - param.subKmerSize <= 31 - param.subKmerSize - forwardFirstSuffixLength) { // 1st
                            newForwardLong = (Long)reflexedSubKmer.getSeq(2).apply(1) >>> 2 * (31 - param.subKmerSize - forwardFirstSuffixLength);
                            Long maxfirstBlockRestBinary = ~((~0L) << 2 * (reflexedFirstPrefixLength - param.subKmerSize));
                            newForwardLong |= (((Long)reflexedSubKmer.getSeq(2).apply(0) & maxfirstBlockRestBinary) << 2 * (param.subKmerSize + forwardFirstSuffixLength));
                            newForwardLong |= 1L << 2 * (forwardFirstSuffixLength + reflexedFirstPrefixLength); // add C marker
                            newForwardLongArray[0] = newForwardLong;
                        } else if (reflexedFirstPrefixLength >= param.subKmerSize && reflexedFirstPrefixLength - param.subKmerSize > 31 - param.subKmerSize - forwardFirstSuffixLength) {
                            newForwardLong = (Long)reflexedSubKmer.getSeq(2).apply(1) >>> 2 * (31 - param.subKmerSize - forwardFirstSuffixLength);
                            newForwardLong |= ((Long)reflexedSubKmer.getSeq(2).apply(0) << 2 * (param.subKmerSize + forwardFirstSuffixLength));
                            newForwardLong &= maxBlockBinary;
                            newForwardLongArray[1] = newForwardLong;

                            Long maxfirstBlockRestBinary = ~((~0L) << 2 * (reflexedFirstPrefixLength - param.subKmerSize));
                            newForwardLong = (Long)reflexedSubKmer.getSeq(2).apply(0) & maxfirstBlockRestBinary >>> 2 * (31 - param.subKmerSize - forwardFirstSuffixLength);
                            newForwardLong |= 1L << 2 * (reflexedFirstPrefixLength + forwardFirstSuffixLength - 31); // add C marker
                            newForwardLongArray[0] = newForwardLong;
                        } else { // reflexedFirstPrefixLength < param.subKmerSize
                            Long maxSecondBlockRestBinary = ~((~0L) << 2 * (31 - param.subKmerSize + reflexedFirstPrefixLength));
                            newForwardLong = ((Long)reflexedSubKmer.getSeq(2).apply(1) & maxSecondBlockRestBinary) >>> 2 * (31 - param.subKmerSize - forwardFirstSuffixLength);
                            newForwardLong |= 1L << 2 * (forwardFirstSuffixLength + reflexedFirstPrefixLength); // add C marker
                            newForwardLongArray[0] = newForwardLong;
                        }
                    }

                } else if (forwardFirstSuffixLength < 31 && forwardFirstSuffixLength + param.subKmerSize >= 31) {
                    if (reflexedBlockSize >1) {
                        newForwardLong = ((Long)forwardSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary);
                        newForwardLong |= (forwardSubKmer.getLong(0) << 2 * (forwardFirstSuffixLength));
                        newForwardLong &= maxBlockBinary;
                        newForwardLongArray[concatBlockSize - forwardBlockSize] = newForwardLong;

                        newForwardLong = forwardSubKmer.getLong(0) >>> 2 * (31 - forwardFirstSuffixLength);
                        newForwardLong |= ((Long)reflexedSubKmer.getSeq(2).apply(reflexedBlockSize - 1) << 2 * (param.subKmerSize + forwardFirstSuffixLength - 31));
                        if (reflexedBlockSize == 2 && forwardFirstSuffixLength + reflexedFirstPrefixLength <= 31){
                            Long maxFirstBlockRestBinary=  ~((~0L) << 2 * (reflexedFirstPrefixLength + forwardFirstSuffixLength));
                            newForwardLong &= maxFirstBlockRestBinary;
                            newForwardLong |= (1L << 2*(forwardFirstSuffixLength + reflexedFirstPrefixLength));
                        }else {
                            newForwardLong &= maxBlockBinary;
                        }
                        newForwardLongArray[concatBlockSize - forwardBlockSize - 1] = newForwardLong;
                    }else if (reflexedBlockSize == 1 && reflexedFirstPrefixLength >param.subKmerSize){ // forwardFirstSuffixLength + reflexedFirstPrefixLength >31
                        newForwardLong = ((Long)forwardSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary);
                        newForwardLong |= (forwardSubKmer.getLong(0) << 2 * (forwardFirstSuffixLength));
                        newForwardLong &= maxBlockBinary;
                        newForwardLongArray[concatBlockSize - forwardBlockSize] = newForwardLong; // concatBlockSize - forwardBlockSize = 1

                        newForwardLong = forwardSubKmer.getLong(0) >>> 2 * (31 - forwardFirstSuffixLength);
                        Long maxFirstBlockRestBinary = ~((~0L) << 2 * (reflexedFirstPrefixLength - param.subKmerSize));
                        newForwardLong |= (((Long)reflexedSubKmer.getSeq(2).apply(0) & maxFirstBlockRestBinary) << 2*(param.subKmerSize + forwardFirstSuffixLength -31));
                        newForwardLong |= (1L << 2*(forwardFirstSuffixLength+reflexedFirstPrefixLength -31));
                        newForwardLongArray[concatBlockSize - forwardBlockSize -1] = newForwardLong; // concateBlockSize - forwardBlockSize = 0
                    }else if (reflexedBlockSize == 1 && reflexedFirstPrefixLength <= param.subKmerSize && forwardFirstSuffixLength + reflexedFirstPrefixLength > 31){ // reflexedBlockSize == 1 && reflexedFirstPrefixLength <= param.subKmerSize
                        newForwardLong = ((Long)forwardSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary);
                        newForwardLong |= (forwardSubKmer.getLong(0) << 2 * (forwardFirstSuffixLength));
                        newForwardLong &= maxBlockBinary;
                        newForwardLongArray[concatBlockSize - forwardBlockSize] = newForwardLong;

                        newForwardLong = forwardSubKmer.getLong(0) >>> 2*(31 - forwardFirstSuffixLength);
                        Long maxFirstBlockRestBinary = ~((~0L) << 2 * (reflexedFirstPrefixLength + forwardFirstSuffixLength -31));
                        newForwardLong &= maxFirstBlockRestBinary;
                        newForwardLong |= (1L << 2*(forwardFirstSuffixLength+reflexedFirstPrefixLength-31));
                        newForwardLongArray[concatBlockSize - forwardBlockSize -1] =newForwardLong;
                    } else {
                        newForwardLong = ((Long)forwardSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary);
                        newForwardLong |= (forwardSubKmer.getLong(0) << (2 * forwardFirstSuffixLength));
                        Long maxFirstBlockRestBinary = ~((~0L) << 2 * (reflexedFirstPrefixLength + forwardFirstSuffixLength));
                        newForwardLong &= maxFirstBlockRestBinary;
                        newForwardLong |= (1L << 2*(reflexedFirstPrefixLength + forwardFirstSuffixLength));
                        newForwardLongArray[concatBlockSize - forwardBlockSize] = newForwardLong; // concatBlockSize - forwardBlockSize = 0
                    }

                    // reflected 3rd or 2nd and so on
                    int k = concatBlockSize - forwardBlockSize - 1; // concatenate array index
                    for (int i = reflexedBlockSize - 1; i > 2; i--) {
                        k--;
                        newForwardLong = (Long)reflexedSubKmer.getSeq(2).apply(i) >>> 2 * (62 - param.subKmerSize - forwardFirstSuffixLength);
                        newForwardLong |= ((Long)reflexedSubKmer.getSeq(2).apply(i - 1) << 2 * (param.subKmerSize + forwardFirstSuffixLength - 31));
                        newForwardLong &= maxBlockBinary;
                        newForwardLongArray[k] = newForwardLong;
                    }

                    if (reflexedBlockSize >1) {
                        if (reflexedFirstPrefixLength > param.subKmerSize) { // && param.subKmerSize - reflexedFirstPrefixLength + (param.subKmerSize + forwardFirstSuffixLength -31) > 31 is impossible
                            if (reflexedBlockSize > 2) {
                                newForwardLong = (Long)reflexedSubKmer.getSeq(2).apply(2) >>> 2 * (62 - param.subKmerSize - forwardFirstSuffixLength);
                                newForwardLong |= ((Long)reflexedSubKmer.getSeq(2).apply(1) << 2 * (param.subKmerSize + forwardFirstSuffixLength - 31));
                                newForwardLong &= maxBlockBinary;
                                newForwardLongArray[1] = newForwardLong;
                            }

                            newForwardLong = (Long)reflexedSubKmer.getSeq(2).apply(1) >>> 2 * (62 - param.subKmerSize - forwardFirstSuffixLength);
                            Long maxfirstBlockRestBinary = ~((~0L) << 2 * (reflexedFirstPrefixLength - param.subKmerSize));
                            newForwardLong |= (((Long)reflexedSubKmer.getSeq(2).apply(0) & maxfirstBlockRestBinary) << 2 * (param.subKmerSize + forwardFirstSuffixLength - 31));  // also removed C marker
                            newForwardLong |= 1L << 2 * (reflexedFirstPrefixLength + forwardFirstSuffixLength - 31); // add C marker
                            newForwardLongArray[0] = newForwardLong;
                        } else if (reflexedFirstPrefixLength <= param.subKmerSize && forwardFirstSuffixLength + reflexedFirstPrefixLength > 31) {
                            if (reflexedBlockSize >2) {
                                newForwardLong = (Long)reflexedSubKmer.getSeq(2).apply(2) >>> 2 * (62 - param.subKmerSize - forwardFirstSuffixLength);
                                newForwardLong |= ((Long)reflexedSubKmer.getSeq(2).apply(1) << 2 * (param.subKmerSize + forwardFirstSuffixLength - 31));
                                newForwardLong &= maxBlockBinary;
                                newForwardLongArray[1] = newForwardLong;
                            }

                            newForwardLong = (Long)reflexedSubKmer.getSeq(2).apply(1) >>> 2 * (62 - param.subKmerSize - forwardFirstSuffixLength);
                            Long maxfirstBlockRestBinary = ~((~0L) << 2 * (reflexedFirstPrefixLength + forwardFirstSuffixLength - 31));
                            newForwardLong &= maxfirstBlockRestBinary;
                            newForwardLong |= (1L << 2 * (reflexedFirstPrefixLength + forwardFirstSuffixLength - 31));
                            newForwardLongArray[0] = newForwardLong;
                        } else { // forwardFirstSuffixLength+reflexedFirstPrefixLength <= 31
                            if (reflexedBlockSize >= 3) {
                                newForwardLong = (Long)reflexedSubKmer.getSeq(2).apply(2) >>> 2 * (62 - param.subKmerSize - forwardFirstSuffixLength);
                                newForwardLong |= ((Long)reflexedSubKmer.getSeq(2).apply(1) << 2 * (param.subKmerSize + forwardFirstSuffixLength - 31));
                                Long maxfirstBlockRestBinary = ~((~0L) << 2 * (reflexedFirstPrefixLength + forwardFirstSuffixLength));
                                newForwardLong &= maxfirstBlockRestBinary;
                                newForwardLong |= (1L << 2 * (reflexedFirstPrefixLength + forwardFirstSuffixLength)); // add C marker
                                newForwardLongArray[0] = newForwardLong;
                            }
                        }
                    }

                } else {// forwardFirstSuffixLength == 31
                    newForwardLong = (Long)forwardSubKmer.getSeq(2).apply(0) & maxBlockBinary; // remove C marker
                    newForwardLongArray[concatBlockSize - forwardBlockSize] = newForwardLong;

                    if (reflexedBlockSize >1) {
                        newForwardLong = forwardSubKmer.getLong(0);
                        newForwardLong |= ((Long)reflexedSubKmer.getSeq(2).apply(reflexedBlockSize-1) << 2*param.subKmerSize);
                        newForwardLong &= maxBlockBinary;
                        newForwardLongArray[concatBlockSize - forwardBlockSize - 1] = newForwardLong;
                    }else{
                        newForwardLong = forwardSubKmer.getLong(0);
                        if (reflexedFirstPrefixLength > param.subKmerSize){
                            newForwardLong |= (((Long)reflexedSubKmer.getSeq(2).apply(0) & maxPrefixLengthBinary) << 2* param.subKmerSize);
                        }
                        Long maxfirstBlockRestBinary = ~((~0L) << 2 * (reflexedFirstPrefixLength + forwardFirstSuffixLength -31));
                        newForwardLong &= maxfirstBlockRestBinary;
                        newForwardLong |= (1L << 2*(reflexedFirstPrefixLength + forwardFirstSuffixLength -31));
                        newForwardLongArray[0] = newForwardLong;
                    }

                    int k = concatBlockSize - forwardBlockSize - 1; // concatenate array index
                    for (int i = reflexedBlockSize - 1; i > 1; i--) {
                        k--;
                        newForwardLong = (Long)reflexedSubKmer.getSeq(2).apply(i) >>> 2*(31-param.subKmerSize);
                        newForwardLong |= (Long)reflexedSubKmer.getSeq(2).apply(i-1) << 2*param.subKmerSize;
                        newForwardLong &= maxBlockBinary;
                        newForwardLongArray[k] = newForwardLong;
                    }

                    if (reflexedBlockSize >1) {
                        if (reflexedFirstPrefixLength >= param.subKmerSize) {
                            newForwardLong = (Long)reflexedSubKmer.getSeq(2).apply(1) >>> 2 * (31 - param.subKmerSize);
                            newForwardLong |= ((Long)reflexedSubKmer.getSeq(2).apply(0) << 2 * param.subKmerSize);
                            Long maxfirstBlockRestBinary = ~((~0L) << 2 * (reflexedFirstPrefixLength + forwardFirstSuffixLength - 31));
                            newForwardLong &= maxfirstBlockRestBinary;
                            newForwardLong |= (1L << 2 * (reflexedFirstPrefixLength + forwardFirstSuffixLength - 31)); // add C marker
                            newForwardLongArray[0] = newForwardLong;
                        } else { // reflexedFirstPrefixLength < param.subKmerSize
                            newForwardLong = (Long)reflexedSubKmer.getSeq(2).apply(1) >>> 2 * (31 - param.subKmerSize);
                            Long maxfirstBlockRestBinary = ~((~0L) << 2 * (reflexedFirstPrefixLength + forwardFirstSuffixLength - 31));
                            newForwardLong &= maxfirstBlockRestBinary;
                            newForwardLong |= (1L << 2 * (reflexedFirstPrefixLength + forwardFirstSuffixLength - 31)); // add C marker
                            newForwardLongArray[0] = newForwardLong;
                        }
                    }
                }



                if (bubbleDistance < 0) {
                    reflexivKmerConcatList.add(
                            RowFactory.create(newForwardSubKmer,
                                            randomReflexivMarker, newForwardLongArray, reflexedSubKmer.getInt(3), forwardSubKmer.getInt(4)
                            )
                    );
                } else {
                    if (forwardSubKmer.getInt(3) > 0) {
                        reflexivKmerConcatList.add(
                                RowFactory.create(newForwardSubKmer,
                                                randomReflexivMarker, newForwardLongArray, bubbleDistance, forwardSubKmer.getInt(4)
                                )
                        );
                    } else { // reflexedSubKmer.getInt(4) >0
                        reflexivKmerConcatList.add(
                                RowFactory.create(newForwardSubKmer,
                                                randomReflexivMarker, newForwardLongArray, reflexedSubKmer.getInt(3), bubbleDistance
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
            }else {
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



    class DSExtendReflexivKmerToArrayFirstTime implements MapPartitionsFunction<Row, Row>, Serializable{

        /* marker to identify similar SubKmers in the loop sequence */
        private int lineMarker=1;

        /* 1 stands for forward sub-kmer */
        /* 2 stands for reflexiv sub-kmer */
        //       private int randomReflexivMarker = ThreadLocalRandom.current().nextInt(1, 3);
        private int randomReflexivMarker =2;

        long maxSubKmerBinary = ~((~0L) << 2*param.subKmerSize);
        long maxBlockBinary = ~((~0L) << 2*31); // a block has 31 nucleotide


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
        public Iterator<Row> call (Iterator<Row> sIterator) {

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
                            if (s.getLong(0) == tmpReflexivKmerExtendList.get(i).getLong(0)) {
                                if (s.getInt(1) == 1) {
                                    if (tmpReflexivKmerExtendList.get(i).getInt(1) == 2) {
                                        int tmpReflexivKmerSuffixLength = Long.SIZE/2 - (Long.numberOfLeadingZeros(tmpReflexivKmerExtendList.get(i).getLong(2))/2 + 1);
                                        int currentReflexivKmerSuffixLength = Long.SIZE/2 - (Long.numberOfLeadingZeros(s.getLong(2))/2 + 1);
                                        if (s.getInt(3) < 0 && tmpReflexivKmerExtendList.get(i).getInt(4) < 0) {
                                            reflexivExtend(s, tmpReflexivKmerExtendList.get(i), -1);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        }else if (s.getInt(3)>=0 && tmpReflexivKmerExtendList.get(i).getInt(4)>=0) {
                                            reflexivExtend(s, tmpReflexivKmerExtendList.get(i), -1);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        }else if (s.getInt(3)>=0 && s.getInt(3)-tmpReflexivKmerSuffixLength>=0) {
                                            reflexivExtend(s, tmpReflexivKmerExtendList.get(i), s.getInt(3)-tmpReflexivKmerSuffixLength);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        }else if (tmpReflexivKmerExtendList.get(i).getInt(4) >=0 && tmpReflexivKmerExtendList.get(i).getInt(4)-currentReflexivKmerSuffixLength>=0){
                                            reflexivExtend(s, tmpReflexivKmerExtendList.get(i), tmpReflexivKmerExtendList.get(i).getInt(4)-currentReflexivKmerSuffixLength);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        }else{
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
                                        int tmpReflexivKmerSuffixLength = Long.SIZE/2 - (Long.numberOfLeadingZeros(tmpReflexivKmerExtendList.get(i).getLong(2))/2 + 1);
                                        int currentReflexivKmerSuffixLength = Long.SIZE/2 - (Long.numberOfLeadingZeros(s.getLong(2))/2 + 1);
                                        if (s.getInt(4) < 0 && tmpReflexivKmerExtendList.get(i).getInt(3) < 0) {
                                            reflexivExtend(tmpReflexivKmerExtendList.get(i), s, -1);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        }else if (s.getInt(4)>=0 && tmpReflexivKmerExtendList.get(i).getInt(3)>=0) {
                                            reflexivExtend(tmpReflexivKmerExtendList.get(i), s, -1);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        }else if (s.getInt(4)>=0 && s.getInt(4)-tmpReflexivKmerSuffixLength>=0) {
                                            reflexivExtend(tmpReflexivKmerExtendList.get(i), s, s.getInt(4)-tmpReflexivKmerSuffixLength);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        }else if (tmpReflexivKmerExtendList.get(i).getInt(3) >=0 && tmpReflexivKmerExtendList.get(i).getInt(3)-currentReflexivKmerSuffixLength >=0){
                                            reflexivExtend(tmpReflexivKmerExtendList.get(i), s, tmpReflexivKmerExtendList.get(i).getInt(3)-currentReflexivKmerSuffixLength);
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
        public void singleKmerRandomizer(Row currentSubKmer){


            if (currentSubKmer.getInt(1) == 1){
                int currentSuffixLength = Long.SIZE/2 - (Long.numberOfLeadingZeros(currentSubKmer.getLong(2))/2 + 1);
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
                        newReflexivSubKmer = currentSubKmer.getLong(0) << (currentSuffixLength*2);
                        newReflexivSubKmer &= maxSubKmerBinary;
                        newReflexivSubKmer |= (currentSubKmer.getLong(2) & maxSuffixLengthBinary);


                        newReflexivLong = currentSubKmer.getLong(0) >>> (2*(param.subKmerSize- currentSuffixLength));
                        newReflexivLong |= (1L<<(2*currentSuffixLength)); // add C marker in the front
                        /**
                         * to array
                         */
                        newReflexivLongArray = new Long[1];
                        newReflexivLongArray[0]=newReflexivLong;
                    }


                    reflexivKmerConcatList.add(
                            RowFactory.create(newReflexivSubKmer,
                                            randomReflexivMarker, newReflexivLongArray, currentSubKmer.getInt(3), currentSubKmer.getInt(4)
                            )
                    );
                }else{
                    newReflexivLongArray = new Long[1];
                    newReflexivLongArray[0] = currentSubKmer.getLong(2);

                    reflexivKmerConcatList.add(
                            RowFactory.create(currentSubKmer.getLong(0),
                                            currentSubKmer.getInt(1), newReflexivLongArray, currentSubKmer.getInt(3), currentSubKmer.getInt(4)
                            )
                    );
                }
            }else{ /* currentSubKmer.getInt(1) == 2 */
                int currentPrefixLength = Long.SIZE/2 - (Long.numberOfLeadingZeros(currentSubKmer.getLong(2))/2 + 1);
                long maxSuffixLengthBinary = ~((~0L) << (2*currentPrefixLength));
                Long newReflexivSubKmer=0L;
                Long newReflexivLong=0L;
                Long[] newReflexivLongArray;
                if (randomReflexivMarker == 2) {
                    newReflexivLongArray = new Long[1];
                    newReflexivLongArray[0] = currentSubKmer.getLong(2);

                    reflexivKmerConcatList.add(
                            RowFactory.create(currentSubKmer.getLong(0),
                                            currentSubKmer.getInt(1), newReflexivLongArray, currentSubKmer.getInt(3), currentSubKmer.getInt(4)
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
                        newReflexivSubKmer = (currentSubKmer.getLong(2) & maxSuffixLengthBinary) << (2*(param.subKmerSize - currentPrefixLength));;
                        //newReflexivSubKmer <<= (2*(param.subKmerSize - currentPrefixLength));
                        newReflexivSubKmer |=(currentSubKmer.getLong(0) >>> (2*currentPrefixLength));

                        newReflexivLong = currentSubKmer.getLong(0) & maxSuffixLengthBinary;
                        newReflexivLong |= (1L<<2*currentPrefixLength); // add C marker in the front
                        /**
                         * to array
                         */
                        newReflexivLongArray = new Long[1];
                        newReflexivLongArray[0]=newReflexivLong;
                    }

                    reflexivKmerConcatList.add(
                            RowFactory.create(newReflexivSubKmer,
                                            randomReflexivMarker, newReflexivLongArray, currentSubKmer.getInt(3), currentSubKmer.getInt(4)
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
        public void directKmerComparison(Row currentSubKmer){
            tmpReflexivKmerExtendList.add(currentSubKmer);
        }

        /**
         *
         * @param forwardSubKmer
         * @param reflexedSubKmer
         */
        public void reflexivExtend(Row forwardSubKmer, Row reflexedSubKmer, int bubbleDistance) {

             /* forward   ATCGATCG, 1, ------ */
             /* reflexed  ------, 2, ATCGATCG */

            int forwardSuffixLength = Long.SIZE/2 - (Long.numberOfLeadingZeros(forwardSubKmer.getLong(2))/2 + 1);
            int reflexedPrefixLength = Long.SIZE/2 - (Long.numberOfLeadingZeros(reflexedSubKmer.getLong(2))/2 + 1);
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

                    newReflexivSubKmer = forwardSubKmer.getLong(0) << (2*forwardSuffixLength);
                    newReflexivSubKmer &= maxSubKmerBinary;
                    newReflexivSubKmer |= (forwardSubKmer.getLong(2) & maxSuffixLengthBinary);
                    if (concatenateLength > 31){ // 31 for one block
                        Long newReflexivLonghead = reflexedSubKmer.getLong(2) >>> (2*(31-forwardSuffixLength)); // do not remove the C maker
                        newReflexivLong= reflexedSubKmer.getLong(2) << (2*forwardSuffixLength);
                        newReflexivLong &= maxBlockBinary;
                        newReflexivLong |= (forwardSubKmer.getLong(0) >>> 2*(param.subKmerSize - forwardSuffixLength));

                        newReflexivLongArray = new Long[concatenateLength/31+1];
                        newReflexivLongArray[0] = newReflexivLonghead;
                        newReflexivLongArray[1] = newReflexivLong;
                    }else {
                        newReflexivLong = reflexedSubKmer.getLong(2) << (2 * forwardSuffixLength); // do not remove the C marker as it will be used again
                        newReflexivLong |= (forwardSubKmer.getLong(0) >>> (2 * (param.subKmerSize - forwardSuffixLength))); // do not have to add the C marker

                        // the first time only one element in the array
                        newReflexivLongArray = new Long[1];
                        newReflexivLongArray[0] = newReflexivLong;
                    }
                }

                if (bubbleDistance <0) {
                    reflexivKmerConcatList.add(
                            RowFactory.create(newReflexivSubKmer,
                                            randomReflexivMarker, newReflexivLongArray, reflexedSubKmer.getInt(3), forwardSubKmer.getInt(4)
                            )
                    );
                }else {
                    if (forwardSubKmer.getInt(3) > 0) {
                        reflexivKmerConcatList.add(
                                RowFactory.create(newReflexivSubKmer,
                                                randomReflexivMarker, newReflexivLongArray, bubbleDistance, forwardSubKmer.getInt(4)
                                )
                        );
                    }else{
                        reflexivKmerConcatList.add(
                                RowFactory.create(newReflexivSubKmer,
                                                randomReflexivMarker, newReflexivLongArray, reflexedSubKmer.getInt(3), bubbleDistance
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
                    newForwardSubKmer = (reflexedSubKmer.getLong(2) & maxPrefixLengthBinary) << (2*(param.subKmerSize - reflexedPrefixLength));
                    newForwardSubKmer |= reflexedSubKmer.getLong(0) >>> 2*reflexedPrefixLength;

                    if (concatenateLength>31){
                        Long newForwardLonghead = forwardSubKmer.getLong(0) & maxPrefixLengthBinary;
                        newForwardLonghead >>>= 2*(31 - forwardSuffixLength);
                        newForwardLonghead |= (1L << 2*(concatenateLength -31)); // add the C maker

                        newForwardLong = forwardSubKmer.getLong(0) << 2*forwardSuffixLength;
                        newForwardLong |= (forwardSubKmer.getLong(2) & maxSuffixLengthBinary);
                        newForwardLong &= maxBlockBinary;

                        newForwardLongArray = new Long[concatenateLength/31+1];
                        newForwardLongArray[0] = newForwardLonghead;
                        newForwardLongArray[1] = newForwardLong;
                    }else {
                        newForwardLong = reflexedSubKmer.getLong(0) & maxPrefixLengthBinary;
                        newForwardLong |= (1L << (2 * reflexedPrefixLength)); // add the C marker
                        newForwardLong <<= (2 * forwardSuffixLength);
                        newForwardLong |= (forwardSubKmer.getLong(2) & maxSuffixLengthBinary);

                        // the first time only one element
                        newForwardLongArray = new Long[1];
                        newForwardLongArray[0] = newForwardLong;
                    }
                }

                if (bubbleDistance <0) {
                    reflexivKmerConcatList.add(
                            RowFactory.create(newForwardSubKmer,
                                            randomReflexivMarker, newForwardLongArray, reflexedSubKmer.getInt(3), forwardSubKmer.getInt(4)
                            )
                    );
                }else {
                    if (forwardSubKmer.getInt(3) > 0) {
                        reflexivKmerConcatList.add(
                                RowFactory.create(newForwardSubKmer,
                                                randomReflexivMarker, newForwardLongArray, bubbleDistance, forwardSubKmer.getInt(4)
                                )
                        );
                    }else{ // reflexedSubKmer.getInt(4) >0
                        reflexivKmerConcatList.add(
                                RowFactory.create(newForwardSubKmer,
                                                randomReflexivMarker, newForwardLongArray, reflexedSubKmer.getInt(3), bubbleDistance
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
            }else {
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
                    RowFactory.create(S.getLong(0),
                                    S.getInt(1), S.getLong(2), S.getInt(3), S.getInt(4)
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


    class DSExtendReflexivKmer implements MapPartitionsFunction<Row, Row>, Serializable{

        /* marker to identify similar SubKmers in the loop sequence */
        private int lineMarker=1;

        /* 1 stands for forward sub-kmer */
        /* 2 stands for reflexiv sub-kmer */
        //     private int randomReflexivMarker = ThreadLocalRandom.current().nextInt(1, 3);
        private int randomReflexivMarker=2;

        long maxSubKmerBinary = ~((~0L) << 2*param.subKmerSize);


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
        public Iterator<Row> call (Iterator<Row> sIterator) {

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
                            if (s.getLong(0) == tmpReflexivKmerExtendList.get(i).getLong(0)) {
                                if (s.getInt(1) == 1) {
                                    if (tmpReflexivKmerExtendList.get(i).getInt(1) == 2) {
                                        int tmpReflexivKmerSuffixLength = Long.SIZE/2 - (Long.numberOfLeadingZeros(tmpReflexivKmerExtendList.get(i).getLong(2))/2 + 1);
                                        int currentReflexivKmerSuffixLength = Long.SIZE/2 - (Long.numberOfLeadingZeros(s.getLong(2))/2 + 1);
                                        if (s.getInt(3) < 0 && tmpReflexivKmerExtendList.get(i).getInt(4) < 0) {
                                            reflexivExtend(s, tmpReflexivKmerExtendList.get(i), -1);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        }else if (s.getInt(3)>=0 && tmpReflexivKmerExtendList.get(i).getInt(4)>=0) {
                                            reflexivExtend(s, tmpReflexivKmerExtendList.get(i), -1);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        }else if (s.getInt(3)>=0 && s.getInt(3)-tmpReflexivKmerSuffixLength>=0) {
                                            reflexivExtend(s, tmpReflexivKmerExtendList.get(i), s.getInt(3)-tmpReflexivKmerSuffixLength);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        }else if (tmpReflexivKmerExtendList.get(i).getInt(4) >=0 && tmpReflexivKmerExtendList.get(i).getInt(4)-currentReflexivKmerSuffixLength>=0){
                                            reflexivExtend(s, tmpReflexivKmerExtendList.get(i), tmpReflexivKmerExtendList.get(i).getInt(4)-currentReflexivKmerSuffixLength);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        }else{
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
                                        int tmpReflexivKmerSuffixLength = Long.SIZE/2 - (Long.numberOfLeadingZeros(tmpReflexivKmerExtendList.get(i).getLong(2))/2 + 1);
                                        int currentReflexivKmerSuffixLength = Long.SIZE/2 - (Long.numberOfLeadingZeros(s.getLong(2))/2 + 1);
                                        if (s.getInt(4) < 0 && tmpReflexivKmerExtendList.get(i).getInt(3) < 0) {
                                            reflexivExtend(tmpReflexivKmerExtendList.get(i), s, -1);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        }else if (s.getInt(4)>=0 && tmpReflexivKmerExtendList.get(i).getInt(3)>=0) {
                                            reflexivExtend(tmpReflexivKmerExtendList.get(i), s, -1);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        }else if (s.getInt(4)>=0 && s.getInt(4)-tmpReflexivKmerSuffixLength>=0) {
                                            reflexivExtend(tmpReflexivKmerExtendList.get(i), s, s.getInt(4)-tmpReflexivKmerSuffixLength);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        }else if (tmpReflexivKmerExtendList.get(i).getInt(3) >=0 && tmpReflexivKmerExtendList.get(i).getInt(3)-currentReflexivKmerSuffixLength >=0){
                                            reflexivExtend(tmpReflexivKmerExtendList.get(i), s, tmpReflexivKmerExtendList.get(i).getInt(3)-currentReflexivKmerSuffixLength);
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
        public void singleKmerRandomizer(Row currentSubKmer){


            if (currentSubKmer.getInt(1) == 1){
                int currentSuffixLength = Long.SIZE/2 - (Long.numberOfLeadingZeros(currentSubKmer.getLong(2))/2 + 1);
                long maxSuffixLengthBinary = ~((~0L) << (2*currentSuffixLength));
                Long newReflexivSubKmer=0L;
                Long newReflexivLong=0L;

                if (randomReflexivMarker == 2) {
                    if (currentSuffixLength > param.subKmerSize) {
                        System.out.println("what? not possible. Tell the author to check his program. He knows");

                    }else if (currentSuffixLength == param.subKmerSize){
                        System.out.println("what? not possible. Tell the author to check his program. He knows");

                    }else{
                        newReflexivSubKmer = currentSubKmer.getLong(0) << (currentSuffixLength*2);
                        newReflexivSubKmer &= maxSubKmerBinary;
                        newReflexivSubKmer |= (currentSubKmer.getLong(2) & maxSuffixLengthBinary);


                        newReflexivLong = currentSubKmer.getLong(0) >>> (2*(param.subKmerSize- currentSuffixLength));
                        newReflexivLong |= (1L<<(2*currentSuffixLength)); // add C marker in the front


                    }

                    reflexivKmerConcatList.add(
                            RowFactory.create(newReflexivSubKmer, randomReflexivMarker, newReflexivLong, currentSubKmer.getInt(3), currentSubKmer.getInt(4))
                    );
                }else{
                    reflexivKmerConcatList.add(currentSubKmer);
                }
            }else{ /* currentSubKmer.getInt(1) == 2 */
                int currentPrefixLength = Long.SIZE/2 - (Long.numberOfLeadingZeros(currentSubKmer.getLong(2))/2 + 1);
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
                        newReflexivSubKmer = (currentSubKmer.getLong(2) & maxSuffixLengthBinary) << (2*(param.subKmerSize - currentPrefixLength));;
                        //newReflexivSubKmer <<= (2*(param.subKmerSize - currentPrefixLength));
                        newReflexivSubKmer |=(currentSubKmer.getLong(0) >>> (2*currentPrefixLength));

                        newReflexivLong = currentSubKmer.getLong(0) & maxSuffixLengthBinary;
                        newReflexivLong |= (1L<<2*currentPrefixLength); // add C marker in the front
                    }

                    reflexivKmerConcatList.add(
                            RowFactory.create(newReflexivSubKmer, randomReflexivMarker, newReflexivLong, currentSubKmer.getInt(3), currentSubKmer.getInt(4))
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
        public void directKmerComparison(Row currentSubKmer){
            tmpReflexivKmerExtendList.add(currentSubKmer);
        }

        /**
         *
         * @param forwardSubKmer
         * @param reflexedSubKmer
         */
        public void reflexivExtend(Row forwardSubKmer, Row reflexedSubKmer, int bubbleDistance) {

             /* forward   ATCGATCG, 1, ------ */
             /* reflexed  ------, 2, ATCGATCG */

            int forwardSuffixLength = Long.SIZE/2 - (Long.numberOfLeadingZeros(forwardSubKmer.getLong(2))/2 + 1);
            int reflexedPrefixLength = Long.SIZE/2 - (Long.numberOfLeadingZeros(reflexedSubKmer.getLong(2))/2 + 1);
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
                    newReflexivSubKmer = forwardSubKmer.getLong(0) << (2*forwardSuffixLength);
                    newReflexivSubKmer &= maxSubKmerBinary;
                    newReflexivSubKmer |= (forwardSubKmer.getLong(2) & maxSuffixLengthBinary);

                    newReflexivLong = reflexedSubKmer.getLong(2) << (2*forwardSuffixLength); // do not remove the C marker as it will be used again
                    newReflexivLong |= (forwardSubKmer.getLong(0) >>> (2*(param.subKmerSize - forwardSuffixLength))); // do not have to add the C marker
                }

                if (bubbleDistance <0) {
                    reflexivKmerConcatList.add(
                            RowFactory.create(newReflexivSubKmer, randomReflexivMarker, newReflexivLong, reflexedSubKmer.getInt(3), forwardSubKmer.getInt(4))
                    );
                }else {
                    if (forwardSubKmer.getInt(3) > 0) {
                        reflexivKmerConcatList.add(
                                RowFactory.create(newReflexivSubKmer, randomReflexivMarker, newReflexivLong, bubbleDistance, forwardSubKmer.getInt(4))
                        );
                    }else{
                        reflexivKmerConcatList.add(
                                RowFactory.create(newReflexivSubKmer, randomReflexivMarker, newReflexivLong, reflexedSubKmer.getInt(3), bubbleDistance)
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
                    newForwardSubKmer = (reflexedSubKmer.getLong(2) & maxPrefixLengthBinary) << (2*(param.subKmerSize - reflexedPrefixLength));
                    newForwardSubKmer |= reflexedSubKmer.getLong(0) >>> 2*reflexedPrefixLength;

                    newForwardLong = reflexedSubKmer.getLong(0) & maxPrefixLengthBinary;
                    newForwardLong |= (1L << (2*reflexedPrefixLength)); // add the C marker
                    newForwardLong <<= (2*forwardSuffixLength);
                    newForwardLong |= (forwardSubKmer.getLong(2) & maxSuffixLengthBinary);
                }

                if (bubbleDistance <0) {
                    reflexivKmerConcatList.add(
                            RowFactory.create(newForwardSubKmer, randomReflexivMarker, newForwardLong, reflexedSubKmer.getInt(3), forwardSubKmer.getInt(4))
                    );
                }else {
                    if (forwardSubKmer.getInt(3) > 0) {
                        reflexivKmerConcatList.add(
                                RowFactory.create(newForwardSubKmer,
                                                randomReflexivMarker, newForwardLong, bubbleDistance, forwardSubKmer.getInt(4)
                                )
                        );
                    }else{ // reflexedSubKmer.getInt(4) >0
                        reflexivKmerConcatList.add(
                                RowFactory.create(newForwardSubKmer,
                                                randomReflexivMarker, newForwardLong, reflexedSubKmer.getInt(3), bubbleDistance
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
            }else {
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
                    RowFactory.create(S.getLong(0),
                                    S.getInt(1), S.getLong(2), S.getInt(3), S.getInt(4)
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
                            RowFactory.create(subKmer.getLong(0), subKmer.getInt(1), subKmer.getLong(2), subKmer.getInt(3), -1)
                    );
                } else {
                    if (subKmer.getLong(0) == (HighCoverageSubKmer.get(HighCoverageSubKmer.size()-1).getLong(0))){
                        if (subKmer.getInt(3) > HighCoverageSubKmer.get(HighCoverageSubKmer.size()-1).getInt(3)){
                            HighCoverageSubKmer.set(HighCoverageSubKmer.size()-1,
                                    RowFactory.create(subKmer.getLong(0), subKmer.getInt(1), subKmer.getLong(2), subKmer.getInt(3), param.subKmerSize)
                            );
                        } else if (subKmer.getInt(3) == HighCoverageSubKmer.get(HighCoverageSubKmer.size() -1).getInt(3)){
                            if (subKmer.getLong(2) > HighCoverageSubKmer.get(HighCoverageSubKmer.size()-1).getLong(2)){
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size()-1,
                                        RowFactory.create(subKmer.getLong(0), subKmer.getInt(1), subKmer.getLong(2), subKmer.getInt(3), param.subKmerSize)
                                );
                            }else {
                                /**
                                 * can be optimized
                                 */
                                subKmer = HighCoverageSubKmer.get(HighCoverageSubKmer.size()-1);
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size()-1,
                                        RowFactory.create(subKmer.getLong(0), subKmer.getInt(1), subKmer.getLong(2), subKmer.getInt(3), param.subKmerSize)
                                );
                            }
                        } else {
                            subKmer = HighCoverageSubKmer.get(HighCoverageSubKmer.size() -1);
                            HighCoverageSubKmer.set(HighCoverageSubKmer.size()-1,
                                    RowFactory.create(subKmer.getLong(0), subKmer.getInt(1), subKmer.getLong(2), subKmer.getInt(3), param.subKmerSize)
                            );
                        }
                    } else {
                        HighCoverageSubKmer.add(
                                RowFactory.create(subKmer.getLong(0), subKmer.getInt(1), subKmer.getLong(2), subKmer.getInt(3), -1)
                        );
                    }
                }
            }

            return HighCoverageSubKmer.iterator();
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
                if (HighCoverageSubKmer.size() == 0) {
                    HighCoverageSubKmer.add(
                            RowFactory.create(subKmer.getLong(0), subKmer.getInt(1), subKmer.getLong(2), subKmer.getInt(3), -1-subKmer.getInt(3))
                    );
                } else {
                    if (subKmer.getLong(0) == HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1).getLong(0)) {
                        if (subKmer.getInt(3) > HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1).getInt(3)) {
                            if (HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1).getInt(3) <= param.minErrorCoverage && subKmer.getInt(3) >= 2 * HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1).getInt(3)) {
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        RowFactory.create(subKmer.getLong(0), subKmer.getInt(1), subKmer.getLong(2), subKmer.getInt(3), -1-subKmer.getInt(3))
                                );
                            } else {
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        RowFactory.create(subKmer.getLong(0), subKmer.getInt(1), subKmer.getLong(2), subKmer.getInt(3), param.subKmerSize)
                                );
                            }
                        } else if (subKmer.getInt(3) == HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1).getInt(3)) {
                            if (subKmer.getLong(2) > HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1).getLong(2)) {
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        RowFactory.create(subKmer.getLong(0), subKmer.getInt(1), subKmer.getLong(2), subKmer.getInt(3), param.subKmerSize)
                                );
                            } else {
                                subKmer = HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1);
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        RowFactory.create(subKmer.getLong(0), subKmer.getInt(1), subKmer.getLong(2), subKmer.getInt(3), param.subKmerSize)
                                );
                            }
                        } else {
                            if (subKmer.getInt(3) <= param.minErrorCoverage && HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1).getInt(3) >= 2 * subKmer.getInt(3)) {
                                subKmer = HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1);
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        RowFactory.create(subKmer.getLong(0), subKmer.getInt(1), subKmer.getLong(2), subKmer.getInt(3), -1-subKmer.getInt(3))
                                );
                            } else {
                                subKmer = HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1);
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        RowFactory.create(subKmer.getLong(0), subKmer.getInt(1), subKmer.getLong(2), subKmer.getInt(3), param.subKmerSize)
                                );
                            }
                        }
                    } else {
                        HighCoverageSubKmer.add(
                                RowFactory.create(subKmer.getLong(0), subKmer.getInt(1), subKmer.getLong(2), subKmer.getInt(3), -1-subKmer.getInt(3))
                        );
                    }
                }
            }

            return HighCoverageSubKmer.iterator();
        }
    }

    class DSFilterForkReflectedSubKmer implements MapPartitionsFunction<Row, Row>, Serializable{
        List<Row> HighCoverageSubKmer = new ArrayList<Row>();
        Integer HighCoverLastCoverage = 0;
//        Row HighCoverKmer=null;
//                new Row("",
        //                       new Tuple4<Integer, Long, Integer, Integer>(0, "", 0, 0));

        public Iterator<Row> call (Iterator<Row> s){
            while (s.hasNext()){
                Row subKmer = s.next();
                if (HighCoverageSubKmer.size() == 0){
                    HighCoverLastCoverage = subKmer.getInt(3);
                    HighCoverageSubKmer.add(
                            RowFactory.create(subKmer.getLong(0), subKmer.getInt(1), subKmer.getLong(2), -1, subKmer.getInt(4))
                    );
                }else {
                    if (subKmer.getLong(0) == HighCoverageSubKmer.get(HighCoverageSubKmer.size()-1).getLong(0)) {
                        if (subKmer.getInt(3) > HighCoverLastCoverage) {
                            HighCoverLastCoverage = subKmer.getInt(3);
                            HighCoverageSubKmer.set(HighCoverageSubKmer.size()-1,
                                    RowFactory.create(subKmer.getLong(0),subKmer.getInt(1), subKmer.getLong(2), param.subKmerSize, subKmer.getInt(4))
                            );
                        } else if (subKmer.getInt(3) == HighCoverLastCoverage){
                            int subKmerFirstSuffixLength = Long.SIZE/2 - (Long.numberOfLeadingZeros(subKmer.getLong(2))/2 + 1);
                            int HighCoverageSubKmerFirstSuffixLength = Long.SIZE/2 - ((Long.numberOfLeadingZeros(HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1).getLong(2)))/2 + 1);
                            Long subKmerFirstSuffix = subKmer.getLong(2) >>> 2*(subKmerFirstSuffixLength-1);
                            Long HighCoverageSubKmerFirstSuffix = HighCoverageSubKmer.get(HighCoverageSubKmer.size()-1).getLong(2) >>> 2*(HighCoverageSubKmerFirstSuffixLength);

                            if (subKmerFirstSuffix.compareTo(HighCoverageSubKmerFirstSuffix) >0){
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size()-1,
                                        RowFactory.create(subKmer.getLong(0), subKmer.getInt(1), subKmer.getLong(2), param.subKmerSize, subKmer.getInt(4))
                                );
                            }else{
                                subKmer = HighCoverageSubKmer.get(HighCoverageSubKmer.size()-1); // re assign
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size()-1,
                                        RowFactory.create(subKmer.getLong(0), subKmer.getInt(1), subKmer.getLong(2), param.subKmerSize, subKmer.getInt(4))
                                );
                            }
                        } else {
                            subKmer = HighCoverageSubKmer.get(HighCoverageSubKmer.size()-1);
                            HighCoverageSubKmer.set(HighCoverageSubKmer.size()-1,
                                    RowFactory.create(subKmer.getLong(0), subKmer.getInt(1), subKmer.getLong(2), param.subKmerSize, subKmer.getInt(4))
                            );
                        }
                    }else{
                        HighCoverLastCoverage = subKmer.getInt(3);
                        HighCoverageSubKmer.add(
                                RowFactory.create(subKmer.getLong(0), subKmer.getInt(1), subKmer.getLong(2), -1, subKmer.getInt(4))
                        );
                    }
                }
            }

            return HighCoverageSubKmer.iterator();
        }
    }

    class DSFilterForkReflectedSubKmerWithErrorCorrection implements MapPartitionsFunction<Row, Row>, Serializable{
        List<Row> HighCoverageSubKmer = new ArrayList<Row>();
        Integer HighCoverLastCoverage = 0;
//        Row HighCoverKmer=null;
//                new Tuple2<Long, Tuple4<Integer, Long, Integer, Integer>>("",
        //                       new Tuple4<Integer, Long, Integer, Integer>(0, "", 0, 0));

        public Iterator<Row> call (Iterator<Row> s){
            while (s.hasNext()){
                Row subKmer = s.next();
                if (HighCoverageSubKmer.size() == 0){
                    HighCoverLastCoverage = subKmer.getInt(3);
                    HighCoverageSubKmer.add(
                            RowFactory.create(subKmer.getLong(0), subKmer.getInt(1), subKmer.getLong(2), -1-subKmer.getInt(3), subKmer.getInt(4))
                    );
                }else {
                    if (subKmer.getLong(0) == HighCoverageSubKmer.get(HighCoverageSubKmer.size()-1).getLong(0)) {
                        if (subKmer.getInt(3) > HighCoverLastCoverage) {
                            if (HighCoverLastCoverage <= param.minErrorCoverage && subKmer.getInt(3) >= 2*HighCoverLastCoverage){
                                HighCoverLastCoverage = subKmer.getInt(3);
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        RowFactory.create(subKmer.getLong(0), subKmer.getInt(1), subKmer.getLong(2), -1-subKmer.getInt(3), subKmer.getInt(4))
                                );
                            }else {
                                HighCoverLastCoverage = subKmer.getInt(3);
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        RowFactory.create(subKmer.getLong(0), subKmer.getInt(1), subKmer.getLong(2), param.subKmerSize, subKmer.getInt(4))
                                );
                            }
                        } else if (subKmer.getInt(3) == HighCoverLastCoverage){
                            int subKmerFirstSuffixLength = Long.SIZE/2 - (Long.numberOfLeadingZeros(subKmer.getLong(2))/2 + 1);
                            int HighCoverageSubKmerFirstSuffixLength = Long.SIZE/2 - ((Long.numberOfLeadingZeros(HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1).getLong(2)))/2 + 1);
                            Long subKmerFirstSuffix = subKmer.getLong(2) >>> 2*(subKmerFirstSuffixLength-1);
                            Long HighCoverageSubKmerFirstSuffix = HighCoverageSubKmer.get(HighCoverageSubKmer.size()-1).getLong(2) >>> 2*(HighCoverageSubKmerFirstSuffixLength);

                            if (subKmerFirstSuffix.compareTo(HighCoverageSubKmerFirstSuffix) >0){
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size()-1,
                                        RowFactory.create(subKmer.getLong(0),
                                                subKmer.getInt(1), subKmer.getLong(2), param.subKmerSize, subKmer.getInt(4))
                                );
                            }else{
                                subKmer = HighCoverageSubKmer.get(HighCoverageSubKmer.size()-1); // re assign
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size()-1,
                                        RowFactory.create(subKmer.getLong(0),
                                                subKmer.getInt(1), subKmer.getLong(2), param.subKmerSize, subKmer.getInt(4))
                                );
                            }
                        } else {
                            if (subKmer.getInt(3) <= param.minErrorCoverage && HighCoverLastCoverage >= 2*subKmer.getInt(3)) {
                                subKmer = HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1);
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        RowFactory.create(subKmer.getLong(0),
                                                subKmer.getInt(1), subKmer.getLong(2), subKmer.getInt(3), subKmer.getInt(4))
                                );
                            }else {
                                subKmer = HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1);
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        RowFactory.create(subKmer.getLong(0),
                                                subKmer.getInt(1), subKmer.getLong(2), param.subKmerSize, subKmer.getInt(4))
                                );
                            }
                        }
                    }else{
                        HighCoverLastCoverage = subKmer.getInt(3);
                        HighCoverageSubKmer.add(
                                RowFactory.create(subKmer.getLong(0),
                                        subKmer.getInt(1), subKmer.getLong(2), -1-subKmer.getInt(3), subKmer.getInt(4))
                        );
                    }
                }
            }

            return HighCoverageSubKmer.iterator();
        }
    }

    class DSForwardSubKmerExtraction implements MapPartitionsFunction<Row, Row>, Serializable {
        List<Row> TupleList = new ArrayList<Row>();
        Long suffixBinary;
        Long prefixBinary;
        Row kmerTuple;

        public Iterator<Row> call(Iterator<Row> s) {

            while (s.hasNext()) {
                kmerTuple = s.next();
                /**
                 * normal Sub-kmer
                 *        Kmer      ATGCACGTTATG
                 *        Sub-Kmer  ATGCACGTTAT         marked as Integer 1 in Tuple2
                 *        Left      -----------G
                 */
                suffixBinary = kmerTuple.getLong(0) & 3L;
                prefixBinary = kmerTuple.getLong(0) >>> 2;

                TupleList.add(
                        RowFactory.create(prefixBinary, 1, suffixBinary, kmerTuple.getInt(1), kmerTuple.getInt(1))
                );
            }

            return TupleList.iterator();
        }
    }


    /**
     *
     */


    class DSReflectedSubKmerExtractionFromForward implements MapPartitionsFunction<Row, Row>, Serializable {
        List<Row> TupleList = new ArrayList<Row>();
        Long suffixBinary;
        Long prefixBinary;
        Row kmerTuple;
        int shift =(2*(param.subKmerSize-1));
        Long maxSubKmerBinary = ~((~0L)<<2*param.subKmerSize);

            public Iterator<Row> call(Iterator<Row> s) {

            while (s.hasNext()) {
                kmerTuple = s.next();
                /**
                 * reflected Sub-kmer
                 *        Kmer      ATGCACGTTATG
                 *        Sub-Kmer  ATGCACGTTAT         marked as Integer 1 in Tuple2
                 *        Left      -----------G
                 */
                suffixBinary = 3L << shift;
                suffixBinary = kmerTuple.getLong(0) & suffixBinary;
                suffixBinary >>>= shift;
                suffixBinary |=4L; // add C marker in the front 0100 = 4L

                prefixBinary = kmerTuple.getLong(0) <<2 & maxSubKmerBinary;
                prefixBinary |= kmerTuple.getLong(2);

                TupleList.add(
                        RowFactory.create(prefixBinary, 2, suffixBinary, kmerTuple.getInt(3), kmerTuple.getInt(4))
                );
            }

            return TupleList.iterator();
        }
    }

    class DSkmerRandomReflection implements MapPartitionsFunction<Row, Row>, Serializable{
        /* 0 stands for forward sub-kmer */
        /* 1 stands for reflexiv sub-kmer */
        private int randomReflexivMarker = 2;

        List<Row> reflexivKmerConcatList = new ArrayList<Row>();
        Row kmerTuple;
        long maxSubKmerBinary = ~((~0L) << 2*param.subKmerSize);

        public Iterator<Row> call (Iterator<Row> s){
            while (s.hasNext()) {
                kmerTuple = s.next();

                singleKmerRandomizer(kmerTuple);
            }
            return reflexivKmerConcatList.iterator();
        }

        public void singleKmerRandomizer(Row currentSubKmer){

            if (currentSubKmer.getInt(1) == 1){
                /**
                 * 00000000000000110010111010010   Long.SIZE
                 * --------------C-G-G-G-T-C-A-G   Long.SIZE - (Long.numberOfLeadingZeros / 2 + 1)
                 * --------------^-Length marker
                 */
                int currentSuffixLength = Long.SIZE/2 - (Long.numberOfLeadingZeros(currentSubKmer.getLong(2))/2 + 1);
                long maxSuffixLengthBinary = ~(~0L << 2*currentSuffixLength);
                Long newReflexivSubKmer;
                Long newReflexivLong;

                if (randomReflexivMarker == 2) {
                    if (currentSuffixLength > param.subKmerSize) { // not possible in the first five (include initial) rounds
                        newReflexivSubKmer = currentSubKmer.getLong(0) << currentSuffixLength*2 & maxSubKmerBinary;
                        newReflexivSubKmer |= (currentSubKmer.getLong(2) & maxSuffixLengthBinary);

                        newReflexivLong = currentSubKmer.getLong(0) >>> 2*currentSuffixLength;
                        newReflexivLong |= (1L<<2*currentSuffixLength); // add C marker in front
                        // not finished
                    }else if (currentSuffixLength == param.subKmerSize){ // not possible in the first five (include initial) rounds
                        newReflexivSubKmer = currentSubKmer.getLong(0) << currentSuffixLength*2 & maxSubKmerBinary;
                        newReflexivSubKmer |= (currentSubKmer.getLong(2) & maxSuffixLengthBinary);

                        newReflexivLong = currentSubKmer.getLong(0) >>> 2*currentSuffixLength;
                        newReflexivLong |= (1L<<2*currentSuffixLength); // add C marker in front
                        // not finished
                    }else{ // now this is possible in the first five
                        newReflexivSubKmer = currentSubKmer.getLong(0) << currentSuffixLength*2;
                        newReflexivSubKmer &= maxSubKmerBinary;
                        newReflexivSubKmer |= (currentSubKmer.getLong(2) & maxSuffixLengthBinary);

                        newReflexivLong = currentSubKmer.getLong(0) >>> 2*(param.subKmerSize - currentSuffixLength);
                        newReflexivLong |= (1L<<2*currentSuffixLength); // add C marker in the front
                    }

                    reflexivKmerConcatList.add(
                            RowFactory.create(newReflexivSubKmer, randomReflexivMarker, newReflexivLong, currentSubKmer.getInt(3), currentSubKmer.getInt(4))
                    );
                }else{
                    reflexivKmerConcatList.add(currentSubKmer);
                }
            }else{ /* currentSubKmer._2._1() == 2 */
                int currentPrefixLength = Long.SIZE/2 - (Long.numberOfLeadingZeros(currentSubKmer.getLong(2))/2 + 1);
                long maxSuffixLengthBinary = ~((~0L) << (2*currentPrefixLength));
                Long newReflexivSubKmer;
                Long newReflexivLong;

                if (randomReflexivMarker == 2) {
                    reflexivKmerConcatList.add(currentSubKmer);
                }else{ /* randomReflexivMarker == 1 */
                    if (currentPrefixLength > param.subKmerSize){ //
                        newReflexivSubKmer = currentSubKmer.getLong(2) << 2*(param.subKmerSize - currentPrefixLength);

                        newReflexivLong = currentSubKmer.getLong(0) & maxSuffixLengthBinary;
                        newReflexivLong |= (1L<<2*currentPrefixLength); // add C marker in the front
                    }else if (currentPrefixLength == param.subKmerSize){ //
                        newReflexivSubKmer = currentSubKmer.getLong(2) << 2*(param.subKmerSize - currentPrefixLength);

                        newReflexivLong = currentSubKmer.getLong(0) & maxSuffixLengthBinary;
                        newReflexivLong |= (1L<<2*currentPrefixLength); // add C marker in the front
                    }else{ /* currentPreffixLength < param.subKmerSize */
                        newReflexivSubKmer = (currentSubKmer.getLong(2) & maxSuffixLengthBinary) << (2*(param.subKmerSize - currentPrefixLength));;
                        //newReflexivSubKmer <<= (2*(param.subKmerSize - currentPrefixLength));
                        newReflexivSubKmer |=(currentSubKmer.getLong(0) >>> (2*currentPrefixLength));

                        newReflexivLong = currentSubKmer.getLong(0) & maxSuffixLengthBinary;
                        newReflexivLong |= (1L<<2*currentPrefixLength); // add C marker in the front
                    }

                    reflexivKmerConcatList.add(
                            RowFactory.create(newReflexivSubKmer, randomReflexivMarker, newReflexivLong, currentSubKmer.getInt(3), currentSubKmer.getInt(4))
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
     *
     */


    /**
     * interface class for RDD implementation, used in step 5
     */

    /**
     * interface class for RDD implementation, used in step 4
     */


    class DSKmerReverseComplement implements MapPartitionsFunction<Row, Row>, Serializable{
        /* a capsule for all Kmers and reverseComplementKmers */
        List<Row> kmerList = new ArrayList<Row>();
        Long reverseComplement;
        Row kmerTuple;
        Long lastTwoBits;
        Long kmerBinary;


        public Iterator<Row> call(Iterator<Row> s){


            while (s.hasNext()) {
                kmerTuple = s.next();
                kmerBinary = kmerTuple.getLong(0);
                reverseComplement=0L;
                for (int i = 0; i < param.kmerSize; i++) {
                    reverseComplement<<=2;

                    lastTwoBits = kmerBinary & 3L ^ 3L;
                    kmerBinary >>>=2;
                    reverseComplement|=lastTwoBits;
                }

                kmerList.add(RowFactory.create(kmerTuple.getLong(0), kmerTuple.getInt(1)));
                kmerList.add(RowFactory.create(reverseComplement, kmerTuple.getInt(1)));
            }

            return kmerList.iterator();
        }
    }

    class DSKmerReverseComplementLong implements MapPartitionsFunction<Row, Row>, Serializable{
        /* a capsule for all Kmers and reverseComplementKmers */
        List<Row> kmerList = new ArrayList<Row>();
        Long reverseComplement;
        Row kmerTuple;
        Long lastTwoBits;
        Long kmerBinary;


        public Iterator<Row> call(Iterator<Row> s){


            while (s.hasNext()) {
                kmerTuple = s.next();
                kmerBinary = kmerTuple.getLong(0);
                reverseComplement=0L;
                for (int i = 0; i < param.kmerSize; i++) {
                    reverseComplement<<=2;

                    lastTwoBits = kmerBinary & 3L ^ 3L;
                    kmerBinary >>>=2;
                    reverseComplement|=lastTwoBits;
                }

                kmerList.add(RowFactory.create(kmerTuple.getLong(0), (int)kmerTuple.getLong(1)));
                kmerList.add(RowFactory.create(reverseComplement, (int)kmerTuple.getLong(1)));
            }

            return kmerList.iterator();
        }
    }

    class KmerBinarizer implements MapPartitionsFunction<Row, Row>, Serializable {

        List<Row> kmerList = new ArrayList<Row>();
        Row units;
        String kmer;
        int cover;
        char nucleotide;
        long nucleotideInt;
        //     Long suffixBinary;
        //     Long[] suffixBinaryArray;

        public Iterator<Row> call(Iterator<Row> s) {

            while (s.hasNext()) {

                units =s.next();

                kmer = units.getString(0);

                if (kmer.startsWith("(")){
                    kmer = kmer.substring(1);
                }

                if (units.getString(1).endsWith(")")){
                    if (units.getString(1).length() >=11){
                        cover = 1000000000;
                    }else {
                        cover = Integer.parseInt(StringUtils.chop(units.getString(1)));
                    }
                }else {
                    if (units.getString(1).length() >= 10 ){
                        cover = 1000000000;
                    }else {
                        cover = Integer.parseInt(units.getString(1));
                    }
                }

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
                        RowFactory.create(nucleotideBinary, cover)
                );

          //      kmerList.add(
            //            new Tuple2<Long, Integer>(
              //                  nucleotideBinary, cover
                //        )
                //);
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

    class ReverseComplementKmerBinaryExtractionFromDataset implements MapPartitionsFunction<String, Long>, Serializable{
        long maxKmerBits= ~((~0L) << (2*param.kmerSize));

        List<Long> kmerList = new ArrayList<Long>();
        int readLength;
        String[] units;
        String read;
        char nucleotide;
        long nucleotideInt;
        long nucleotideIntComplement;

        public Iterator<Long> call(Iterator<String> s){

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


    class DSFastqUnitFilter implements FilterFunction<String>, Serializable{
        public boolean call(String s){
            return s != null;
        }
    }

    /**
     * interface class for RDD implementation, Used in step 1
     */


    class DSFastqFilterWithQual implements MapFunction<String, String>, Serializable{
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
