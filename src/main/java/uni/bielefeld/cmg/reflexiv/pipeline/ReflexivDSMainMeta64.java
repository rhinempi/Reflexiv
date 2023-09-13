package uni.bielefeld.cmg.reflexiv.pipeline;


import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

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
public class ReflexivDSMainMeta64 implements Serializable {
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

    /**
     *
     */
    public void assembly() {
        SparkSession spark = setSparkSessionConfiguration(param.shufflePartition);

        info.readMessage("Initiating Spark context ...");
        info.screenDump();
        info.readMessage("Start Spark framework");
        info.screenDump();

        spark.sparkContext().setCheckpointDir("/tmp");

        Dataset<String> FastqDS;
        Dataset<Row> KmerBinaryDS;

        Dataset<Row> KmerBinaryCountLongDS;
        Dataset<Row> KmerBinaryCountDS;

        StructType kmerCountTupleStruct = new StructType();
        kmerCountTupleStruct = kmerCountTupleStruct.add("kmerBlocks", DataTypes.createArrayType(DataTypes.LongType), false);
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

        Dataset<Row> ReflexivSubKmerStringDS;
        StructType ReflexivKmerStringStruct = new StructType();
        ReflexivKmerStringStruct = ReflexivKmerStringStruct.add("k-1", DataTypes.StringType, false);
        ReflexivKmerStringStruct = ReflexivKmerStringStruct.add("reflection", DataTypes.IntegerType, false);
        ReflexivKmerStringStruct = ReflexivKmerStringStruct.add("extension", DataTypes.StringType, false);
        ReflexivKmerStringStruct = ReflexivKmerStringStruct.add("left", DataTypes.IntegerType, false);
        ReflexivKmerStringStruct = ReflexivKmerStringStruct.add("right", DataTypes.IntegerType, false);
        ExpressionEncoder<Row> ReflexivKmerStringEncoder = RowEncoder.apply(ReflexivKmerStringStruct);

        Dataset<Row> ReflexivLongSubKmerDS;
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

        Dataset<Row> ContigRows;
        StructType ContigLongKmerStringStruct = new StructType();
        ContigLongKmerStringStruct = ContigLongKmerStringStruct.add("ID", DataTypes.StringType, false);
        ContigLongKmerStringStruct = ContigLongKmerStringStruct.add("contig", DataTypes.StringType, false);
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

        ReverseComplementKmerBinaryExtractionFromDataset64 DSExtractRCKmerBinaryFromFastq = new ReverseComplementKmerBinaryExtractionFromDataset64();
        KmerBinaryDS = FastqDS.mapPartitions(DSExtractRCKmerBinaryFromFastq, KmerBinaryCountEncoder);

        KmerBinaryCountLongDS = KmerBinaryDS.groupBy("kmerBlocks")
                .count()
                .toDF("kmerBlocks", "count");

        KmerBinaryCountLongDS = KmerBinaryCountLongDS.filter(col("count")
                .geq(param.minKmerCoverage)
                .and(col("count")
                        .leq(param.maxKmerCoverage)
                )
        );

        /**
         * Extract reverse complementary kmer
         */
        DSKmerReverseComplement DSRCKmer = new DSKmerReverseComplement();
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
            } else {
                DSFilterForkSubKmerWithErrorCorrection DShighCoverageErrorRemovalSelector = new DSFilterForkSubKmerWithErrorCorrection();
                ReflexivSubKmerDS = ReflexivSubKmerDS.mapPartitions(DShighCoverageErrorRemovalSelector, ReflexivSubKmerEncoder);
            }

            DSReflectedSubKmerExtractionFromForward DSreflectionExtractor = new DSReflectedSubKmerExtractionFromForward();
            ReflexivSubKmerDS = ReflexivSubKmerDS.mapPartitions(DSreflectionExtractor, ReflexivSubKmerEncoder);

            ReflexivSubKmerDS = ReflexivSubKmerDS.sort("k-1");
            if (param.minErrorCoverage == 0) {
                DSFilterForkReflectedSubKmer DShighCoverageReflectedSelector = new DSFilterForkReflectedSubKmer();
                ReflexivSubKmerDS = ReflexivSubKmerDS.mapPartitions(DShighCoverageReflectedSelector, ReflexivSubKmerEncoder);
            } else {
                DSFilterForkReflectedSubKmerWithErrorCorrection DShighCoverageReflectedErrorRemovalSelector = new DSFilterForkReflectedSubKmerWithErrorCorrection();
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
        for (int i = 1; i < 4; i++) {
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
            if (iterations >= param.minimumIteration) {
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
    public void assemblyFromKmer() {
        SparkSession spark = setSparkSessionConfiguration(param.shufflePartition);

        info.readMessage("Initiating Spark context ...");
        info.screenDump();
        info.readMessage("Start Spark framework");
        info.screenDump();

        spark.sparkContext().setCheckpointDir("/tmp");

        Dataset<Row> KmerCountDS;

        Dataset<Row> KmerBinaryCountDS;
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

        Dataset<Row> ReflexivSubKmerStringDS;
        StructType ReflexivKmerStringStruct = new StructType();
        ReflexivKmerStringStruct = ReflexivKmerStringStruct.add("k-1", DataTypes.StringType, false);
        ReflexivKmerStringStruct = ReflexivKmerStringStruct.add("reflection", DataTypes.IntegerType, false);
        ReflexivKmerStringStruct = ReflexivKmerStringStruct.add("extension", DataTypes.StringType, false);
        ReflexivKmerStringStruct = ReflexivKmerStringStruct.add("left", DataTypes.IntegerType, false);
        ReflexivKmerStringStruct = ReflexivKmerStringStruct.add("right", DataTypes.IntegerType, false);
        ExpressionEncoder<Row> ReflexivKmerStringEncoder = RowEncoder.apply(ReflexivKmerStringStruct);

        Dataset<Row> ReflexivLongSubKmerDS;
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


        Dataset<Row> ContigRows;
        Dataset<Row> SmallContigRows;
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

//        KmerBinaryCountDS.show();

        /**
         * Extract forward sub kmer
         */


        DSForwardSubKmerExtraction DSextractForwardSubKmer = new DSForwardSubKmerExtraction();
        ReflexivSubKmerDS = KmerBinaryCountDS.mapPartitions(DSextractForwardSubKmer, ReflexivSubKmerEncoder);

//        ReflexivSubKmerDS.show();

        if (param.bubble == true) {
            ReflexivSubKmerDS = ReflexivSubKmerDS.sort("k-1");
            if (param.minErrorCoverage == 0) {
                DSFilterForkSubKmer DShighCoverageSelector = new DSFilterForkSubKmer();
                ReflexivSubKmerDS = ReflexivSubKmerDS.mapPartitions(DShighCoverageSelector, ReflexivSubKmerEncoder);
            } else {
                DSFilterForkSubKmerWithErrorCorrection DShighCoverageErrorRemovalSelector = new DSFilterForkSubKmerWithErrorCorrection();
                ReflexivSubKmerDS = ReflexivSubKmerDS.mapPartitions(DShighCoverageErrorRemovalSelector, ReflexivSubKmerEncoder);
            }

            DSReflectedSubKmerExtractionFromForward DSreflectionExtractor = new DSReflectedSubKmerExtractionFromForward();
            ReflexivSubKmerDS = ReflexivSubKmerDS.mapPartitions(DSreflectionExtractor, ReflexivSubKmerEncoder);

            ReflexivSubKmerDS = ReflexivSubKmerDS.sort("k-1");
            if (param.minErrorCoverage == 0) {
                DSFilterForkReflectedSubKmer DShighCoverageReflectedSelector = new DSFilterForkReflectedSubKmer();
                ReflexivSubKmerDS = ReflexivSubKmerDS.mapPartitions(DShighCoverageReflectedSelector, ReflexivSubKmerEncoder);
            } else {
                DSFilterForkReflectedSubKmerWithErrorCorrection DShighCoverageReflectedErrorRemovalSelector = new DSFilterForkReflectedSubKmerWithErrorCorrection();
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
        for (int i = 1; i < 4; i++) {
            iterations++;
            ReflexivSubKmerDS = ReflexivSubKmerDS.sort("k-1");
            ReflexivSubKmerDS = ReflexivSubKmerDS.mapPartitions(DSKmerExtention, ReflexivSubKmerEncoder);
        }

        ReflexivSubKmerDS = ReflexivSubKmerDS.sort("k-1");
        //      ReflexivSubKmerDS.cache();

        iterations++;

        /**
         * Extract Long sub kmer
         */


        DSExtendReflexivKmerToArrayFirstTime DSKmerExtentionToArrayFirst = new DSExtendReflexivKmerToArrayFirstTime();
        ReflexivLongSubKmerDS = ReflexivSubKmerDS.mapPartitions(DSKmerExtentionToArrayFirst, ReflexivLongKmerEncoder);


        ReflexivLongSubKmerDS = ReflexivLongSubKmerDS.sort("k-1");

        DSExtendReflexivKmerToArrayLoop DSKmerExtenstionArrayToArray = new DSExtendReflexivKmerToArrayLoop();

        DSBinaryReflexivKmerArrayToString DSArrayStringOutput = new DSBinaryReflexivKmerArrayToString();

        //       ReflexivSubKmerDS.unpersist();
        ReflexivLongSubKmerDS.cache();
        int partitionNumber = ReflexivLongSubKmerDS.toJavaRDD().getNumPartitions();
        long contigNumber = 0;
        while (iterations <= param.maximumIteration) {
            iterations++;
            if (iterations >= param.minimumIteration) {
                if (iterations % 3 == 0) {

                    /**
                     *  problem ------------------------------------------v
                     */
                    ReflexivLongSubKmerDS.cache();
                    if (iterations % 30 ==0){
                        ReflexivLongSubKmerDS=ReflexivLongSubKmerDS.localCheckpoint();
                    }

                    long currentContigNumber = ReflexivLongSubKmerDS.count();
                    if (contigNumber == currentContigNumber) {
                        if (param.scramble == 2){
                            param.scramble =3;
                            contigNumber = currentContigNumber;
                        }else {
                            break;
                        }
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


        /**
         *
         */

        ReflexivLongSubKmerStringDS = ReflexivLongSubKmerDS.mapPartitions(DSArrayStringOutput, ReflexivLongKmerStringEncoder);
        ReflexivLongSubKmerStringDS.cache();

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


        while (param.kmerSize + param.kmerIncrease <= param.maxKmerSize){

            param.kmerSize = param.kmerSize + param.kmerIncrease;

            info.readMessage("Re-assemble using k-mer: " + param.kmerSize);
            info.screenDump();

            param.setAllbyKmerSize(param.kmerSize);

            ReflexivLongSubKmerStringDS = ReflexivLongSubKmerStringDS.localCheckpoint();

            KmerBinaryExtractionFromStringDS64 DSContigToKmerBinary = new KmerBinaryExtractionFromStringDS64();
            ReflexivSubKmerDS = ReflexivLongSubKmerStringDS.mapPartitions(DSContigToKmerBinary, ReflexivSubKmerEncoder);

            DSkmerRandomReflectionFromLongNotSeq DSrandomizeSubKmerFromLongArrayCast = new DSkmerRandomReflectionFromLongNotSeq();
            ReflexivSubKmerDS = ReflexivSubKmerDS.mapPartitions(DSrandomizeSubKmerFromLongArrayCast, ReflexivSubKmerEncoder);
            ReflexivSubKmerDS = ReflexivSubKmerDS.sort("k-1");

            DSKmerExtention = new DSExtendReflexivKmer();
            ReflexivSubKmerDS = ReflexivSubKmerDS.mapPartitions(DSKmerExtention, ReflexivSubKmerEncoder);
            ReflexivSubKmerDS.cache();

            iterations = 0;
            for (int i = 1; i < 4; i++) {
                iterations++;
                ReflexivSubKmerDS = ReflexivSubKmerDS.sort("k-1");
                ReflexivSubKmerDS = ReflexivSubKmerDS.mapPartitions(DSKmerExtention, ReflexivSubKmerEncoder);
            }

            ReflexivSubKmerDS = ReflexivSubKmerDS.sort("k-1");

            iterations++;

            DSKmerExtentionToArrayFirst = new DSExtendReflexivKmerToArrayFirstTime();
            ReflexivLongSubKmerDS = ReflexivSubKmerDS.mapPartitions(DSKmerExtentionToArrayFirst, ReflexivLongKmerEncoder);
            ReflexivLongSubKmerDS = ReflexivLongSubKmerDS.sort("k-1");

            DSKmerExtenstionArrayToArray = new DSExtendReflexivKmerToArrayLoop();

            ReflexivLongSubKmerDS.cache();

   //         ReflexivLongSubKmerDS= spark.createDataFrame(ReflexivLongSubKmerDS.toJavaRDD(), ReflexivLongKmerStruct); // too truncate the lineage of the spark DAG schema

            partitionNumber = ReflexivLongSubKmerDS.toJavaRDD().getNumPartitions();
            contigNumber = 0;
            while (iterations <= param.maximumIteration) {
                iterations++;

                if (iterations >= param.minimumIteration) {
                    if (iterations % 3 == 0) {

                        /**
                         *  problem ------------------------------------------v
                         */

                        ReflexivLongSubKmerDS.cache();
                        long currentContigNumber = ReflexivLongSubKmerDS.count();
                        if (contigNumber == currentContigNumber) {
                            if (param.scramble == 2) {
                                param.scramble = 3;
                                contigNumber = currentContigNumber;
                            } else {
                                break;
                            }
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

               // ReflexivLongSubKmerDS= spark.createDataFrame(ReflexivLongSubKmerDS.toJavaRDD(), ReflexivLongKmerStruct); // too truncate the lineage of the spark DAG schema
                ReflexivLongSubKmerDS = ReflexivLongSubKmerDS.sort("k-1");
                ReflexivLongSubKmerDS = ReflexivLongSubKmerDS.mapPartitions(DSKmerExtenstionArrayToArray, ReflexivLongKmerEncoder);
            }

            ReflexivLongSubKmerDS = ReflexivLongSubKmerDS.sort("k-1");

            ReflexivLongSubKmerStringDS = ReflexivLongSubKmerDS.mapPartitions(DSArrayStringOutput, ReflexivLongKmerStringEncoder);
            ReflexivLongSubKmerStringDS.cache();

            ContigRows = ReflexivLongSubKmerStringDS.mapPartitions(contigformaterDS, ContigStringEncoder);
            ContigRowsRDD = ContigRows.toJavaRDD();

            ContigRowsRDD.cache();

            ContigsRDDIndex = ContigRowsRDD.zipWithIndex();

            ContigRDD = ContigsRDDIndex.flatMap(DSIdLabeling);

            ContigRDD.saveAsTextFile(param.outputPath + "_" + param.kmerSize);
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

    class DSKmerToSmallContig implements MapPartitionsFunction<Row, Row>, Serializable {

        public Iterator<Row> call(Iterator<Row> sIterator) {
            List<Row> contigList = new ArrayList<Row>();

            while (sIterator.hasNext()) {
                Row s = sIterator.next();
                if (s.getInt(1) == 1) {
                    String contig = s.getString(0) + s.getString(2);
                    int length = contig.length();
                    if (length < param.minContig) {
                        String ID = ">Contig-" + length;
                        String formatedContig = changeLine(contig, length, 100);
                        contigList.add(RowFactory.create(ID, formatedContig));
                    }
                } else { // (randomReflexivMarker == 2) {
                    String contig = s.getString(2) + s.getString(0);
                    int length = contig.length();
                    if (length < param.minContig) {
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
        public Iterator<Row> call(Iterator<Row> sIterator) {

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
                                        } else if (tmpReflexivKmerExtendList.get(i).getInt(3) >= 0 && tmpReflexivKmerExtendList.get(i).getInt(4) - currentBlockSize >= 0) {
                                            reflexivExtend(tmpReflexivKmerExtendList.get(i), s, tmpReflexivKmerExtendList.get(i).getInt(4) - currentBlockSize);
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
        public void singleKmerRandomizer(Row currentSubKmer) {
            int blockSize = currentSubKmer.getSeq(2).length();

            Long[] newReflexivLongArray = new Long[blockSize];

            if (currentSubKmer.getInt(1) == 1) {
                int firstSuffixBlockLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros((Long) currentSubKmer.getSeq(2).apply(0)) / 2 + 1);
                long maxSuffixLengthBinary = ~((~0L) << (2 * firstSuffixBlockLength));
                long[] newReflexivSubKmer = new long[param.subKmerBinarySlots];
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
                                newReflexivSubKmer[param.subKmerBinarySlots - blockSize - 1] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) >>> 2 * (param.subKmerSizeResidue - firstSuffixBlockLength));
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
                    */

                 //   System.out.println(" DSExtendReflexivKmerToArrayLoop random " + randomReflexivMarker);

                    reflexivKmerConcatList.add(
                            RowFactory.create(newReflexivSubKmer,
                                    randomReflexivMarker, newReflexivLongArray, currentSubKmer.getInt(3), currentSubKmer.getInt(4)
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
                    */

             //       System.out.println(" DSExtendReflexivKmerToArrayLoop random same " + randomReflexivMarker);

                    reflexivKmerConcatList.add(currentSubKmer);
                }
            } else { /* currentSubKmer.getInt(1) == 2 */
                int firstPrefixLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros((Long) currentSubKmer.getSeq(2).apply(0)) / 2 + 1);
                long maxPrefixLengthBinary = ~((~0L) << (2 * firstPrefixLength));
                long[] newReflexivSubKmer = new long[param.subKmerBinarySlots];
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
                */
               //     System.out.println(" DSExtendReflexivKmerToArrayLoop random same " + randomReflexivMarker);

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

*/
                  //  System.out.println(" DSExtendReflexivKmerToArrayLoop random " + randomReflexivMarker);

                    reflexivKmerConcatList.add(
                            RowFactory.create(newReflexivSubKmer,
                                    randomReflexivMarker, newReflexivLongArray, currentSubKmer.getInt(3), currentSubKmer.getInt(4)
                            )
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
        public void reflexivExtend(Row forwardSubKmer, Row reflexedSubKmer, int bubbleDistance) {

             /* forward   ATCGATCG, 1, ------ */
             /* reflexed  ------, 2, ATCGATCG */

            int forwardFirstSuffixLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros((Long) forwardSubKmer.getSeq(2).apply(0)) / 2 + 1);
            int forwardBlockSize = forwardSubKmer.getSeq(2).length();
            int forwardKmerLength = (forwardSubKmer.getSeq(2).length() - 1) * 31 + forwardFirstSuffixLength;
            int reflexedFirstPrefixLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros((Long) reflexedSubKmer.getSeq(2).apply(0)) / 2 + 1);
            int reflexedBlockSize = reflexedSubKmer.getSeq(2).length();
            int reflexedKmerLength = (reflexedSubKmer.getSeq(2).length() - 1) * 31 + reflexedFirstPrefixLength;
            int concatenateLength = forwardKmerLength + reflexedKmerLength;
            int concatBlockSize = concatenateLength / 31;
            if (concatenateLength % 31 != 0) {
                concatBlockSize++;
            }

            long maxSuffixLengthBinary = ~((~0L) << 2 * forwardFirstSuffixLength);
            long maxPrefixLengthBinary = ~((~0L) << 2 * reflexedFirstPrefixLength);


            //          if (randomReflexivMarker == 2) {
            Long[] newReflexivSubKmer = new Long[param.subKmerBinarySlots];
            Long newReflexivLong;
            Long[] newReflexivLongArray = new Long[concatBlockSize];

            if (forwardBlockSize > 1) {
                if (forwardFirstSuffixLength >= param.subKmerSizeResidue) {
                    if (forwardBlockSize >= param.subKmerBinarySlots) {
                        newReflexivSubKmer[param.subKmerBinarySlots - 1] = ((Long) forwardSubKmer.getSeq(2).apply(forwardBlockSize - 1) & maxSubKmerResidueBinary);

                        long transit1 = (Long) forwardSubKmer.getSeq(2).apply(forwardBlockSize - 1) >>> (2 * param.subKmerSizeResidue);
                        long transit2 = 0L;
                        for (int i = param.subKmerBinarySlots - 2; i >= 0; i--) {
                            int j = forwardBlockSize - param.subKmerBinarySlots + i; // index of suffix long array
                            transit2 = (Long) forwardSubKmer.getSeq(2).apply(j) >>> (2 * param.subKmerSizeResidue);

                            newReflexivSubKmer[i] = transit1;
                            newReflexivSubKmer[i] |= ((Long) forwardSubKmer.getSeq(2).apply(j) << 2 * (31 - param.subKmerSizeResidue));
                            newReflexivSubKmer[i] &= maxSubKmerBinary;

                            transit1 = transit2;
                        }

                        for (int i = forwardBlockSize - param.subKmerBinarySlots; i > 0; i--) {
                            int j = param.subKmerBinarySlots + i + concatBlockSize - forwardBlockSize - 1; // index of the new prefix long array
                            newReflexivLongArray[j] = ((Long) forwardSubKmer.getSeq(2).apply(i) >>> 2 * param.subKmerSizeResidue);
                            newReflexivLongArray[j] |= (Long) forwardSubKmer.getSeq(2).apply(i - 1) << 2 * (31 - param.subKmerSizeResidue);
                            newReflexivLongArray[j] &= maxSubKmerBinary;
                        }

                        //   if (forwardBlockSize== param.subKmerBinarySlots){ // in the context of forwardBlockSize >= param.subKmerBinarySlots
                        newReflexivLongArray[param.subKmerBinarySlots + concatBlockSize - forwardBlockSize - 1] = ((Long) forwardSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary) >>> (2 * param.subKmerSizeResidue);
                        newReflexivLongArray[param.subKmerBinarySlots + concatBlockSize - forwardBlockSize - 1] |= ((Long) forwardSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) << 2 * (forwardFirstSuffixLength - param.subKmerSizeResidue));
                        newReflexivLongArray[param.subKmerBinarySlots + concatBlockSize - forwardBlockSize - 1] |= ((Long) forwardSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 2) << 2 * forwardFirstSuffixLength);
                        newReflexivLongArray[param.subKmerBinarySlots + concatBlockSize - forwardBlockSize - 1] &= maxSubKmerBinary;
                        //  }

                        for (int i = param.subKmerBinarySlots + concatBlockSize - forwardBlockSize - 2; i > concatBlockSize - forwardBlockSize; i--) {
                            int j = i - concatBlockSize + forwardBlockSize;
                            newReflexivLongArray[i] = (Long) forwardSubKmer.getSeq(0).apply(j) >>> 2 * (31 - forwardFirstSuffixLength);
                            newReflexivLongArray[i] |= ((Long) forwardSubKmer.getSeq(0).apply(j - 1) << 2 * forwardFirstSuffixLength);
                            newReflexivLongArray[i] &= maxSubKmerBinary;
                        }

                        newReflexivLongArray[concatBlockSize - forwardBlockSize] = (Long) forwardSubKmer.getSeq(0).apply(0) >>> 2 * (31 - forwardFirstSuffixLength);
                        newReflexivLongArray[concatBlockSize - forwardBlockSize] |= ((Long) reflexedSubKmer.getSeq(2).apply(reflexedBlockSize - 1) << 2 * forwardFirstSuffixLength);
                        newReflexivLongArray[concatBlockSize - forwardBlockSize] &= maxSubKmerBinary;

                        if (forwardFirstSuffixLength + reflexedFirstPrefixLength > 31) {
                            for (int i = concatBlockSize - forwardBlockSize - 1; i > 0; i--) {
                                newReflexivLongArray[i] = (Long) reflexedSubKmer.getSeq(2).apply(i) >>> 2 * (31 - forwardFirstSuffixLength);
                                newReflexivLongArray[i] |= ((Long) reflexedSubKmer.getSeq(2).apply(i - 1) << 2 * forwardFirstSuffixLength);
                                newReflexivLongArray[i] &= maxSubKmerBinary;
                            }
                            newReflexivLongArray[0] = (Long) reflexedSubKmer.getSeq(2).apply(0) >>> 2 * (31 - forwardFirstSuffixLength);
                            newReflexivLongArray[0] |= (1L << 2 * (forwardFirstSuffixLength + reflexedFirstPrefixLength - 31)); // add C marker in the front
                        } else { // forwardFirstSuffixLength+reflexedFirstPrefixLength <= 31)
                            for (int i = concatBlockSize - forwardBlockSize - 1; i > 0; i--) {
                                newReflexivLongArray[i] = (Long) reflexedSubKmer.getSeq(2).apply(i + 1) >>> 2 * (31 - forwardFirstSuffixLength);
                                newReflexivLongArray[i] |= ((Long) reflexedSubKmer.getSeq(2).apply(i) << 2 * forwardFirstSuffixLength);
                                newReflexivLongArray[i] &= maxSubKmerBinary;
                            }
                            if (reflexedSubKmer.getSeq(2).length() >1) {
                                newReflexivLongArray[0] = (Long) reflexedSubKmer.getSeq(2).apply(1) >>> 2 * (31- forwardFirstSuffixLength);
                                newReflexivLongArray[0] |= ((Long) reflexedSubKmer.getSeq(2).apply(0) << 2 * forwardFirstSuffixLength);
                         //       newReflexivLongArray[0] |= (1L << 2 * (forwardFirstSuffixLength + reflexedFirstPrefixLength)); // add C marker in the front
                            }else{
                                newReflexivLongArray[0] = (Long) reflexedSubKmer.getSeq(0).apply(0) >>> 2 * (31- forwardFirstSuffixLength);
                                newReflexivLongArray[0] |= ((Long) reflexedSubKmer.getSeq(2).apply(0) << 2 * forwardFirstSuffixLength);
                          //      newReflexivLongArray[0] |= (1L << 2 * (forwardFirstSuffixLength + reflexedFirstPrefixLength)); // add C marker in the front
                            }
                        }
                    } else { // forwardBlockSize < param.subKmerSizeResidue
                        newReflexivSubKmer[param.subKmerBinarySlots - 1] = ((Long) forwardSubKmer.getSeq(2).apply(forwardBlockSize - 1) & maxSubKmerResidueBinary);

                        long transit1 = (Long) forwardSubKmer.getSeq(2).apply(forwardBlockSize - 1) >>> (2 * param.subKmerSizeResidue);
                        long transit2 = 0L;
                        for (int i = param.subKmerBinarySlots - 2; i >= param.subKmerBinarySlots - forwardBlockSize; i--) {
                            int j = forwardBlockSize - param.subKmerBinarySlots + i; // index of suffix long array
                            transit2 = (Long) forwardSubKmer.getSeq(2).apply(j) >>> (2 * param.subKmerSizeResidue);

                            newReflexivSubKmer[i] = transit1;
                            newReflexivSubKmer[i] |= ((Long) forwardSubKmer.getSeq(2).apply(j) << 2 * (31 - param.subKmerSizeResidue));
                            newReflexivSubKmer[i] &= maxSubKmerBinary;

                            transit1 = transit2;
                        }

                        newReflexivSubKmer[param.subKmerBinarySlots - forwardBlockSize - 1] = ((Long) forwardSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary) >>> (2 * param.subKmerSizeResidue);
                        newReflexivSubKmer[param.subKmerBinarySlots - forwardBlockSize - 1] |= ((Long) forwardSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) << 2 * (forwardFirstSuffixLength - param.subKmerSizeResidue));
                        newReflexivSubKmer[param.subKmerBinarySlots - forwardBlockSize - 1] |= ((Long) forwardSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 2) << 2 * forwardFirstSuffixLength);
                        newReflexivSubKmer[param.subKmerBinarySlots - forwardBlockSize - 1] &= maxSubKmerBinary;

                        for (int i = param.subKmerBinarySlots - forwardBlockSize - 2; i >= 0; i--) {
                            int j = forwardBlockSize + i; // index of the subkmer

                            newReflexivSubKmer[i] = (Long) forwardSubKmer.getSeq(0).apply(j) >>> 2 * (31 - forwardFirstSuffixLength);
                            newReflexivSubKmer[i] |= ((Long) forwardSubKmer.getSeq(0).apply(j - 1) << 2 * forwardFirstSuffixLength);
                            newReflexivSubKmer[i] &= maxSubKmerBinary;
                        }

                        for (int i = forwardBlockSize - 1; i > 0; i--) {
                            int j = i + concatBlockSize - forwardBlockSize;
                            newReflexivLongArray[j] = (Long) forwardSubKmer.getSeq(0).apply(i) >>> 2 * (31 - forwardFirstSuffixLength);
                            newReflexivLongArray[j] |= ((Long) forwardSubKmer.getSeq(0).apply(i - 1) << 2 * forwardFirstSuffixLength);
                            newReflexivLongArray[j] &= maxSubKmerBinary;
                        }

                        newReflexivLongArray[concatBlockSize - forwardBlockSize] = (Long) forwardSubKmer.getSeq(0).apply(0) >>> 2 * (31 - forwardFirstSuffixLength);
                        newReflexivLongArray[concatBlockSize - forwardBlockSize] |= ((Long) reflexedSubKmer.getSeq(2).apply(reflexedBlockSize - 1) << 2 * forwardFirstSuffixLength);
                        newReflexivLongArray[concatBlockSize - forwardBlockSize] &= maxSubKmerBinary;

                        if (forwardFirstSuffixLength + reflexedFirstPrefixLength > 31) {
                            for (int i = concatBlockSize - forwardBlockSize - 1; i > 0; i--) {
                                newReflexivLongArray[i] = (Long) reflexedSubKmer.getSeq(2).apply(i) >>> 2 * (31 - forwardFirstSuffixLength);
                                newReflexivLongArray[i] |= ((Long) reflexedSubKmer.getSeq(2).apply(i - 1) << 2 * forwardFirstSuffixLength);
                                newReflexivLongArray[i] &= maxSubKmerBinary;
                            }
                            newReflexivLongArray[0] = (Long) reflexedSubKmer.getSeq(2).apply(0) >>> 2 * (31 - forwardFirstSuffixLength);
                            newReflexivLongArray[0] |= (1L << 2 * (forwardFirstSuffixLength + reflexedFirstPrefixLength - 31)); // add C marker in the front
                        } else { // forwardFirstSuffixLength+reflexedFirstPrefixLength <= 31)
                            for (int i = concatBlockSize - forwardBlockSize - 1; i > 0; i--) {
                                newReflexivLongArray[i] = (Long) reflexedSubKmer.getSeq(2).apply(i + 1) >>> 2 * (31 - forwardFirstSuffixLength);
                                newReflexivLongArray[i] |= ((Long) reflexedSubKmer.getSeq(2).apply(i) << 2 * forwardFirstSuffixLength);
                                newReflexivLongArray[i] &= maxSubKmerBinary;
                            }
                            if (reflexedSubKmer.getSeq(2).length() >1) {
                                newReflexivLongArray[0] = (Long) reflexedSubKmer.getSeq(2).apply(1) >>> 2 * (31- forwardFirstSuffixLength);
                                newReflexivLongArray[0] |= ((Long) reflexedSubKmer.getSeq(2).apply(0) << 2 * forwardFirstSuffixLength);
                            //    newReflexivLongArray[0] |= (1L << 2 * (forwardFirstSuffixLength + reflexedFirstPrefixLength)); // add C marker in the front
                            }else{
                                newReflexivLongArray[0] = (Long) reflexedSubKmer.getSeq(0).apply(0) >>> 2 * (31- forwardFirstSuffixLength);
                                newReflexivLongArray[0] |= ((Long) reflexedSubKmer.getSeq(2).apply(0) << 2 * forwardFirstSuffixLength); // already has c marker
                                // newReflexivLongArray[0] |= (1L << 2 * (forwardFirstSuffixLength + reflexedFirstPrefixLength)); // add C marker in the front
                            }
                        }
                    }
                } else { // forwardFirstSuffixLength < param.subKmerSizeResidue
                    if (forwardBlockSize >= param.subKmerBinarySlots) {
                        newReflexivSubKmer[param.subKmerBinarySlots - 1] = ((Long) forwardSubKmer.getSeq(2).apply(forwardBlockSize - 1) & maxSubKmerResidueBinary);

                        long transit1 = (Long) forwardSubKmer.getSeq(2).apply(forwardBlockSize - 1) >>> (2 * param.subKmerSizeResidue);
                        long transit2 = 0L;
                        for (int i = param.subKmerBinarySlots - 2; i >0; i--) {
                            int j = forwardBlockSize - param.subKmerBinarySlots + i; // index of suffix long array
                            transit2 = (Long) forwardSubKmer.getSeq(2).apply(j) >>> (2 * param.subKmerSizeResidue);

                            newReflexivSubKmer[i] = transit1;
                            newReflexivSubKmer[i] |= ((Long) forwardSubKmer.getSeq(2).apply(j) << 2 * (31 - param.subKmerSizeResidue));
                            newReflexivSubKmer[i] &= maxSubKmerBinary;

                            transit1 = transit2;
                        }

                        newReflexivSubKmer[0] = transit1;
                        if (forwardBlockSize > param.subKmerBinarySlots) {
                            newReflexivSubKmer[0] |= ((Long) forwardSubKmer.getSeq(2).apply(forwardBlockSize - param.subKmerBinarySlots) << 2 * (31 - param.subKmerSizeResidue));
                        }else { // (forwardBlockSize== param.subKmerBinarySlots){
                            newReflexivSubKmer[0] |= (( (Long) forwardSubKmer.getSeq(2).apply(forwardBlockSize - param.subKmerBinarySlots) & maxSuffixLengthBinary ) << 2 * (31 - param.subKmerSizeResidue));
                            newReflexivSubKmer[0] |= ((Long) forwardSubKmer.getSeq(0).apply(param.subKmerBinarySlots-1) << 2*(31+ forwardFirstSuffixLength-param.subKmerSizeResidue));
                        }
                        newReflexivSubKmer[0] &= maxSubKmerBinary;

                        //newReflexivSubKmer[0] = transit1;
                        //newReflexivSubKmer[0] |= ((Long) forwardSubKmer.getSeq(2).apply(forwardBlockSize-param.subKmerBinarySlots) << 2*(31-param.subKmerSizeResidue));


                        for (int i = forwardBlockSize - param.subKmerBinarySlots; i > 1; i--) {
                            int j = param.subKmerBinarySlots + i + concatBlockSize - forwardBlockSize - 1; // index of the new prefix long array
                            newReflexivLongArray[j] = ((Long) forwardSubKmer.getSeq(2).apply(i) >>> 2 * param.subKmerSizeResidue);
                            newReflexivLongArray[j] |= (Long) forwardSubKmer.getSeq(2).apply(i - 1) << 2 * (31 - param.subKmerSizeResidue);
                            newReflexivLongArray[j] &= maxSubKmerBinary;
                        }

                        if (forwardBlockSize > param.subKmerBinarySlots) {
                            newReflexivLongArray[param.subKmerBinarySlots + concatBlockSize - forwardBlockSize] = ((Long) forwardSubKmer.getSeq(2).apply(1) >>> 2 * param.subKmerSizeResidue);
                            newReflexivLongArray[param.subKmerBinarySlots + concatBlockSize - forwardBlockSize] |= ((Long) forwardSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary) << 2 * (31 - param.subKmerSizeResidue);
                            newReflexivLongArray[param.subKmerBinarySlots + concatBlockSize - forwardBlockSize] |= ((Long) forwardSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) << 2 * (31 + forwardFirstSuffixLength - param.subKmerSizeResidue));
                            newReflexivLongArray[param.subKmerBinarySlots + concatBlockSize - forwardBlockSize] &= maxSubKmerBinary;
                        }

                        //   if (forwardBlockSize== param.subKmerBinarySlots){ // in the context of forwardBlockSize >= param.subKmerBinarySlots
                        //  newReflexivLongArray[param.subKmerBinarySlots-1] = ((Long) forwardSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary) >>> (2 * param.subKmerSizeResidue);
                        newReflexivLongArray[param.subKmerBinarySlots + concatBlockSize - forwardBlockSize - 1] = ((Long) forwardSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) >>> 2 * (param.subKmerSizeResidue - forwardFirstSuffixLength));
                        newReflexivLongArray[param.subKmerBinarySlots + concatBlockSize - forwardBlockSize - 1] |= ((Long) forwardSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 2) << 2 * forwardFirstSuffixLength);
                        newReflexivLongArray[param.subKmerBinarySlots + concatBlockSize - forwardBlockSize - 1] &= maxSubKmerBinary;
                        //  }

                        for (int i = param.subKmerBinarySlots + concatBlockSize - forwardBlockSize - 2; i > concatBlockSize - forwardBlockSize; i--) {
                            int j = i - concatBlockSize + forwardBlockSize;
                            newReflexivLongArray[i] = (Long) forwardSubKmer.getSeq(0).apply(j) >>> 2 * (31 - forwardFirstSuffixLength);
                            newReflexivLongArray[i] |= ((Long) forwardSubKmer.getSeq(0).apply(j - 1) << 2 * forwardFirstSuffixLength);
                            newReflexivLongArray[i] &= maxSubKmerBinary;
                        }

                        newReflexivLongArray[concatBlockSize - forwardBlockSize] = (Long) forwardSubKmer.getSeq(0).apply(0) >>> 2 * (31 - forwardFirstSuffixLength);
                        newReflexivLongArray[concatBlockSize - forwardBlockSize] |= ((Long) reflexedSubKmer.getSeq(2).apply(reflexedBlockSize - 1) << 2 * forwardFirstSuffixLength);
                        newReflexivLongArray[concatBlockSize - forwardBlockSize] &= maxSubKmerBinary;

                        if (forwardFirstSuffixLength + reflexedFirstPrefixLength > 31) {
                            for (int i = concatBlockSize - forwardBlockSize - 1; i > 0; i--) {
                                newReflexivLongArray[i] = (Long) reflexedSubKmer.getSeq(2).apply(i) >>> 2 * (31 - forwardFirstSuffixLength);
                                newReflexivLongArray[i] |= ((Long) reflexedSubKmer.getSeq(2).apply(i - 1) << 2 * forwardFirstSuffixLength);
                                newReflexivLongArray[i] &= maxSubKmerBinary;
                            }
                            newReflexivLongArray[0] = (Long) reflexedSubKmer.getSeq(2).apply(0) >>> 2 * (31 - forwardFirstSuffixLength);
                            newReflexivLongArray[0] |= (1L << 2 * (forwardFirstSuffixLength + reflexedFirstPrefixLength - 31)); // add C marker in the front
                        } else { // forwardFirstSuffixLength+reflexedFirstPrefixLength <= 31)
                            for (int i = concatBlockSize - forwardBlockSize - 1; i > 0; i--) {
                                newReflexivLongArray[i] = (Long) reflexedSubKmer.getSeq(2).apply(i + 1) >>> 2 * (31 - forwardFirstSuffixLength);
                                newReflexivLongArray[i] |= ((Long) reflexedSubKmer.getSeq(2).apply(i) << 2 * forwardFirstSuffixLength);
                                newReflexivLongArray[i] &= maxSubKmerBinary;
                            }
                            if (reflexedBlockSize>1) {
                                newReflexivLongArray[0] = (Long) reflexedSubKmer.getSeq(2).apply(1) >>> 2 * (31 - forwardFirstSuffixLength);
                                newReflexivLongArray[0] |= ((Long) reflexedSubKmer.getSeq(2).apply(0) << 2 * forwardFirstSuffixLength); // already has c marker
                            }else{
                                newReflexivLongArray[0] = (Long) reflexedSubKmer.getSeq(0).apply(0) >>> 2 * (31 - forwardFirstSuffixLength);
                                newReflexivLongArray[0] |= ((Long) reflexedSubKmer.getSeq(2).apply(0) << 2 * forwardFirstSuffixLength);
                            }
                        }
                    } else { // forwardBlockSize < param.subKmerBinarySlots
                        newReflexivSubKmer[param.subKmerBinarySlots - 1] = ((Long) forwardSubKmer.getSeq(2).apply(forwardBlockSize - 1) & maxSubKmerResidueBinary);

                        long transit1 = (Long) forwardSubKmer.getSeq(2).apply(forwardBlockSize - 1) >>> (2 * param.subKmerSizeResidue);
                        long transit2 = 0L;
                        for (int i = param.subKmerBinarySlots - 2; i > param.subKmerBinarySlots - forwardBlockSize; i--) {
                            int j = forwardBlockSize - param.subKmerBinarySlots + i; // index of suffix long array
                            transit2 = (Long) forwardSubKmer.getSeq(2).apply(j) >>> (2 * param.subKmerSizeResidue);

                            newReflexivSubKmer[i] = transit1;
                            newReflexivSubKmer[i] |= ((Long) forwardSubKmer.getSeq(2).apply(j) << 2 * (31 - param.subKmerSizeResidue));
                            newReflexivSubKmer[i] &= maxSubKmerBinary;

                            transit1 = transit2;
                        }

                        newReflexivSubKmer[param.subKmerBinarySlots - forwardBlockSize] = (Long) forwardSubKmer.getSeq(2).apply(1) >>> 2 * param.subKmerSizeResidue;
                        newReflexivSubKmer[param.subKmerBinarySlots - forwardBlockSize] |= ((Long) forwardSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary) << 2 * (31 - param.subKmerSizeResidue);
                        newReflexivSubKmer[param.subKmerBinarySlots - forwardBlockSize] |= ((Long) forwardSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) << 2 * (31 - param.subKmerSizeResidue + forwardFirstSuffixLength));
                        newReflexivSubKmer[param.subKmerBinarySlots - forwardBlockSize] &= maxSubKmerBinary;

                        // newReflexivSubKmer[param.subKmerBinarySlots-forwardBlockSize-1] = ((Long) forwardSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary) >>> (2 * param.subKmerSizeResidue);
                        newReflexivSubKmer[param.subKmerBinarySlots - forwardBlockSize - 1] = ((Long) forwardSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) >>> 2 * (param.subKmerSizeResidue - forwardFirstSuffixLength));
                        newReflexivSubKmer[param.subKmerBinarySlots - forwardBlockSize - 1] |= ((Long) forwardSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 2) << 2 * forwardFirstSuffixLength);
                        newReflexivSubKmer[param.subKmerBinarySlots - forwardBlockSize - 1] &= maxSubKmerBinary;

                        for (int i = param.subKmerBinarySlots - forwardBlockSize - 2; i >= 0; i--) {
                            int j = forwardBlockSize + i; // index of the subkmer

                            newReflexivSubKmer[i] = (Long) forwardSubKmer.getSeq(0).apply(j) >>> 2 * (31 - forwardFirstSuffixLength);
                            newReflexivSubKmer[i] |= ((Long) forwardSubKmer.getSeq(0).apply(j - 1) << 2 * forwardFirstSuffixLength);
                            newReflexivSubKmer[i] &= maxSubKmerBinary;
                        }

                        for (int i = concatBlockSize - 1; i > concatBlockSize - forwardBlockSize; i--) {
                            int j = i - concatBlockSize + forwardBlockSize;
                            newReflexivLongArray[i] = (Long) forwardSubKmer.getSeq(0).apply(j) >>> 2 * (31 - forwardFirstSuffixLength);
                            newReflexivLongArray[i] |= ((Long) forwardSubKmer.getSeq(0).apply(j - 1) << 2 * forwardFirstSuffixLength);
                            newReflexivLongArray[i] &= maxSubKmerBinary;
                        }


                        newReflexivLongArray[concatBlockSize - forwardBlockSize] = (Long) forwardSubKmer.getSeq(0).apply(0) >>> 2 * (31 - forwardFirstSuffixLength);
                        newReflexivLongArray[concatBlockSize - forwardBlockSize] |= ((Long) reflexedSubKmer.getSeq(2).apply(reflexedBlockSize - 1) << 2 * forwardFirstSuffixLength);
                        newReflexivLongArray[concatBlockSize - forwardBlockSize] &= maxSubKmerBinary;

                        if (forwardFirstSuffixLength + reflexedFirstPrefixLength > 31) {
                            for (int i = concatBlockSize - forwardBlockSize - 1; i > 0; i--) {
                                newReflexivLongArray[i] = (Long) reflexedSubKmer.getSeq(2).apply(i) >>> 2 * (31 - forwardFirstSuffixLength);
                                newReflexivLongArray[i] |= ((Long) reflexedSubKmer.getSeq(2).apply(i - 1) << 2 * forwardFirstSuffixLength);
                                newReflexivLongArray[i] &= maxSubKmerBinary;
                            }
                            newReflexivLongArray[0] = (Long) reflexedSubKmer.getSeq(2).apply(0) >>> 2 * (31 - forwardFirstSuffixLength);
                            newReflexivLongArray[0] |= (1L << 2 * (forwardFirstSuffixLength + reflexedFirstPrefixLength - 31)); // add C marker in the front
                        } else { // forwardFirstSuffixLength+reflexedFirstPrefixLength <= 31)
                            for (int i = concatBlockSize - forwardBlockSize - 1; i > 0; i--) {
                                newReflexivLongArray[i] = (Long) reflexedSubKmer.getSeq(2).apply(i + 1) >>> 2 * (31 - forwardFirstSuffixLength);
                                newReflexivLongArray[i] |= ((Long) reflexedSubKmer.getSeq(2).apply(i) << 2 * forwardFirstSuffixLength);
                                newReflexivLongArray[i] &= maxSubKmerBinary;
                            }
                            if (reflexedSubKmer.getSeq(2).length() >1) {
                                newReflexivLongArray[0] = (Long) reflexedSubKmer.getSeq(2).apply(1) >>> 2 * (31- forwardFirstSuffixLength);
                                newReflexivLongArray[0] |= ((Long) reflexedSubKmer.getSeq(2).apply(0) << 2 * forwardFirstSuffixLength);
                                //    newReflexivLongArray[0] |= (1L << 2 * (forwardFirstSuffixLength + reflexedFirstPrefixLength)); // add C marker in the front
                            }else{
                                newReflexivLongArray[0] = (Long) reflexedSubKmer.getSeq(0).apply(0) >>> 2 * (31- forwardFirstSuffixLength);
                                newReflexivLongArray[0] |= ((Long) reflexedSubKmer.getSeq(2).apply(0) << 2 * forwardFirstSuffixLength); // already has c marker
                                // newReflexivLongArray[0] |= (1L << 2 * (forwardFirstSuffixLength + reflexedFirstPrefixLength)); // add C marker in the front
                            }
                        }

                        //newReflexivLongArray[0] = (Long) forwardSubKmer.getSeq(0).apply(0) >>> 2 * (31 - forwardFirstSuffixLength);
                        //newReflexivLongArray[0] |= (1L << 2 * forwardFirstSuffixLength); // add C marker in the front
                    }
                }


            } else { // forward block size ==1
                if (forwardFirstSuffixLength > param.subKmerSizeResidue) { // forwardFirstSuffixLength is longer than the length of the last block (element) of sub kmer long array
                    long transitBit1 = (Long) forwardSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) << 2 * (forwardFirstSuffixLength - param.subKmerSizeResidue);
                    transitBit1 |= (((Long)forwardSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary) >>> 2 * param.subKmerSizeResidue);
                    long transitBit2 = 0L;

                    newReflexivSubKmer[param.subKmerBinarySlots - 1] = ((Long) forwardSubKmer.getSeq(2).apply(0) & maxSubKmerResidueBinary);

                    for (int i = param.subKmerBinarySlots - 2; i >= 0; i--) {
                        transitBit2 = (Long) forwardSubKmer.getSeq(0).apply(i) >>> 2 * (31 - forwardFirstSuffixLength);

                        newReflexivSubKmer[i] = (Long) forwardSubKmer.getSeq(0).apply(i) << 2 * forwardFirstSuffixLength;
                        newReflexivSubKmer[i] |= transitBit1;
                        newReflexivSubKmer[i] &= maxSubKmerBinary;

                        transitBit1 = transitBit2;
                    }

                    newReflexivLongArray[concatBlockSize - 1] = transitBit1;
                    newReflexivLongArray[concatBlockSize - 1] |= ((Long) reflexedSubKmer.getSeq(2).apply(reflexedBlockSize - 1) << 2 * forwardFirstSuffixLength);
                    if (forwardFirstSuffixLength + reflexedFirstPrefixLength > 31) {
                        for (int i = concatBlockSize - forwardBlockSize - 1; i > 0; i--) {
                            newReflexivLongArray[i] = (Long) reflexedSubKmer.getSeq(2).apply(i) >>> 2 * (31 - forwardFirstSuffixLength);
                            newReflexivLongArray[i] |= ((Long) reflexedSubKmer.getSeq(2).apply(i - 1) << 2 * forwardFirstSuffixLength);
                            newReflexivLongArray[i] &= maxSubKmerBinary;
                        }

                        newReflexivLongArray[0] = (Long) reflexedSubKmer.getSeq(2).apply(0) >>> 2 * (31 - forwardFirstSuffixLength);
                    } else {
                        for (int i = concatBlockSize - forwardBlockSize - 1; i > 0; i--) {
                            newReflexivLongArray[i] = (Long) reflexedSubKmer.getSeq(2).apply(i + 1) >>> 2 * (31 - forwardFirstSuffixLength);
                            newReflexivLongArray[i] |= ((Long) reflexedSubKmer.getSeq(2).apply(i) << 2 * forwardFirstSuffixLength);
                            newReflexivLongArray[i] &= maxSubKmerBinary;
                        }

                        if (reflexedSubKmer.getSeq(2).size() > 1) {
                            newReflexivLongArray[0] = (Long) reflexedSubKmer.getSeq(2).apply(1) >>> 2 * (31 - forwardFirstSuffixLength);
                            newReflexivLongArray[0] |= ((Long) reflexedSubKmer.getSeq(2).apply(0) << 2 * forwardFirstSuffixLength);
                            //  newReflexivLongArray[0] |= (1L << 2 * (reflexedFirstPrefixLength + reflexedFirstPrefixLength - 31)); // add C marker in the front
                        } else { // reflexedSubKmer.getSeq(2).size() == 1
                            newReflexivLongArray[0] = (Long) reflexedSubKmer.getSeq(0).apply(0) >>> 2 * (31 - forwardFirstSuffixLength);
                            newReflexivLongArray[0] |= ((Long) reflexedSubKmer.getSeq(2).apply(0) << 2 * forwardFirstSuffixLength);
                            //   newReflexivLongArray[0] |= (1L << 2 * (reflexedFirstPrefixLength + reflexedFirstPrefixLength - 31)); // add C marker in the front
                        }
                    }
                } else if (forwardFirstSuffixLength == param.subKmerSizeResidue) {
                    long transitBit1 = (Long) forwardSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                    long transitBit2 = 0L;

                    newReflexivSubKmer[param.subKmerBinarySlots - 1] = ((Long) forwardSubKmer.getSeq(2).apply(0) & maxSubKmerResidueBinary);

                    for (int i = param.subKmerBinarySlots - 2; i >= 0; i--) {
                        transitBit2 = (Long) forwardSubKmer.getSeq(0).apply(i) >>> 2 * (31 - forwardFirstSuffixLength);

                        newReflexivSubKmer[i] = (Long) forwardSubKmer.getSeq(0).apply(i) << 2 * forwardFirstSuffixLength;
                        newReflexivSubKmer[i] |= transitBit1;
                        newReflexivSubKmer[i] &= maxSubKmerBinary;

                        transitBit1 = transitBit2;
                    }

                    newReflexivLongArray[concatBlockSize - 1] = transitBit1;
                    newReflexivLongArray[concatBlockSize - 1] |= ((Long) reflexedSubKmer.getSeq(2).apply(reflexedBlockSize - 1) << 2 * forwardFirstSuffixLength);
                    if (forwardFirstSuffixLength + reflexedFirstPrefixLength > 31) {
                        for (int i = concatBlockSize - forwardBlockSize - 1; i > 0; i--) {
                            newReflexivLongArray[i] = (Long) reflexedSubKmer.getSeq(2).apply(i) >>> 2 * (31 - forwardFirstSuffixLength);
                            newReflexivLongArray[i] |= ((Long) reflexedSubKmer.getSeq(2).apply(i - 1) << 2 * forwardFirstSuffixLength);
                            newReflexivLongArray[i] &= maxSubKmerBinary;
                        }

                        newReflexivLongArray[0] = (Long) reflexedSubKmer.getSeq(2).apply(0) >>> 2 * (31 - forwardFirstSuffixLength);
                    } else {
                        for (int i = concatBlockSize - forwardBlockSize - 1; i > 0; i--) {
                            newReflexivLongArray[i] = (Long) reflexedSubKmer.getSeq(2).apply(i + 1) >>> 2 * (31 - forwardFirstSuffixLength);
                            newReflexivLongArray[i] |= ((Long) reflexedSubKmer.getSeq(2).apply(i) << 2 * forwardFirstSuffixLength);
                            newReflexivLongArray[i] &= maxSubKmerBinary;
                        }

                        if (reflexedSubKmer.getSeq(2).size()>1) {
                            newReflexivLongArray[0] = (Long) reflexedSubKmer.getSeq(2).apply(1) >>> 2 * (31 - forwardFirstSuffixLength);
                            newReflexivLongArray[0] |= ((Long) reflexedSubKmer.getSeq(2).apply(0) << 2 * forwardFirstSuffixLength);
                            //  newReflexivLongArray[0] |= (1L << 2 * (reflexedFirstPrefixLength + reflexedFirstPrefixLength - 31)); // add C marker in the front
                        }else { // reflexedSubKmer.getSeq(2).size() == 1
                            newReflexivLongArray[0] = (Long) reflexedSubKmer.getSeq(0).apply(0) >>> 2 * (31 - forwardFirstSuffixLength);
                            newReflexivLongArray[0] |= ((Long) reflexedSubKmer.getSeq(2).apply(0) << 2 * forwardFirstSuffixLength);
                            //   newReflexivLongArray[0] |= (1L << 2 * (reflexedFirstPrefixLength + reflexedFirstPrefixLength - 31)); // add C marker in the front
                        }

                    }
                } else { //forwardFirstSuffixLength < param.subKmerSizeResidue
                    long transitBit1 = (Long) forwardSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) >>> 2 * (param.subKmerSizeResidue - forwardFirstSuffixLength);
                    long transitBit2 = 0L;

                    newReflexivSubKmer[param.subKmerBinarySlots - 1] = (Long) forwardSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) << 2 * forwardFirstSuffixLength;
                    newReflexivSubKmer[param.subKmerBinarySlots - 1] |= ((Long) forwardSubKmer.getSeq(2).apply(0) & maxSuffixLengthBinary);
                    newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;

                    for (int i = param.subKmerBinarySlots - 2; i >= 0; i--) {
                        transitBit2 = (Long) forwardSubKmer.getSeq(0).apply(i) >>> 2 * (31 - forwardFirstSuffixLength);

                        newReflexivSubKmer[i] = (Long) forwardSubKmer.getSeq(0).apply(i) << 2 * forwardFirstSuffixLength;
                        newReflexivSubKmer[i] |= transitBit1;
                        newReflexivSubKmer[i] &= maxSubKmerBinary;

                        transitBit1 = transitBit2;
                    }

                    newReflexivLongArray[concatBlockSize - 1] = transitBit1;
                    newReflexivLongArray[concatBlockSize - 1] |= ((Long) reflexedSubKmer.getSeq(2).apply(reflexedBlockSize - 1) << 2 * forwardFirstSuffixLength);
                    if (forwardFirstSuffixLength + reflexedFirstPrefixLength > 31) {
                        for (int i = concatBlockSize - forwardBlockSize - 1; i > 0; i--) {
                            newReflexivLongArray[i] = (Long) reflexedSubKmer.getSeq(2).apply(i) >>> 2 * (31 - forwardFirstSuffixLength);
                            newReflexivLongArray[i] |= ((Long) reflexedSubKmer.getSeq(2).apply(i - 1) << 2 * forwardFirstSuffixLength);
                            newReflexivLongArray[i] &= maxSubKmerBinary;
                        }


                        newReflexivLongArray[0] = (Long) reflexedSubKmer.getSeq(2).apply(0) >>> 2 * (31 - forwardFirstSuffixLength);
                    } else {
                        for (int i = concatBlockSize - forwardBlockSize - 1; i > 0; i--) {
                            newReflexivLongArray[i] = (Long) reflexedSubKmer.getSeq(2).apply(i + 1) >>> 2 * (31 - forwardFirstSuffixLength);
                            newReflexivLongArray[i] |= ((Long) reflexedSubKmer.getSeq(2).apply(i) << 2 * forwardFirstSuffixLength);
                            newReflexivLongArray[i] &= maxSubKmerBinary;
                        }
                        if (reflexedSubKmer.getSeq(2).size()>1) {
                            newReflexivLongArray[0] = (Long) reflexedSubKmer.getSeq(2).apply(1) >>> 2 * (31 - forwardFirstSuffixLength);
                            newReflexivLongArray[0] |= ((Long) reflexedSubKmer.getSeq(2).apply(0) << 2 * forwardFirstSuffixLength);
                          //  newReflexivLongArray[0] |= (1L << 2 * (reflexedFirstPrefixLength + reflexedFirstPrefixLength - 31)); // add C marker in the front
                        }else{ // reflexedSubKmer.getSeq(2).size() == 1
                            newReflexivLongArray[0] = (Long) reflexedSubKmer.getSeq(0).apply(0) >>> 2 * (31 - forwardFirstSuffixLength);
                            newReflexivLongArray[0] |= ((Long) reflexedSubKmer.getSeq(2).apply(0) << 2 * forwardFirstSuffixLength);
                         //   newReflexivLongArray[0] |= (1L << 2 * (reflexedFirstPrefixLength + reflexedFirstPrefixLength - 31)); // add C marker in the front
                        }
                    }
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
*/
           // System.out.println(" DSExtendReflexivKmerToArrayLoop extend 2");


            Row newReflectedKmer;
            if (bubbleDistance < 0) {
                newReflectedKmer = RowFactory.create(JavaConverters.collectionAsScalaIterableConverter(Arrays.asList(newReflexivSubKmer)).asScala().toSeq(),
                        2, JavaConverters.asScalaIteratorConverter(Arrays.asList(newReflexivLongArray).iterator()).asScala().toSeq(), reflexedSubKmer.getInt(3), forwardSubKmer.getInt(4)
                );
            } else {
                if (forwardSubKmer.getInt(3) > 0) {
                    newReflectedKmer = RowFactory.create(JavaConverters.collectionAsScalaIterableConverter(Arrays.asList(newReflexivSubKmer)).asScala().toSeq(),
                            2, JavaConverters.asScalaIteratorConverter(Arrays.asList(newReflexivLongArray).iterator()).asScala().toSeq(), bubbleDistance, forwardSubKmer.getInt(4)
                    );
                } else {

                    newReflectedKmer = RowFactory.create(JavaConverters.collectionAsScalaIterableConverter(Arrays.asList(newReflexivSubKmer)).asScala().toSeq(),
                            2, JavaConverters.asScalaIteratorConverter(Arrays.asList(newReflexivLongArray).iterator()).asScala().toSeq(), reflexedSubKmer.getInt(3), bubbleDistance
                    );
                }
            }


            singleKmerRandomizer(newReflectedKmer);

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

        /**
         *
         */
        public void tmpKmerRandomizer() {
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
        public Iterator<Row> call(Iterator<Row> sIterator) {

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
                            if (subKmerSlotComparator(s.getSeq(0), tmpReflexivKmerExtendList.get(i).getSeq(0))) {
                                if (s.getInt(1) == 1) {
                                    if (tmpReflexivKmerExtendList.get(i).getInt(1) == 2) {
                                        int tmpReflexivKmerSuffixLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros(tmpReflexivKmerExtendList.get(i).getLong(2)) / 2 + 1);
                                        int currentReflexivKmerSuffixLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros(s.getLong(2)) / 2 + 1);
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
                                        int tmpReflexivKmerSuffixLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros(tmpReflexivKmerExtendList.get(i).getLong(2)) / 2 + 1);
                                        int currentReflexivKmerSuffixLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros(s.getLong(2)) / 2 + 1);
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
                                        } else if (tmpReflexivKmerExtendList.get(i).getInt(3) >= 0 && tmpReflexivKmerExtendList.get(i).getInt(4) - currentReflexivKmerSuffixLength >= 0) {
                                            reflexivExtend(tmpReflexivKmerExtendList.get(i), s, tmpReflexivKmerExtendList.get(i).getInt(4) - currentReflexivKmerSuffixLength);
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
        public void singleKmerRandomizer(Row currentSubKmer) {


            if (currentSubKmer.getInt(1) == 1) {
                int currentSuffixLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros(currentSubKmer.getLong(2)) / 2 + 1);
                long maxSuffixLengthBinary = ~((~0L) << (2 * currentSuffixLength));
                long[] newReflexivSubKmer = new long[param.subKmerBinarySlots];
                long newReflexivLong = 0L;
                long[] newReflexivLongArray = new long[1];

                if (randomReflexivMarker == 2) {
                    // long transitBit1;
                    if (currentSuffixLength > param.subKmerSizeResidue) { // currentSuffixLength is longer than the length of the last block (element) of sub kmer long array
                        long transitBit1 = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) << 2 * (currentSuffixLength - param.subKmerSizeResidue);
                        transitBit1 |= ((currentSubKmer.getLong(2) & maxSuffixLengthBinary) >>> 2 * param.subKmerSizeResidue);
                        long transitBit2 = 0L;

                        newReflexivSubKmer[param.subKmerBinarySlots - 1] = (currentSubKmer.getLong(2) & maxSubKmerResidueBinary);

                        for (int i = param.subKmerBinarySlots - 2; i >= 0; i--) {
                            transitBit2 = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * (31 - currentSuffixLength);

                            newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(i) << 2 * currentSuffixLength;
                            newReflexivSubKmer[i] |= transitBit1;
                            newReflexivSubKmer[i] &= maxSubKmerBinary;

                            transitBit1 = transitBit2;
                        }

                        newReflexivLong = transitBit2;
                        newReflexivLong |= (1L << 2 * currentSuffixLength);
                        newReflexivLongArray[0] = newReflexivLong;

                    } else if (currentSuffixLength == param.subKmerSizeResidue) {
                        long transitBit1 = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                        long transitBit2 = 0L;

                        newReflexivSubKmer[param.subKmerBinarySlots - 1] = (currentSubKmer.getLong(2) & maxSubKmerResidueBinary);

                        for (int i = param.subKmerBinarySlots - 2; i >= 0; i--) {
                            transitBit2 = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * (31 - currentSuffixLength);

                            newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(i) << 2 * currentSuffixLength;
                            newReflexivSubKmer[i] |= transitBit1;
                            newReflexivSubKmer[i] &= maxSubKmerBinary;

                            transitBit1 = transitBit2;
                        }

                        newReflexivLong = transitBit2;
                        newReflexivLong |= (1L << 2 * currentSuffixLength);
                        newReflexivLongArray[0] = newReflexivLong;

                    } else { //currentSuffixLength < param.subKmerSizeResidue
                        long transitBit1 = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) >>> 2 * (param.subKmerSizeResidue - currentSuffixLength);
                        long transitBit2 = 0L;

                        newReflexivSubKmer[param.subKmerBinarySlots - 1] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) << 2 * currentSuffixLength;
                        newReflexivSubKmer[param.subKmerBinarySlots - 1] |= (currentSubKmer.getLong(2) & maxSuffixLengthBinary);
                        newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;

                        for (int i = param.subKmerBinarySlots - 2; i >= 0; i--) {
                            transitBit2 = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * (31 - currentSuffixLength);

                            newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(i) << 2 * currentSuffixLength;
                            newReflexivSubKmer[i] |= transitBit1;
                            newReflexivSubKmer[i] &= maxSubKmerBinary;

                            transitBit1 = transitBit2;
                        }

                        newReflexivLong = transitBit2;
                        newReflexivLong |= (1L << 2 * currentSuffixLength); // add C marker
                        newReflexivLongArray[0] = newReflexivLong;
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

                    if (newReflexivSubKmer.length >=3) {
                        for (int k = 5; k >= 0; k--) {
                            long a = newReflexivSubKmer[2] >>> 2 * k;
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
*/
                  //  System.out.println(" DSExtendReflexivKmerToArrayFirstTime random " + randomReflexivMarker);

                    reflexivKmerConcatList.add(
                            RowFactory.create(newReflexivSubKmer,
                                    randomReflexivMarker, newReflexivLongArray, currentSubKmer.getInt(3), currentSubKmer.getInt(4)
                            )
                    );
                } else {
                    newReflexivLongArray = new long[1];
                    newReflexivLongArray[0] = currentSubKmer.getLong(2);
/*
                    for (int k = 30; k >= 0; k--) {
                        long a = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    for (int k = 30; k >= 0; k--) {
                        long a = (Long) currentSubKmer.getSeq(0).apply(1) >>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    if (currentSubKmer.getSeq(0).length() >=3) {
                        for (int k = 5; k >= 0; k--) {
                            long a = (Long) currentSubKmer.getSeq(0).apply(2) >>> 2 * k;
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
*/
                //    System.out.println(" DSExtendReflexivKmerToArrayFirstTime random " + randomReflexivMarker);

                    reflexivKmerConcatList.add(
                            RowFactory.create(currentSubKmer.getSeq(0),
                                    currentSubKmer.getInt(1), newReflexivLongArray, currentSubKmer.getInt(3), currentSubKmer.getInt(4)
                            )
                    );
                }
            } else { /* currentSubKmer.getInt(1) == 2 */
                int currentPrefixLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros(currentSubKmer.getLong(2)) / 2 + 1);
                long maxPrefixLengthBinary = ~((~0L) << (2 * currentPrefixLength));
                long[] newReflexivSubKmer = new long[param.subKmerBinarySlots];
                long newReflexivLong = 0L;
                long[] newReflexivLongArray = new long[1];
                if (randomReflexivMarker == 2) {
                    newReflexivLongArray = new long[1];
                    newReflexivLongArray[0] = currentSubKmer.getLong(2);

                    /*
                    for (int k = 30; k >= 0; k--) {
                        long a = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    for (int k = 30; k >= 0; k--) {
                        long a = (Long) currentSubKmer.getSeq(0).apply(1) >>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    if (currentSubKmer.getSeq(0).length() >=3) {
                        for (int k = 5; k >= 0; k--) {
                            long a = (Long) currentSubKmer.getSeq(0).apply(2) >>> 2 * k;
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
                    */

                //    System.out.println(" DSExtendReflexivKmerToArrayFirstTime random " + randomReflexivMarker);


                    reflexivKmerConcatList.add(
                            RowFactory.create(currentSubKmer.getSeq(0),
                                    currentSubKmer.getInt(1), newReflexivLongArray, currentSubKmer.getInt(3), currentSubKmer.getInt(4)
                            )
                    );
                } else { /* randomReflexivMarker == 1 */
                    if (currentPrefixLength > param.subKmerSizeResidue) {
                        long transitBit1 = (Long) currentSubKmer.getSeq(0).apply(0) & maxPrefixLengthBinary;

                        newReflexivSubKmer[0] = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * currentPrefixLength;
                        newReflexivSubKmer[0] |= ((currentSubKmer.getLong(2) & maxPrefixLengthBinary) << 2 * (31 - currentPrefixLength));
                        newReflexivSubKmer[0] &= maxSubKmerBinary;

                        for (int i = 1; i < param.subKmerBinarySlots - 1; i++) {
                            long transitBit2 = (Long) currentSubKmer.getSeq(0).apply(i) & maxPrefixLengthBinary;

                            newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * currentPrefixLength;
                            newReflexivSubKmer[i] |= (transitBit1 << 2 * (31 - currentPrefixLength));
                            newReflexivSubKmer[i] &= maxSubKmerBinary;

                            transitBit1 = transitBit2;
                        }

                        newReflexivSubKmer[param.subKmerBinarySlots - 1] = transitBit1 >>> 2*(currentPrefixLength - param.subKmerSizeResidue);
                        newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;

                        newReflexivLong = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                        newReflexivLong |= (transitBit1 << 2 * param.subKmerSizeResidue);
                        newReflexivLong &= maxPrefixLengthBinary;
                        newReflexivLong |= (1L << 2 * currentPrefixLength); // add C marker in the front

                        newReflexivLongArray[0] = newReflexivLong;
                    } else if (currentPrefixLength == param.subKmerSizeResidue) {
                        long transitBit1 = (Long) currentSubKmer.getSeq(0).apply(0) & maxPrefixLengthBinary;

                        newReflexivSubKmer[0] = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * currentPrefixLength;
                        newReflexivSubKmer[0] |= ((currentSubKmer.getLong(2) & maxPrefixLengthBinary) << 2 * (31 - currentPrefixLength));
                        newReflexivSubKmer[0] &= maxSubKmerBinary;

                        for (int i = 1; i < param.subKmerBinarySlots - 1; i++) {
                            long transitBit2 = (Long) currentSubKmer.getSeq(0).apply(i) & maxPrefixLengthBinary;

                            newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * currentPrefixLength;
                            newReflexivSubKmer[i] |= (transitBit1 << 2 * (31 - currentPrefixLength));
                            newReflexivSubKmer[i] &= maxSubKmerBinary;

                            transitBit1 = transitBit2;
                        }

                        newReflexivSubKmer[param.subKmerBinarySlots - 1] = transitBit1;
                        newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;

                        newReflexivLong = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                        newReflexivLong |= (1L << 2 * currentPrefixLength); // add C marker

                        newReflexivLongArray[0] = newReflexivLong;
                    } else {
                        long transitBit1 = (Long) currentSubKmer.getSeq(0).apply(0) & maxPrefixLengthBinary;

                        newReflexivSubKmer[0] = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * currentPrefixLength;
                        newReflexivSubKmer[0] |= ((currentSubKmer.getLong(2) & maxPrefixLengthBinary) << 2 * (31 - currentPrefixLength));
                        newReflexivSubKmer[0] &= maxSubKmerBinary;

                        for (int i = 1; i < param.subKmerBinarySlots - 1; i++) {
                            long transitBit2 = (Long) currentSubKmer.getSeq(0).apply(i) & maxPrefixLengthBinary;

                            newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * currentPrefixLength;
                            newReflexivSubKmer[i] |= (transitBit1 << 2 * (31 - currentPrefixLength));
                            newReflexivSubKmer[i] &= maxSubKmerBinary;

                            transitBit1 = transitBit2;
                        }

                        newReflexivSubKmer[param.subKmerBinarySlots - 1] = transitBit1 << 2 * (param.subKmerSizeResidue - currentPrefixLength);
                        newReflexivSubKmer[param.subKmerBinarySlots - 1] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) >>> 2 * currentPrefixLength);
                        newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;


                        newReflexivLong = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) & maxPrefixLengthBinary;
                        newReflexivLong |= (1L << 2 * currentPrefixLength); // add C marker
                        newReflexivLongArray[0] = newReflexivLong;
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

                    if (newReflexivSubKmer.length >=3) {
                        for (int k = 5; k >= 0; k--) {
                            long a = newReflexivSubKmer[2] >>> 2 * k;
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
                    */

                 //   System.out.println(" DSExtendReflexivKmerToArrayFirstTime random " + randomReflexivMarker);

                    reflexivKmerConcatList.add(
                            RowFactory.create(newReflexivSubKmer,
                                    randomReflexivMarker, newReflexivLongArray, currentSubKmer.getInt(3), currentSubKmer.getInt(4)
                            )
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
        public void reflexivExtend(Row forwardSubKmer, Row reflexedSubKmer, int bubbleDistance) {

             /* forward   ATCGATCG, 1, ------ */
             /* reflexed  ------, 2, ATCGATCG */

            int forwardSuffixLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros(forwardSubKmer.getLong(2)) / 2 + 1);
            int reflexedPrefixLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros(reflexedSubKmer.getLong(2)) / 2 + 1);
            int concatenateLength = forwardSuffixLength + reflexedPrefixLength;
            long maxSuffixLengthBinary = ~((~0L) << 2 * forwardSuffixLength);
            long maxPrefixLengthBinary = ~((~0L) << 2 * reflexedPrefixLength);
            long maxConcatenateBinary = ~((~0L) << 2 * (forwardSuffixLength + reflexedPrefixLength)); // when forwardSuf + backwardPre < 31


            if (randomReflexivMarker == 2) {
                long[] newReflexivSubKmer = new long[param.subKmerBinarySlots];
                Long newReflexivLong = 0L;
                long[] newReflexivLongArray;

                if (forwardSuffixLength > param.subKmerSizeResidue) { // forwardSuffixLength is longer than the length of the last block (element) of sub kmer long array
                    long transitBit1 = (Long) forwardSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) << 2 * (forwardSuffixLength - param.subKmerSizeResidue);
                    transitBit1 |= ((forwardSubKmer.getLong(2) & maxSuffixLengthBinary) >>> 2 * param.subKmerSizeResidue);
                    long transitBit2 = 0L;

                    newReflexivSubKmer[param.subKmerBinarySlots - 1] = (forwardSubKmer.getLong(2) & maxSubKmerResidueBinary);

                    for (int i = param.subKmerBinarySlots - 2; i >= 0; i--) {
                        transitBit2 = (Long) forwardSubKmer.getSeq(0).apply(i) >>> 2 * (31 - forwardSuffixLength);

                        newReflexivSubKmer[i] = (Long) forwardSubKmer.getSeq(0).apply(i) << 2 * forwardSuffixLength;
                        newReflexivSubKmer[i] |= transitBit1;
                        newReflexivSubKmer[i] &= maxSubKmerBinary;

                        transitBit1 = transitBit2;
                    }

                    if (concatenateLength > 31) {
                        newReflexivLongArray = new long[concatenateLength / 31 + 1];
                        newReflexivLongArray[newReflexivLongArray.length - 1] = reflexedSubKmer.getLong(2) << 2 * forwardSuffixLength;
                        newReflexivLongArray[newReflexivLongArray.length - 1] |= transitBit2;
                        newReflexivLongArray[newReflexivLongArray.length - 1] &= maxSubKmerBinary;


                        newReflexivLongArray[0] = reflexedSubKmer.getLong(2) >>> 2 * (31 - forwardSuffixLength); // do not remove the C marker as it will be used again
                    } else {
                        newReflexivLongArray = new long[1];
                        newReflexivLongArray[0] = reflexedSubKmer.getLong(2) << 2 * forwardSuffixLength; // do not remove the C marker as it will be used again
                        newReflexivLongArray[0] |= transitBit2;

                    }

                    // newReflexivLong |= (1L<<2*forwardSuffixLength);

                } else if (forwardSuffixLength == param.subKmerSizeResidue) {
                    long transitBit1 = (Long) forwardSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                    long transitBit2 = 0L;

                    newReflexivSubKmer[param.subKmerBinarySlots - 1] = (forwardSubKmer.getLong(2) & maxSubKmerResidueBinary);

                    for (int i = param.subKmerBinarySlots - 2; i >= 0; i--) {
                        transitBit2 = (Long) forwardSubKmer.getSeq(0).apply(i) >>> 2 * (31 - forwardSuffixLength);

                        newReflexivSubKmer[i] = (Long) forwardSubKmer.getSeq(0).apply(i) << 2 * forwardSuffixLength;
                        newReflexivSubKmer[i] |= transitBit1;
                        newReflexivSubKmer[i] &= maxSubKmerBinary;

                        transitBit1 = transitBit2;
                    }

                    if (concatenateLength > 31) {
                        newReflexivLongArray = new long[concatenateLength / 31 + 1];
                        newReflexivLongArray[newReflexivLongArray.length - 1] = reflexedSubKmer.getLong(2) << 2 * forwardSuffixLength;
                        newReflexivLongArray[newReflexivLongArray.length - 1] |= transitBit2;
                        newReflexivLongArray[newReflexivLongArray.length - 1] &= maxSubKmerBinary;


                        newReflexivLongArray[0] = reflexedSubKmer.getLong(2) >>> 2 * (31 - forwardSuffixLength); // do not remove the C marker as it will be used again
                    } else {
                        newReflexivLongArray = new long[1];
                        newReflexivLongArray[0] = reflexedSubKmer.getLong(2) << 2 * forwardSuffixLength;
                        newReflexivLongArray[0] |= transitBit2;

                    }

                } else { //forwardSuffixLength < param.subKmerSizeResidue
                    long transitBit1 = (Long) forwardSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) >>> 2 * (param.subKmerSizeResidue - forwardSuffixLength);
                    long transitBit2 = 0L;

                    newReflexivSubKmer[param.subKmerBinarySlots - 1] = (Long) forwardSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) << 2 * forwardSuffixLength;
                    newReflexivSubKmer[param.subKmerBinarySlots - 1] |= (forwardSubKmer.getLong(2) & maxSuffixLengthBinary);
                    newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;

                    for (int i = param.subKmerBinarySlots - 2; i >= 0; i--) {
                        transitBit2 = (Long) forwardSubKmer.getSeq(0).apply(i) >>> 2 * (31 - forwardSuffixLength);

                        newReflexivSubKmer[i] = (Long) forwardSubKmer.getSeq(0).apply(i) << 2 * forwardSuffixLength;
                        newReflexivSubKmer[i] |= transitBit1;
                        newReflexivSubKmer[i] &= maxSubKmerBinary;

                        transitBit1 = transitBit2;
                    }

                    if (concatenateLength > 31) {
                        newReflexivLongArray = new long[concatenateLength / 31 + 1];
                        newReflexivLongArray[newReflexivLongArray.length - 1] = reflexedSubKmer.getLong(2) << 2 * forwardSuffixLength; // do not remove the C marker as it will be used again
                        newReflexivLongArray[newReflexivLongArray.length - 1] |= transitBit2;
                        newReflexivLongArray[newReflexivLongArray.length - 1] &= maxSubKmerBinary;


                        newReflexivLongArray[0] = reflexedSubKmer.getLong(2) >>> 2 * (31 - forwardSuffixLength); // do not remove the C marker as it will be used again
                    } else {
                        newReflexivLongArray = new long[1];
                        newReflexivLongArray[0] = reflexedSubKmer.getLong(2) << 2 * forwardSuffixLength;
                        newReflexivLongArray[0] |= transitBit2;
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
                    for (int k = 5; k >= 0; k--) {
                        long a = newReflexivSubKmer[2] >>> 2 * k;
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
                */

            //    System.out.println(" DSExtendReflexivKmerToArrayFirstTime extend 1");

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
            } else { /* randomReflexivMarker == 1 */
                long[] newForwardSubKmer = new long[param.subKmerBinarySlots];
                Long newForwardLong = 0L;
                long[] newForwardLongArray;


                if (reflexedPrefixLength > param.subKmerSizeResidue) {
                    long transitBit1 = (Long) reflexedSubKmer.getSeq(0).apply(0) & maxPrefixLengthBinary;

                    newForwardSubKmer[0] = (Long) reflexedSubKmer.getSeq(0).apply(0) >>> 2 * reflexedPrefixLength;
                    newForwardSubKmer[0] |= ((reflexedSubKmer.getLong(2) & maxPrefixLengthBinary) << 2 * (31 - reflexedPrefixLength));

                    for (int i = 1; i < param.subKmerBinarySlots - 1; i++) {
                        long transitBit2 = (Long) reflexedSubKmer.getSeq(0).apply(i) & maxPrefixLengthBinary;

                        newForwardSubKmer[i] = (Long) reflexedSubKmer.getSeq(0).apply(i) >>> 2 * reflexedPrefixLength;
                        newForwardSubKmer[i] |= (transitBit1 << 2 * (31 - reflexedPrefixLength));

                        transitBit1 = transitBit2;
                    }

                    newForwardSubKmer[param.subKmerBinarySlots - 1] = transitBit1 >>> 2 * (reflexedPrefixLength - param.subKmerSizeResidue);

                    if (concatenateLength > 31) {
                        newForwardLongArray = new long[concatenateLength / 31 + 1];

                        newForwardLongArray[newForwardLongArray.length - 1] = (Long) reflexedSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                        newForwardLongArray[newForwardLongArray.length - 1] <<= 2 * forwardSuffixLength;
                        newForwardLongArray[newForwardLongArray.length - 1] |= (forwardSubKmer.getLong(2) & maxSuffixLengthBinary);
                        if ((param.subKmerSizeResidue + forwardSuffixLength) > 31) {
                            newForwardLongArray[newForwardLongArray.length - 1] &= maxSubKmerBinary;

                            newForwardLongArray[0] = (Long) reflexedSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) >>> (2 * (31 - forwardSuffixLength));
                            newForwardLongArray[0] |= (transitBit1 << 2 * (param.subKmerSizeResidue + forwardSuffixLength - 31));
                            newForwardLongArray[0] |= (1L << 2 * (concatenateLength - 31));
                        } else {
                            newForwardLongArray[newForwardLongArray.length - 1] |= (transitBit1 << 2 * (param.subKmerSizeResidue + forwardSuffixLength));
                            newForwardLongArray[newForwardLongArray.length - 1] &= maxSubKmerBinary;

                            newForwardLongArray[0] = transitBit1 >>> 2 * (31 - (forwardSuffixLength + param.subKmerSizeResidue));
                            newForwardLongArray[0] |= (1L << 2 * (concatenateLength - 31)); // add C marker in the front
                        }

                    } else {
                        newForwardLongArray = new long[1];

                        newForwardLongArray[0] = (Long) reflexedSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                        newForwardLongArray[0] <<= (2 * forwardSuffixLength);
                        newForwardLongArray[0] |= (forwardSubKmer.getLong(2) & maxSuffixLengthBinary);
                        newForwardLongArray[0] |= (transitBit1 << 2 * (param.subKmerSizeResidue + forwardSuffixLength));
                        newForwardLongArray[0] &= maxConcatenateBinary;
                        newForwardLongArray[0] |= (1L << 2 * concatenateLength);
                    }
                } else if (reflexedPrefixLength == param.subKmerSizeResidue) {
                    long transitBit1 = (Long) reflexedSubKmer.getSeq(0).apply(0) & maxPrefixLengthBinary;

                    newForwardSubKmer[0] = (Long) reflexedSubKmer.getSeq(0).apply(0) >>> 2 * reflexedPrefixLength;
                    newForwardSubKmer[0] |= ((reflexedSubKmer.getLong(2) & maxPrefixLengthBinary) << 2 * (31 - reflexedPrefixLength));

                    for (int i = 1; i < param.subKmerBinarySlots - 1; i++) {
                        long transitBit2 = (Long) reflexedSubKmer.getSeq(0).apply(i) & maxPrefixLengthBinary;

                        newForwardSubKmer[i] = (Long) reflexedSubKmer.getSeq(0).apply(i) >>> 2 * reflexedPrefixLength;
                        newForwardSubKmer[i] |= (transitBit1 << 2 * (31 - reflexedPrefixLength));

                        transitBit1 = transitBit2;
                    }

                    newForwardSubKmer[param.subKmerBinarySlots - 1] = transitBit1;

                    if (concatenateLength > 31) {
                        newForwardLongArray = new long[concatenateLength / 31 + 1];

                        newForwardLongArray[newForwardLongArray.length - 1] = (Long) reflexedSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                        newForwardLongArray[newForwardLongArray.length - 1] <<= 2 * forwardSuffixLength;
                        newForwardLongArray[newForwardLongArray.length - 1] |= (forwardSubKmer.getLong(2) & maxSuffixLengthBinary);
                        newForwardLongArray[newForwardLongArray.length - 1] &= maxSubKmerBinary;

                        newForwardLongArray[0] = (Long) reflexedSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                        newForwardLongArray[0] >>>= (2 * (31 - forwardSuffixLength));
                        newForwardLongArray[0] |= (1L << 2 * (concatenateLength - 31));

                    } else {
                        newForwardLongArray = new long[1];

                        newForwardLongArray[0] = (Long) reflexedSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                        newForwardLongArray[0] <<= 2 * forwardSuffixLength;
                        newForwardLongArray[0] |= (forwardSubKmer.getLong(2) & maxSuffixLengthBinary);
                        newForwardLongArray[0] &= maxConcatenateBinary;
                        newForwardLongArray[0] |= (1L << 2 * concatenateLength);
                    }

                } else { // reflexedPrefixLength < param.subKmerSizeResidue
                    long transitBit1 = (Long) reflexedSubKmer.getSeq(0).apply(0) & maxPrefixLengthBinary;

                    newForwardSubKmer[0] = (Long) reflexedSubKmer.getSeq(0).apply(0) >>> 2 * reflexedPrefixLength;
                    newForwardSubKmer[0] |= ((reflexedSubKmer.getLong(2) & maxPrefixLengthBinary) << 2 * (31 - reflexedPrefixLength));

                    for (int i = 1; i < param.subKmerBinarySlots - 1; i++) {
                        long transitBit2 = (Long) reflexedSubKmer.getSeq(0).apply(i) & maxPrefixLengthBinary;

                        newForwardSubKmer[i] = (Long) reflexedSubKmer.getSeq(0).apply(i) >>> 2 * reflexedPrefixLength;
                        newForwardSubKmer[i] |= (transitBit1 << 2 * (31 - reflexedPrefixLength));

                        transitBit1 = transitBit2;
                    }

                    newForwardSubKmer[param.subKmerBinarySlots - 1] = transitBit1 << 2 * (param.subKmerSizeResidue - reflexedPrefixLength);
                    newForwardSubKmer[param.subKmerBinarySlots - 1] |= ((Long) reflexedSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) >>> 2 * reflexedPrefixLength);

                    if (concatenateLength > 31) {
                        newForwardLongArray = new long[concatenateLength / 31 + 1];

                        newForwardLongArray[newForwardLongArray.length - 1] = forwardSubKmer.getLong(2) & maxSuffixLengthBinary;
                        newForwardLongArray[newForwardLongArray.length - 1] |= ((Long) forwardSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) << (2 * forwardSuffixLength));
                        newForwardLongArray[newForwardLongArray.length - 1] &= maxSubKmerBinary;

                        newForwardLongArray[0] = (Long) reflexedSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                        newForwardLongArray[0] >>>= (2 * (31 - forwardSuffixLength));
                        newForwardLongArray[0] |= (1L << 2 * (concatenateLength - 31));
                    } else {
                        newForwardLongArray = new long[1];

                        newForwardLongArray[0] = (Long) reflexedSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                        newForwardLongArray[0] <<= 2 * forwardSuffixLength;
                        newForwardLongArray[0] |= (forwardSubKmer.getLong(2) & maxSuffixLengthBinary);
                        newForwardLongArray[0] &= maxConcatenateBinary;
                        newForwardLongArray[0] |= (1L << 2 * concatenateLength);
                    }
                }
                /*

                for (int k = 30; k >= 0; k--) {
                    long a = newForwardSubKmer[0] >> 2 * k;
                    a &= 3L;
                    char b = BinaryToNucleotide(a);
                    System.out.print(b);
                }

                for (int k = 30; k >= 0; k--) {
                    long a = newForwardSubKmer[1] >>> 2 * k;
                    a &= 3L;
                    char b = BinaryToNucleotide(a);
                    System.out.print(b);
                }

                if (newForwardSubKmer.length >=3){
                    for (int k = 5; k >= 0; k--) {
                        long a = newForwardSubKmer[2] >>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }
                }

                System.out.print(" ");
                for (int k = 30; k >= 0; k--) {
                    long a = newForwardLongArray[0] >>> 2 * k;
                    a &= 3L;
                    char b = BinaryToNucleotide(a);
                    System.out.print(b);
                }
                */

             //   System.out.println(" DSExtendReflexivKmerToArrayFirstTime extend 2");

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
                            S.getInt(1), S.getLong(2), S.getInt(3), S.getInt(4)
                    )
            );
        }

        /**
         *
         */
        public void tmpKmerRandomizer() {
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
        public Iterator<Row> call(Iterator<Row> sIterator) {

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
                            if (subKmerSlotComparator(s.getSeq(0), tmpReflexivKmerExtendList.get(i).getSeq(0))) {
                                if (s.getInt(1) == 1) {
                                    if (tmpReflexivKmerExtendList.get(i).getInt(1) == 2) {
                                        int tmpReflexivKmerSuffixLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros(tmpReflexivKmerExtendList.get(i).getLong(2)) / 2 + 1);
                                        int currentReflexivKmerSuffixLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros(s.getLong(2)) / 2 + 1);
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
                                        int tmpReflexivKmerSuffixLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros(tmpReflexivKmerExtendList.get(i).getLong(2)) / 2 + 1);
                                        int currentReflexivKmerSuffixLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros(s.getLong(2)) / 2 + 1);
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
                                        } else if (tmpReflexivKmerExtendList.get(i).getInt(3) >= 0 && tmpReflexivKmerExtendList.get(i).getInt(4) - currentReflexivKmerSuffixLength >= 0) {
                                            reflexivExtend(tmpReflexivKmerExtendList.get(i), s, tmpReflexivKmerExtendList.get(i).getInt(4) - currentReflexivKmerSuffixLength);
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
        public void singleKmerRandomizer(Row currentSubKmer) {

            if (currentSubKmer.getInt(1) == 1) {
                /**
                 * 00000000000000110010111010010   Long.SIZE
                 * --------------C-G-G-G-T-C-A-G   Long.SIZE - (Long.numberOfLeadingZeros / 2 + 1)
                 * --------------^-Length marker
                 */
                int currentSuffixLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros(currentSubKmer.getLong(2)) / 2 + 1);
                long maxSuffixLengthBinary = ~(~0L << 2 * currentSuffixLength);
                long[] newReflexivSubKmer = new long[param.subKmerBinarySlots];
                long newReflexivLong;

                if (randomReflexivMarker == 2) {

                    // long transitBit1;
                    if (currentSuffixLength > param.subKmerSizeResidue) { // currentSuffixLength is longer than the length of the last block (element) of sub kmer long array
                        long transitBit1 = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) << 2 * (currentSuffixLength - param.subKmerSizeResidue);
                        transitBit1 |= ((currentSubKmer.getLong(2) & maxSuffixLengthBinary) >>> 2 * param.subKmerSizeResidue);
                        long transitBit2 = 0L;

                        newReflexivSubKmer[param.subKmerBinarySlots - 1] = (currentSubKmer.getLong(2) & maxSubKmerResidueBinary);

                        for (int i = param.subKmerBinarySlots - 2; i >= 0; i--) {
                            transitBit2 = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * (31 - currentSuffixLength);

                            newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(i) << 2 * currentSuffixLength;
                            newReflexivSubKmer[i] |= transitBit1;
                            newReflexivSubKmer[i] &= maxSubKmerBinary;

                            transitBit1 = transitBit2;
                        }

                        newReflexivLong = transitBit2;
                        newReflexivLong |= (1L << 2 * currentSuffixLength);

                    } else if (currentSuffixLength == param.subKmerSizeResidue) {
                        long transitBit1 = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                        long transitBit2 = 0L;

                        newReflexivSubKmer[param.subKmerBinarySlots - 1] = (currentSubKmer.getLong(2) & maxSubKmerResidueBinary);

                        for (int i = param.subKmerBinarySlots - 2; i >= 0; i--) {
                            transitBit2 = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * (31 - currentSuffixLength);

                            newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(i) << 2 * currentSuffixLength;
                            newReflexivSubKmer[i] |= transitBit1;
                            newReflexivSubKmer[i] &= maxSubKmerBinary;

                            transitBit1 = transitBit2;
                        }

                        newReflexivLong = transitBit2;
                        newReflexivLong |= (1L << 2 * currentSuffixLength);

                    } else { //currentSuffixLength < param.subKmerSizeResidue
                        long transitBit1 = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) >>> 2 * (param.subKmerSizeResidue - currentSuffixLength);
                        long transitBit2 = 0L;

                        newReflexivSubKmer[param.subKmerBinarySlots - 1] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) << 2 * currentSuffixLength;
                        newReflexivSubKmer[param.subKmerBinarySlots - 1] |= (currentSubKmer.getLong(2) & maxSuffixLengthBinary);
                        newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;

                        for (int i = param.subKmerBinarySlots - 2; i >= 0; i--) {
                            transitBit2 = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * (31 - currentSuffixLength);

                            newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(i) << 2 * currentSuffixLength;
                            newReflexivSubKmer[i] |= transitBit1;
                            newReflexivSubKmer[i] &= maxSubKmerBinary;

                            transitBit1 = transitBit2;
                        }

                        newReflexivLong = transitBit2;
                        newReflexivLong |= (1L << 2 * currentSuffixLength); // add C marker
                    }

/*
                    for (int k = 30; k >= 0; k--) {
                        long a = newReflexivSubKmer[0] >>> 2 * k;
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

                    if (newReflexivSubKmer.length >=3) {
                        for (int k = 5; k >= 0; k--) {
                            long a = newReflexivSubKmer[2] >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    System.out.print(" ");
                    for (int k = 30; k >= 0; k--) {
                        long a = newReflexivLong >>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

*/
                 //   System.out.println(" DSExtendReflexivKmer random " + randomReflexivMarker + " " + currentSubKmer.getInt(3) + " " + currentSubKmer.getInt(4));


                    reflexivKmerConcatList.add(
                            RowFactory.create(newReflexivSubKmer, randomReflexivMarker, newReflexivLong, currentSubKmer.getInt(3), currentSubKmer.getInt(4))
                    );
                } else {

                    /*
                    for (int k = 30; k >= 0; k--) {
                        long a = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    for (int k = 30; k >= 0; k--) {
                        long a = (Long) currentSubKmer.getSeq(0).apply(1) >>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    if (currentSubKmer.getSeq(0).length() >=3) {
                        for (int k = 5; k >= 0; k--) {
                            long a = (Long) currentSubKmer.getSeq(0).apply(2) >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    System.out.print(" ");
                    for (int k = 30; k >= 0; k--) {
                        long a = currentSubKmer.getLong(2) >>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }
*/
                 //   System.out.println(" DSExtendReflexivKmer random " + randomReflexivMarker + " " + currentSubKmer.getInt(3) + " " + currentSubKmer.getInt(4));

                    reflexivKmerConcatList.add(currentSubKmer);
                }
            } else { /* currentSubKmer._2._1() == 2 */
                int currentPrefixLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros(currentSubKmer.getLong(2)) / 2 + 1);
                long maxSuffixLengthBinary = ~((~0L) << (2 * currentPrefixLength));
                long[] newReflexivSubKmer = new long[param.subKmerBinarySlots];
                Long newReflexivLong;


                if (randomReflexivMarker == 2) {
                    /*
                    for (int k = 30; k >= 0; k--) {
                        long a = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    for (int k = 30; k >= 0; k--) {
                        long a = (Long) currentSubKmer.getSeq(0).apply(1) >>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }

                    if (currentSubKmer.getSeq(0).length() >=3) {
                        for (int k = 5; k >= 0; k--) {
                            long a = (Long) currentSubKmer.getSeq(0).apply(2) >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    System.out.print(" ");
                    for (int k = 30; k >= 0; k--) {
                        long a = currentSubKmer.getLong(2) >>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }
                    */

                 //   System.out.println(" DSExtendReflexivKmer random " + randomReflexivMarker + " " + currentSubKmer.getInt(3) + " " + currentSubKmer.getInt(4));

                    reflexivKmerConcatList.add(currentSubKmer);
                } else { /* randomReflexivMarker == 1 */
                    if (currentPrefixLength > param.subKmerSizeResidue) {
                        long transitBit1 = (Long) currentSubKmer.getSeq(0).apply(0) & maxSuffixLengthBinary;

                        newReflexivSubKmer[0] = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * currentPrefixLength;
                        newReflexivSubKmer[0] |= ((currentSubKmer.getLong(2) & maxSuffixLengthBinary) << 2 * (31 - currentPrefixLength));
                        newReflexivSubKmer[0] &= maxSubKmerBinary;

                        for (int i = 1; i < param.subKmerBinarySlots - 1; i++) {
                            long transitBit2 = (Long) currentSubKmer.getSeq(0).apply(i) & maxSuffixLengthBinary;

                            newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * currentPrefixLength;
                            newReflexivSubKmer[i] |= (transitBit1 << 2 * (31 - currentPrefixLength));
                            newReflexivSubKmer[i] &= maxSubKmerBinary;

                            transitBit1 = transitBit2;
                        }

                        newReflexivSubKmer[param.subKmerBinarySlots - 1] = transitBit1 >>> 2*(currentPrefixLength-param.subKmerSizeResidue);
                        newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;

                        newReflexivLong = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                        newReflexivLong |= (transitBit1 << 2 * param.subKmerSizeResidue);
                        newReflexivLong &= maxSuffixLengthBinary;
                        newReflexivLong |= (1L << 2 * currentPrefixLength); // add C marker in the front
                    } else if (currentPrefixLength == param.subKmerSizeResidue) {
                        long transitBit1 = (Long) currentSubKmer.getSeq(0).apply(0) & maxSuffixLengthBinary;

                        newReflexivSubKmer[0] = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * currentPrefixLength;
                        newReflexivSubKmer[0] |= ((currentSubKmer.getLong(2) & maxSuffixLengthBinary) << 2 * (31 - currentPrefixLength));
                        newReflexivSubKmer[0] &= maxSubKmerBinary;

                        for (int i = 1; i < param.subKmerBinarySlots - 1; i++) {
                            long transitBit2 = (Long) currentSubKmer.getSeq(0).apply(i) & maxSuffixLengthBinary;

                            newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * currentPrefixLength;
                            newReflexivSubKmer[i] |= (transitBit1 << 2 * (31 - currentPrefixLength));
                            newReflexivSubKmer[i] &= maxSubKmerBinary;

                            transitBit1 = transitBit2;
                        }

                        newReflexivSubKmer[param.subKmerBinarySlots - 1] = transitBit1;
                        newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;

                        newReflexivLong = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                        newReflexivLong |= (1L << 2 * currentPrefixLength); // add C marker
                    } else {
                        long transitBit1 = (Long) currentSubKmer.getSeq(0).apply(0) & maxSuffixLengthBinary;

                        newReflexivSubKmer[0] = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * currentPrefixLength;
                        newReflexivSubKmer[0] |= ((currentSubKmer.getLong(2) & maxSuffixLengthBinary) << 2 * (31 - currentPrefixLength));
                        newReflexivSubKmer[0] &= maxSubKmerBinary;

                        for (int i = 1; i < param.subKmerBinarySlots - 1; i++) {
                            long transitBit2 = (Long) currentSubKmer.getSeq(0).apply(i) & maxSuffixLengthBinary;

                            newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * currentPrefixLength;
                            newReflexivSubKmer[i] |= (transitBit1 << 2 * (31 - currentPrefixLength));
                            newReflexivSubKmer[i] &= maxSubKmerBinary;

                            transitBit1 = transitBit2;
                        }

                        newReflexivSubKmer[param.subKmerBinarySlots - 1] = transitBit1 << 2 * (param.subKmerSizeResidue - currentPrefixLength);
                        newReflexivSubKmer[param.subKmerBinarySlots - 1] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) >>> 2 * currentPrefixLength);
                        newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;


                        newReflexivLong = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) & maxSuffixLengthBinary;
                        newReflexivLong |= (1L << 2 * currentPrefixLength); // add C marker
                    }

                    /*
                    for (int k = 30; k >= 0; k--) {
                        long a = newReflexivSubKmer[0] >>> 2 * k;
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

                    if (newReflexivSubKmer.length >=3) {
                        for (int k = 5; k >= 0; k--) {
                            long a = newReflexivSubKmer[2] >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                    }

                    System.out.print(" ");
                    for (int k = 30; k >= 0; k--) {
                        long a = newReflexivLong >>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }
                    */

                 //   System.out.println(" DSExtendReflexivKmer random " + randomReflexivMarker + " " + currentSubKmer.getInt(3) + " " + currentSubKmer.getInt(4));

                    reflexivKmerConcatList.add(
                            RowFactory.create(newReflexivSubKmer, randomReflexivMarker, newReflexivLong, currentSubKmer.getInt(3), currentSubKmer.getInt(4))
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
        public void reflexivExtend(Row forwardSubKmer, Row reflexedSubKmer, int bubbleDistance) {

             /* forward   ATCGATCG, 1, ------ */
             /* reflexed  ------, 2, ATCGATCG */

            int forwardSuffixLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros(forwardSubKmer.getLong(2)) / 2 + 1);
            int reflexedPrefixLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros(reflexedSubKmer.getLong(2)) / 2 + 1);
            long maxSuffixLengthBinary = ~((~0L) << 2 * forwardSuffixLength);
            long maxPrefixLengthBinary = ~((~0L) << 2 * reflexedPrefixLength);


            if (randomReflexivMarker == 2) {
                long[] newReflexivSubKmer = new long[param.subKmerBinarySlots];
                Long newReflexivLong = 0L;

                if (forwardSuffixLength > param.subKmerSizeResidue) { // forwardSuffixLength is longer than the length of the last block (element) of sub kmer long array
                    long transitBit1 = (Long) forwardSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) << 2 * (forwardSuffixLength - param.subKmerSizeResidue);
                    transitBit1 |= ((forwardSubKmer.getLong(2) & maxSuffixLengthBinary) >>> 2 * param.subKmerSizeResidue);
                    long transitBit2 = 0L;

                    newReflexivSubKmer[param.subKmerBinarySlots - 1] = (forwardSubKmer.getLong(2) & maxSubKmerResidueBinary);

                    for (int i = param.subKmerBinarySlots - 2; i >= 0; i--) {
                        transitBit2 = (Long) forwardSubKmer.getSeq(0).apply(i) >>> 2 * (31 - forwardSuffixLength);

                        newReflexivSubKmer[i] = (Long) forwardSubKmer.getSeq(0).apply(i) << 2 * forwardSuffixLength;
                        newReflexivSubKmer[i] |= transitBit1;
                        newReflexivSubKmer[i] &= maxSubKmerBinary;

                        transitBit1 = transitBit2;
                    }

                    newReflexivLong = reflexedSubKmer.getLong(2) << 2 * forwardSuffixLength; // do not remove the C marker as it will be used again
                    newReflexivLong |= transitBit2;
                    // newReflexivLong |= (1L<<2*forwardSuffixLength);

                } else if (forwardSuffixLength == param.subKmerSizeResidue) {
                    long transitBit1 = (Long) forwardSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                    long transitBit2 = 0L;

                    newReflexivSubKmer[param.subKmerBinarySlots - 1] = (forwardSubKmer.getLong(2) & maxSubKmerResidueBinary);

                    for (int i = param.subKmerBinarySlots - 2; i >= 0; i--) {
                        transitBit2 = (Long) forwardSubKmer.getSeq(0).apply(i) >>> 2 * (31 - forwardSuffixLength);

                        newReflexivSubKmer[i] = (Long) forwardSubKmer.getSeq(0).apply(i) << 2 * forwardSuffixLength;
                        newReflexivSubKmer[i] |= transitBit1;
                        newReflexivSubKmer[i] &= maxSubKmerBinary;

                        transitBit1 = transitBit2;
                    }

                    newReflexivLong = reflexedSubKmer.getLong(2) << 2 * forwardSuffixLength; // do not remove the C marker as it will be used again
                    newReflexivLong |= transitBit2;
                    // newReflexivLong |= (1L<<2*forwardSuffixLength);

                } else { //forwardSuffixLength < param.subKmerSizeResidue
                    long transitBit1 = (Long) forwardSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) >>> 2 * (param.subKmerSizeResidue - forwardSuffixLength);
                    long transitBit2 = 0L;

                    newReflexivSubKmer[param.subKmerBinarySlots - 1] = (Long) forwardSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) << 2 * forwardSuffixLength;
                    newReflexivSubKmer[param.subKmerBinarySlots - 1] |= (forwardSubKmer.getLong(2) & maxSuffixLengthBinary);
                    newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;

                    for (int i = param.subKmerBinarySlots - 2; i >= 0; i--) {
                        transitBit2 = (Long) forwardSubKmer.getSeq(0).apply(i) >>> 2 * (31 - forwardSuffixLength);

                        newReflexivSubKmer[i] = (Long) forwardSubKmer.getSeq(0).apply(i) << 2 * forwardSuffixLength;
                        newReflexivSubKmer[i] |= transitBit1;
                        newReflexivSubKmer[i] &= maxSubKmerBinary;

                        transitBit1 = transitBit2;
                    }

                    newReflexivLong = reflexedSubKmer.getLong(2) << 2 * forwardSuffixLength;
                    newReflexivLong |= transitBit2;
                    //newReflexivLong |= (1L<<2*forwardSuffixLength); // add C marker
                }

                /*
                for (int k = 30; k >= 0; k--) {
                    long a = newReflexivSubKmer[0] >>> 2 * k;
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

                if (newReflexivSubKmer.length >=3) {
                    for (int k = 5; k >= 0; k--) {
                        long a = newReflexivSubKmer[2] >>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }
                }

                System.out.print(" ");
                for (int k = 30; k >= 0; k--) {
                    long a = newReflexivLong >>> 2 * k;
                    a &= 3L;
                    char b = BinaryToNucleotide(a);
                    System.out.print(b);
                }
                */

            //    System.out.println(" DSExtendReflexivKmer extend " + randomReflexivMarker);

                if (bubbleDistance < 0) {
                    reflexivKmerConcatList.add(
                            RowFactory.create(newReflexivSubKmer, randomReflexivMarker, newReflexivLong, reflexedSubKmer.getInt(3), forwardSubKmer.getInt(4))
                    );
                } else {
                    if (forwardSubKmer.getInt(3) > 0) {
                        reflexivKmerConcatList.add(
                                RowFactory.create(newReflexivSubKmer, randomReflexivMarker, newReflexivLong, bubbleDistance, forwardSubKmer.getInt(4))
                        );
                    } else {
                        reflexivKmerConcatList.add(
                                RowFactory.create(newReflexivSubKmer, randomReflexivMarker, newReflexivLong, reflexedSubKmer.getInt(3), bubbleDistance)
                        );
                    }
                }

                randomReflexivMarker = 1; /* an action of randomization */
            } else { /* randomReflexivMarker == 1 */
                long[] newForwardSubKmer = new long[param.subKmerBinarySlots];
                long newForwardLong = 0L;

                if (reflexedPrefixLength > param.subKmerSizeResidue) {
                    long transitBit1 = (Long) reflexedSubKmer.getSeq(0).apply(0) & maxPrefixLengthBinary;

                    newForwardSubKmer[0] = (Long) reflexedSubKmer.getSeq(0).apply(0) >>> 2 * reflexedPrefixLength;
                    newForwardSubKmer[0] |= ((reflexedSubKmer.getLong(2) & maxPrefixLengthBinary) << 2 * (31 - reflexedPrefixLength));

                    for (int i = 1; i < param.subKmerBinarySlots - 1; i++) {
                        long transitBit2 = (Long) reflexedSubKmer.getSeq(0).apply(i) & maxPrefixLengthBinary;

                        newForwardSubKmer[i] = (Long) reflexedSubKmer.getSeq(0).apply(i) >>> 2 * reflexedPrefixLength;
                        newForwardSubKmer[i] |= (transitBit1 << 2 * (31 - reflexedPrefixLength));

                        transitBit1 = transitBit2;
                    }

                    newForwardSubKmer[param.subKmerBinarySlots - 1] = transitBit1 >>> 2 * (reflexedPrefixLength - param.subKmerSizeResidue);

                    newForwardLong = (Long) reflexedSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                    newForwardLong |= (transitBit1 << 2 * param.subKmerSizeResidue);
                    newForwardLong &= maxPrefixLengthBinary;
                    newForwardLong |= (1L << 2 * reflexedPrefixLength); // add C marker in the front
                    newForwardLong <<= 2 * forwardSuffixLength;
                    newForwardLong |= (forwardSubKmer.getLong(2) & maxSuffixLengthBinary);
                } else if (reflexedPrefixLength == param.subKmerSizeResidue) {
                    long transitBit1 = (Long) reflexedSubKmer.getSeq(0).apply(0) & maxPrefixLengthBinary;

                    newForwardSubKmer[0] = (Long) reflexedSubKmer.getSeq(0).apply(0) >>> 2 * reflexedPrefixLength;
                    newForwardSubKmer[0] |= ((reflexedSubKmer.getLong(2) & maxPrefixLengthBinary) << 2 * (31 - reflexedPrefixLength));

                    for (int i = 1; i < param.subKmerBinarySlots - 1; i++) {
                        long transitBit2 = (Long) reflexedSubKmer.getSeq(0).apply(i) & maxPrefixLengthBinary;

                        newForwardSubKmer[i] = (Long) reflexedSubKmer.getSeq(0).apply(i) >>> 2 * reflexedPrefixLength;
                        newForwardSubKmer[i] |= (transitBit1 << 2 * (31 - reflexedPrefixLength));

                        transitBit1 = transitBit2;
                    }

                    newForwardSubKmer[param.subKmerBinarySlots - 1] = transitBit1;

                    newForwardLong = (Long) reflexedSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                    newForwardLong |= (1L << 2 * reflexedPrefixLength); // add C marker
                    newForwardLong <<= 2 * forwardSuffixLength;
                    newForwardLong |= (forwardSubKmer.getLong(2) & maxSuffixLengthBinary);
                } else {
                    long transitBit1 = (Long) reflexedSubKmer.getSeq(0).apply(0) & maxPrefixLengthBinary;

                    newForwardSubKmer[0] = (Long) reflexedSubKmer.getSeq(0).apply(0) >>> 2 * reflexedPrefixLength;
                    newForwardSubKmer[0] |= ((reflexedSubKmer.getLong(2) & maxPrefixLengthBinary) << 2 * (31 - reflexedPrefixLength));
                    newForwardSubKmer[0] &= maxSubKmerBinary;

                    for (int i = 1; i < param.subKmerBinarySlots - 1; i++) {
                        long transitBit2 = (Long) reflexedSubKmer.getSeq(0).apply(i) & maxPrefixLengthBinary;

                        newForwardSubKmer[i] = (Long) reflexedSubKmer.getSeq(0).apply(i) >>> 2 * reflexedPrefixLength;
                        newForwardSubKmer[i] |= (transitBit1 << 2 * (31 - reflexedPrefixLength));
                        newForwardSubKmer[i] &= maxSubKmerBinary;

                        transitBit1 = transitBit2;
                    }

                    newForwardSubKmer[param.subKmerBinarySlots - 1] = transitBit1 << 2 * (param.subKmerSizeResidue - reflexedPrefixLength);
                    newForwardSubKmer[param.subKmerBinarySlots - 1] |= ((Long) reflexedSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) >>> 2 * reflexedPrefixLength);
                    newForwardSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;


                    newForwardLong = (Long) reflexedSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) & maxPrefixLengthBinary;
                    newForwardLong |= (1L << 2 * reflexedPrefixLength); // add C marker
                    newForwardLong <<= 2 * forwardSuffixLength;
                    newForwardLong |= (forwardSubKmer.getLong(2) & maxSuffixLengthBinary);
                }
                /*

                for (int k = 30; k >= 0; k--) {
                    long a = newForwardSubKmer[0] >>> 2 * k;
                    a &= 3L;
                    char b = BinaryToNucleotide(a);
                    System.out.print(b);
                }

                for (int k = 30; k >= 0; k--) {
                    long a = newForwardSubKmer[1] >>> 2 * k;
                    a &= 3L;
                    char b = BinaryToNucleotide(a);
                    System.out.print(b);
                }

                if (newForwardSubKmer.length >=3) {
                    for (int k = 5; k >= 0; k--) {
                        long a = newForwardSubKmer[2] >>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }
                }

                System.out.print(" ");
                for (int k = 30; k >= 0; k--) {
                    long a = newForwardLong >>> 2 * k;
                    a &= 3L;
                    char b = BinaryToNucleotide(a);
                    System.out.print(b);
                }
                */

             //   System.out.println(" DSExtendReflexivKmer extend " + randomReflexivMarker);

                if (bubbleDistance < 0) {
                    reflexivKmerConcatList.add(
                            RowFactory.create(newForwardSubKmer, randomReflexivMarker, newForwardLong, reflexedSubKmer.getInt(3), forwardSubKmer.getInt(4))
                    );
                } else {
                    if (forwardSubKmer.getInt(3) > 0) {
                        reflexivKmerConcatList.add(
                                RowFactory.create(newForwardSubKmer,
                                        randomReflexivMarker, newForwardLong, bubbleDistance, forwardSubKmer.getInt(4)
                                )
                        );
                    } else { // reflexedSubKmer.getInt(4) >0
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
                            S.getInt(1), S.getLong(2), S.getInt(3), S.getInt(4)
                    )
            );
        }

        /**
         *
         */
        public void tmpKmerRandomizer() {
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
                if (HighCoverageSubKmer.size() == 0) {
                    HighCoverageSubKmer.add(
                            RowFactory.create(subKmer.getSeq(0), subKmer.getInt(1), subKmer.getLong(2), subKmer.getInt(3), -1)
                    );
                } else {
                    if (subKmerSlotComparator(subKmer.getSeq(0), HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1).getSeq(0)) == true) {
                        if (subKmer.getInt(3) > HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1).getInt(3)) {
                            if (HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1).getInt(3) <= param.minErrorCoverage && subKmer.getInt(3) >= 2 * HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1).getInt(3)) {
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        RowFactory.create(subKmer.getSeq(0), subKmer.getInt(1), subKmer.getLong(2), subKmer.getInt(3), -1)
                                );
                            } else {
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        RowFactory.create(subKmer.getSeq(0), subKmer.getInt(1), subKmer.getLong(2), subKmer.getInt(3), param.subKmerSize)
                                );
                            }
                        } else if (subKmer.getInt(3) == HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1).getInt(3)) {
                            if (subKmer.getLong(2) > HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1).getLong(2)) {
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        RowFactory.create(subKmer.getSeq(0), subKmer.getInt(1), subKmer.getLong(2), subKmer.getInt(3), param.subKmerSize)
                                );
                            } else {
                                subKmer = HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1);
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        RowFactory.create(subKmer.getSeq(0), subKmer.getInt(1), subKmer.getLong(2), subKmer.getInt(3), param.subKmerSize)
                                );
                            }
                        } else {
                            if (subKmer.getInt(3) <= param.minErrorCoverage && HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1).getInt(3) >= 2 * subKmer.getInt(3)) {
                                subKmer = HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1);
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        RowFactory.create(subKmer.getSeq(0), subKmer.getInt(1), subKmer.getLong(2), subKmer.getInt(3), -1)
                                );
                            } else {
                                subKmer = HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1);
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        RowFactory.create(subKmer.getSeq(0), subKmer.getInt(1), subKmer.getLong(2), subKmer.getInt(3), param.subKmerSize)
                                );
                            }
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
                if (HighCoverageSubKmer.size() == 0) {
                    HighCoverLastCoverage = subKmer.getInt(3);
                    HighCoverageSubKmer.add(
                            RowFactory.create(subKmer.getSeq(0), subKmer.getInt(1), subKmer.getLong(2), -1, subKmer.getInt(4))
                    );
                } else {
                    if (subKmerSlotComparator(subKmer.getSeq(0), HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1).getSeq(0)) == true) {
                        if (subKmer.getInt(3) > HighCoverLastCoverage) {
                            if (HighCoverLastCoverage <= param.minErrorCoverage && subKmer.getInt(3) >= 2 * HighCoverLastCoverage) {
                                HighCoverLastCoverage = subKmer.getInt(3);
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        RowFactory.create(subKmer.getSeq(0), subKmer.getInt(1), subKmer.getLong(2), -1, subKmer.getInt(4))
                                );
                            } else {
                                HighCoverLastCoverage = subKmer.getInt(3);
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        RowFactory.create(subKmer.getSeq(0), subKmer.getInt(1), subKmer.getLong(2), param.subKmerSize, subKmer.getInt(4))
                                );
                            }
                        } else if (subKmer.getInt(3) == HighCoverLastCoverage) {
                            int subKmerFirstSuffixLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros(subKmer.getLong(2)) / 2 + 1);
                            int HighCoverageSubKmerFirstSuffixLength = Long.SIZE / 2 - ((Long.numberOfLeadingZeros(HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1).getLong(2))) / 2 + 1);
                            Long subKmerFirstSuffix = subKmer.getLong(2) >>> 2 * (subKmerFirstSuffixLength - 1);
                            Long HighCoverageSubKmerFirstSuffix = HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1).getLong(2) >>> 2 * (HighCoverageSubKmerFirstSuffixLength);

                            if (subKmerFirstSuffix.compareTo(HighCoverageSubKmerFirstSuffix) > 0) {
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        RowFactory.create(subKmer.getSeq(0),
                                                subKmer.getInt(1), subKmer.getLong(2), param.subKmerSize, subKmer.getInt(4))
                                );
                            } else {
                                subKmer = HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1); // re assign
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        RowFactory.create(subKmer.getSeq(0),
                                                subKmer.getInt(1), subKmer.getLong(2), param.subKmerSize, subKmer.getInt(4))
                                );
                            }
                        } else {
                            if (subKmer.getInt(3) <= param.minErrorCoverage && HighCoverLastCoverage >= 2 * subKmer.getInt(3)) {
                                subKmer = HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1);
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        RowFactory.create(subKmer.getSeq(0),
                                                subKmer.getInt(1), subKmer.getLong(2), -1, subKmer.getInt(4))
                                );
                            } else {
                                subKmer = HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1);
                                HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                        RowFactory.create(subKmer.getSeq(0),
                                                subKmer.getInt(1), subKmer.getLong(2), param.subKmerSize, subKmer.getInt(4))
                                );
                            }
                        }
                    } else {
                        HighCoverLastCoverage = subKmer.getInt(3);
                        HighCoverageSubKmer.add(
                                RowFactory.create(subKmer.getSeq(0),
                                        subKmer.getInt(1), subKmer.getLong(2), -1, subKmer.getInt(4))
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

    class DSForwardSubKmerExtraction implements MapPartitionsFunction<Row, Row>, Serializable {
        List<Row> TupleList = new ArrayList<Row>();
        Long suffixBinary;
        long[] prefixBinarySlot;
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

                if (param.kmerSizeResidueAssemble == 1) {
                    prefixBinarySlot = new long[param.subKmerBinarySlots];

                    suffixBinary = ((long[]) kmerTuple.get(0))[param.kmerBinarySlotsAssemble - 1];
                    for (int i = 0; i < param.subKmerBinarySlots; i++) {
                        prefixBinarySlot[i] = ((long[]) kmerTuple.get(0))[i];
                    }
                } else {
                    prefixBinarySlot = new long[param.subKmerBinarySlots];

                    suffixBinary = ((long[]) kmerTuple.get(0))[param.kmerBinarySlotsAssemble - 1] & 3L;
                    for (int i = 0; i < param.subKmerBinarySlots - 1; i++) {
                        prefixBinarySlot[i] = ((long[]) kmerTuple.get(0))[i];
                    }
                    prefixBinarySlot[param.kmerBinarySlotsAssemble - 1] = ((long[]) kmerTuple.get(0))[param.kmerBinarySlotsAssemble - 1] >>> 2;
                }

/*
                for (int k = 30; k >= 0; k--) {
                    long a = prefixBinarySlot[0] >>> 2 * k;
                    a &= 3L;
                    char b = BinaryToNucleotide(a);
                    System.out.print(b);
                }


                for (int k = 30; k >= 0; k--) {
                    long a = prefixBinarySlot[1] >>> 2 * k;
                    a &= 3L;
                    char b = BinaryToNucleotide(a);
                    System.out.print(b);
                }

                if (prefixBinarySlot.length>=3) {
                    for (int k = 5; k >= 0; k--) {
                        long a = prefixBinarySlot[2] >>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }
                }
                */

             //   System.out.println(" " + suffixBinary + "DSForwardSubKmerExtraction");

                TupleList.add(
                        RowFactory.create(prefixBinarySlot, 1, suffixBinary, kmerTuple.getInt(1), kmerTuple.getInt(1))
                );
            }

            return TupleList.iterator();
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

        public Iterator<Row> call(Iterator<Row> s) {

            while (s.hasNext()) {
                kmerTuple = s.next();

                long[] prefixBinarySlot = new long[param.subKmerBinarySlots];

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

                long transmitBit1 = (Long) kmerTuple.getSeq(0).apply(param.subKmerBinarySlots - 1) >>> 2 * (param.subKmerSizeResidue - 1);
                prefixBinarySlot[param.subKmerBinarySlots - 1] = (Long) kmerTuple.getSeq(0).apply(param.subKmerBinarySlots - 1) << 2;
                prefixBinarySlot[param.subKmerBinarySlots - 1] &= maxSubKmerResdueBinary;
                prefixBinarySlot[param.subKmerBinarySlots - 1] |= (kmerTuple.getLong(2) & 3L);

                for (int i = param.subKmerBinarySlots - 2; i >= 0; i--) {
                    long transmitBit2 = (Long) kmerTuple.getSeq(0).apply(i) >>> 2*30;

                    prefixBinarySlot[i] = (Long) kmerTuple.getSeq(0).apply(i) << 2;
                    prefixBinarySlot[i] &= maxSubKmerBinary;
                    prefixBinarySlot[i] |= transmitBit1;

                    transmitBit1 = transmitBit2;
                }

                /*
                for (int k = 30; k >= 0; k--) {
                    long a = (Long) kmerTuple.getSeq(0).apply(0) >>> 2 * k;
                    a &= 3L;
                    char b = BinaryToNucleotide(a);
                    System.out.print(b);
                }


                for (int k = 30; k >= 0; k--) {
                    long a = (Long) kmerTuple.getSeq(0).apply(1) >>> 2 * k;
                    a &= 3L;
                    char b = BinaryToNucleotide(a);
                    System.out.print(b);
                }

                if (prefixBinarySlot.length >=3){
                    for (int k = 5; k >= 0; k--) {
                        long a = (Long) kmerTuple.getSeq(0).apply(2) >>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }
                }

              //  System.out.println();

                for (int k = 30; k >= 0; k--) {
                    long a = prefixBinarySlot[0] >>> 2 * k;
                    a &= 3L;
                    char b = BinaryToNucleotide(a);
                    System.out.print(b);
                }


                for (int k = 30; k >= 0; k--) {
                    long a = prefixBinarySlot[1] >>> 2 * k;
                    a &= 3L;
                    char b = BinaryToNucleotide(a);
                    System.out.print(b);
                }

                if (prefixBinarySlot.length >=3){
                    for (int k = 5; k >= 0; k--) {
                        long a = prefixBinarySlot[2] >>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }
                }

                System.out.print(" ");

                for (int k = 30; k >= 0; k--) {
                    long a = suffixBinary >>> 2 * k;
                    a &= 3L;
                    char b = BinaryToNucleotide(a);
                    System.out.print(b);
                }
                */

             //   System.out.println(" DSReflectedSubKmerExtractionFromForward");

                TupleList.add(
                        RowFactory.create(prefixBinarySlot, 2, suffixBinary, kmerTuple.getInt(3), kmerTuple.getInt(4))
                );
            }

            return TupleList.iterator();
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

    class DSkmerRandomReflectionFromLongNotSeq implements MapPartitionsFunction<Row, Row>, Serializable {
        /* 0 stands for forward sub-kmer */
        /* 1 stands for reflexiv sub-kmer */
        private int randomReflexivMarker = 2;

        List<Row> reflexivKmerConcatList = new ArrayList<Row>();
        Row kmerTuple;
        long maxSubKmerResidueBinary = ~((~0L) << 2 * param.subKmerSizeResidue);
        long maxSubKmerBinary = ~((~0L) << 2 * 31);

        public Iterator<Row> call(Iterator<Row> s) {
            while (s.hasNext()) {
                kmerTuple = s.next();

                singleKmerRandomizer(kmerTuple);
            }
            return reflexivKmerConcatList.iterator();
        }

        public void singleKmerRandomizer(Row currentSubKmer) {

            if (currentSubKmer.getInt(1) == 1) {
                /**
                 * 00000000000000110010111010010   Long.SIZE
                 * --------------C-G-G-G-T-C-A-G   Long.SIZE - (Long.numberOfLeadingZeros / 2 + 1)
                 * --------------^-Length marker
                 */
                int currentSuffixLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros(currentSubKmer.getLong(2)) / 2 + 1);
                long maxSuffixLengthBinary = ~(~0L << 2 * currentSuffixLength);
                long[] newReflexivSubKmer = new long[param.subKmerBinarySlots];
                long newReflexivLong;

                if (randomReflexivMarker == 2) {

                    // long transitBit1;
                    if (currentSuffixLength > param.subKmerSizeResidue) { // currentSuffixLength is longer than the length of the last block (element) of sub kmer long array
                        long transitBit1 = ((long[]) currentSubKmer.get(0))[param.subKmerBinarySlots - 1] << 2 * (currentSuffixLength - param.subKmerSizeResidue);
                        transitBit1 |= ((currentSubKmer.getLong(2) & maxSuffixLengthBinary) >>> 2 * param.subKmerSizeResidue);
                        long transitBit2 = 0L;

                        newReflexivSubKmer[param.subKmerBinarySlots - 1] = (currentSubKmer.getLong(2) & maxSubKmerResidueBinary);

                        for (int i = param.subKmerBinarySlots - 2; i >= 0; i--) {
                            transitBit2 = ((long[]) currentSubKmer.get(0))[i] >>> 2 * (31 - currentSuffixLength);

                            newReflexivSubKmer[i] = ((long[]) currentSubKmer.get(0))[i]  << 2 * currentSuffixLength;
                            newReflexivSubKmer[i] |= transitBit1;
                            newReflexivSubKmer[i] &= maxSubKmerBinary;

                            transitBit1 = transitBit2;
                        }

                        newReflexivLong = transitBit2;
                        newReflexivLong |= (1L << 2 * currentSuffixLength);

                    } else if (currentSuffixLength == param.subKmerSizeResidue) {
                        long transitBit1 = ((long[]) currentSubKmer.get(0))[param.subKmerBinarySlots - 1];
                        long transitBit2 = 0L;

                        newReflexivSubKmer[param.subKmerBinarySlots - 1] = (currentSubKmer.getLong(2) & maxSubKmerResidueBinary);

                        for (int i = param.subKmerBinarySlots - 2; i >= 0; i--) {
                            transitBit2 = ((long[]) currentSubKmer.get(0))[i] >>> 2 * (31 - currentSuffixLength);

                            newReflexivSubKmer[i] = ((long[]) currentSubKmer.get(0))[i] << 2 * currentSuffixLength;
                            newReflexivSubKmer[i] |= transitBit1;
                            newReflexivSubKmer[i] &= maxSubKmerBinary;

                            transitBit1 = transitBit2;
                        }

                        newReflexivLong = transitBit2;
                        newReflexivLong |= (1L << 2 * currentSuffixLength);

                    } else { //currentSuffixLength < param.subKmerSizeResidue
                        long transitBit1 = ((long[]) currentSubKmer.get(0))[param.subKmerBinarySlots - 1] >>> 2 * (param.subKmerSizeResidue - currentSuffixLength);
                        long transitBit2 = 0L;

                        newReflexivSubKmer[param.subKmerBinarySlots - 1] = ((long[]) currentSubKmer.get(0))[param.subKmerBinarySlots - 1] << 2 * currentSuffixLength;
                        newReflexivSubKmer[param.subKmerBinarySlots - 1] |= (currentSubKmer.getLong(2) & maxSuffixLengthBinary);
                        newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;

                        for (int i = param.subKmerBinarySlots - 2; i >= 0; i--) {
                            transitBit2 = ((long[]) currentSubKmer.get(0))[i] >>> 2 * (31 - currentSuffixLength);

                            newReflexivSubKmer[i] = ((long[]) currentSubKmer.get(0))[i] << 2 * currentSuffixLength;
                            newReflexivSubKmer[i] |= transitBit1;
                            newReflexivSubKmer[i] &= maxSubKmerBinary;

                            transitBit1 = transitBit2;
                        }

                        newReflexivLong = transitBit2;
                        newReflexivLong |= (1L << 2 * currentSuffixLength); // add C marker
                    }

                    reflexivKmerConcatList.add(
                            RowFactory.create(newReflexivSubKmer, randomReflexivMarker, newReflexivLong, currentSubKmer.getInt(3), currentSubKmer.getInt(4))
                    );
                } else {
                    reflexivKmerConcatList.add(currentSubKmer);
                }
            } else { /* currentSubKmer._2._1() == 2 */
                int currentPrefixLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros(currentSubKmer.getLong(2)) / 2 + 1);
                long maxSuffixLengthBinary = ~((~0L) << (2 * currentPrefixLength));
                long[] newReflexivSubKmer = new long[param.subKmerBinarySlots];
                Long newReflexivLong;

                if (randomReflexivMarker == 2) {
                    reflexivKmerConcatList.add(currentSubKmer);
                } else { /* randomReflexivMarker == 1 */
                    if (currentPrefixLength > param.subKmerSizeResidue) {
                        long transitBit1 = ((long[]) currentSubKmer.get(0))[0] & maxSuffixLengthBinary;

                        newReflexivSubKmer[0] = ((long[]) currentSubKmer.get(0))[0] >>> 2 * currentPrefixLength;
                        newReflexivSubKmer[0] |= ((currentSubKmer.getLong(2) & maxSuffixLengthBinary) << 2 * (31 - currentPrefixLength));
                        newReflexivSubKmer[0] &= maxSubKmerBinary;

                        for (int i = 1; i < param.subKmerBinarySlots - 1; i++) {
                            long transitBit2 = ((long[]) currentSubKmer.get(0))[i] & maxSuffixLengthBinary;

                            newReflexivSubKmer[i] = ((long[]) currentSubKmer.get(0))[i] >>> 2 * currentPrefixLength;
                            newReflexivSubKmer[i] |= (transitBit1 << 2 * (31 - currentPrefixLength));
                            newReflexivSubKmer[i] &= maxSubKmerBinary;

                            transitBit1 = transitBit2;
                        }

                        newReflexivSubKmer[param.subKmerBinarySlots - 1] = transitBit1 & maxSubKmerResidueBinary;
                        newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;

                        newReflexivLong = ((long[]) currentSubKmer.get(0))[param.subKmerBinarySlots - 1];
                        newReflexivLong |= (transitBit1 << 2 * param.subKmerSizeResidue);
                        newReflexivLong &= maxSuffixLengthBinary;
                        newReflexivLong |= (1L << 2 * currentPrefixLength); // add C marker in the front
                    } else if (currentPrefixLength == param.subKmerSizeResidue) {
                        long transitBit1 = ((long[]) currentSubKmer.get(0))[0] & maxSuffixLengthBinary;

                        newReflexivSubKmer[0] = ((long[]) currentSubKmer.get(0))[0] >>> 2 * currentPrefixLength;
                        newReflexivSubKmer[0] |= ((currentSubKmer.getLong(2) & maxSuffixLengthBinary) << 2 * (31 - currentPrefixLength));
                        newReflexivSubKmer[0] &= maxSubKmerBinary;

                        for (int i = 1; i < param.subKmerBinarySlots - 1; i++) {
                            long transitBit2 = ((long[]) currentSubKmer.get(0))[i] & maxSuffixLengthBinary;

                            newReflexivSubKmer[i] = ((long[]) currentSubKmer.get(0))[i] >>> 2 * currentPrefixLength;
                            newReflexivSubKmer[i] |= (transitBit1 << 2 * (31 - currentPrefixLength));
                            newReflexivSubKmer[i] &= maxSubKmerBinary;

                            transitBit1 = transitBit2;
                        }

                        newReflexivSubKmer[param.subKmerBinarySlots - 1] = transitBit1;
                        newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;

                        newReflexivLong = ((long[]) currentSubKmer.get(0))[param.subKmerBinarySlots - 1];
                        newReflexivLong |= (1L << 2 * currentPrefixLength); // add C marker
                    } else {
                        long transitBit1 = ((long[]) currentSubKmer.get(0))[0] & maxSuffixLengthBinary;

                        newReflexivSubKmer[0] = ((long[]) currentSubKmer.get(0))[0] >>> 2 * currentPrefixLength;
                        newReflexivSubKmer[0] |= ((currentSubKmer.getLong(2) & maxSuffixLengthBinary) << 2 * (31 - currentPrefixLength));
                        newReflexivSubKmer[0] &= maxSubKmerBinary;

                        for (int i = 1; i < param.subKmerBinarySlots - 1; i++) {
                            long transitBit2 = ((long[]) currentSubKmer.get(0))[i] & maxSuffixLengthBinary;

                            newReflexivSubKmer[i] = ((long[]) currentSubKmer.get(0))[i] >>> 2 * currentPrefixLength;
                            newReflexivSubKmer[i] |= (transitBit1 << 2 * (31 - currentPrefixLength));
                            newReflexivSubKmer[i] &= maxSubKmerBinary;

                            transitBit1 = transitBit2;
                        }

                        newReflexivSubKmer[param.subKmerBinarySlots - 1] = transitBit1 << 2 * (param.subKmerSizeResidue - currentPrefixLength);
                        newReflexivSubKmer[param.subKmerBinarySlots - 1] |= (((long[]) currentSubKmer.get(0))[param.subKmerBinarySlots - 1] >>> 2 * currentPrefixLength);
                        newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;


                        newReflexivLong = ((long[]) currentSubKmer.get(0))[param.subKmerBinarySlots - 1] & maxSuffixLengthBinary;
                        newReflexivLong |= (1L << 2 * currentPrefixLength); // add C marker
                    }

                    reflexivKmerConcatList.add(
                            RowFactory.create(newReflexivSubKmer, randomReflexivMarker, newReflexivLong, currentSubKmer.getInt(3), currentSubKmer.getInt(4))
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
    }


    class DSkmerRandomReflection implements MapPartitionsFunction<Row, Row>, Serializable {
        /* 0 stands for forward sub-kmer */
        /* 1 stands for reflexiv sub-kmer */
        private int randomReflexivMarker = 2;

        List<Row> reflexivKmerConcatList = new ArrayList<Row>();
        Row kmerTuple;
        long maxSubKmerResidueBinary = ~((~0L) << 2 * param.subKmerSizeResidue);
        long maxSubKmerBinary = ~((~0L) << 2 * 31);

        public Iterator<Row> call(Iterator<Row> s) {
            while (s.hasNext()) {
                kmerTuple = s.next();

                singleKmerRandomizer(kmerTuple);
            }
            return reflexivKmerConcatList.iterator();
        }

        public void singleKmerRandomizer(Row currentSubKmer) {

            if (currentSubKmer.getInt(1) == 1) {
                /**
                 * 00000000000000110010111010010   Long.SIZE
                 * --------------C-G-G-G-T-C-A-G   Long.SIZE - (Long.numberOfLeadingZeros / 2 + 1)
                 * --------------^-Length marker
                 */
                int currentSuffixLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros(currentSubKmer.getLong(2)) / 2 + 1);
                long maxSuffixLengthBinary = ~(~0L << 2 * currentSuffixLength);
                long[] newReflexivSubKmer = new long[param.subKmerBinarySlots];
                long newReflexivLong;

                if (randomReflexivMarker == 2) {

                    // long transitBit1;
                    if (currentSuffixLength > param.subKmerSizeResidue) { // currentSuffixLength is longer than the length of the last block (element) of sub kmer long array
                        long transitBit1 = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) << 2 * (currentSuffixLength - param.subKmerSizeResidue);
                        transitBit1 |= ((currentSubKmer.getLong(2) & maxSuffixLengthBinary) >>> 2 * param.subKmerSizeResidue);
                        long transitBit2 = 0L;

                        newReflexivSubKmer[param.subKmerBinarySlots - 1] = (currentSubKmer.getLong(2) & maxSubKmerResidueBinary);

                        for (int i = param.subKmerBinarySlots - 2; i >= 0; i--) {
                            transitBit2 = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * (31 - currentSuffixLength);

                            newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(i) << 2 * currentSuffixLength;
                            newReflexivSubKmer[i] |= transitBit1;
                            newReflexivSubKmer[i] &= maxSubKmerBinary;

                            transitBit1 = transitBit2;
                        }

                        newReflexivLong = transitBit2;
                        newReflexivLong |= (1L << 2 * currentSuffixLength);

                    } else if (currentSuffixLength == param.subKmerSizeResidue) {
                        long transitBit1 = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                        long transitBit2 = 0L;

                        newReflexivSubKmer[param.subKmerBinarySlots - 1] = (currentSubKmer.getLong(2) & maxSubKmerResidueBinary);

                        for (int i = param.subKmerBinarySlots - 2; i >= 0; i--) {
                            transitBit2 = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * (31 - currentSuffixLength);

                            newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(i) << 2 * currentSuffixLength;
                            newReflexivSubKmer[i] |= transitBit1;
                            newReflexivSubKmer[i] &= maxSubKmerBinary;

                            transitBit1 = transitBit2;
                        }

                        newReflexivLong = transitBit2;
                        newReflexivLong |= (1L << 2 * currentSuffixLength);

                    } else { //currentSuffixLength < param.subKmerSizeResidue
                        long transitBit1 = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) >>> 2 * (param.subKmerSizeResidue - currentSuffixLength);
                        long transitBit2 = 0L;

                        newReflexivSubKmer[param.subKmerBinarySlots - 1] = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) << 2 * currentSuffixLength;
                        newReflexivSubKmer[param.subKmerBinarySlots - 1] |= (currentSubKmer.getLong(2) & maxSuffixLengthBinary);
                        newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;

                        for (int i = param.subKmerBinarySlots - 2; i >= 0; i--) {
                            transitBit2 = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * (31 - currentSuffixLength);

                            newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(i) << 2 * currentSuffixLength;
                            newReflexivSubKmer[i] |= transitBit1;
                            newReflexivSubKmer[i] &= maxSubKmerBinary;

                            transitBit1 = transitBit2;
                        }

                        newReflexivLong = transitBit2;
                        newReflexivLong |= (1L << 2 * currentSuffixLength); // add C marker
                    }

                    reflexivKmerConcatList.add(
                            RowFactory.create(newReflexivSubKmer, randomReflexivMarker, newReflexivLong, currentSubKmer.getInt(3), currentSubKmer.getInt(4))
                    );
                } else {
                    reflexivKmerConcatList.add(currentSubKmer);
                }
            } else { /* currentSubKmer._2._1() == 2 */
                int currentPrefixLength = Long.SIZE / 2 - (Long.numberOfLeadingZeros(currentSubKmer.getLong(2)) / 2 + 1);
                long maxSuffixLengthBinary = ~((~0L) << (2 * currentPrefixLength));
                long[] newReflexivSubKmer = new long[param.subKmerBinarySlots];
                Long newReflexivLong;

                if (randomReflexivMarker == 2) {
                    reflexivKmerConcatList.add(currentSubKmer);
                } else { /* randomReflexivMarker == 1 */
                    if (currentPrefixLength > param.subKmerSizeResidue) {
                        long transitBit1 = (Long) currentSubKmer.getSeq(0).apply(0) & maxSuffixLengthBinary;

                        newReflexivSubKmer[0] = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * currentPrefixLength;
                        newReflexivSubKmer[0] |= ((currentSubKmer.getLong(2) & maxSuffixLengthBinary) << 2 * (31 - currentPrefixLength));
                        newReflexivSubKmer[0] &= maxSubKmerBinary;

                        for (int i = 1; i < param.subKmerBinarySlots - 1; i++) {
                            long transitBit2 = (Long) currentSubKmer.getSeq(0).apply(i) & maxSuffixLengthBinary;

                            newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * currentPrefixLength;
                            newReflexivSubKmer[i] |= (transitBit1 << 2 * (31 - currentPrefixLength));
                            newReflexivSubKmer[i] &= maxSubKmerBinary;

                            transitBit1 = transitBit2;
                        }

                        newReflexivSubKmer[param.subKmerBinarySlots - 1] = transitBit1 & maxSubKmerResidueBinary;
                        newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;

                        newReflexivLong = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                        newReflexivLong |= (transitBit1 << 2 * param.subKmerSizeResidue);
                        newReflexivLong &= maxSuffixLengthBinary;
                        newReflexivLong |= (1L << 2 * currentPrefixLength); // add C marker in the front
                    } else if (currentPrefixLength == param.subKmerSizeResidue) {
                        long transitBit1 = (Long) currentSubKmer.getSeq(0).apply(0) & maxSuffixLengthBinary;

                        newReflexivSubKmer[0] = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * currentPrefixLength;
                        newReflexivSubKmer[0] |= ((currentSubKmer.getLong(2) & maxSuffixLengthBinary) << 2 * (31 - currentPrefixLength));
                        newReflexivSubKmer[0] &= maxSubKmerBinary;

                        for (int i = 1; i < param.subKmerBinarySlots - 1; i++) {
                            long transitBit2 = (Long) currentSubKmer.getSeq(0).apply(i) & maxSuffixLengthBinary;

                            newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * currentPrefixLength;
                            newReflexivSubKmer[i] |= (transitBit1 << 2 * (31 - currentPrefixLength));
                            newReflexivSubKmer[i] &= maxSubKmerBinary;

                            transitBit1 = transitBit2;
                        }

                        newReflexivSubKmer[param.subKmerBinarySlots - 1] = transitBit1;
                        newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;

                        newReflexivLong = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1);
                        newReflexivLong |= (1L << 2 * currentPrefixLength); // add C marker
                    } else {
                        long transitBit1 = (Long) currentSubKmer.getSeq(0).apply(0) & maxSuffixLengthBinary;

                        newReflexivSubKmer[0] = (Long) currentSubKmer.getSeq(0).apply(0) >>> 2 * currentPrefixLength;
                        newReflexivSubKmer[0] |= ((currentSubKmer.getLong(2) & maxSuffixLengthBinary) << 2 * (31 - currentPrefixLength));
                        newReflexivSubKmer[0] &= maxSubKmerBinary;

                        for (int i = 1; i < param.subKmerBinarySlots - 1; i++) {
                            long transitBit2 = (Long) currentSubKmer.getSeq(0).apply(i) & maxSuffixLengthBinary;

                            newReflexivSubKmer[i] = (Long) currentSubKmer.getSeq(0).apply(i) >>> 2 * currentPrefixLength;
                            newReflexivSubKmer[i] |= (transitBit1 << 2 * (31 - currentPrefixLength));
                            newReflexivSubKmer[i] &= maxSubKmerBinary;

                            transitBit1 = transitBit2;
                        }

                        newReflexivSubKmer[param.subKmerBinarySlots - 1] = transitBit1 << 2 * (param.subKmerSizeResidue - currentPrefixLength);
                        newReflexivSubKmer[param.subKmerBinarySlots - 1] |= ((Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) >>> 2 * currentPrefixLength);
                        newReflexivSubKmer[param.subKmerBinarySlots - 1] &= maxSubKmerResidueBinary;


                        newReflexivLong = (Long) currentSubKmer.getSeq(0).apply(param.subKmerBinarySlots - 1) & maxSuffixLengthBinary;
                        newReflexivLong |= (1L << 2 * currentPrefixLength); // add C marker
                    }

                    reflexivKmerConcatList.add(
                            RowFactory.create(newReflexivSubKmer, randomReflexivMarker, newReflexivLong, currentSubKmer.getInt(3), currentSubKmer.getInt(4))
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


        public Iterator<Row> call(Iterator<Row> s) {


            while (s.hasNext()) {
                kmerTuple = s.next();
                kmerBinarySeq = kmerTuple.getSeq(0);
                //reverseComplement=0L;

                forwardKmer = new long[param.kmerBinarySlotsAssemble];
                reverseComplement = new long[param.kmerBinarySlotsAssemble];

                for (int i = 0; i < param.kmerSize; i++) {
                    int RCindex = param.kmerSize - i - 1; //  ------------- ------------- ---------**-- RC index goes reverse
                    //  ------------- ------------- -------**----  <--
                    reverseComplement[i / 31] <<= 2;

                    if (RCindex >= param.kmerSize - param.kmerSizeResidueAssemble) {
                        lastTwoBits = (Long) kmerBinarySeq.apply(RCindex / 31) >>> 2 * (param.kmerSizeResidueAssemble - (RCindex % 31) - 1);    //  ------------- ------------- ------|----**
                        lastTwoBits &= 3L;
                        lastTwoBits ^= 3L;
                    } else {
                        lastTwoBits = (Long) kmerBinarySeq.apply(RCindex / 31) >>> 2 * (31 - (RCindex % 31) - 1);
                        lastTwoBits &= 3L;
                        lastTwoBits ^= 3L;
                    }

                    reverseComplement[i / 31] |= lastTwoBits;
                }

                for (int i = 0; i < param.kmerBinarySlotsAssemble; i++) {
                    forwardKmer[i] = (Long) kmerTuple.getSeq(0).apply(i);
                }

                kmerList.add(RowFactory.create(forwardKmer, kmerTuple.getInt(1)));
                kmerList.add(RowFactory.create(reverseComplement, kmerTuple.getInt(1)));

                /*
                for (int k = 30; k >= 0; k--) {
                    long a = forwardKmer[0] >>> 2 * k;
                    a &= 3L;
                    char b = BinaryToNucleotide(a);
                    System.out.print(b);
                }

                for (int k = 30; k >= 0; k--) {
                    long a = forwardKmer[1] >>> 2 * k;
                    a &= 3L;
                    char b = BinaryToNucleotide(a);
                    System.out.print(b);
                }

                if (forwardKmer.length >=3) {
                    for (int k = 6; k >= 0; k--) {
                        long a = forwardKmer[2] >>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }
                }

                if (forwardKmer.length>=4) {
                    for (int k = 0; k >= 0; k--) {
                        long a = forwardKmer[3] >>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }
                }

             //   System.out.println();


                for (int k = 30; k >= 0; k--) {
                    long a = reverseComplement[0] >>> 2 * k;
                    a &= 3L;
                    char b = BinaryToNucleotide(a);
                    System.out.print(b);
                }

                for (int k = 30; k >= 0; k--) {
                    long a = reverseComplement[1] >>> 2 * k;
                    a &= 3L;
                    char b = BinaryToNucleotide(a);
                    System.out.print(b);
                }

                if (reverseComplement.length >=3) {
                    for (int k = 6; k >= 0; k--) {
                        long a = reverseComplement[2] >>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }
                }

                if (forwardKmer.length>=4) {
                    for (int k = 30; k >= 0; k--) {
                        long a = reverseComplement[3] >>> 2 * k;
                        a &= 3L;
                        char b = BinaryToNucleotide(a);
                        System.out.print(b);
                    }
                }
                */

             //   System.out.println();
             //   System.out.println();
            }

            return kmerList.iterator();
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

                units = s.next();

                kmer = units.getString(0);

                if (kmer.startsWith("(")) {
                    kmer = kmer.substring(1);
                }

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

                long[] nucleotideBinarySlot = new long[param.kmerBinarySlotsAssemble];
                //       Long nucleotideBinary = 0L;

                for (int i = 0; i < param.kmerSize; i++) {
                    nucleotide = kmer.charAt(i);
                    if (nucleotide >= 256) nucleotide = 255;
                    nucleotideInt = nucleotideValue(nucleotide);
                    // forward kmer in bits
                    nucleotideBinarySlot[i / 31] <<= 2;
                    nucleotideBinarySlot[i / 31] |= nucleotideInt;

                    //   nucleotideBinary <<= 2;
                    //   nucleotideBinary |= nucleotideInt;
                }

                kmerList.add(
                        RowFactory.create(nucleotideBinarySlot, cover)
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

    class KmerBinaryExtractionFromStringDS64 implements MapPartitionsFunction<Row, Row>, Serializable{
        long maxSubKmerAssembleBits= ~((~0L) << (2*param.kmerSizeResidueAssemble));

        List<Row> kmerList = new ArrayList<Row>();
        int readLength;
        String[] units;
        String read;
        char nucleotide;
        long nucleotideInt;
        long nucleotideIntComplement;

        long[] prefixBinarySlot;
        long suffixBinary;

        public Iterator<Row> call(Iterator<Row> s){

            while (s.hasNext()) {
                Row S = s.next();
                read = S.getString(0);
                if (S.getInt(1)==1) {
                    read += S.getString(2);
                }else { // reflected
                    read = S.getString(2) + read;
                }

                readLength = read.length();

                if (readLength - param.kmerSize +1 <= 0) {
                    continue;
                }

                Long nucleotideBinary = 0L;

                long[] nucleotideBinarySlot = new long[param.kmerBinarySlotsAssemble];


                for (int i = 0; i < readLength; i++) {
                    nucleotide = read.charAt(i);
                    if (nucleotide >= 256) nucleotide = 255;
                    nucleotideInt = nucleotideValue(nucleotide);

                    // forward kmer in bits
                    if (i <= param.kmerSize-1) {
                        nucleotideBinary <<= 2;
                        nucleotideBinary |= nucleotideInt;

                        if ((i+1) % 31 == 0) { // each 32 nucleotides fill a slot
                            nucleotideBinarySlot[(i+1) / 31 - 1] = nucleotideBinary;
                            nucleotideBinary = 0L;
                        }

                        if (i == param.kmerSize-1) { // start completing the first kmer
                            nucleotideBinary &= maxSubKmerAssembleBits;
                            nucleotideBinarySlot[(i+1) / 31] = nucleotideBinary; // (i-param.frontClip+1)/32 == nucleotideBinarySlot.length -1
                            nucleotideBinary = 0L;

                            // reverse complement

                        }
                    }else{
                        // the last block, which is shorter than 31 mer
                        Long transitBit1 = nucleotideBinarySlot[param.kmerBinarySlotsAssemble-1] >>> 2*(param.kmerSizeResidueAssemble-1) ;  // 0000**----------  -> 000000000000**
                        // for the next block
                        Long transitBit2; // for the next block

                        // update the last block of kmer binary array
                        nucleotideBinarySlot[param.kmerBinarySlotsAssemble-1] <<= 2;    // 0000-------------  -> 00------------00
                        nucleotideBinarySlot[param.kmerBinarySlotsAssemble-1] |= nucleotideInt;  // 00------------00  -> 00------------**
                        nucleotideBinarySlot[param.kmerBinarySlotsAssemble-1] &= maxSubKmerAssembleBits; // 00------------**  -> 0000----------**

                        // the rest
                        for (int j = param.kmerBinarySlotsAssemble-2; j >=0; j--) {
                            transitBit2 = nucleotideBinarySlot[j] >>> (2*30);   // **---------------  -> 0000000000000**
                            nucleotideBinarySlot[j] <<=2;    // ---------------  -> --------------00
                            nucleotideBinarySlot[j] |= transitBit1;  // -------------00 -> -------------**
                            transitBit1= transitBit2;
                        }
                    }

                    // reach the first complete K-mer
                    if (i >= param.kmerSize - 1) {

                        if (param.kmerSizeResidueAssemble == 1) {
                            prefixBinarySlot = new long[param.subKmerBinarySlots];

                            suffixBinary= nucleotideBinarySlot[param.kmerBinarySlotsAssemble - 1];
                            suffixBinary|=4L; // add C marker
                            for (int j = 0; j < param.subKmerBinarySlots; j++) {
                                prefixBinarySlot[j] = nucleotideBinarySlot[j];
                            }
                        } else {
                            prefixBinarySlot = new long[param.subKmerBinarySlots];

                            suffixBinary = nucleotideBinarySlot[param.kmerBinarySlotsAssemble - 1] & 3L;
                            suffixBinary|=4L; // add C marker
                            for (int j = 0; j < param.subKmerBinarySlots - 1; j++) {
                                prefixBinarySlot[j] = nucleotideBinarySlot[j];
                            }
                            prefixBinarySlot[param.kmerBinarySlotsAssemble - 1] = nucleotideBinarySlot[param.kmerBinarySlotsAssemble - 1] >>> 2;
                        }

                        /*
                        long[] nucleotideBinarySlotPreRow = new long[param.kmerBinarySlots];
                        for (int j = 0; j < nucleotideBinarySlot.length; j++) {
                            nucleotideBinarySlotPreRow[j] = nucleotideBinarySlot[j];
                        }
                        */
                        if (i==param.kmerSize-1){
                            kmerList.add(RowFactory.create(prefixBinarySlot, 1, suffixBinary, S.getInt(3), -1));
                        }else if (i==readLength-1){
                            kmerList.add(RowFactory.create(prefixBinarySlot, 1, suffixBinary, -1, S.getInt(4)));
                        }else {
                            kmerList.add(RowFactory.create(prefixBinarySlot, 1, suffixBinary, -1, -1));  // the number does not matter, as the count is based on units
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

                //            System.out.println(read);

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

/*
                        for (int k=31; k>=0;k--){
                            long a = nucleotideBinarySlot[0] >>> 2*k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }

                       // System.out.println();

                        for (int k=31; k>=0;k--){
                            long a = nucleotideBinarySlot[1] >>> 2*k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                      //  System.out.println();

                        for (int k=31; k>=0;k--){
                            long a = nucleotideBinarySlot[2] >>> 2*k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }

                        for (int k=31; k>=0;k--){
                            long a = nucleotideBinarySlot[3] >>> 2*k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }

                        for (int k=31; k>=0;k--){
                            long a = nucleotideBinarySlot[4] >>> 2*k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }

                        for (int k=30; k>=0;k--){
                            long a = nucleotideBinarySlot[5] >>> 2*k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }


                        System.out.println();

                        for (int k = 31; k >= 0; k--) {
                            long a = nucleotideBinaryReverseComplementSlot[0] >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }
                     //   System.out.println();


                            for (int k = 31; k >= 0; k--) {
                                long a = nucleotideBinaryReverseComplementSlot[1] >>> 2 * k;
                                a &= 3L;
                                char b = BinaryToNucleotide(a);
                                System.out.print(b);
                            }
                  //          System.out.println();

                            for (int k = 31; k >= 0; k--) {
                                long a = nucleotideBinaryReverseComplementSlot[2] >>> 2 * k;
                                a &= 3L;
                                char b = BinaryToNucleotide(a);
                                System.out.print(b);
                            }

                        for (int k = 31; k >= 0; k--) {
                            long a = nucleotideBinaryReverseComplementSlot[3] >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }

                        for (int k = 31; k >= 0; k--) {
                            long a = nucleotideBinaryReverseComplementSlot[4] >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }

                        for (int k = 30; k >= 0; k--) {
                            long a = nucleotideBinaryReverseComplementSlot[5] >>> 2 * k;
                            a &= 3L;
                            char b = BinaryToNucleotide(a);
                            System.out.print(b);
                        }




                            System.out.println();
                            System.out.println();
                            */


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
