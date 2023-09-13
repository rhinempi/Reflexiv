package uni.bielefeld.cmg.reflexiv.pipeline;


import com.fing.mapreduce.FourMcTextInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.plans.logical.MapPartitions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.collection.Seq;
import uni.bielefeld.cmg.reflexiv.util.DefaultParam;
import uni.bielefeld.cmg.reflexiv.util.InfoDumper;

import java.io.IOException;
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
public class ReflexivDSDynamicMercyKmer implements Serializable {
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
        conf.set("spark.cleaner.referenceTracking.cleanCheckpoints", "true");
        conf.set("spark.checkpoint.compress", "true");
        conf.set("spark.hadoop.mapred.max.split.size", "6000000");
        conf.set("spark.sql.files.maxPartitionBytes", "6000000");
        conf.set("spark.sql.adaptive.coalescePartitions.parallelismFirst", "false");
        conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes","12000000");
        conf.set("spark.driver.maxResultSize","1000g");
        conf.set("spark.memory.fraction","0.7");
        conf.set("spark.network.timeout","60000s");
        conf.set("spark.executor.heartbeatInterval","20000s");

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
                .config("spark.sql.files.maxPartitionBytes", "6000000")
                .config("spark.sql.adaptive.coalescePartitions.parallelismFirst", false)
                .config("spark.sql.adaptive.advisoryPartitionSizeInBytes","12mb")
                .config("spark.driver.maxResultSize","1000g")
                .config("spark.memory.fraction","0.7")
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
    public void assemblyFromKmer() throws IOException {
        SparkSession spark = setSparkSessionConfiguration(param.shufflePartition);

        info.readMessage("Initiating Spark SQL context ...");
        info.screenDump();
        info.readMessage("Start Spark SQL framework");
        info.screenDump();

        SparkContext sc = spark.sparkContext();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc);

        sc.setCheckpointDir("/tmp/checkpoints");
        String checkpointDir= sc.getCheckpointDir().get();

        Dataset<Row> KmerCountDS;

        StructType kmerBinaryCountTupleLongStruct = new StructType();
        kmerBinaryCountTupleLongStruct = kmerBinaryCountTupleLongStruct.add("ID", DataTypes.LongType, false);
        kmerBinaryCountTupleLongStruct = kmerBinaryCountTupleLongStruct.add("kmer", DataTypes.createArrayType(DataTypes.LongType), false);
        ExpressionEncoder<Row> KmerBinaryCountLongEncoder = RowEncoder.apply(kmerBinaryCountTupleLongStruct);

        Dataset<Row> markerKmerRow;
        Dataset<Row> markerTupleRow;
        Dataset<Row> markerRangeRow;
        Dataset<Row> ContigDS;


        Dataset<String> FastqDS;
        JavaPairRDD<String, Long> FastqIndex;
        Dataset<Tuple2<String, Long>> FastqDSTuple;


        if (param.inputFormat.equals("4mc")){
            Configuration baseConfiguration = new Configuration();

            Job jobConf = Job.getInstance(baseConfiguration);

            JavaPairRDD<LongWritable, Text> FastqPairRDD = jsc.newAPIHadoopFile(param.inputFqPath, FourMcTextInputFormat.class, LongWritable.class, Text.class, jobConf.getConfiguration());

            if (param.partitions > 0) {
                FastqPairRDD = FastqPairRDD.repartition(param.partitions);
            }

            DSInputTupleToString tupleToString = new DSInputTupleToString();

            JavaRDD<String> FastqRDD = FastqPairRDD.mapPartitions(tupleToString);

            FastqDS = spark.createDataset(FastqRDD.rdd(), Encoders.STRING());
        }else {
            FastqDS = spark.read().text(param.inputFqPath).as(Encoders.STRING());

            if (param.partitions > 0) {
                FastqDS = FastqDS.repartition(param.partitions);
            }

            if (!param.inputFormat.equals("line")) {
                DSFastqFilterOnlySeq DSFastqFilterToSeq = new DSFastqFilterOnlySeq(); // for reflexiv
                FastqDS = FastqDS.mapPartitions(DSFastqFilterToSeq, Encoders.STRING());
            }

        }


        if (param.cache) {
            FastqDS.cache();
        }


        FastqIndex = FastqDS.toJavaRDD().zipWithIndex();

        FastqDSTuple = spark.createDataset(FastqIndex.rdd(), Encoders.tuple(Encoders.STRING(), Encoders.LONG()));

        FastqDSTuple.persist(StorageLevel.DISK_ONLY());

        ReverseComplementKmerBinaryExtractionFromDataset DSExtractRCKmerBinaryFromFastq = new ReverseComplementKmerBinaryExtractionFromDataset();

        Dataset<Row> ReadSeedDS;
        Dataset<Row> ContigSeedDS;
        StructType ReadAndContigSeedStruct = new StructType();
        ReadAndContigSeedStruct = ReadAndContigSeedStruct.add("seed", DataTypes.createArrayType(DataTypes.LongType), false);
        ReadAndContigSeedStruct = ReadAndContigSeedStruct.add("ID", DataTypes.LongType, false);
        ReadAndContigSeedStruct = ReadAndContigSeedStruct.add("index", DataTypes.LongType, false);
        ExpressionEncoder<Row> ReadAndContigSeedEncoder = RowEncoder.apply(ReadAndContigSeedStruct);

        ReadSeedDS = FastqDSTuple.mapPartitions(DSExtractRCKmerBinaryFromFastq, ReadAndContigSeedEncoder);


        /**
         * loading Kmer counts
         */
        KmerCountDS = spark.read().csv(param.inputKmerPath);

        if (param.partitions > 0) {
            KmerCountDS = KmerCountDS.repartition(param.partitions);
        }

        DynamicKmerBinarizerFromReducedToSubKmer ReducedKmerToSubKmer= new DynamicKmerBinarizerFromReducedToSubKmer();
        markerKmerRow = KmerCountDS.mapPartitions(ReducedKmerToSubKmer, ReadAndContigSeedEncoder);
       // markerTupleRow.persist(StorageLevel.DISK_ONLY());

        markerKmerRow = markerKmerRow.union(ReadSeedDS);

        markerKmerRow = markerKmerRow.sort("seed");


        StructType ReadAndIndexStruct = new StructType();
        ReadAndIndexStruct = ReadAndIndexStruct.add("ID", DataTypes.LongType, false);
        ReadAndIndexStruct = ReadAndIndexStruct.add("index", DataTypes.LongType, false);
        ExpressionEncoder<Row> ReadAndIndexEncoder = RowEncoder.apply(ReadAndIndexStruct);

        RamenReadExtraction extractRamen = new RamenReadExtraction();
        markerTupleRow = markerKmerRow.mapPartitions(extractRamen, ReadAndIndexEncoder);

        markerTupleRow = markerTupleRow.sort("ID");

        RamenReadRangeCal CalculateReadRange = new RamenReadRangeCal ();

        StructType ReadAndRangeStruct = new StructType();
        ReadAndRangeStruct = ReadAndRangeStruct.add("ID", DataTypes.LongType, false);
        ReadAndRangeStruct = ReadAndRangeStruct.add("ranges", DataTypes.createArrayType(DataTypes.LongType), false);
        ExpressionEncoder<Row> ReadAndRangeEncoder = RowEncoder.apply(ReadAndRangeStruct);
        markerRangeRow = markerTupleRow.mapPartitions(CalculateReadRange, ReadAndRangeEncoder);

        Dataset<Row> FastqIndexedDS;
        FastqTuple2Dataset FastqTupleChange = new FastqTuple2Dataset();
        FastqIndexedDS = FastqDSTuple.mapPartitions(FastqTupleChange,ReadAndRangeEncoder);

        FastqIndexedDS = FastqIndexedDS.union(markerRangeRow);

        FastqIndexedDS = FastqIndexedDS.sort("ID");

        ExtractMercyKmerFromRead mercyKmerExtraction = new ExtractMercyKmerFromRead();




        ContigKmerMarkerExtraction extractContigTails = new ContigKmerMarkerExtraction();
        ContigSeedDS = markerTupleRow.mapPartitions(extractContigTails, ReadAndContigSeedEncoder);

        ContigSeedDS = ContigSeedDS.union(ReadSeedDS);

        ContigSeedDS = ContigSeedDS.sort("seed");

        Dataset<Row> RACpairDS;
        StructType RACPairStruct = new StructType();
        RACPairStruct = RACPairStruct.add("read", DataTypes.LongType, false);
        RACPairStruct = RACPairStruct.add("contig", DataTypes.LongType, false);
        RACPairStruct = RACPairStruct.add("index", DataTypes.IntegerType, false);
        ExpressionEncoder<Row> RACPairEncoder = RowEncoder.apply(RACPairStruct);

        ReadAndContigPairs matchReadAndContig = new ReadAndContigPairs();
        RACpairDS = ContigSeedDS.mapPartitions(matchReadAndContig, RACPairEncoder);

        RACpairDS= RACpairDS.sort("read", "contig");

        Dataset<Row> CCPairDS;
        StructType CCPairStruct = new StructType();
        CCPairStruct = CCPairStruct.add("left", DataTypes.LongType, false);
        CCPairStruct = CCPairStruct.add("right", DataTypes.LongType, false);
        CCPairStruct = CCPairStruct.add("index", DataTypes.LongType, false);
        CCPairStruct = CCPairStruct.add("seq", DataTypes.LongType, false);
        ExpressionEncoder<Row> CCPairEncoder = RowEncoder.apply(CCPairStruct);

        CreatCCPairs matchContigToContig = new CreatCCPairs();
        CCPairDS = RACpairDS.mapPartitions(matchContigToContig, CCPairEncoder);

        CCPairDS = CCPairDS.sort("left", "right");

        StructType CCPairStructCount = new StructType();
        CCPairStructCount = CCPairStructCount.add("left", DataTypes.LongType, false);
        CCPairStructCount = CCPairStructCount.add("right", DataTypes.LongType, false);
        CCPairStructCount = CCPairStructCount.add("index", DataTypes.LongType, false);
        CCPairStructCount = CCPairStructCount.add("seq", DataTypes.LongType, false);
        CCPairStructCount = CCPairStructCount.add("count", DataTypes.LongType, false);
        ExpressionEncoder<Row> CCPairEncoderCount = RowEncoder.apply(CCPairStructCount);

        CCPairsToConnections filterForCCpair = new CCPairsToConnections();
        CCPairDS= CCPairDS.mapPartitions(filterForCCpair,CCPairEncoderCount);

        CCPairDS=CCPairDS.sort(col("right").asc(), col("count").desc());

        Dataset<Row> MarkedReads;
        StructType CCNetStruct = new StructType();
        CCNetStruct = CCNetStruct.add("read", DataTypes.LongType, false);
        CCNetStruct = CCNetStruct.add("CCMeta", DataTypes.createArrayType(DataTypes.LongType), false);
        ExpressionEncoder<Row> CCNetEncoder = RowEncoder.apply(CCNetStruct);

        CCPairsToConnectionsRight filterForCCpairRight = new CCPairsToConnectionsRight();
        MarkedReads = CCPairDS.mapPartitions(filterForCCpairRight, CCNetEncoder);

       // Dataset<Row> FastqIndexedDS;
       // FastqTuple2Dataset FastqTupleChange = new FastqTuple2Dataset();
       // FastqIndexedDS = FastqDSTuple.mapPartitions(FastqTupleChange,CCNetEncoder);

        MarkedReads = MarkedReads.union(FastqIndexedDS);

        MarkedReads = MarkedReads.sort("read");

        Dataset<Row> CCNetWithSeq;
        StructType ContigSeqStruct = new StructType();
        ContigSeqStruct = ContigSeqStruct.add("ID", DataTypes.LongType, false);
        ContigSeqStruct = ContigSeqStruct.add("seq", DataTypes.createArrayType(DataTypes.LongType), false);
        ExpressionEncoder<Row> ContigSeqEncoder = RowEncoder.apply(ContigSeqStruct);

        ExtractReadSequenceForCCNet ReadSeqExtraction = new ExtractReadSequenceForCCNet();
        CCNetWithSeq = MarkedReads.mapPartitions(ReadSeqExtraction, ContigSeqEncoder);

        CCNetWithSeq = CCNetWithSeq.union(markerTupleRow);
        CCNetWithSeq= CCNetWithSeq.sort("ID");

        Dataset<Row> reflexivKmer;
        StructType ReflexivLongKmerStructCompressed = new StructType();
        ReflexivLongKmerStructCompressed= ReflexivLongKmerStructCompressed.add("k-1", DataTypes.createArrayType(DataTypes.LongType), false);
        ReflexivLongKmerStructCompressed= ReflexivLongKmerStructCompressed.add("attribute", DataTypes.LongType, false);
        ReflexivLongKmerStructCompressed= ReflexivLongKmerStructCompressed.add("extension", DataTypes.createArrayType(DataTypes.LongType), false);
        ExpressionEncoder<Row> ReflexivLongSubKmerEncoderCompressed = RowEncoder.apply(ReflexivLongKmerStructCompressed);

        ChangingEndsOfConnectableContigs ModifyContig = new ChangingEndsOfConnectableContigs();
        reflexivKmer = CCNetWithSeq.mapPartitions(ModifyContig, ReflexivLongSubKmerEncoderCompressed);

        DSExtendConnectableContigLoop connectContig = new DSExtendConnectableContigLoop();

        reflexivKmer = reflexivKmer.sort("k-1");

        //loop
        for (int i=0; i<30; i++) {
            reflexivKmer = reflexivKmer.sort("k-1");
            reflexivKmer = reflexivKmer.mapPartitions(connectContig, ReflexivLongSubKmerEncoderCompressed);
        }


        Dataset<Row> reflexivFullKmer;
        StructType markerTupleStruct = new StructType();
        markerTupleStruct = markerTupleStruct.add("kmer", DataTypes.createArrayType(DataTypes.LongType), false);
        ExpressionEncoder<Row> markerTupleEncoder = RowEncoder.apply(markerTupleStruct);

        DSBinaryFixingKmerToFullKmer FixingKmer2FullKmer = new DSBinaryFixingKmerToFullKmer();
        reflexivFullKmer = reflexivKmer.mapPartitions(FixingKmer2FullKmer, markerTupleEncoder);

        reflexivFullKmer.persist(StorageLevel.DISK_ONLY());
        JavaPairRDD<Row, Long> ContigsRDDIndex;
        ContigsRDDIndex = reflexivFullKmer.toJavaRDD().zipWithIndex();

        Dataset<Tuple2<Row, Long>> markerTuple;
        markerTuple = spark.createDataset(ContigsRDDIndex.rdd(), Encoders.tuple(markerTupleEncoder, Encoders.LONG()));

        StructType ContigLongKmerStringStruct = new StructType();
        ContigLongKmerStringStruct = ContigLongKmerStringStruct.add("ID", DataTypes.StringType, false);
        ContigLongKmerStringStruct = ContigLongKmerStringStruct.add("contig", DataTypes.StringType, false);
        ExpressionEncoder<Row> ContigStringEncoder = RowEncoder.apply(ContigLongKmerStringStruct);

        TagRowContigRDDID DSContigIDLabel = new TagRowContigRDDID();
        ContigDS = markerTuple.flatMap(DSContigIDLabel, ContigStringEncoder);

        ContigDS.write().
                mode(SaveMode.Overwrite).
                format("csv").
                option("compression", "gzip").save(param.outputPath + "/Assembly_intermediate/04Patching");

        spark.stop();

    }

    class TagRowContigRDDID implements FlatMapFunction<Tuple2<Row, Long>, Row>, Serializable {

        List<Row> contigList;

        public Iterator<Row> call(Tuple2<Row, Long> s) {

            contigList = new ArrayList<Row>();

            String contig = BinaryBlocksToString(seq2array(s._1().getSeq(0)));
            int length = contig.length();
            if (length >= param.minContig) {
                String ID = ">Contig-" + length + "-" + s._2();
                String formatedContig = changeLine(contig, length, 10000000);
                contigList.add(RowFactory.create(ID, formatedContig));
            }

            return contigList.iterator();
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

    class DSFastqFilterOnlySeq implements MapPartitionsFunction<String, String>, Serializable{
        ArrayList<String> seqArray = new ArrayList<String>();
        //String line;
        //int lineMark = 0;

        public Iterator<String> call(Iterator<String> sIterator) {
            while (sIterator.hasNext()) {
                String s = sIterator.next();
                if (s.length()<= 20) {
                    continue;
                } else if (s.startsWith("@")) {
                    continue;
                } else if (s.startsWith("+")) {
                    continue;
                } else if (!checkSeq(s.charAt(0))) {
                    continue;
                } else if (!checkSeq(s.charAt(4))){
                    continue;
                } else if (!checkSeq(s.charAt(9))){
                    continue;
                } else if (!checkSeq(s.charAt(14))){
                    continue;
                } else if (!checkSeq(s.charAt(19))){
                    continue;
                } else {
                    seqArray.add(s);
                }
            }

            return seqArray.iterator();
        }

        private boolean checkSeq(char a){
            int match =0;
            if (a=='A'){
                match++;
            }else if (a=='T'){
                match++;
            }else if (a=='C'){
                match++;
            }else if (a=='G'){
                match++;
            }else if (a=='N'){
                match++;
            }

            if (match >0){
                return true;
            }else{
                return false;
            }
        }

        /*
        public Iterator<String> call(Iterator<String> sIterator) {
            while (sIterator.hasNext()) {
                String s = sIterator.next();
                if (lineMark == 2) {
                    lineMark++;
                } else if (lineMark == 3) {
                    lineMark++;
                    seqArray.add(line);
                } else if (s.startsWith("@")) {
                    lineMark = 1;
                } else if (lineMark == 1) {
                    line = s;
                    lineMark++;
                }
            }

            return seqArray.iterator();
        }
        */

/*
        public String call(String s) {
            if (lineMark == 2) {
                lineMark++;
                return null;
            } else if (lineMark == 3) {
                lineMark++;
                return line;
            } else if (s.startsWith("@")) {
                lineMark = 1;
                return null;
            } else if (lineMark == 1) {
                line = s;
                lineMark++;
                return null;
            }else{
                return null;
            }
        }
        */
    }

    class DSInputTupleToString implements FlatMapFunction<Iterator<Tuple2<LongWritable, Text>>, String>, Serializable {
        List<String> reflexivKmerStringList = new ArrayList<String>();
        String seq;

        public Iterator<String> call(Iterator<Tuple2<LongWritable, Text>> sIterator) throws Exception {
            while (sIterator.hasNext()) {

                Tuple2<LongWritable, Text> s = sIterator.next();
                seq = s._2().toString();
/*
                if (seq.length()<= 20) {
                    continue;
                } else if (seq.startsWith("@")) {
                    continue;
                } else if (seq.startsWith("+")) {
                    continue;
                } else if (!checkSeq(seq.charAt(0))) {
                    continue;
                } else if (!checkSeq(seq.charAt(4))){
                    continue;
                } else if (!checkSeq(seq.charAt(9))){
                    continue;
                } else if (!checkSeq(seq.charAt(14))){
                    continue;
                } else if (!checkSeq(seq.charAt(19))){
                    continue;
                } else {
                    reflexivKmerStringList.add(seq);
                }
*/
                reflexivKmerStringList.add(seq);
            }
            return reflexivKmerStringList.iterator();
        }

        private boolean checkSeq(char a){
            int match =0;
            if (a=='A'){
                match++;
            }else if (a=='T'){
                match++;
            }else if (a=='C'){
                match++;
            }else if (a=='G'){
                match++;
            }else if (a=='N'){
                match++;
            }

            if (match >0){
                return true;
            }else{
                return false;
            }
        }
    }

    class DynamicKmerBinarizerFromReducedToSubKmer implements MapPartitionsFunction<Row, Row>, Serializable{
        List<Row> kmerList = new ArrayList<Row>();
        Row units;
        String ID;
        String extension;
        int currentExtensionSize;
        int currentExtensionBlockSize;
        long attribute;
        char nucleotide;
        long nucleotideInt;
        //     Long suffixBinary;
        //     Long[] suffixBinaryArray;


        public Iterator<Row> call(Iterator<Row> s) {

            while (s.hasNext()) {
                units = s.next();

                // ID = units.getString(0);
                extension = units.getString(0);

                //if (ID.startsWith("(")) {
                //    ID = ID.substring(1);
                //}


                currentExtensionSize = extension.length();
                currentExtensionBlockSize = (currentExtensionSize-1)/31+1;

        //        if (!kmerSizeCheck(kmer, param.kmerListHash)){continue;} // the kmer length does not fit into any of the kmers in the list.
/*
                if (units.getString(0).endsWith(")")) {
                    String[] attributeStringArray = ID.split("\\-");
                    attribute =Long.parseLong(attributeStringArray[2]);
                    // attribute = Long.parseLong(StringUtils.chop(units.getString(1)));
                } else {
                    String[] attributeStringArray = ID.split("\\-");
                    attribute =Long.parseLong(attributeStringArray[2]);
                    // attribute = Long.parseLong(units.getString(1));
                }
*/

                long[] extensionBinarySlot = new long[currentExtensionBlockSize];

                for (int i = 0; i < currentExtensionSize; i++) {
                    nucleotide = extension.charAt(i);
                    if (nucleotide >= 256) nucleotide = 255;
                    nucleotideInt = nucleotideValue(nucleotide);
                    // forward kmer in bits
                    nucleotideInt <<= 2*(32-1-(i%31)); // shift to the left   [ATCGGATCC-,ATCGGATCC-]

                    extensionBinarySlot[i / 31] |= nucleotideInt;
                }

                long kmerEndMark = 1L;

                kmerEndMark <<= 2*(32-1-((currentExtensionSize-1)%31+1));
                extensionBinarySlot[currentExtensionBlockSize-1] |= kmerEndMark; // param.kmerListHash.get(currentKmerSize)] == currentKmerBlockSize

                // attribute= onlyChangeReflexivMarker(attribute,1);
                kmerList.add(
                        RowFactory.create(extensionBinarySlot, 0, -1) // -1 as marker for k-mer more than 2 coverage
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

        private long onlyChangeReflexivMarker(long oldMarker, int reflexivMarker){
            Long maxSubKmerBinary = ~((~0L) << 2 * 31);
            long newMarker = oldMarker & maxSubKmerBinary;
            newMarker |= ((long) reflexivMarker) << 2*(32-1);
            return newMarker;
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

    }

    class DSBinaryFixingKmerToFullKmer implements MapPartitionsFunction<Row, Row>, Serializable{
        List<Row> reflexivKmerStringList = new ArrayList<Row>();

        long[] subKmerArray = new long[1];
        long[] combinedArray;
        long[] extensionArray;


        public Iterator<Row> call(Iterator<Row> sIterator) throws Exception {
            while (sIterator.hasNext()) {
                Row s = sIterator.next();

                if (s.get(0) instanceof Seq){
                    subKmerArray = seq2array(s.getSeq(0));
                }else{
                    subKmerArray = (long[]) s.get(0);
                }

                if (s.get(2) instanceof  Seq) {
                    extensionArray = seq2array(s.getSeq(2));
                }else{
                    extensionArray = (long[]) s.get(2);
                }

                if (getReflexivMarker(s.getLong(1)) ==1){
                    combinedArray = combineTwoLongBlocks( subKmerArray, extensionArray);
                }else{
                    combinedArray = combineTwoLongBlocks( extensionArray, subKmerArray );
                }


                if (currentKmerSizeFromBinaryBlockArray(combinedArray) < param.minContig){
                    continue;
                }


                reflexivKmerStringList.add(
                        RowFactory.create(combinedArray)
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

        private int getReflexivMarker(long attribute){
            int reflexivMarker = (int) (attribute >>> 2*(32-1)); // 01-------- -> ---------01 reflexiv marker
            return reflexivMarker;
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
                    //  String rightShift= BinaryBlocksToString(rightBlocksLeftShiftedArray);
                    //  System.out.println("rightShift: " + rightShift);
                }

                // String mergedKmer= BinaryBlocksToString(newBlocks);

                //System.out.println(" left Blocks:" + leftBlocksString + " Right blocks: " + rightBlocksString + " rightLength: " + rightNucleotideLength + " leftNucleotideLength: " + leftNucleotideLength + " leftRelativeNTLength: " + leftRelativeNTLength + " leftVacancy: " + leftVacancy + " rightNucleotideLength: " + rightNucleotideLength + " combinedBlockSize: " + combinedBlockSize + " newBlock: " + mergedKmer);
            }

            return newBlocks;
        }
    }

    class ChangingEndsOfConnectableContigs implements MapPartitionsFunction<Row, Row>, Serializable{
        List<Row> ReflexibleKmerList = new ArrayList<Row>();
        List<long[]> tmpConsecutiveContigs = new ArrayList<long[]>();
        Row lastID=null;
        long attribute;

        public Iterator<Row> call(Iterator<Row> sIterator) throws Exception{
            while (sIterator.hasNext()){
                Row s = sIterator.next();

                if (s.getSeq(1).length()<4){
                    continue;
                }

                //   System.out.println("contigID: " + s.getLong(0));

                if (lastID==null){
                    lastID=s;
                    continue;
                }

                if (s.getLong(0) == lastID.getLong(0)){
                    System.out.println("added: " +  s);
                    tmpConsecutiveContigs.add(seq2array(s.getSeq(1)));
                }else{
                    attribute = buildingAlongFromThreeInt(1, -1, -1);
                    if (tmpConsecutiveContigs.size()>0){
                        tmpConsecutiveContigs.add(seq2array(lastID.getSeq(1)));
                        System.out.println("added: " +  lastID);
                        if (tmpConsecutiveContigs.size()>3){
                            for (int i=0;i<tmpConsecutiveContigs.size(); i++) {
                                System.out.println("id with more than 3 contigs 1: " +  BinaryBlocksToString(tmpConsecutiveContigs.get(i)));
                            }
                        }
                        long[] modifiedContig = updatingContig(tmpConsecutiveContigs);
                        ReflexibleKmerList.add(RowFactory.create(leftShiftOutFromArray(modifiedContig,61), attribute, leftShiftArray(modifiedContig, 61)));

                        tmpConsecutiveContigs=new ArrayList<long[]>();
                    }else { // lastID is alone and probably a normal contig
                        long[] contigArray = seq2array(lastID.getSeq(1));
                        long[] subKmer = leftShiftOutFromArray(contigArray, 61);
                        long[] extension = leftShiftArray(contigArray, 61);
                        ReflexibleKmerList.add(RowFactory.create(subKmer, attribute, extension));
                    }

                    lastID = s;
                }
            }

            attribute = buildingAlongFromThreeInt(1, -1, -1);
            if (tmpConsecutiveContigs.size()>0){
                tmpConsecutiveContigs.add(seq2array(lastID.getSeq(1)));
                if (tmpConsecutiveContigs.size()>3){
                    System.out.println("id with more than 3 contigs 2: " + lastID.getLong(0));
                }
                long[] modifiedContig = updatingContig(tmpConsecutiveContigs);
                ReflexibleKmerList.add(RowFactory.create(leftShiftOutFromArray(modifiedContig,61), attribute, leftShiftArray(modifiedContig, 61)));

                tmpConsecutiveContigs=new ArrayList<long[]>();
            }else { // lastID is alone and probably a normal contig
                long[] contigArray = seq2array(lastID.getSeq(1));
                long[] subKmer = leftShiftOutFromArray(contigArray, 61);
                long[] extension = leftShiftArray(contigArray, 61);
                ReflexibleKmerList.add(RowFactory.create(subKmer, attribute, extension));
            }

            return ReflexibleKmerList.iterator();
        }

        private long[] seq2array(Seq a){
            long[] array =new long[a.length()];
            for (int i = 0; i < a.length(); i++) {
                array[i] = (Long) a.apply(i);
            }
            return array;
        }

        private long[] updatingContig(List<long[]> contigSet) throws Exception {
            long[] newContig = null;
            long[] rightContig = null;
            long[] leftContig = null;

            if (contigSet.size()>3){ // left and right
                // interesting scenario, should not happen
                    System.out.println("debugging needed 1");
            }else if (contigSet.size()>2){

                if (contigSet.get(0)[contigSet.get(0).length-2]!=0){ // the contig
                    if (contigSet.get(1)[contigSet.get(1).length-2] !=0){
                        // another contig?
                        System.out.println("debugging needed 2");
                    }else if (contigSet.get(2)[contigSet.get(2).length-2] !=0){
                        // another contig?
                        System.out.println("debugging needed 3");
                    }
                    newContig = contigSet.get(0);

                    if (contigSet.get(1)[contigSet.get(1).length-1] <0){ // right contig
                        rightContig = contigSet.get(1);
                    }else if (contigSet.get(1)[contigSet.get(1).length-1]>0){ // left contig
                        leftContig = contigSet.get(1);
                    }

                    if (contigSet.get(2)[contigSet.get(2).length-1] <0){ // right contig
                        rightContig = contigSet.get(2);
                    }else if (contigSet.get(2)[contigSet.get(2).length-1] >0){ // left contig
                        leftContig= contigSet.get(2);
                    }
                }else if (contigSet.get(1)[contigSet.get(1).length-1]!=0){ // second one is the contig
                    if (contigSet.get(0)[contigSet.get(0).length-2] !=0){
                        // another contig?
                        System.out.println("debugging needed 4");
                    }else if (contigSet.get(2)[contigSet.get(2).length-2] !=0){
                        // another contig?
                        System.out.println("debugging needed 5");
                    }
                    newContig=contigSet.get(1);

                    if (contigSet.get(0)[contigSet.get(0).length-1] <0){ // right contig
                        rightContig = contigSet.get(0);
                    }else if (contigSet.get(0)[contigSet.get(0).length-1]>0){ // left contig
                        leftContig = contigSet.get(0);
                    }

                    if (contigSet.get(2)[contigSet.get(2).length-1] <0){ // right contig
                        rightContig = contigSet.get(2);
                    }else if (contigSet.get(2)[contigSet.get(2).length-1] >0){ // left contig
                        leftContig= contigSet.get(2);
                    }

                }else { // the last one is the contig

                    if (contigSet.get(0)[contigSet.get(0).length-2] !=0){
                        // another contig?
                        System.out.println("debugging needed 6");
                    }else if (contigSet.get(1)[contigSet.get(1).length-2] !=0){
                        // another contig?
                        System.out.println("debugging needed 7");
                    }
                    newContig=contigSet.get(2);

                    if (contigSet.get(0)[contigSet.get(0).length-1] <0){ // right contig
                        rightContig = contigSet.get(0);
                    }else if (contigSet.get(0)[contigSet.get(0).length-1]>0){ // left contig
                        leftContig = contigSet.get(0);
                    }

                    if (contigSet.get(1)[contigSet.get(1).length-1] <0){ // right contig
                        rightContig = contigSet.get(1);
                    }else if (contigSet.get(1)[contigSet.get(1).length-1] >0){ // left contig
                        leftContig= contigSet.get(1);
                    }

                }
            }else { // if (contigSet.size()==2){
                if (contigSet.get(0)[contigSet.get(0).length-2]!=0) { // the contig
                    newContig=contigSet.get(0);
                    if (contigSet.get(1)[contigSet.get(1).length-1] <0){ // right contig
                        rightContig = contigSet.get(1);
                    }else if (contigSet.get(1)[contigSet.get(1).length-1]>0){ // left contig
                        leftContig = contigSet.get(1);
                    }
                }else{
                    newContig=contigSet.get(1);
                    if (contigSet.get(0)[contigSet.get(0).length-1] <0){ // right contig
                        rightContig = contigSet.get(0);
                    }else if (contigSet.get(0)[contigSet.get(0).length-1]>0){ // left contig
                        leftContig = contigSet.get(0);
                    }

                }
            }

           // System.out.println("oldContig: " + BinaryBlocksToString(newContig));

            if (leftContig!=null){
                int contigLength = currentKmerSizeFromBinaryBlockArray(newContig);
                if (leftContig[leftContig.length-1] < contigLength) {
                    newContig = leftShiftOutFromArray(newContig, (int)leftContig[leftContig.length-1]);
                    leftContig = removeTailingTwoSlots(leftContig);

                   // System.out.println("leftRead: " + BinaryBlocksToString(leftContig));
                    newContig = combineTwoLongBlocks(newContig, leftContig); // get all bases from read and give to leftContig
                   // System.out.println("newContig: " + BinaryBlocksToString(newContig));
                }else{
                  //  System.out.println("leftContig Index: " + leftContig[leftContig.length-1] + " is bigger or equal to contig length: " + contigLength);
                }
            }

            if (rightContig!=null){
                if (rightContig[rightContig.length-2]!=0){
                    System.out.println("Warning: not a right Contig");
                }
                int negativeIndex = (int)rightContig[rightContig.length-1];

                if (negativeIndex>=0){
                    System.out.println("right contig should not have positive index: " + negativeIndex);
                }

                rightContig = removeTailingTwoSlots(rightContig);

               // System.out.println("rightRead: " + BinaryBlocksToString(rightContig));
                int readLength= currentKmerSizeFromBinaryBlockArray(rightContig);
                int offset = readLength + negativeIndex;

                newContig = leftShiftArray(newContig, offset);
                rightContig = leftShiftArray(rightContig, readLength-61); // shift out 31 nt for right contig
                newContig = combineTwoLongBlocks(rightContig,newContig);
              //  System.out.println("newContig: " + BinaryBlocksToString(newContig));
            }




            return newContig;
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

        private long[] removeTailingTwoSlots(long[] withTails){
            long[] withOutTails = new long[withTails.length-2];
            for (int i=0; i<withTails.length-2; i++){
                withOutTails[i]= withTails[i];
            }
            return withOutTails;
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
                    //  String rightShift= BinaryBlocksToString(rightBlocksLeftShiftedArray);
                    //  System.out.println("rightShift: " + rightShift);
                }

                // String mergedKmer= BinaryBlocksToString(newBlocks);

                //System.out.println(" left Blocks:" + leftBlocksString + " Right blocks: " + rightBlocksString + " rightLength: " + rightNucleotideLength + " leftNucleotideLength: " + leftNucleotideLength + " leftRelativeNTLength: " + leftRelativeNTLength + " leftVacancy: " + leftVacancy + " rightNucleotideLength: " + rightNucleotideLength + " combinedBlockSize: " + combinedBlockSize + " newBlock: " + mergedKmer);
            }

            return newBlocks;
        }
    }

    class ExtractReadSequenceForCCNet implements MapPartitionsFunction<Row, Row>, Serializable{
        List<Row> CCReads = new ArrayList<Row>();
        Row lastRead=null;
        Row lastMarker=null;
        long[] leftContigReadMeta;
        long[] rightContigReadMeta;

        public Iterator<Row> call(Iterator<Row> sIterator) throws Exception{
            while (sIterator.hasNext()){
                Row s= sIterator.next();

                if ((Long) s.getSeq(1).apply(0) == 0) { // a marker
                    if (lastRead!=null) {
                        if (lastRead.getLong(0) == s.getLong(0)) {
                         //   System.out.println("read: " + s.getLong(0) + " marker: " + s.getSeq(1).apply(0) + " index: " + getLeftIndex((Long) s.getSeq(1).apply(1)) + " | " + getRightIndex((Long) s.getSeq(1).apply(1)) + " leftContig: " + s.getSeq(1).apply(2) + " rightContig: " + s.getSeq(1).apply(3));


                            int leftIndex = getLeftIndex((Long) s.getSeq(1).apply(1));
                            int rightIndex = getRightIndex((Long) s.getSeq(1).apply(1));

                            long[] readSeq= seq2array(lastRead.getSeq(1));
                            if ((Long) s.getSeq(1).apply(4) ==1){
                                readSeq = binaryBlockReverseComplementary(readSeq);
                            }

                            // String read= BinaryBlocksToString (readSeq);

                            leftContigReadMeta = arrayWithTwoMoreSlots(readSeq);
                            leftContigReadMeta[leftContigReadMeta.length - 2] = 0L;
                            leftContigReadMeta[leftContigReadMeta.length - 1] = leftIndex;

                            rightContigReadMeta = arrayWithTwoMoreSlots(readSeq);
                            rightContigReadMeta[rightContigReadMeta.length - 2] = 0L;
                            rightContigReadMeta[rightContigReadMeta.length - 1] = rightIndex;

                            CCReads.add(RowFactory.create(s.getSeq(1).apply(2), leftContigReadMeta));   // contig, seqArray, 0L, index
                            CCReads.add(RowFactory.create(s.getSeq(1).apply(3), rightContigReadMeta));

                         //   System.out.println("modify: " + s.getSeq(1).apply(2) + " left index: " + leftContigReadMeta[leftContigReadMeta.length - 1] + " readID " + lastRead.getLong(0) + " seq " + read);
                         //   System.out.println("modify: " + s.getSeq(1).apply(3) + " right index: " + rightContigReadMeta[rightContigReadMeta.length - 1] + " readID " + lastRead.getLong(0) + " seq " + read );
                        }
                    }

                    lastMarker = s;
                }else{ // a read
                    if (lastMarker!=null) {

                        if (s.getLong(0) == lastMarker.getLong(0)) { // matches last marker
                         //   System.out.println("read: " + lastMarker.getLong(0) + " marker: " + lastMarker.getSeq(1).apply(0) + " index: " + getLeftIndex((Long) lastMarker.getSeq(1).apply(1)) + " | " + getRightIndex((Long) lastMarker.getSeq(1).apply(1)) + " leftContig: " + lastMarker.getSeq(1).apply(2) + " rightContig: " + lastMarker.getSeq(1).apply(3));


                            int leftIndex = getLeftIndex((Long) lastMarker.getSeq(1).apply(1));
                            int rightIndex = getRightIndex((Long) lastMarker.getSeq(1).apply(1));

                            long[] readSeq= seq2array(s.getSeq(1));
                            if ((Long) lastMarker.getSeq(1).apply(4) ==1){
                                readSeq = binaryBlockReverseComplementary(readSeq);
                            }

                            // String read= BinaryBlocksToString (readSeq);

                            leftContigReadMeta = arrayWithTwoMoreSlots(readSeq);
                            leftContigReadMeta[leftContigReadMeta.length - 2] = 0L;
                            leftContigReadMeta[leftContigReadMeta.length - 1] = leftIndex;

                            rightContigReadMeta = arrayWithTwoMoreSlots(readSeq);
                            rightContigReadMeta[rightContigReadMeta.length - 2] = 0L;
                            rightContigReadMeta[rightContigReadMeta.length - 1] = rightIndex;

                            CCReads.add(RowFactory.create(lastMarker.getSeq(1).apply(2), leftContigReadMeta));
                            CCReads.add(RowFactory.create(lastMarker.getSeq(1).apply(3), rightContigReadMeta));

                         //   System.out.println("modify: " + lastMarker.getSeq(1).apply(2) + " left index: " + leftContigReadMeta[leftContigReadMeta.length - 1] + " readID " + s.getLong(0) + " seq " + read);
                         //   System.out.println("modify: " + lastMarker.getSeq(1).apply(3) + " right index: " + rightContigReadMeta[rightContigReadMeta.length - 1] + " readID " + s.getLong(0) + " seq " + read);

                        }
                    }

                    lastRead=s;
                }


            }


            return CCReads.iterator();
        }

        private int getLeftIndex(long combinedDuo){
            return (int) (combinedDuo >>> 2*16);
        }

        private int getRightIndex(long combinedDuo){
            return (int) combinedDuo;
        }

        private long[] arrayWithTwoMoreSlots(long[] a){ // add two slots at the end for other meta data
            long[] array =new long[a.length+2];
            for (int i = 0; i < a.length; i++) {
                array[i] = (Long) a[i];
            }
            return array;
        }

        private long[] seq2arrayWithTwoMoreSlots(Seq a){ // add two slots at the end for other meta data
            long[] array =new long[a.length()+2];
            for (int i = 0; i < a.length(); i++) {
                array[i] = (Long) a.apply(i);
            }
            return array;
        }

        private long[] binaryBlockReverseComplementary(long[] forward){
            int currentKmerResidue = currentKmerResidueFromBlockArray(forward);
            int currentKmerSize = currentKmerSizeFromBinaryBlockArray(forward);
            int currentKmerBlockSize=forward.length;
            long[] reverseComplement;
            long lastTwoBits;

            reverseComplement = new long[currentKmerBlockSize];

            for (int i = 0; i < currentKmerSize; i++) {
                int RCindex = currentKmerSize - i - 1; //  ------------- ------------- ---------**-- RC index goes reverse
                //  ------------- ------------- -------**----  <--
                // reverseComplement[i / 31] <<= 2;

                if (RCindex >= currentKmerSize - currentKmerResidue) {
                    lastTwoBits = forward[RCindex / 31] >>> 2 * (32-(RCindex % 31)-1);    //  ------------- ------------- ------|----**
                    lastTwoBits &= 3L;
                    lastTwoBits ^= 3L;
                } else { // the same
                    lastTwoBits = forward[RCindex / 31] >>> 2 * (32 - (RCindex % 31) - 1);
                    lastTwoBits &= 3L;
                    lastTwoBits ^= 3L;
                }

                reverseComplement[i / 31] |= lastTwoBits;
                reverseComplement[i / 31] <<=2; // the order of these two lines are very important

            }
            reverseComplement[(currentKmerSize-1)/31] <<= 2*(32-currentKmerResidue-1); //  ---xxxxxxx -> xxxxxxx--- extra -1 because there are a vacancy from the step above
            reverseComplement[(currentKmerSize-1)/31]|=(1L<<2*(32-currentKmerResidue-1)); // adding ending marker C

            return reverseComplement;
        }

        private int currentKmerResidueFromBlockArray(long[] binaryBlocks){
            final int suffix0s = Long.numberOfTrailingZeros(binaryBlocks[binaryBlocks.length-1]);
            return Long.SIZE/2 - suffix0s/2 -1;
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

    class CCPairsToConnectionsRight implements MapPartitionsFunction<Row, Row>, Serializable{
        List<Row> CCNet = new ArrayList<Row>();
        int mark=0;
        Row lastHighest=null;
        Row secondHighest=null;

        public Iterator<Row> call(Iterator<Row> sIterator) throws Exception{
            while (sIterator.hasNext()){
                Row s = sIterator.next();

                /**
                 * C1 C2 index read
                 * C1 C3 index read
                 * C1 C4 index read
                 * C2 C5 index read
                 * C3 C6 index read
                 */

            //    System.out.println("leftContig: " + s.getLong(0) + " rightContig: " + s.getLong(1) +  " index: " + getLeftIndex(s.getLong(2)) + " | " + getRightIndex(s.getLong(2)) + " read " + s.getLong(3) + " count " + s.getLong(4) );

                if (lastHighest==null){
                    lastHighest=s;
                    mark=1;
                }

                if (s.getLong(1) == lastHighest.getLong(1)) {
                    if (mark==0) {
                        lastHighest = s;
                        mark++;
                    }else if (mark == 1){
                        secondHighest = s;
                        mark++;
                    }//else{
                      // other coverages are not important
                    //}

                }else{ // new left contig

                    if (lastHighest == null) {
                        lastHighest=s;
                    }else if (secondHighest==null){
                        long[] markerReadInfo = new long[5];
                        markerReadInfo[0] =0L;
                        markerReadInfo[1] = lastHighest.getLong(2);
                        markerReadInfo[2] = lastHighest.getLong(0);
                        markerReadInfo[3] = lastHighest.getLong(1);
                        markerReadInfo[4] = 0;

                        long readID= lastHighest.getLong(3);
                        if (lastHighest.getLong(3)<0){
                            readID = -lastHighest.getLong(3);
                            markerReadInfo[4] = 1;
                        }

                        CCNet.add(RowFactory.create(readID, markerReadInfo));
               //         System.out.println("leftContig Uniq: " + readID+ " index " + getLeftIndex(markerReadInfo[1]) + " | " + getRightIndex(markerReadInfo[1]) + " leftContig " + markerReadInfo[2] + " rightContig " + markerReadInfo[3] + " reverse or not " + markerReadInfo[4]);
                    }else{ // two different left contig

                        if (secondHighest.getLong(4) <=4 && lastHighest.getLong(4) / secondHighest.getLong(4) >=3) {

                            long[] markerReadInfo = new long[5];
                            markerReadInfo[0] = 0L;
                            markerReadInfo[1] = lastHighest.getLong(2);
                            markerReadInfo[2] = lastHighest.getLong(0);
                            markerReadInfo[3] = lastHighest.getLong(1);
                            markerReadInfo[4] = 0;

                            long readID=lastHighest.getLong(3);
                            if (lastHighest.getLong(3)<0){
                                readID = -lastHighest.getLong(3);
                                markerReadInfo[4] = 1;
                            }

                            CCNet.add(RowFactory.create(readID, markerReadInfo));
                  //          System.out.println("leftContig Uniq: " + readID+ " index " + getLeftIndex(markerReadInfo[1]) + " | " + getRightIndex(markerReadInfo[1]) + " leftContig " + markerReadInfo[2] + " rightContig " + markerReadInfo[3] + " reverse or not " + markerReadInfo[4]);
                        }
                    }

                    lastHighest=s;
                    mark=1;
                    secondHighest=null;

                }

            }

            if (lastHighest == null) {
                // last right contig does not have a match
            }else if (secondHighest==null){
                long[] markerReadInfo = new long[5];
                markerReadInfo[0] =0L;
                markerReadInfo[1] = lastHighest.getLong(2);
                markerReadInfo[2] = lastHighest.getLong(0);
                markerReadInfo[3] = lastHighest.getLong(1);
                markerReadInfo[4] = 0;

                long readID= lastHighest.getLong(3);
                if (lastHighest.getLong(3)<0){
                    readID = -lastHighest.getLong(3);
                    markerReadInfo[4] = 1;
                }

                CCNet.add(RowFactory.create(readID, markerReadInfo));

          //      System.out.println("leftContig Uniq: " + readID + " index " + getLeftIndex(markerReadInfo[1]) + " | " + getRightIndex(markerReadInfo[1]) + " leftContig " + markerReadInfo[2] + " rightContig " + markerReadInfo[3] + " reverse or not " + markerReadInfo[4]);
            }else{ // two different left contig

                if (secondHighest.getLong(4) <=4 && lastHighest.getLong(4) / secondHighest.getLong(4) >=3) {

                    long[] markerReadInfo = new long[5];
                    markerReadInfo[0] = 0L;
                    markerReadInfo[1] = lastHighest.getLong(2);
                    markerReadInfo[2] = lastHighest.getLong(0);
                    markerReadInfo[3] = lastHighest.getLong(1);
                    markerReadInfo[4] = 0;

                    long readID= lastHighest.getLong(3);
                    if (lastHighest.getLong(3)<0){
                        readID = -lastHighest.getLong(3);
                        markerReadInfo[4] = 1;
                    }

                    CCNet.add(RowFactory.create(readID, markerReadInfo));
            //        System.out.println("leftContig Uniq: " + readID + " index " + getLeftIndex(markerReadInfo[1]) + " | " + getRightIndex(markerReadInfo[1]) + " leftContig " + markerReadInfo[2] + " rightContig " + markerReadInfo[3] + " reverse or not " + markerReadInfo[4]);
                }
            }

            return CCNet.iterator();
        }

        private int getLeftIndex(long combinedDuo){
            return (int) (combinedDuo >>> 2*16);
        }

        private int getRightIndex(long combinedDuo){
            return (int) combinedDuo;
        }
    }

    class CCPairsToConnections implements MapPartitionsFunction<Row, Row>, Serializable{
        List<Row> CCNet = new ArrayList<Row>();
        long lastLeftContig=-1;
        long lastRightTarget=-1;
        long lastIndex=0;
        long lastRead=0;
        int lastRightCount=1;
        long[] targetAndCount;
        List<long[]> rightTargetAndCount  = new ArrayList<long[]>();

        public Iterator<Row> call(Iterator<Row> sIterator) throws Exception{
            while (sIterator.hasNext()){
                Row s = sIterator.next();

                /**
                 * C1 C2 index read
                 * C1 C3 index read
                 * C1 C4 index read
                 * C2 C5 index read
                 * C3 C6 index read
                 */

            //    System.out.println("leftContig: " + s.getLong(0) + " rightContig: " + s.getLong(1) + " index: " + getLeftIndex(s.getLong(2)) + " | " + getRightIndex(s.getLong(2)) + " read " + s.getLong(3));

                if (lastLeftContig == -1){
                    lastLeftContig=s.getLong(0);
                    lastRightCount=1;
                    lastRightTarget=s.getLong(1);
                    lastIndex=s.getLong(2);
                    lastRead=s.getLong(3);
                    continue;
                }

                if (s.getLong(0) == lastLeftContig) {
                    if (s.getLong(1) == lastRightTarget) {
                        lastRightCount++;
                    } else {
                        if (lastRightCount>=2) {
                            targetAndCount = new long[5];
                            targetAndCount[0] = lastRightTarget;
                            targetAndCount[1] = lastRightCount;
                            targetAndCount[2] = lastIndex;
                            targetAndCount[3] = lastRead;
                            targetAndCount[4] = lastLeftContig;
                            rightTargetAndCount.add(targetAndCount);
                        }
                        //rightTargetCounts.add();
                        lastRightTarget = s.getLong(1);
                        lastIndex= s.getLong(2);
                        lastRead=s.getLong(3);
                        lastRightCount = 1;
                    }
                }else{ // new left contig

                    if (lastRightCount>=2) {
                        targetAndCount = new long[5];
                        targetAndCount[0] = lastRightTarget;
                        targetAndCount[1] = lastRightCount;
                        targetAndCount[2] = lastIndex;
                        targetAndCount[3] = lastRead;
                        targetAndCount[4] = lastLeftContig;
                        rightTargetAndCount.add(targetAndCount);
                    }
                   // lastRightTarget = s.getLong(1);
                   // lastRightCount = 1;

                    if (rightTargetAndCount.size()>1) {
                        Collections.sort(rightTargetAndCount, new Comparator<long[]>() {
                            @Override
                            public int compare(long[] o1, long[] o2) {
                                return o1[1] < o2[1] ? 1 : o1[1] == o2[1] ? 0 : -1;  // descending
                                ///    return 0;
                            }
                        });

                        long secondHighest = rightTargetAndCount.get(1)[1];
                        if (secondHighest <= 4) {
                            if (rightTargetAndCount.get(0)[1] / secondHighest >= 3) {

                                CCNet.add(RowFactory.create(rightTargetAndCount.get(0)[4], rightTargetAndCount.get(0)[0],rightTargetAndCount.get(0)[2],rightTargetAndCount.get(0)[3],  rightTargetAndCount.get(0)[1]));
                               // CCNet.add(RowFactory.create(rightTargetAndCount.get(0)[3], rightTargetAndCount.get(0)[1], markerReadInfo));

          //                      System.out.println("lastRead: " + rightTargetAndCount.get(0)[3] + " count " + rightTargetAndCount.get(0)[1] + " index " + getLeftIndex(rightTargetAndCount.get(0)[2]) + " | " + getRightIndex(rightTargetAndCount.get(0)[2]) + " leftContig " + lastLeftContig + " rightContig " + rightTargetAndCount.get(0)[0]);
                            }
                        }
                    }else if (rightTargetAndCount.size()==1){

                        CCNet.add(RowFactory.create(rightTargetAndCount.get(0)[4], rightTargetAndCount.get(0)[0], rightTargetAndCount.get(0)[2],rightTargetAndCount.get(0)[3],  rightTargetAndCount.get(0)[1]));
                        // CCNet.add(RowFactory.create(rightTargetAndCount.get(0)[3], rightTargetAndCount.get(0)[1], markerReadInfo));
         //               System.out.println("lastRead: " + rightTargetAndCount.get(0)[3] + " count " + rightTargetAndCount.get(0)[1] + " index " + getLeftIndex(rightTargetAndCount.get(0)[2]) + " | " + getRightIndex(rightTargetAndCount.get(0)[2])+ " leftContig " + lastLeftContig + " rightContig " + rightTargetAndCount.get(0)[0]);
                    }


                    rightTargetAndCount=new ArrayList<long[]>();
                    lastRightCount=1;
                    lastRightTarget = s.getLong(1);
                    lastLeftContig=s.getLong(0);
                    lastIndex= s.getLong(2);
                    lastRead= s.getLong(3);
                }

            }

            if (lastRightCount>=2) {
                targetAndCount = new long[5];
                targetAndCount[0] = lastRightTarget;
                targetAndCount[1] = lastRightCount;
                targetAndCount[2] = lastIndex;
                targetAndCount[3] = lastRead;
                targetAndCount[4] = lastLeftContig;
                rightTargetAndCount.add(targetAndCount);
             }

            //lastRightTarget = s.getLong(1);
            // lastRightCount = 1;

            if (rightTargetAndCount.size()>1) {
                Collections.sort(rightTargetAndCount, new Comparator<long[]>() {
                    @Override
                    public int compare(long[] o1, long[] o2) {
                        return o1[1] < o2[1] ? 1 : o1[1] == o2[1] ? 0 : -1; // descending
                        ///    return 0;
                    }
                });

                long secondHighest = rightTargetAndCount.get(1)[1];
                if (secondHighest <= 4) {
                    if (rightTargetAndCount.get(0)[1] / secondHighest >= 3) {

                        CCNet.add(RowFactory.create(rightTargetAndCount.get(0)[4], rightTargetAndCount.get(0)[0],rightTargetAndCount.get(0)[2],rightTargetAndCount.get(0)[3],  rightTargetAndCount.get(0)[1]));
                        // CCNet.add(RowFactory.create(rightTargetAndCount.get(0)[3], rightTargetAndCount.get(0)[1], markerReadInfo));
          //              System.out.println("lastRead: " + rightTargetAndCount.get(0)[3] + " count " + rightTargetAndCount.get(0)[1] + " index " + getLeftIndex(rightTargetAndCount.get(0)[2]) + " | " + getRightIndex(rightTargetAndCount.get(0)[2])+ " leftContig " + rightTargetAndCount.get(0)[4] + " rightContig " + rightTargetAndCount.get(0)[0]);
                    }
                }
            }else if (rightTargetAndCount.size() ==1){

                CCNet.add(RowFactory.create(rightTargetAndCount.get(0)[4], rightTargetAndCount.get(0)[0], rightTargetAndCount.get(0)[2],rightTargetAndCount.get(0)[3],  rightTargetAndCount.get(0)[1]));
                // CCNet.add(RowFactory.create(rightTargetAndCount.get(0)[3], rightTargetAndCount.get(0)[1], markerReadInfo));
         //       System.out.println("lastRead: " + rightTargetAndCount.get(0)[3] + " count " + rightTargetAndCount.get(0)[1] + " index " + getLeftIndex(rightTargetAndCount.get(0)[2]) + " | " + getRightIndex(rightTargetAndCount.get(0)[2])+ " leftContig " + rightTargetAndCount.get(0)[4] + " rightContig " + rightTargetAndCount.get(0)[0]);
            }

            return CCNet.iterator();
        }

        private int getLeftIndex(long combinedDuo){
            return (int) (combinedDuo >>> 2*16);
        }

        private int getRightIndex(long combinedDuo){
            return (int) combinedDuo;
        }

        private boolean compareTwoTargets(long[] target1, long[] target2){
            return true;
        }
    }

    class CreatCCPairs implements MapPartitionsFunction<Row, Row>, Serializable{
        List<Row> CCPairs = new ArrayList<Row>();
        List<Long> leftContigList = new ArrayList<Long>();
        List<Integer> leftIndexList = new ArrayList<Integer>();
        List<Long> rightContigList = new ArrayList<Long>();
        List<Integer> rightIndexList= new ArrayList<Integer>();
        List<Long> contigList = new ArrayList<Long>();
        List<Integer> indexList = new ArrayList<Integer>();
        long lastRead =0;
        long lastContig=0;

        public Iterator<Row> call(Iterator<Row> sIterator) throws Exception{
            while (sIterator.hasNext()) {
                Row s = sIterator.next();


              //  System.out.println("Read: " + s.getLong(0) + " contig: " + s.getLong(1) + " index: " + s.getInt(2));
                // R1  C1  index
                // R1  C1  index
                // R1  C2  index
                if (s.getLong(0) == lastRead){
                    if (s.getLong(1) == lastContig){
                        indexList.add(s.getInt(2));
                    }else {
                        int finalIndex;
                            finalIndex = highestFrequency(indexList);


                        if (finalIndex> -100000000) {
                            if (finalIndex < 0) { //right contig
                                rightContigList.add(lastContig);
                                rightIndexList.add(finalIndex);
                            } else if (finalIndex > param.maxKmerSize) { // left contig
                                leftContigList.add(lastContig);
                                leftIndexList.add(finalIndex);
                            }
                        }


                        lastContig = s.getLong(1);
                        indexList = new ArrayList<Integer>();
                        indexList.add(s.getInt(2));
                    }


                    // R1 C1 index
                    // R2 C2 index
                }else{

                    // unload the last read and contig
                    if (indexList.size()>0){
                        int finalIndex;

                            finalIndex = highestFrequency(indexList);


                        if (finalIndex> -100000000) {
                            if (finalIndex < 0) { //right contig
                                rightContigList.add(lastContig);
                                rightIndexList.add(finalIndex);
                            } else if (finalIndex > param.maxKmerSize) { // left contig
                                leftContigList.add(lastContig);
                                leftIndexList.add(finalIndex);
                            }
                        }

                        lastContig = s.getLong(1);
                        indexList = new ArrayList<Integer>();
                        indexList.add(s.getInt(2));
                    }else{
                        indexList.add(s.getInt(2));
                    }

                    // building contig contig pairs
                    for (int i =0 ;i<leftContigList.size();i++){
                        for (int j=0; j <rightContigList.size(); j++){
                            long indexDuo = combineTwoInt(leftIndexList.get(i), rightIndexList.get(j));
                            if (!leftContigList.get(i).equals(rightContigList.get(j))) {
                                CCPairs.add(RowFactory.create(leftContigList.get(i), rightContigList.get(j), indexDuo, lastRead));
                            }
                        //     System.out.println("left Contig: " + leftContigList.get(i) + " right Contig: " + rightContigList.get(j) + " index " + getLeftIndex(indexDuo) + " | " + getRightIndex(indexDuo) + " last read " + lastRead );
                        }
                    }

                    lastRead = s.getLong(0);
                    lastContig = s.getLong(1);
                    leftContigList = new ArrayList<Long>();
                    leftIndexList= new ArrayList<Integer>();
                    rightContigList = new ArrayList<Long>();
                    rightIndexList= new ArrayList<Integer>();
                }
            }

            // unload the last read and contig
            if (indexList.size()>0){
                int finalIndex;

                    finalIndex = highestFrequency(indexList);

                    // finalIndex = indexList.get(0);


                if (finalIndex> -100000000) {
                    if (finalIndex < 0) { //right contig
                        rightContigList.add(lastContig);
                        rightIndexList.add(finalIndex);
                    } else if (finalIndex > param.maxKmerSize) { // left contig
                        leftContigList.add(lastContig);
                        leftIndexList.add(finalIndex);
                    }
                }

                // last element, no need to reset
                // lastContig = s.getLong(1);
                //indexList = null;
                // indexList.add(s.getInt(2));
            }else{
                // indexList.add(s.getInt(2));
            }

            for (int i =0 ;i<leftContigList.size();i++){
                for (int j=0; j <rightContigList.size(); j++){
                    long indexDuo = combineTwoInt(leftIndexList.get(i), rightIndexList.get(j));
                    if (!leftContigList.get(i).equals(rightContigList.get(j))) {
                        CCPairs.add(RowFactory.create(leftContigList.get(i), rightContigList.get(j), indexDuo, lastRead));
                    }
            //        System.out.println("left Contig: " + leftContigList.get(i) + " right Contig: " + rightContigList.get(j) + " index " + getLeftIndex(indexDuo) + " | " + getRightIndex(indexDuo) + " last read " + lastRead );
                }
            }

            return CCPairs.iterator();
        }

        private long combineTwoInt (int leftIndex, int rightIndex){
            long ForCast = (long) rightIndex ;
            ForCast <<= 2*16;
            ForCast >>>= 2*16;

            return ((long)leftIndex) <<2*16 | ForCast ;
        }

        private int highestFrequency (List<Integer> numberlist){

            if (numberlist.size()==1){
                return numberlist.get(0);
            }

            Collections.sort(numberlist);

            int finalIndex=numberlist.get(0);
            int lastIndex=numberlist.get(0);
            int frequency =1;
            int highestFrequency=1;
            for (int i=1;i < numberlist.size(); i++){

    //            System.out.println("test highestFrequency: " + numberlist.get(i));

                if (numberlist.get(i) == lastIndex){
                    frequency++;
                    if (frequency > highestFrequency){
                        highestFrequency = frequency;

                        finalIndex= numberlist.get(i);
                    }
                }else{
                    lastIndex= numberlist.get(i);
                    frequency=1;
                }
            }

       //     if (highestFrequency<2){
       //         return -100000000;
       //     }

        //    System.out.println("highest: " + finalIndex + " frequency: " + highestFrequency);

            return finalIndex;
        }

        private int getLeftIndex(long combinedDuo){
            return (int) (combinedDuo >>> 2*16);
        }

        private int getRightIndex(long combinedDuo){
            return (int) combinedDuo;
        }
    }

    class ReadAndContigPairs implements MapPartitionsFunction<Row, Row>, Serializable{
        List<Row> RACpairs = new ArrayList<Row>();
        int lastSeed=0;
        int seedKmerSize = 15;
        List<Long> readList = new ArrayList<Long>();
        List<Integer> indexList = new ArrayList<Integer>();
        List<Long> contigList = new ArrayList<Long>();
        List<Integer> contigIndexList = new ArrayList<Integer>();

        boolean emptyContig =false;


        public Iterator<Row> call(Iterator<Row> sIterator) throws Exception {
            while (sIterator.hasNext()) {
                Row s = sIterator.next();

                int seed = getSeedIntFromLong(s.getLong(0));
                int index = getIndexIntFromLong(s.getLong(0));
                int ROC = getROCIntFromLong(s.getLong(0));

     //           System.out.println("seed: " + BinaryLongToString(s.getLong(0)) + " seedValue" + seed + " index " + index + " Mark " + ROC);
                if (seed == lastSeed){ // they same k-mer seed
                    if (emptyContig){
                        continue;
                    }

                    if (ROC ==0){ // contig
                        contigList.add(s.getLong(1));
                        contigIndexList.add(index);
                    }else { // read
                        for (int i = 0; i < contigList.size(); i++) { // iterate all contigs

                            int relativeIndex = contigIndexList.get(i) - index; // read's relative index on the contig
                            RACpairs.add(RowFactory.create(s.getLong(1), contigList.get(i), relativeIndex));

     //                       System.out.println("Read: " + s.getLong(1) + " contigID: " + contigList.get(i) + " index " + relativeIndex);

                            //    ---xxxxx-------------------- contig
                            //-------xxxxx----- read
                            //| reads relative location on the contig (negative value)
                            // if (contigIndexList.get(i) < param.maxKmerSize - seedKmerSize) {
                                // left
                            //    relativeIndex = contigIndexList.get(i) - index;
                            //} else {
                            //    ------------------xxxxx--- contig
                            //                 -----xxxxx-------- read
                            //                 |reads relative location
                                // right
                            //    relativeIndex = contigIndexList.get(i) - index;
                            // }
                        }
                    }

                }else{ // a new k-mer seed

                    if (ROC ==1){ // this k-mer only exists in reads not contigs
                        emptyContig = true;
                    }else{
                        emptyContig = false;

                        contigList=new ArrayList<Long>();
                        contigIndexList=new ArrayList<Integer>();
                        contigList.add(s.getLong(1));
                        contigIndexList.add(index);

                        lastSeed = seed;
                    }
                }

                lastSeed = seed;

            }

            return RACpairs.iterator();
        }

        private String BinaryLongToString (long binaryBlocks){ // this one has been modified for k-mer 15
            //           String KmerString="";
            // int KmerLength = currentKmerSizeFromBinaryBlockArray(binaryBlocks);
            int KmerLength = 15;
            StringBuilder sb= new StringBuilder();
            char currentNucleotide;

            for (int i=0; i< KmerLength; i++){
                Long currentNucleotideBinary = binaryBlocks>>> 2 * (32 - (i%31+1));
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

        private int getSeedIntFromLong(long compress) {
            // xxxxxxxxC|R--index
            // xxxxxxxxC
            compress >>>= 2*16;
            return (int) compress;
        }

        private int getIndexIntFromLong(long compress){
            // xxxxxxxxC|R--index
            // index
            long indexMaxBit = ~((~0L) <<2*15);
            compress &=indexMaxBit;
            return (int) compress;
        }

        private int getROCIntFromLong(long compress){
            // xxxxxxxxC|R--index
            // R

            return (int)compress >>> 2*15;
        }

    }

    class ExtractMercyKmerFromRead implements MapPartitionsFunction<Row, Row>, Serializable{
        List<Row> MercyKmer = new ArrayList<>();

        Row lastRead = null;
        Row lastMarker = null;
        List<Row> lastMarkerArray = new ArrayList<Row>();
        long[] seqArray;

        public Iterator<Row> call(Iterator<Row> sIterator) throws Exception{

            // ------------ read
            // 0-1---   RC marker
            // 0+1---   marker
            // ------------ read
            while (sIterator.hasNext()){
                Row s = sIterator.next();

                seqArray = seq2array(s.getSeq(1));

                if (seqArray[0]== 0){ // a marker with ranges , might have two markers because of reverse complement
                    if (lastRead != null){
                        if (lastRead.getLong(0) == s.getLong(0)){
                            extractKmer(lastRead.getSeq(1), s.getSeq(1));
                        }else{
                            lastMarkerArray.add(s);
                        }
                    }else {
                        lastMarkerArray.add(s);
                    }
                }else{ // a read with sequence
                    for (int i=0; i<lastMarkerArray.size(); i++){
                        if (s.getLong(0) == lastMarkerArray.get(i).getLong(0)){
                            extractKmer(s.getSeq(1), lastMarkerArray.get(i).getSeq(1));
                        }
                    }

                    lastMarkerArray=new ArrayList<Row>();

                    lastRead = s;
                }

            }

            // the leftover of lastMarkerArray will not find a read anymore

            return MercyKmer.iterator();
        }

        private void extractKmer(Seq markerSeq, Seq readSeq) throws Exception {
            long[] markerArray = seq2array(markerSeq);
            long[] readArray = seq2array(readSeq);

            for (int i=1; i<markerArray.length; i++){
                int startIndex=getLeftMarker(markerArray[i]);
                int endIndex = getRightMarker(markerArray[i]);

                long[] kmer = leftShiftArray(readArray, startIndex);
                kmer = leftShiftOutFromArray(kmer, endIndex-startIndex+1);

                MercyKmer.add(RowFactory.create(kmer));
            }
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

    class RamenReadRangeCal implements MapPartitionsFunction<Row, Row>, Serializable{
        List<Row> ReadsAndRange = new ArrayList<Row>();

        Row lastKmer=null;

        long[] rangeArray = new long[2];

        List<Integer> indices = new ArrayList<Integer>();

        public Iterator<Row> call(Iterator<Row> sIterator) throws Exception{
            while (sIterator.hasNext()){
                Row s = sIterator.next();
                if (lastKmer == null){
                    lastKmer = s;
                    indices.add((int)s.getLong(1));
                }else{
                    if (s.getLong(0) == lastKmer.getLong(0)){
                        indices.add((int)s.getLong(1));
                    }else{
                        if (indices.size()>1) {
                            List<Long> ranges = findRange(indices, lastKmer.getLong(0));
                           // rangeArray[0] = 0;
                           // rangeArray[1] = range;


                            if (ranges.size() > 2){

                                if (ranges.get(1) <0){ // reverse complement
                                    ReadsAndRange.add(
                                            RowFactory.create(-lastKmer.getLong(0), ranges)
                                    );
                                }else {
                                    ReadsAndRange.add(
                                            RowFactory.create(lastKmer.getLong(0), ranges)
                                    );
                                }
                            }
                        }

                        indices = new ArrayList<Integer>();
                    }
                }
            }

            if (indices.size()>1){
                List<Long> ranges = findRange(indices, lastKmer.getLong(0));

                if (ranges.size() > 1){
                    if (ranges.get(1) <0){ // reverse complement , ranges.get(1) should equal lastKmer.getLong(0)
                        ReadsAndRange.add(
                                RowFactory.create(-lastKmer.getLong(0), ranges)
                        );
                    }else {
                        ReadsAndRange.add(
                                RowFactory.create(lastKmer.getLong(0), ranges)
                        );
                    }
                }
            }

            return ReadsAndRange.iterator();
        }

        private List<Long> findRange(List<Integer> i, long index){
            long range=0;
            long[] gapsArray;

            List<Long> gaps = new ArrayList<Long>();
            gaps.add(0L); // add an 0 in the front of the list
            gaps.add(index); // for reverse complement detection

            Collections.sort(i);
            int lastIndex = i.get(0);

            int a = 0;
            int b = 0;
            for (int j =1 ; j <i.size(); j++){
                if (i.get(j) - lastIndex >1){
                    a= lastIndex;
                    b = j;

                    range = buildingAlongFromThreeInt(1, a, b);
                    gaps.add(range);
                }
            }

            gapsArray = new long[gaps.size()];

            for (int k=0; k< gaps.size(); k++){

            }

            return gaps;
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

    class RamenReadExtraction implements MapPartitionsFunction<Row, Row>, Serializable{
        List<Row> ReadsAndIndices = new ArrayList<Row>();

        Row lastKmer=null;

        List<Row> ReadsKmerBuffer= new ArrayList<Row>();

        public Iterator<Row> call(Iterator<Row> sIterator) throws Exception{

            while (sIterator.hasNext()){
                Row s = sIterator.next();

                // ------- k-mer
                // ------- read
                // ------- read
                // ------- k-mer
                if (s.getLong(2) == -1){ // a more than 2x coverage k-mer
                    if (ReadsKmerBuffer.size()>0){

                        for (int i =0; i< ReadsKmerBuffer.size();i++){
                            if (dynamicSubKmerComparator(ReadsKmerBuffer.get(i).getSeq(0), s.getSeq(0)) == true){
                                ReadsAndIndices.add(
                                        RowFactory.create(ReadsKmerBuffer.get(i).getLong(1), ReadsKmerBuffer.get(i).getLong(2))
                                );
                            }
                        }

                        ReadsKmerBuffer = new ArrayList<Row>();
                    }

                    lastKmer = s;
                }else{ // a read k-mer
                    if (lastKmer !=null){
                        if (dynamicSubKmerComparator(lastKmer.getSeq(0), s.getSeq(0)) == true){
                            ReadsAndIndices.add(
                                    RowFactory.create(s.getLong(1), s.getLong(2))
                            );
                        }else {
                            // --------- read 1 k-mer
                            // --------- read 2 k-mer different
                            // -----
                            if (ReadsKmerBuffer.size()>0) {
                                if (dynamicSubKmerComparator(ReadsKmerBuffer.get(ReadsKmerBuffer.size() - 1).getSeq(0), s.getSeq(0)) == true) {
                                    ReadsKmerBuffer.add(s);
                                } else {
                                    ReadsKmerBuffer = new ArrayList<Row>();
                                    ReadsKmerBuffer.add(s);
                                }
                            }else{
                                ReadsKmerBuffer.add(s);
                            }
                        }
                    }else{
                        ReadsKmerBuffer.add(s);
                    }
                }

            }

            return ReadsAndIndices.iterator();
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
    }

    class ContigKmerMarkerExtraction implements MapPartitionsFunction<Row, Row>, Serializable{
        List<Row> SeedKmerList = new ArrayList<Row>();
        long[] fullKmerArray;
        long contigID;
        int kmerLength;
        int SeedKmerSize =15;
        long maxKmerBinary =(~0L) << 2 * (32-SeedKmerSize);

        public Iterator<Row> call(Iterator<Row> sIterator) throws Exception {
            while (sIterator.hasNext()) {
                Row s = sIterator.next();
                //fullKmerArray =  seq2array(s.getSeq(1));
                fullKmerArray= (long[]) s.get(1);
                contigID=s.getLong(0);
                kmerLength = currentKmerSizeFromBinaryBlockArray(fullKmerArray);

             //   System.out.println("contig: " + BinaryBlocksToString(fullKmerArray));

                if (kmerLength >=2*param.maxKmerSize){
                    long[] fixedKmerLeft;
                    long[] fixedKmerRight;

                    for (int i=0;i<param.maxKmerSize-SeedKmerSize+1; i++){
                        // xxxxxxxxxxx---------
                        // xxxx
                        //  xxxx
                        //   xxxx
                        fixedKmerLeft = leftShiftOutFromArray(fullKmerArray,i+SeedKmerSize);
                        fixedKmerLeft=leftShiftArray(fixedKmerLeft, i);
                        SeedKmerList.add(RowFactory.create(buildingAlongForCompression(fixedKmerLeft[0], i, 0),contigID));

                     //   System.out.println("seed: " + BinaryLongToString(fixedKmerLeft[0]) + " ID " + contigID + " index " + i);

                        // ---------xxxxxxxxxxx
                        //                 xxxx
                        //                xxxx
                        //               xxxx

                        fixedKmerRight= leftShiftArray(fullKmerArray, kmerLength-i-SeedKmerSize);
                        fixedKmerRight= leftShiftOutFromArray(fixedKmerRight, SeedKmerSize);
                        SeedKmerList.add(RowFactory.create(buildingAlongForCompression(fixedKmerRight[0], kmerLength-i-SeedKmerSize, 0),contigID));

                        int index = kmerLength-i-SeedKmerSize;
                     //   System.out.println("seed: " + BinaryLongToString(fixedKmerRight[0]) + " ID " + contigID + " index " + index);
                    }

                }

            }

            return SeedKmerList.iterator();
        }

        private String BinaryLongToString (long binaryBlocks){ // this one has been modified for k-mer 15
            //           String KmerString="";
            // int KmerLength = currentKmerSizeFromBinaryBlockArray(binaryBlocks);
            int KmerLength = 15;
            StringBuilder sb= new StringBuilder();
            char currentNucleotide;

            for (int i=0; i< KmerLength; i++){
                Long currentNucleotideBinary = binaryBlocks>>> 2 * (32 - (i%31+1));
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

        private long buildingAlongForCompression(long kmer, int index, int ROC){ // ROC read or contig
            // xxxxxxxxxC|R----index    assuming contig length smaller than 1G

            long ROCLong = (long) ROC << 2*15;
            kmer|= ROCLong;
            return  kmer|(long) index;
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
                if (getReflexivMarker(s.getLong(1)) == 1) {
                    String contig = s.getString(0) + s.getString(2);
                    int length = contig.length();
                    if (length >= param.minContig) {
                        String ID = ">Contig-" + length + "-" + getLeftMarker(s.getLong(1)) + "-" + getRightMarker(s.getLong(1));
                        String formatedContig = changeLine(contig, length, 10000000);
                        contigList.add(RowFactory.create(ID, formatedContig));
                    }
                } else { // (randomReflexivMarker == 2) {
                    String contig = s.getString(2) + s.getString(0);
                    int length = contig.length();
                    if (length >= param.minContig) {
                        String ID = ">Contig-" + length +  "-" + getLeftMarker(s.getLong(1)) + "-" + getRightMarker(s.getLong(1));
                        String formatedContig = changeLine(contig, length, 10000000);
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

    class DSExtendConnectableContigLoop implements MapPartitionsFunction<Row, Row>, Serializable {

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
                                //   System.out.println("loop array extend. first leftMarker: " + getLeftMarker(s.getLong(1)) + " rightMarker: " + getRightMarker(s.getLong(1)) + " second leftMarker: " + getLeftMarker(tmpReflexivKmerExtendList.get(i).getLong(1)) + " rightMarker: " + getRightMarker(tmpReflexivKmerExtendList.get(i).getLong(1)));
                                if (getReflexivMarker(s.getLong(1))== 1) {
                                    if (getReflexivMarker(tmpReflexivKmerExtendList.get(i).getLong(1)) == 2) {
                                        // residue length
                                        int tmpReflexivKmerSuffixLength = Long.SIZE / 2 - (Long.numberOfTrailingZeros((Long) tmpReflexivKmerExtendList.get(i).getSeq(2).apply(tmpReflexivKmerExtendList.get(i).getSeq(2).size()-1)) / 2 + 1);
                                        // extended overall length
                                        int tmpBlockSize = (tmpReflexivKmerExtendList.get(i).getSeq(2).length() - 1) * 31 + tmpReflexivKmerSuffixLength;
                                        int currentReflexivKmerSuffixLength = Long.SIZE / 2 - (Long.numberOfTrailingZeros((Long) s.getSeq(2).apply(s.getSeq(2).size()-1)) / 2 + 1);
                                        int currentBlockSize = (s.getSeq(2).length() - 1) * 31 + currentReflexivKmerSuffixLength;


                                        int lengthS = currentKmerSizeFromBinaryBlockArray(seq2array(s.getSeq(0)));
                                        int lengthTemp= currentKmerSizeFromBinaryBlockArray(seq2array(tmpReflexivKmerExtendList.get(i).getSeq(0)));

                                        int extraLength=0;
                                        if (lengthTemp< lengthS){
                                            extraLength=lengthS-lengthTemp;
                                        }

                                        if (lengthS<lengthTemp){ // longer kmer overlapped shorter kmer
                                            singleKmerRandomizer(s);
                                            break;
                                        }
                                      //  else if (lengthS/lengthTemp >=10){  // only for read based patching, too long contig is not going to connect with shorter contig
                                      //      singleKmerRandomizer(s);
                                      //      break;
                                      //  }
                                        else if (lengthTemp <=500){ // only for read based patching, too long contig is not going to connect with shorter contig
                                            singleKmerRandomizer(s);
                                            break;
                                        }
                                        else if (getLeftMarker(s.getLong(1))< 0 && getRightMarker(tmpReflexivKmerExtendList.get(i).getLong(1))< 0) {
                                            reflexivExtend(s, tmpReflexivKmerExtendList.get(i), -1);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else if (getLeftMarker(s.getLong(1)) >= 0 && getRightMarker(tmpReflexivKmerExtendList.get(i).getLong(1))>= 0) {
                                            reflexivExtend(s, tmpReflexivKmerExtendList.get(i), -1);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else if (getLeftMarker(s.getLong(1)) >= 0 && getLeftMarker(s.getLong(1)) - tmpBlockSize >= 0) {
                                            reflexivExtend(s, tmpReflexivKmerExtendList.get(i), getLeftMarker(s.getLong(1))- tmpBlockSize);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else if (getRightMarker(tmpReflexivKmerExtendList.get(i).getLong(1))>= 0 && getRightMarker(tmpReflexivKmerExtendList.get(i).getLong(1))- currentBlockSize -extraLength >= 0) {
                                            reflexivExtend(s, tmpReflexivKmerExtendList.get(i), getRightMarker(tmpReflexivKmerExtendList.get(i).getLong(1))- currentBlockSize);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else {
                                            singleKmerRandomizer(s);
                                            break;
                                        }
                                    } else if (getReflexivMarker(tmpReflexivKmerExtendList.get(i).getLong(1))== 1) {
                                        singleKmerRandomizer(s);
                                        //directKmerComparison(s);
                                        break;
                                    }
                                } else { /* if (s.getInt(1) == 2) { */
                                    if (getReflexivMarker(tmpReflexivKmerExtendList.get(i).getLong(1))== 2) {
                                        singleKmerRandomizer(s);
                                        //directKmerComparison(s);
                                        break;
                                    } else if (getReflexivMarker(tmpReflexivKmerExtendList.get(i).getLong(1))== 1) {
                                        // residue length
                                        int tmpReflexivKmerSuffixLength = Long.SIZE / 2 - (Long.numberOfTrailingZeros((Long) tmpReflexivKmerExtendList.get(i).getSeq(2).apply(tmpReflexivKmerExtendList.get(i).getSeq(2).size()-1)) / 2 + 1);
                                        // extended overall length
                                        int tmpBlockSize = (tmpReflexivKmerExtendList.get(i).getSeq(2).length() - 1) * 31 + tmpReflexivKmerSuffixLength;
                                        int currentReflexivKmerSuffixLength = Long.SIZE / 2 - (Long.numberOfTrailingZeros((Long) s.getSeq(2).apply(s.getSeq(2).size()-1)) / 2 + 1);
                                        int currentBlockSize = (s.getSeq(2).length() - 1) * 31 + currentReflexivKmerSuffixLength;

                                        int lengthS = currentKmerSizeFromBinaryBlockArray(seq2array(s.getSeq(0)));
                                        int lengthTemp= currentKmerSizeFromBinaryBlockArray(seq2array(tmpReflexivKmerExtendList.get(i).getSeq(0)));

                                        int extraLength=0;
                                        if (lengthS< lengthTemp){
                                            extraLength=lengthTemp-lengthS;
                                        }

                                        if (lengthTemp<lengthS){ // longer kmer overlapped shorter kmer
                                            singleKmerRandomizer(s);
                                            break;
                                        }
                                        //else if (lengthTemp/lengthS >=10){ // only for read based patching, too long contig is not going to connect with shorter contig
                                         //   singleKmerRandomizer(s);
                                        //    break;
                                        // }
                                        else if (lengthS <=500){ // only for read based patching, too long contig is not going to connect with shorter contig
                                            singleKmerRandomizer(s);
                                            break;
                                        }
                                        else if (getRightMarker(s.getLong(1)) < 0 && getLeftMarker(tmpReflexivKmerExtendList.get(i).getLong(1))< 0) {
                                            reflexivExtend(tmpReflexivKmerExtendList.get(i), s, -1);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else if (getRightMarker(s.getLong(1)) >= 0 && getLeftMarker(tmpReflexivKmerExtendList.get(i).getLong(1)) >= 0) {
                                            reflexivExtend(tmpReflexivKmerExtendList.get(i), s, -1);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else if (getRightMarker(s.getLong(1))>= 0 && getRightMarker(s.getLong(1))- tmpBlockSize -extraLength>= 0) {
                                            reflexivExtend(tmpReflexivKmerExtendList.get(i), s, getRightMarker(s.getLong(1))- tmpBlockSize);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else if (getLeftMarker(tmpReflexivKmerExtendList.get(i).getLong(1))>= 0 && getLeftMarker(tmpReflexivKmerExtendList.get(i).getLong(1)) - currentBlockSize >= 0) {
                                            reflexivExtend(tmpReflexivKmerExtendList.get(i), s, getLeftMarker(tmpReflexivKmerExtendList.get(i).getLong(1)) - currentBlockSize);
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
            long[] currentSubKmerArray = seq2array(currentSubKmer.getSeq(0));
            long[] currentReflexivArray = seq2array(currentSubKmer.getSeq(2));

            if (getReflexivMarker(currentSubKmer.getLong(1)) == 1) {
                /**
                 * 00000000000000110010111010010   Long.SIZE
                 * --------------C-G-G-G-T-C-A-G   Long.SIZE - (Long.numberOfLeadingZeros / 2 + 1)
                 * --------------^-Length marker
                 */
                int currentSuffixLength =  currentKmerSizeFromBinaryBlockArray(currentReflexivArray);  // Long.SIZE / 2 - (Long.numberOfTrailingZeros(currentSubKmer.getLong(2)) / 2 + 1); // xx01-------
                long[] newReflexivSubKmer; //= new long[param.subKmerBinarySlots];
                // long newReflexivLong;

                if (randomReflexivMarker == 2) {

                    long[] combinedKmerArray = combineTwoLongBlocks(currentSubKmerArray, currentReflexivArray);

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

                int currentSubKmerSize= currentKmerSizeFromBinaryBlockArray(currentSubKmerArray);

                if (randomReflexivMarker == 2) {
                    reflexivKmerConcatList.add(currentSubKmer);
                } else { /* randomReflexivMarker == 1 */

                    long[] combinedKmerArray = combineTwoLongBlocks(currentReflexivArray, currentSubKmerArray);

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
            long[] arrayA = seq2array(a);
            long[] arrayB = seq2array(b);

            int aLength= currentKmerSizeFromBinaryBlockArray(arrayA);
            int bLength= currentKmerSizeFromBinaryBlockArray(arrayB);

            //     String arrayAString = BinaryBlocksToString(arrayA);
            //    String arrayBString = BinaryBlocksToString(arrayB);

            //     System.out.println("different comparator: " + arrayAString + " B: " + arrayBString);

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

            int forwardSuffixLength = currentKmerSizeFromBinaryBlockArray(seq2array(forwardSubKmer.getSeq(2)));
            int forwardSubKmerLength = currentKmerSizeFromBinaryBlockArray(seq2array(forwardSubKmer.getSeq(0)));

            int reflexedPrefixLength = currentKmerSizeFromBinaryBlockArray(seq2array(reflexedSubKmer.getSeq(2)));
            int reflexedSubKmerLength = currentKmerSizeFromBinaryBlockArray(seq2array(reflexedSubKmer.getSeq(0)));


            int newSubKmerLength;
            long[] longerSubKmer;

            int extraLength=0;
            if (forwardSubKmerLength>reflexedSubKmerLength){
                extraLength=forwardSubKmerLength-reflexedSubKmerLength;
            }

            if (forwardSubKmerLength >= reflexedSubKmerLength){ // In reality, it is always forwardSubKmer longer than or equal to reflexedSubKmer
                newSubKmerLength=forwardSubKmerLength;
                longerSubKmer=seq2array(forwardSubKmer.getSeq(0));
            }else{
                newSubKmerLength=reflexedSubKmerLength;
                longerSubKmer=seq2array(reflexedSubKmer.getSeq(0));
            }

            long[] reflexedPrefixArray = seq2array(reflexedSubKmer.getSeq(2));
            long[] forwardSuffixArray = seq2array(forwardSubKmer.getSeq(2));
            long attribute = 0;


            if (randomReflexivMarker == 2) {

                long[] newReflexivSubKmer = combineTwoLongBlocks(longerSubKmer, forwardSuffixArray); // xxxxx xxxxx xxx-- + xxx--- = xxxxx xxxxx xxxxx x----
                long[] newReflexivLongArray= leftShiftOutFromArray(newReflexivSubKmer, forwardSuffixLength); // xxx--  | ---xx xxxxx xxxxx x----

                newReflexivSubKmer = leftShiftArray(newReflexivSubKmer, forwardSuffixLength); // xxxxx xxxxx xxx---
                newReflexivLongArray = combineTwoLongBlocks(reflexedPrefixArray, newReflexivLongArray); // xx--- + xxx--

                if (bubbleDistance < 0) {

                    int left=0;
                    int right=0;
                    if (getLeftMarker(reflexedSubKmer.getLong(1))>=0){
                        left = getLeftMarker(reflexedSubKmer.getLong(1));
                    }else{
                        left= getLeftMarker(forwardSubKmer.getLong(1))-reflexedPrefixLength;
                    }

                    if (getRightMarker(forwardSubKmer.getLong(1))>=0){
                        right = getRightMarker(forwardSubKmer.getLong(1));
                    }else {
                        right = getRightMarker(reflexedSubKmer.getLong(1))-forwardSuffixLength-extraLength;
                    }

                    attribute = buildingAlongFromThreeInt(randomReflexivMarker, left, right);
                    reflexivKmerConcatList.add(
                            RowFactory.create(newReflexivSubKmer,
                                    attribute, newReflexivLongArray
                            )
                    );
                } else {
                    if (getLeftMarker(forwardSubKmer.getLong(1)) > 0) {
                        if (getRightMarker(forwardSubKmer.getLong(1)) >=0) {
                            attribute = buildingAlongFromThreeInt(randomReflexivMarker, bubbleDistance, getRightMarker(forwardSubKmer.getLong(1)));
                        }else{
                            attribute= buildingAlongFromThreeInt(randomReflexivMarker, bubbleDistance, getRightMarker(reflexedSubKmer.getLong(1))-forwardSuffixLength-extraLength);
                        }
                        reflexivKmerConcatList.add(
                                RowFactory.create(newReflexivSubKmer,
                                        attribute, newReflexivLongArray
                                )
                        );
                    } else { // reflexedSubKmer right >0
                        if (getLeftMarker(reflexedSubKmer.getLong(1))>=0) {
                            attribute = buildingAlongFromThreeInt(randomReflexivMarker, getLeftMarker(reflexedSubKmer.getLong(1)), bubbleDistance-extraLength);
                        }else{
                            attribute = buildingAlongFromThreeInt(randomReflexivMarker, getLeftMarker(forwardSubKmer.getLong(1))-reflexedPrefixLength, bubbleDistance-extraLength);
                        }
                        reflexivKmerConcatList.add(
                                RowFactory.create(newReflexivSubKmer,
                                        attribute, newReflexivLongArray
                                )
                        );
                    }
                }

                //   String newReflexivSubKmerString = BinaryBlocksToString(newReflexivSubKmer);
                //   String newReflexivLongArrayString = BinaryBlocksToString(newReflexivLongArray);

                //       System.out.println("Prefix " + newReflexivLongArrayString + " combined: " + newReflexivSubKmerString + " reflexivMarker: " + getReflexivMarker(attribute) + " leftMarker: " + getLeftMarker(attribute) + " rightMarker: " + getRightMarker(attribute));


                randomReflexivMarker = 1; /* an action of randomization */
            } else { /* randomReflexivMarker == 1 */

                long[] newForwardSubKmer = combineTwoLongBlocks(reflexedPrefixArray, longerSubKmer); // xx--- + xxxxx xxxxx xx--- = xxxxx xxxxx xxxx-
                long[] newForwardLongArray = leftShiftArray(newForwardSubKmer, newSubKmerLength);  // xxxxx xxxxx xxxx-  -> xx--

                newForwardSubKmer = leftShiftOutFromArray(newForwardSubKmer, newSubKmerLength); // xxxxx xxxxx xxxx- -> xxxxx xxxxx xx---|xx-
                newForwardLongArray = combineTwoLongBlocks(newForwardLongArray, forwardSuffixArray); // xx-- + xxx-- -> xxxxx

                if (bubbleDistance < 0) {
                    int left=0;
                    int right=0;
                    if (getLeftMarker(reflexedSubKmer.getLong(1))>=0){
                        left = getLeftMarker(reflexedSubKmer.getLong(1));
                    }else{
                        left= getLeftMarker(forwardSubKmer.getLong(1))-reflexedPrefixLength;
                    }

                    if (getRightMarker(forwardSubKmer.getLong(1))>=0){
                        right = getRightMarker(forwardSubKmer.getLong(1));
                    }else {
                        right = getRightMarker(reflexedSubKmer.getLong(1))-forwardSuffixLength-extraLength;
                    }

                    attribute = buildingAlongFromThreeInt(randomReflexivMarker, left, right);
                    reflexivKmerConcatList.add(
                            RowFactory.create(newForwardSubKmer,
                                    attribute, newForwardLongArray
                            )
                    );
                } else {

                    if (getLeftMarker(forwardSubKmer.getLong(1)) > 0) {
                        if (getRightMarker(forwardSubKmer.getLong(1)) >=0) {
                            attribute = buildingAlongFromThreeInt(randomReflexivMarker, bubbleDistance, getRightMarker(forwardSubKmer.getLong(1)));
                        }else{
                            attribute= buildingAlongFromThreeInt(randomReflexivMarker, bubbleDistance, getRightMarker(reflexedSubKmer.getLong(1))-forwardSuffixLength-extraLength);
                        }
                        reflexivKmerConcatList.add(
                                RowFactory.create(newForwardSubKmer,
                                        attribute, newForwardLongArray
                                )
                        );
                    } else { // reflexedSubKmer.getInt(4) >0
                        if (getLeftMarker(reflexedSubKmer.getLong(1))>=0) {
                            attribute = buildingAlongFromThreeInt(randomReflexivMarker, getLeftMarker(reflexedSubKmer.getLong(1)), bubbleDistance-extraLength);
                        }else{
                            attribute = buildingAlongFromThreeInt(randomReflexivMarker, getLeftMarker(forwardSubKmer.getLong(1))-reflexedPrefixLength, bubbleDistance-extraLength);
                        }
                        reflexivKmerConcatList.add(
                                RowFactory.create(newForwardSubKmer,
                                        attribute, newForwardLongArray
                                )
                        );
                    }
                }

                //  String newForwardSubKmerString = BinaryBlocksToString(newForwardSubKmer);
                //  String newForwardLongArrayString = BinaryBlocksToString(newForwardLongArray);

                //   System.out.println("After combine: " + newForwardSubKmerString + " suffix: " + newForwardLongArrayString + " reflexivMarker: " + getReflexivMarker(attribute) + " leftMarker: " + getLeftMarker(attribute) + " rightMarker: " + getRightMarker(attribute));

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
                    //  String rightShift= BinaryBlocksToString(rightBlocksLeftShiftedArray);
                    //  System.out.println("rightShift: " + rightShift);
                }

                // String mergedKmer= BinaryBlocksToString(newBlocks);

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
            long[] arrayA = seq2array(a);
            long[] arrayB = seq2array(b);

            int aLength= currentKmerSizeFromBinaryBlockArray(arrayA);
            int bLength= currentKmerSizeFromBinaryBlockArray(arrayB);

            //       String arrayAString = BinaryBlocksToString(arrayA);
            //       String arrayBString = BinaryBlocksToString(arrayB);
            if (aLength==bLength){
                //       System.out.println("equal comparator: " + arrayAString + " B: " + arrayBString);

            }

            if (a.length() != b.length()){
                return false;
            }

            for (int i = 0; i < a.length(); i++) {
                if (!a.apply(i).equals(b.apply(i))) {
                    return false;
                }
            }

            return true;
        }
    }

    class FastqTuple2Dataset implements MapPartitionsFunction<Tuple2<String, Long>, Row>, Serializable {


        List<Row> kmerList = new ArrayList<Row>();
        int SeedKmerSize=15;
        int readLength;
        Long ID;
        char nucleotide;
        long nucleotideInt;

        int currentExtensionSize;
        int currentExtensionBlockSize;
        String extension;


        public Iterator<Row> call(Iterator<Tuple2<String, Long>> s) {

            while (s.hasNext()) {
                Tuple2<String, Long> sTuple = s.next();
                ID= sTuple._2;
                extension = sTuple._1;
                readLength = extension.length();

                if (readLength - SeedKmerSize - param.endClip <= 1 || param.frontClip > readLength) {
                    continue;
                }

                currentExtensionSize = extension.length();
                currentExtensionBlockSize = (currentExtensionSize-1)/31+1;

                //        if (!kmerSizeCheck(kmer, param.kmerListHash)){continue;} // the kmer length does not fit into any of the kmers in the list.


                long[] extensionBinarySlot = new long[currentExtensionBlockSize];

                for (int i = 0; i < currentExtensionSize; i++) {
                    nucleotide = extension.charAt(i);
                    if (nucleotide >= 256) nucleotide = 255;
                    nucleotideInt = nucleotideValue(nucleotide);
                    // forward kmer in bits
                    nucleotideInt <<= 2*(32-1-(i%31)); // shift to the left   [ATCGGATCC-,ATCGGATCC-]

                    extensionBinarySlot[i / 31] |= nucleotideInt;
                }

                long kmerEndMark = 1L;

                kmerEndMark <<= 2*(32-1-((currentExtensionSize-1)%31+1));
                extensionBinarySlot[currentExtensionBlockSize-1] |= kmerEndMark; // param.kmerListHash.get(currentKmerSize)] == currentKmerBlockSize

                // attribute= onlyChangeReflexivMarker(attribute,1);
                kmerList.add(
                        RowFactory.create(ID, extensionBinarySlot)
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

    class ReverseComplementKmerBinaryExtractionFromDataset implements MapPartitionsFunction<Tuple2<String, Long>, Row>, Serializable {


        List<Row> kmerList = new ArrayList<Row>();
        int SeedKmerSize=15;
        long maxKmerBits = ~((~0L) << (2 * SeedKmerSize));
        int readLength;
        String[] units;
        String read;
        Long ID;
        char nucleotide;
        long nucleotideInt;
        long nucleotideIntComplement;

        long forwardSeed;
        long reverseSeed;

        public Iterator<Row> call(Iterator<Tuple2<String, Long>> s) {

            while (s.hasNext()) {
                Tuple2<String, Long> sTuple = s.next();
                ID = sTuple._2;
                read = sTuple._1;
                readLength = read.length();


                //            System.out.println(read);

                if (readLength - param.kmerSize - param.endClip + 1 <= 0 || param.frontClip > readLength) {
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
                    if (i - param.frontClip <= param.kmerSize - 1) {
                        nucleotideBinary <<= 2;
                        nucleotideBinary |= nucleotideInt;

                        if ((i - param.frontClip + 1) % 32 == 0) { // each 32 nucleotides fill a slot
                            nucleotideBinarySlot[(i - param.frontClip + 1) / 32 - 1] = nucleotideBinary;
                            nucleotideBinary = 0L;
                        }

                        if (i - param.frontClip == param.kmerSize - 1) { // start completing the first kmer
                            nucleotideBinary &= maxKmerBits;
                            nucleotideBinarySlot[(i - param.frontClip + 1) / 32] = nucleotideBinary; // (i-param.frontClip+1)/32 == nucleotideBinarySlot.length -1
                            nucleotideBinary = 0L;

                            // reverse complement

                        }
                    } else {
                        // the last block, which is shorter than 32 mer
                        Long transitBit1 = nucleotideBinarySlot[param.kmerBinarySlots - 1] >>> 2 * (param.kmerSizeResidue - 1);  // 0000**----------  -> 000000000000**
                        // for the next block
                        Long transitBit2; // for the next block

                        // update the last block of kmer binary array
                        nucleotideBinarySlot[param.kmerBinarySlots - 1] <<= 2;    // 0000-------------  -> 00------------00
                        nucleotideBinarySlot[param.kmerBinarySlots - 1] |= nucleotideInt;  // 00------------00  -> 00------------**
                        nucleotideBinarySlot[param.kmerBinarySlots - 1] &= maxKmerBits; // 00------------**  -> 0000----------**

                        // the rest
                        for (int j = param.kmerBinarySlots - 2; j >= 0; j--) {
                            transitBit2 = nucleotideBinarySlot[j] >>> (2 * 31);   // **---------------  -> 0000000000000**
                            nucleotideBinarySlot[j] <<= 2;    // ---------------  -> --------------00
                            nucleotideBinarySlot[j] |= transitBit1;  // -------------00 -> -------------**
                            transitBit1 = transitBit2;
                        }
                    }

                    // reverse kmer binarizationalitivities :) non English native speaking people making fun of English
                    nucleotideIntComplement = nucleotideInt ^ 3;  // 3 is binary 11; complement: 11(T) to 00(A), 10(G) to 01(C)

                    if (i - param.frontClip <= param.kmerSize - 1) {
                        if (i - param.frontClip < param.kmerSizeResidue - 1) {
                            nucleotideIntComplement <<= 2 * (i - param.frontClip);   //
                            nucleotideBinaryReverseComplement |= nucleotideIntComplement;
                        } else if (i - param.frontClip == param.kmerSizeResidue - 1) {
                            nucleotideIntComplement <<= 2 * (i - param.frontClip);
                            nucleotideBinaryReverseComplement |= nucleotideIntComplement;
                            nucleotideBinaryReverseComplementSlot[param.kmerBinarySlots - 1] = nucleotideBinaryReverseComplement; // param.kmerBinarySlot-1 = nucleotideBinaryReverseComplementSlot.length -1
                            nucleotideBinaryReverseComplement = 0L;

                            /**
                             * param.kmerSizeResidue is the last block length;
                             * i-param.frontClip is the index of the nucleotide on the sequence;
                             * +1 change index to length
                             */
                        } else if ((i - param.frontClip - param.kmerSizeResidue + 1) % 32 == 0) {  //

                            nucleotideIntComplement <<= 2 * ((i - param.frontClip - param.kmerSizeResidue) % 32); // length (i- param.frontClip-param.kmerSizeResidue +1) -1 shift
                            nucleotideBinaryReverseComplement |= nucleotideIntComplement;

                            // filling the blocks in a reversed order
                            nucleotideBinaryReverseComplementSlot[param.kmerBinarySlots - ((i - param.frontClip - param.kmerSizeResidue + 1) / 32) - 1] = nucleotideBinaryReverseComplement;
                            nucleotideBinaryReverseComplement = 0L;
                        } else {
                            nucleotideIntComplement <<= 2 * ((i - param.frontClip - param.kmerSizeResidue) % 32); // length (i- param.frontClip-param.kmerSizeResidue +1) -1 shift
                            nucleotideBinaryReverseComplement |= nucleotideIntComplement;
                        }
                    } else {
                        // the first transition bit from the first block
                        long transitBit1 = nucleotideBinaryReverseComplementSlot[0] << 2 * 31;
                        long transitBit2;

                        nucleotideBinaryReverseComplementSlot[0] >>>= 2;
                        nucleotideIntComplement <<= 2 * 31;
                        nucleotideBinaryReverseComplementSlot[0] |= nucleotideIntComplement;

                        for (int j = 1; j < param.kmerBinarySlots - 1; j++) {
                            transitBit2 = nucleotideBinaryReverseComplementSlot[j] << 2 * 31;
                            nucleotideBinaryReverseComplementSlot[j] >>>= 2;
                            // transitBit1 <<= 2*31;
                            nucleotideBinaryReverseComplementSlot[j] |= transitBit1;
                            transitBit1 = transitBit2;
                        }

                        nucleotideBinaryReverseComplementSlot[param.kmerBinarySlots - 1] >>>= 2;
                        transitBit1 >>>= 2 * (31 - param.kmerSizeResidue + 1);
                        nucleotideBinaryReverseComplementSlot[param.kmerBinarySlots - 1] |= transitBit1;
                    }


                    // reach the first complete K-mer
                    if (i - param.frontClip >= param.kmerSize - 1) {

                        kmerList.add(RowFactory.create(nucleotideBinarySlot, ID, i));  // the number does not matter, as the count is based on units
                        kmerList.add(RowFactory.create(nucleotideBinaryReverseComplementSlot, -ID, readLength-param.kmerSize-i));
                    }
                }
            }


            return kmerList.iterator();
        }


        private String BinaryLongToString (long binaryBlocks){ // this one has been modified for k-mer 15
            //           String KmerString="";
            // int KmerLength = currentKmerSizeFromBinaryBlockArray(binaryBlocks);
            int KmerLength = 15;
            StringBuilder sb= new StringBuilder();
            char currentNucleotide;

            for (int i=0; i< KmerLength; i++){
                Long currentNucleotideBinary = binaryBlocks>>> 2 * (32 - (i%31+1));
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


        private long buildingAlongForCompression(long kmer, int index, int ROC){ // ROC read or contig
            // xxxxxxxxxC|R----index    assuming contig length smaller than 1G

            long ROCLong = (long) ROC << 2*15;
            kmer|= ROCLong;
            return  kmer|(long) index;
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
