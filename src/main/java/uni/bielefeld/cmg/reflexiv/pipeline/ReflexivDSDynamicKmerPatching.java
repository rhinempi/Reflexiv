package uni.bielefeld.cmg.reflexiv.pipeline;


import com.fing.mapreduce.FourMcTextInputFormat;
import com.sun.javafx.scene.control.skin.VirtualFlow;
import org.apache.commons.lang.StringUtils;
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
public class ReflexivDSDynamicKmerPatching implements Serializable {
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
        conf.set("spark.hadoop.mapred.max.split.size", "12000000");
        conf.set("spark.sql.files.maxPartitionBytes", "12000000");
        conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes","12mb");
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
                .config("spark.sql.files.maxPartitionBytes", "12000000")
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
        SparkConf conf = setSparkConfiguration();
        info.readMessage("Initiating Spark context ...");
        info.screenDump();
        info.readMessage("Start Spark framework");
        info.screenDump();
        JavaSparkContext sc = new JavaSparkContext(conf);


        SparkSession spark = setSparkSessionConfiguration(param.shufflePartition);

        info.readMessage("Initiating Spark SQL context ...");
        info.screenDump();
        info.readMessage("Start Spark SQL framework");
        info.screenDump();

        sc.setCheckpointDir("/tmp/checkpoints");
        String checkpointDir= sc.getCheckpointDir().get();

        Dataset<Row> KmerCountDS;

        StructType kmerBinaryCountTupleLongStruct = new StructType();
        kmerBinaryCountTupleLongStruct = kmerBinaryCountTupleLongStruct.add("ID", DataTypes.LongType, false);
        kmerBinaryCountTupleLongStruct = kmerBinaryCountTupleLongStruct.add("kmer", DataTypes.createArrayType(DataTypes.LongType), false);
        ExpressionEncoder<Row> KmerBinaryCountLongEncoder = RowEncoder.apply(kmerBinaryCountTupleLongStruct);

        Dataset<Row> markerTupleRow;
        Dataset<Row> ContigDS;


        Dataset<String> FastqDS;
        JavaPairRDD<String, Long> FastqIndex;
        Dataset<Tuple2<String, Long>> FastqDSTuple;


        if (param.inputFormat.equals("4mc")){
            Configuration baseConfiguration = new Configuration();

            Job jobConf = Job.getInstance(baseConfiguration);

            JavaPairRDD<LongWritable, Text> FastqPairRDD = sc.newAPIHadoopFile(param.inputFqPath, FourMcTextInputFormat.class, LongWritable.class, Text.class, jobConf.getConfiguration());

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
        ReadAndContigSeedStruct = ReadAndContigSeedStruct.add("seed", DataTypes.LongType, false);
        ReadAndContigSeedStruct = ReadAndContigSeedStruct.add("ID", DataTypes.LongType, false);
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
        markerTupleRow = KmerCountDS.mapPartitions(ReducedKmerToSubKmer, KmerBinaryCountLongEncoder);

        ContigKmerMarkerExtraction extractContigTails = new ContigKmerMarkerExtraction();
        ContigSeedDS = markerTupleRow.mapPartitions(extractContigTails, ReadAndContigSeedEncoder);

        ContigSeedDS = ContigSeedDS.union(ReadSeedDS);

        ContigSeedDS = ContigSeedDS.sort("seed");

        Dataset<Row> RACpairDS;
        StructType RACPairStruct = new StructType();
        RACPairStruct = RACPairStruct.add("read", DataTypes.LongType, false);
        RACPairStruct = RACPairStruct.add("contig", DataTypes.LongType, false);
        RACPairStruct = RACPairStruct.add("index", DataTypes.LongType, false);
        ExpressionEncoder<Row> RACPairEncoder = RowEncoder.apply(RACPairStruct);

        ReadAndContigPairs matchReadAndContig = new ReadAndContigPairs();
        RACpairDS = ContigSeedDS.mapPartitions(matchReadAndContig, RACPairEncoder);

        RACpairDS= RACpairDS.sort("read");

        Dataset<Row> CCPairDS;
        StructType CCPairStruct = new StructType();
        CCPairStruct = CCPairStruct.add("left", DataTypes.LongType, false);
        CCPairStruct = CCPairStruct.add("right", DataTypes.LongType, false);
        CCPairStruct = CCPairStruct.add("index", DataTypes.LongType, false);
        CCPairStruct = CCPairStruct.add("seq", DataTypes.LongType, false);
        ExpressionEncoder<Row> CCPairEncoder = RowEncoder.apply(CCPairStruct);

        CreatCCPairs matchContigToContig = new CreatCCPairs();
        CCPairDS = RACpairDS.mapPartitions(matchContigToContig, CCPairEncoder);

        CCPairDS = CCPairDS.sort("left");


        Dataset<Row> MarkedReads;
        StructType CCNetStruct = new StructType();
        CCNetStruct = CCNetStruct.add("read", DataTypes.LongType, false);
        CCNetStruct = CCNetStruct.add("CCMeta", DataTypes.createArrayType(DataTypes.LongType), false);
        ExpressionEncoder<Row> CCNetEncoder = RowEncoder.apply(CCNetStruct);

        CCPairsToConnections filterForCCpair = new CCPairsToConnections();
        MarkedReads = CCPairDS.mapPartitions(filterForCCpair, CCNetEncoder);

        Dataset<Row> FastqIndexedDS;
        FastqTuple2Dataset FastqTupleChange = new FastqTuple2Dataset();
        FastqIndexedDS = FastqDSTuple.mapPartitions(FastqTupleChange,CCNetEncoder);

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
        ReflexivLongKmerStructCompressed= ReflexivLongKmerStructCompressed.add("k-1", DataTypes.LongType, false);
        ReflexivLongKmerStructCompressed= ReflexivLongKmerStructCompressed.add("attribute", DataTypes.LongType, false);
        ReflexivLongKmerStructCompressed= ReflexivLongKmerStructCompressed.add("extension", DataTypes.createArrayType(DataTypes.LongType), false);
        ExpressionEncoder<Row> ReflexivLongSubKmerEncoderCompressed = RowEncoder.apply(ReflexivLongKmerStructCompressed);

        ChangingEndsOfConnectableContigs ModifyContig = new ChangingEndsOfConnectableContigs();
        reflexivKmer = CCNetWithSeq.mapPartitions(ModifyContig, ReflexivLongSubKmerEncoderCompressed);

        //loop
        reflexivKmer = reflexivKmer.sort("k-1");

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

                ID = units.getString(0);
                extension = units.getString(1);

                if (ID.startsWith("(")) {
                    ID = ID.substring(1);
                }


                currentExtensionSize = extension.length();
                currentExtensionBlockSize = (currentExtensionSize-1)/31+1;

        //        if (!kmerSizeCheck(kmer, param.kmerListHash)){continue;} // the kmer length does not fit into any of the kmers in the list.

                if (units.getString(0).endsWith(")")) {
                    String[] attributeStringArray = StringUtils.chop(units.getString(0)).split("\\-");
                    attribute =Long.parseLong(attributeStringArray[2]);
                    // attribute = Long.parseLong(StringUtils.chop(units.getString(1)));
                } else {
                    String[] attributeStringArray = units.getString(0).split("\\-");
                    attribute =Long.parseLong(attributeStringArray[2]);
                    // attribute = Long.parseLong(units.getString(1));
                }


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
                        RowFactory.create( attribute, extensionBinarySlot)
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

                subKmerArray[0]=s.getLong(0);

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

                if (s.getSeq(1).length()<2){
                    continue;
                }

                if (lastID==null){
                    lastID=s;
                }

                if (s.getLong(0) == lastID.getLong(0)){
                    tmpConsecutiveContigs.add(seq2array(s.getSeq(1)));
                }else{
                    attribute = buildingAlongFromThreeInt(1, -1, -1);
                    if (tmpConsecutiveContigs.size()>0){
                        tmpConsecutiveContigs.add(seq2array(lastID.getSeq(1)));
                        long[] modifiedContig = updatingContig(tmpConsecutiveContigs);
                        ReflexibleKmerList.add(RowFactory.create(leftShiftOutFromArray(modifiedContig,31)[0], attribute, leftShiftArray(modifiedContig, 31)));

                        tmpConsecutiveContigs=new ArrayList<long[]>();
                    }else { // lastID is alone and probably a normal contig
                        long[] contigArray = seq2array(lastID.getSeq(1));
                        long[] subKmer = leftShiftOutFromArray(contigArray, 31);
                        long[] extension = leftShiftArray(contigArray, currentKmerSizeFromBinaryBlockArray(contigArray)-31);
                        ReflexibleKmerList.add(RowFactory.create(subKmer[0], attribute, extension));
                    }

                    lastID = s;
                }
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
                System.out.println("debugging needed");

            }else if (contigSet.size()>2){
                if (contigSet.get(0)[contigSet.get(0).length-2]!=0){ // the contig
                    if (contigSet.get(1)[contigSet.get(1).length-2] !=0){
                        // another contig?
                        System.out.println("debugging needed");
                    }else if (contigSet.get(2)[contigSet.get(2).length-2] !=0){
                        // another contig?
                        System.out.println("debugging needed");
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
                        System.out.println("debugging needed");
                    }else if (contigSet.get(2)[contigSet.get(2).length-2] !=0){
                        // another contig?
                        System.out.println("debugging needed");
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
                        System.out.println("debugging needed");
                    }else if (contigSet.get(1)[contigSet.get(1).length-2] !=0){
                        // another contig?
                        System.out.println("debugging needed");
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

            if (leftContig!=null){
                int contigLength = currentKmerSizeFromBinaryBlockArray(newContig);
                if (leftContig[leftContig.length-1] < contigLength) {
                    newContig = leftShiftOutFromArray(newContig, (int)leftContig[leftContig.length-1] + 1);
                    leftContig = removeTailingTwoSlots(leftContig);
                    newContig = combineTwoLongBlocks(newContig, leftContig); // get all bases from read and give to leftContig
                }else{
                    // not possible
                }
            }

            if (rightContig!=null){
                if (rightContig[rightContig.length-2]!=0){
                    System.out.println("Warning: not a right Contig");
                }
                int negativeIndex = (int)rightContig[rightContig.length-1];

                rightContig = removeTailingTwoSlots(rightContig);
                int readLength= currentKmerSizeFromBinaryBlockArray(rightContig);
                int offset = readLength + negativeIndex -1;

                newContig = leftShiftArray(newContig, offset);
                rightContig = leftShiftArray(rightContig, readLength-31); // shift out 31 nt for right contig
                newContig = combineTwoLongBlocks(rightContig,newContig);

            }

            return newContig;
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
                            int leftIndex = getLeftIndex((Long) s.getSeq(1).apply(2));
                            int rightIndex = getRightIndex((Long) s.getSeq(1).apply(2));

                            leftContigReadMeta = seq2arrayWithTwoMoreSlots(lastRead.getSeq(1));
                            leftContigReadMeta[leftContigReadMeta.length - 2] = 0L;
                            leftContigReadMeta[leftContigReadMeta.length - 1] = leftIndex;

                            rightContigReadMeta = seq2arrayWithTwoMoreSlots(lastRead.getSeq(1));
                            rightContigReadMeta[rightContigReadMeta.length - 2] = 0L;
                            rightContigReadMeta[rightContigReadMeta.length - 1] = rightIndex;

                            CCReads.add(RowFactory.create(s.getSeq(1).apply(1), leftContigReadMeta));   // contig, seqArray, 0L, index
                            CCReads.add(RowFactory.create(s.getSeq(1).apply(1), rightContigReadMeta));
                        }
                    }

                    lastMarker = s;
                }else{ // a read
                    if (lastMarker!=null) {
                        if (s.getLong(0) == lastMarker.getLong(0)) { // matches last marker
                            int leftIndex = getLeftIndex((Long) lastMarker.getSeq(1).apply(2));
                            int rightIndex = getRightIndex((Long) lastMarker.getSeq(1).apply(2));

                            leftContigReadMeta = seq2arrayWithTwoMoreSlots(s.getSeq(1));
                            leftContigReadMeta[leftContigReadMeta.length - 2] = 0L;
                            leftContigReadMeta[leftContigReadMeta.length - 1] = leftIndex;

                            rightContigReadMeta = seq2arrayWithTwoMoreSlots(s.getSeq(1));
                            rightContigReadMeta[rightContigReadMeta.length - 2] = 0L;
                            rightContigReadMeta[rightContigReadMeta.length - 1] = rightIndex;

                            CCReads.add(RowFactory.create(s.getSeq(1).apply(1), leftContigReadMeta));
                            CCReads.add(RowFactory.create(s.getSeq(1).apply(1), rightContigReadMeta));
                        }
                    }

                    lastRead=s;
                }


            }

            System.out.println("CCReads number:" + CCReads.size());

            return CCReads.iterator();
        }

        private int getLeftIndex(long combinedDuo){
            return (int) (combinedDuo >>> 2*16);
        }

        private int getRightIndex(long combinedDuo){
            return (int) combinedDuo;
        }

        private long[] seq2arrayWithTwoMoreSlots(Seq a){ // add two slots at the end for other meta data
            long[] array =new long[a.length()+2];
            for (int i = 0; i < a.length(); i++) {
                array[i] = (Long) a.apply(i);
            }
            return array;
        }
    }

    class CCPairsToConnections implements MapPartitionsFunction<Row, Row>, Serializable{
        List<Row> CCNet = new ArrayList<Row>();
        long lastLeftContig=0;
        long lastRightTarget=0;
        long lastIndex=0;
        long lastRead=0;
        int lastRightCount=0;
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

                if (s.getLong(0) == lastLeftContig) {
                    if (s.getLong(1) == lastRightTarget) {
                        lastRightCount++;
                    } else {
                        targetAndCount = new long[4];
                        targetAndCount[0]= lastRightTarget;
                        targetAndCount[1]= lastRightCount;
                        targetAndCount[2]= lastIndex;
                        targetAndCount[3]= lastRead;
                        rightTargetAndCount.add(targetAndCount);
                        //rightTargetCounts.add();
                        lastRightTarget = s.getLong(1);
                        lastIndex= s.getLong(2);
                        lastRead=s.getLong(3);
                        lastRightCount = 1;
                    }
                }else{ // new left contig

                    targetAndCount = new long[4];
                    targetAndCount[0]= lastRightTarget;
                    targetAndCount[1]= lastRightCount;
                    targetAndCount[2]= lastIndex;
                    targetAndCount[3]= lastRead;
                    rightTargetAndCount.add(targetAndCount);
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
                        if (secondHighest <= 8) {
                            if (rightTargetAndCount.get(0)[0] / secondHighest >= 2) {
                                long[] markerReadInfo = new long[4];
                                markerReadInfo[0] =0L;
                                markerReadInfo[1] = rightTargetAndCount.get(0)[2];
                                markerReadInfo[2] = lastLeftContig;
                                markerReadInfo[3] = lastRightTarget;
                                CCNet.add(RowFactory.create(rightTargetAndCount.get(0)[3], markerReadInfo));
                            }
                        }
                    }else{
                        long[] markerReadInfo = new long[4];
                        markerReadInfo[0] =0L;
                        markerReadInfo[1] = rightTargetAndCount.get(0)[2];
                        markerReadInfo[2] = lastLeftContig;
                        markerReadInfo[3] = lastRightTarget;

                        CCNet.add(RowFactory.create(rightTargetAndCount.get(0)[3], markerReadInfo));
                    }


                    rightTargetAndCount=null;
                    lastRightCount=1;
                    lastRightTarget = s.getLong(1);
                    lastLeftContig=s.getLong(0);
                    lastIndex= s.getLong(2);
                    lastRead= s.getLong(3);
                }

            }

            targetAndCount = new long[4];
            targetAndCount[0]= lastRightTarget;
            targetAndCount[1]= lastRightCount;
            targetAndCount[2]= lastIndex;
            targetAndCount[3]= lastRead;
            rightTargetAndCount.add(targetAndCount);

           // lastRightTarget = s.getLong(1);
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
                if (secondHighest <= 8) {
                    if (rightTargetAndCount.get(0)[0] / secondHighest >= 2) {

                        long[] markerReadInfo = new long[4];
                        markerReadInfo[0] =0L;
                        markerReadInfo[1] = rightTargetAndCount.get(0)[2];
                        markerReadInfo[2] = lastLeftContig;
                        markerReadInfo[3] = lastRightTarget;

                        CCNet.add(RowFactory.create(rightTargetAndCount.get(0)[3], markerReadInfo));
                    }
                }
            }else{
                long[] markerReadInfo = new long[4];
                markerReadInfo[0] =0L;
                markerReadInfo[1] = rightTargetAndCount.get(0)[2];
                markerReadInfo[2] = lastLeftContig;
                markerReadInfo[3] = lastRightTarget;

                CCNet.add(RowFactory.create(rightTargetAndCount.get(0)[3], markerReadInfo));
            }

            return CCNet.iterator();
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

                // R1  C1  index
                // R1  C1  index
                // R1  C2  index
                if (s.getLong(0) == lastRead){
                    if (s.getLong(1) == lastContig){
                        indexList.add(s.getInt(2));
                    }else {
                        int finalIndex;
                        if (indexList.size()>1) {
                            finalIndex = highestFrequency(indexList);
                        }else{
                            finalIndex = indexList.get(0);
                        }

                        if (finalIndex < 0) { //right contig
                            rightContigList.add(lastContig);
                            rightIndexList.add(finalIndex);
                        } else { // left contig
                            leftContigList.add(lastContig);
                            leftIndexList.add(finalIndex);
                        }


                        lastContig = s.getLong(1);
                        indexList = null;
                        indexList.add(s.getInt(2));
                    }


                    // R1 C1 index
                    // R2 C2 index
                }else{

                    // unload the last read and contig
                    if (indexList.size()>0){
                        int finalIndex;
                        if (indexList.size()>1) {
                            finalIndex = highestFrequency(indexList);
                        }else{
                            finalIndex = indexList.get(0);
                        }

                        if (finalIndex < 0) { //right contig
                            rightContigList.add(lastContig);
                            rightIndexList.add(finalIndex);
                        } else { // left contig
                            leftContigList.add(lastContig);
                            leftIndexList.add(finalIndex);
                        }

                        lastContig = s.getLong(1);
                        indexList = null;
                        indexList.add(s.getInt(2));
                    }else{
                        indexList.add(s.getInt(2));
                    }

                    // building contig contig pairs
                    for (int i =0 ;i<leftContigList.size();i++){
                        for (int j=0; j <rightContigList.size(); j++){
                            long indexDuo = combineTwoInt(leftIndexList.get(i), rightIndexList.get(j));
                            CCPairs.add(RowFactory.create(leftContigList.get(i), rightContigList.get(j), indexDuo, lastRead));
                        }
                    }

                    lastRead = s.getLong(0);
                    lastContig = s.getLong(1);
                    leftContigList = null; // or new ArrayList();
                    leftIndexList= null;
                    rightContigList = null;
                    rightIndexList=null;
                }
            }

            // unload the last read and contig
            if (indexList.size()>0){
                int finalIndex;
                if (indexList.size()>1) {
                    finalIndex = highestFrequency(indexList);
                }else{
                    finalIndex = indexList.get(0);
                }

                if (finalIndex < 0) { //right contig
                    rightContigList.add(lastContig);
                    rightIndexList.add(finalIndex);
                } else { // left contig
                    leftContigList.add(lastContig);
                    leftIndexList.add(finalIndex);
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
                    CCPairs.add(RowFactory.create(leftContigList.get(i), rightContigList.get(j), indexDuo, lastRead));
                }
            }

            return CCPairs.iterator();
        }

        private long combineTwoInt (int leftIndex, int rightIndex){
           return (long)leftIndex <<2*16 | (long) rightIndex ;
        }

        private int highestFrequency (List<Integer> numberlist){
            Collections.sort(numberlist);

            int lastIndex=0;
            int frequency =0;
            int highestFrequency=0;
            for (int i=0;i < numberlist.size(); i++){
                if (numberlist.get(i) == lastIndex){
                    frequency++;
                    if (frequency > highestFrequency){
                        highestFrequency = frequency;
                    }
                }else{
                    frequency=0;
                }
            }

            return highestFrequency;
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
                int ROC = getROCIntFromLong(0);

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
            return (int)compress >>>2*15;
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
                fullKmerArray =  (long[]) s.get(1);
                contigID=s.getLong(0);
                kmerLength = currentKmerSizeFromBinaryBlockArray(fullKmerArray);

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

                        // ---------xxxxxxxxxxx
                        //                 xxxx
                        //                xxxx
                        //               xxxx

                        fixedKmerRight= leftShiftArray(fullKmerArray, kmerLength-i-SeedKmerSize);
                        fixedKmerRight= leftShiftOutFromArray(fixedKmerRight, SeedKmerSize);
                        SeedKmerList.add(RowFactory.create(buildingAlongForCompression(fixedKmerRight[0], kmerLength-i, 0),contigID));
                    }

                }

            }

            return SeedKmerList.iterator();
        }

        private long buildingAlongForCompression(long kmer, int index, int ROC){ // ROC read or contig
            // xxxxxxxxxC|R----index    assuming contig length smaller than 1G

            ROC <<= 2*15;
            kmer|=ROC;
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
                subKmer =  BinaryBlocksToString(seq2array(s.getSeq(0)));
                subString = BinaryBlocksToString(seq2array(s.getSeq(2)));

                //int length = subKmer.length() + subString.length();
              //  System.out.println("Length: " + length + " " + getLeftMarker(s.getLong(1)) + " " + getRightMarker(s.getLong(1)));

                reflexivKmerStringList.add(
                        RowFactory.create(subKmer,
                                s.getLong(1), subString
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
        private long[] seq2array(Seq a){
            long[] array =new long[a.length()];
            for (int i = 0; i < a.length(); i++) {
                array[i] = (Long) a.apply(i);
            }
            return array;
        }


    }


    class DSFilterForkSubKmerWithErrorCorrection implements MapPartitionsFunction<Row, Row>, Serializable {
        List<Row> HighCoverageSubKmer = new ArrayList<Row>();

        public Iterator<Row> call(Iterator<Row> s) {

            while (s.hasNext()) {
                Row subKmer = s.next();

                if (HighCoverageSubKmer.size() == 0) {
                    HighCoverageSubKmer.add(
                            RowFactory.create(subKmer.getLong(0), subKmer.getLong(1), subKmer.getSeq(2))
                    );
                } else {
                    if (currentKmerSizeFromBinaryBlockArray(seq2array(subKmer.getSeq(2)))>1){ // longer kmers
                        if (subKmer.getLong(0) ==HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1).getLong(0)
                                && currentKmerSizeFromBinaryBlockArray(seq2array(HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1).getSeq(2)))==1 )
                        {
                            /*
                            long[] currentArray = new long[1];
                            long[] beforeArray = new long[1];
                            currentArray[0]=subKmer.getLong(0);
                            beforeArray[0]=HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1).getLong(0);
                            String current= BinaryBlocksToString(currentArray);
                            String before = BinaryBlocksToString(beforeArray);
                            System.out.println("Current: " + current + " before: " + before);
*/
                            HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                    RowFactory.create(subKmer.getLong(0), subKmer.getLong(1), subKmer.getSeq(2))
                            );

                        }else {
                            HighCoverageSubKmer.add(
                                    RowFactory.create(subKmer.getLong(0), subKmer.getLong(1), subKmer.getSeq(2))
                            );
                        }
                    }else if (subKmer.getLong(0) ==HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1).getLong(0)) {


                        if (currentKmerSizeFromBinaryBlockArray(seq2array(HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1).getSeq(2))) >
                               currentKmerSizeFromBinaryBlockArray(seq2array(subKmer.getSeq(2))) ){ // currentKmerSizeFromBinaryBlockArray(seq2array(subKmer.getSeq(2))) ==1

                            continue;
                        }else if (seq2array(subKmer.getSeq(2))[0] >>> 2*31 <= seq2array(HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1).getSeq(2))[0] >>> 2*31 ){
                            HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                    RowFactory.create(subKmer.getLong(0), subKmer.getLong(1), subKmer.getSeq(2))
                            );
                        }
                    } else {

                        HighCoverageSubKmer.add(
                                RowFactory.create(subKmer.getLong(0), subKmer.getLong(1), subKmer.getSeq(2))
                        );
                    }
                }

                // System.out.println("first leftMarker: " + leftMarker + " new leftMarker: " + getLeftMarker(attribute));
            }

            return HighCoverageSubKmer.iterator();
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
                    HighCoverageSubKmer.add(
                            RowFactory.create(subKmer.getLong(0), subKmer.getLong(1), subKmer.getSeq(2))
                    );
                } else {
                    if (currentKmerSizeFromBinaryBlockArray(seq2array(subKmer.getSeq(2)))>1){ // longer kmers
                        if (subKmer.getLong(0) ==HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1).getLong(0)
                                && currentKmerSizeFromBinaryBlockArray(seq2array(HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1).getSeq(2)))==1 )
                        {
                            HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                    RowFactory.create(subKmer.getLong(0), subKmer.getLong(1), subKmer.getSeq(2))
                            );

                        }else {
                            HighCoverageSubKmer.add(
                                    RowFactory.create(subKmer.getLong(0), subKmer.getLong(1), subKmer.getSeq(2))
                            );
                        }
                    }else if (subKmer.getLong(0) ==HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1).getLong(0)) {
                        if (currentKmerSizeFromBinaryBlockArray(seq2array(HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1).getSeq(2))) >
                                currentKmerSizeFromBinaryBlockArray(seq2array(subKmer.getSeq(2))) ){

                            continue;
                        }else if (seq2array(subKmer.getSeq(2))[0] >>> 2*31 <= seq2array(HighCoverageSubKmer.get(HighCoverageSubKmer.size() - 1).getSeq(2))[0] >>> 2*31 ){
                            HighCoverageSubKmer.set(HighCoverageSubKmer.size() - 1,
                                    RowFactory.create(subKmer.getLong(0), subKmer.getLong(1), subKmer.getSeq(2))
                            );
                        }
                    } else {
                        HighCoverageSubKmer.add(
                                RowFactory.create(subKmer.getLong(0), subKmer.getLong(1), subKmer.getSeq(2))
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
                ID= sTuple._2;
                read = sTuple._1;
                readLength = read.length();

                if (readLength - SeedKmerSize - param.endClip <= 1 || param.frontClip > readLength) {
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
                    if (i - param.frontClip >= SeedKmerSize) {
                        nucleotideBinary &= maxKmerBits;
                    }

                    // reverse kmer binarizationalitivities :) non English native speaking people making fun of English
                    nucleotideIntComplement = nucleotideInt ^ 3;  // 3 is binary 11; complement: 11(T) to 00(A), 10(G) to 01(C)

                    if (i - param.frontClip >= SeedKmerSize) {
                        nucleotideBinaryReverseComplement >>>= 2;
                        nucleotideIntComplement <<= 2 * (SeedKmerSize - 1);
                    } else {
                        nucleotideIntComplement <<= 2 * (i - param.frontClip);
                    }
                    nucleotideBinaryReverseComplement |= nucleotideIntComplement;

                    // reach the first complete K-mer
                    if (i - param.frontClip >= SeedKmerSize - 1) {
                        if ((i - param.frontClip +1) % SeedKmerSize ==0 ) { // no overlap
                            forwardSeed = nucleotideBinary << 2* (32-SeedKmerSize);
                            forwardSeed |= (1L << 2*(32-SeedKmerSize-1));
                            forwardSeed = buildingAlongForCompression(forwardSeed, i, 1);


                            reverseSeed = nucleotideBinaryReverseComplement << 2*(32-SeedKmerSize);
                            reverseSeed |= (1L << 2*(32-SeedKmerSize-1));
                            reverseSeed = buildingAlongForCompression(reverseSeed, i, 1);

                            // if (nucleotideBinary.compareTo(nucleotideBinaryReverseComplement) < 0) {
                            kmerList.add(RowFactory.create(forwardSeed, ID));
                            // } else {
                            kmerList.add(RowFactory.create(reverseSeed, ID));
                            // }
                        }
                    }
                }
            }
            return kmerList.iterator();
        }

        private long buildingAlongForCompression(long kmer, int index, int ROC){ // ROC read or contig
            // xxxxxxxxxC|R----index    assuming contig length smaller than 1G

            ROC <<= 2*15;
            kmer|=ROC;
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
