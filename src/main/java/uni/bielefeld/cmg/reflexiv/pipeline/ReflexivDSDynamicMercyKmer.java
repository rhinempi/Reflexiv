package uni.bielefeld.cmg.reflexiv.pipeline;


import com.fing.mapreduce.FourMcTextInputFormat;
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
        Dataset<Row> markerKmerRow;
        Dataset<Row> markerTupleRow;
        Dataset<Row> markerRangeRow;
        Dataset<Row> mercyKmerRow;
        Dataset<Row> mercyKmerString;


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


        DynamicKmerBinarizer CountedKmerToBinaryMarker = new DynamicKmerBinarizer();
        markerKmerRow = KmerCountDS.mapPartitions(CountedKmerToBinaryMarker, ReadAndContigSeedEncoder);


       // DynamicKmerBinarizerFromReducedToSubKmer ReducedKmerToSubKmer= new DynamicKmerBinarizerFromReducedToSubKmer();
       // markerKmerRow = KmerCountDS.mapPartitions(ReducedKmerToSubKmer, ReadAndContigSeedEncoder);
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

        StructType MercyKmerStruct = new StructType();
        MercyKmerStruct = MercyKmerStruct.add("kmer", DataTypes.createArrayType(DataTypes.LongType));
        MercyKmerStruct = MercyKmerStruct.add("count", DataTypes.LongType);
        ExpressionEncoder<Row> MercyKmerEncoder = RowEncoder.apply(MercyKmerStruct);

        mercyKmerRow = FastqIndexedDS.mapPartitions(mercyKmerExtraction, MercyKmerEncoder);

        DSBinaryKmerToString BinaryKmerToString = new DSBinaryKmerToString();

        StructType kmerCountTupleStruct = new StructType();
        kmerCountTupleStruct= kmerCountTupleStruct.add("kmer", DataTypes.StringType, false);
        kmerCountTupleStruct= kmerCountTupleStruct.add("count", DataTypes.LongType, false);
        ExpressionEncoder<Row> kmerCountEncoder = RowEncoder.apply(kmerCountTupleStruct);

        mercyKmerString = mercyKmerRow.mapPartitions(BinaryKmerToString, kmerCountEncoder);

        if (param.gzip) {
            mercyKmerString.write().
                    mode(SaveMode.Overwrite).
                    format("csv").
                    option("codec", "org.apache.hadoop.io.compress.GzipCodec").
                    save(param.outputPath + "/Count_" + param.kmerSize + "_mercy");
        }else{
            mercyKmerString.write().
                    mode(SaveMode.Overwrite).
                    format("csv").
                    save(param.outputPath + "/Count_" + param.kmerSize + "_mercy");
        }

        spark.stop();

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

                // long[] RCnucleotideBinarySlot = binaryBlockReverseComplementary(nucleotideBinarySlot);
                System.out.println("kmer Binary: " + BinaryBlocksToString(nucleotideBinarySlot));
                // System.out.println("kmer Binary RC: " + BinaryBlocksToString(RCnucleotideBinarySlot));
                // return
                kmerList.add(
                        RowFactory.create(nucleotideBinarySlot, 0L, -1L)
                );

                //kmerList.add(
                //        RowFactory.create(RCnucleotideBinarySlot, 0L, -1L)
                // );
            }

            return kmerList.iterator();
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

                System.out.println("kmer Binarized: " + BinaryBlocksToString(extensionBinarySlot));

                // attribute= onlyChangeReflexivMarker(attribute,1);
                kmerList.add(
                        RowFactory.create(extensionBinarySlot, 0L, -1L) // -1 as marker for k-mer more than 2 coverage
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

    class DSBinaryKmerToString implements MapPartitionsFunction<Row, Row>, Serializable{
        List<Row> reflexivKmerStringList = new ArrayList<Row>();
        StringBuilder sb= new StringBuilder();


        public Iterator<Row> call(Iterator<Row> sIterator){
            while (sIterator.hasNext()){
                String subKmer;
                sb= new StringBuilder();
                Row s = sIterator.next();

                subKmer = BinaryBlocksToString((long[]) s.get(0));
/*
                for (int i=0; i<(param.kmerSize / 32) *32;i++){
                    Long currentNucleotideBinary = ((long[]) s.get(0))[i/32] >>> 2*(31-i%32);

                    currentNucleotideBinary &= 3L;
                    char currentNucleotide =  BinaryToNucleotide(currentNucleotideBinary);
                    sb.append(currentNucleotide);
                }

                for (int i=(param.kmerSize /32)*32; i<param.kmerSize; i++){
                    Long currentNucleotideBinary = ((long[])s.get(0))[i/32] >>> 2*(param.kmerSizeResidue-1-i%32);

                    currentNucleotideBinary &= 3L;
                    char currentNucleotide =  BinaryToNucleotide(currentNucleotideBinary);
                    sb.append(currentNucleotide);
                }

                subKmer=sb.toString();
*/
                reflexivKmerStringList.add (
                        RowFactory.create(subKmer, s.getLong(1))
                        // new Row(); Tuple2<String, Integer>(subKmer, s._2)
                );
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

        private int currentKmerSizeFromBinaryBlockArray(long[] binaryBlocks){
            int kmerSize;
            int blockSize = binaryBlocks.length;
            kmerSize= (blockSize-1) *31;
            final int suffix0s = Long.numberOfTrailingZeros(binaryBlocks[blockSize - 1]); // ATCG...01---
            int lastMers = Long.SIZE/2-suffix0s/2-1;

            kmerSize+=lastMers;
            return kmerSize;

        }

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
                            extractKmer(s.getSeq(1), lastRead.getSeq(1));
                        }else{
                            lastMarkerArray.add(s);
                        }
                    }else {
                        lastMarkerArray.add(s);
                    }
                }else{ // a read with sequence
                    for (int i=0; i<lastMarkerArray.size(); i++){
                        if (s.getLong(0) == lastMarkerArray.get(i).getLong(0)){
                            extractKmer(lastMarkerArray.get(i).getSeq(1), s.getSeq(1));
                        }
                    }

                    lastMarkerArray=new ArrayList<Row>();

                    lastRead = s;
                }

            }

            // the leftover of lastMarkerArray will not find a read anymore

            for (int i=0; i<MercyKmer.size(); i++){
                System.out.println("Final mercy k-mer : " + BinaryBlocksToString( (long[])MercyKmer.get(i).get(0) ) ) ;
            }

            return MercyKmer.iterator();
        }

        private void extractKmer(Seq markerSeq, Seq readSeq) throws Exception {
            long[] markerArray = seq2array(markerSeq);
            long[] readArray = seq2array(readSeq);

            // reverse complement
            if (markerArray[1] <0){
                readArray = binaryBlockReverseComplementary(readArray);
            }

            // each read might have multiple mercy k-mer windows
            // ****------******------*****
            // 0L, rc marker, range, range, ....
            for (int i=2; i<markerArray.length; i++){
                System.out.println("Markerlong: " + markerArray[i]);
                int startIndex=getLeftMarker(markerArray[i]);
                int endIndex = getRightMarker(markerArray[i]);

                // check if end index is within read length
                // -----------------  read
                //           |        index
                //            ------- kmer size
                int readLength = currentKmerSizeFromBinaryBlockArray(readArray);
                if (endIndex+param.kmerSize >= readLength){ // should be smaller than read length to guarantee at least one high coverage k-mer (>2x) at the end of the read
                    System.out.println("there is something wrong with mercy k-mer index range");
                }

                // step over each mercy k-mer through the window
                for (int j=startIndex; j< endIndex; j++){
                    long[] kmer = leftShiftArray(readArray, j);
                    kmer = leftShiftOutFromArray(kmer, param.kmerSize);
                    long[] rcKmer = binaryBlockReverseComplementary(kmer);

                    if (compareLongArrayBlocks(kmer, rcKmer) == true) {
                        MercyKmer.add(RowFactory.create(kmer, 1L)); // mercy kmer has a coverage of 1
                    }else{
                        MercyKmer.add(RowFactory.create(rcKmer, 1L));
                    }
                }
            }
        }

        private boolean compareLongArrayBlocks(long[] forward, long[] reverse) {
            for (int i = 0; i < forward.length; i++) {

                // binary comparison from left to right, because of signed long
                if (i < forward.length - 1) {
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
                } else {
                    // ***C--------
                    for (int j = 0; j < param.kmerSizeResidue; j++) {
                        long shiftedBinary1 = forward[i] >>> (2 * (32 - j));
                        shiftedBinary1 &= 3L;
                        long shiftedBinary2 = reverse[i] >>> (2 * (32 - j));
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

        private int getLeftMarker(long attribute){
            int leftMarker = (int) (attribute >>> 2*(16)); // 01--xxxx-----xxxx -> 01--xxxx shift out right marker
            int leftMarkerBinaryBits= ~(3 << 30) ; // ---------11 -> 11---------- -> 0011111111111
            leftMarker &= leftMarkerBinaryBits; // remove reflexivMarker

            System.out.println("getLeftMarker before: " + leftMarker);

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
                        System.out.println("sorted read ID: " + s.getLong(0) + " index " + s.getLong(1));
                        indices.add((int)s.getLong(1));
                    }else{
                        System.out.println("sorted current different from last, current: " + s.getLong(0) + " index " + s.getLong(1) + " last: " + lastKmer.getLong(0) + " index "+ lastKmer.getLong(1));
                        indices.add((int)lastKmer.getLong(1)); // add the last one

                        if (indices.size()>1) { // more than one match
                            long[] ranges = findRange(indices, lastKmer.getLong(0));
                           // rangeArray[0] = 0;
                           // rangeArray[1] = range;


                            if (ranges.length > 2){ // at least one gap, two markers as element already in the array

                                if (ranges[1] <0){ // reverse complement
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

                        lastKmer=s;

                        indices = new ArrayList<Integer>();
                    }
                }
            }

            for (int i =0; i< ReadsAndRange.size(); i++){
                System.out.println("Read ID: " + ReadsAndRange.get(i).getLong(0) + " first range: " + getLeftMarker( ((long[]) ReadsAndRange.get(i).get(1))[0] )  + " to " + getRightMarker( ((long[]) ReadsAndRange.get(i).get(1))[0]) );

            }

            indices.add((int)lastKmer.getLong(1)) ; // add the last one
            if (indices.size()>1){
                long[] ranges = findRange(indices, lastKmer.getLong(0));

                if (ranges.length > 1){
                    if (ranges[1] <0){ // reverse complement , ranges.get(1) should equal lastKmer.getLong(0)
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

            if (ReadsAndRange.size()>0) {
                System.out.println("Read ID last: " + ReadsAndRange.get(ReadsAndRange.size() - 1).getLong(0) + " first range: " + getLeftMarker(((long[]) ReadsAndRange.get(ReadsAndRange.size() - 1).get(1))[0]) + " to " + getRightMarker(((long[]) ReadsAndRange.get(ReadsAndRange.size() - 1).get(1))[0]));
            }

            return ReadsAndRange.iterator();
        }

        private long[] findRange(List<Integer> i, long index){
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
                System.out.println("sorted array in findRange: " + i.get(j) + " and " + lastIndex);
                if (i.get(j) - lastIndex >1){ // at least one k-mer gap
                    a= lastIndex;
                    b = i.get(j);

                    System.out.println("a gap: " + index + " from " + a + " to " + i.get(j));

                    /**
                     *  a-1 means that the gap starts from the next k-mer
                     */
                    range = buildingAlongFromThreeInt(1, a+1, b); // a+1 means that the gap starts from the next k-mer
                    gaps.add(range);
                }

                lastIndex=i.get(j);
            }

            gapsArray = new long[gaps.size()];

            for (int k=0; k< gaps.size(); k++){
                gapsArray[k]= gaps.get(k);
            }

            return gapsArray;
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

    class RamenReadExtraction implements MapPartitionsFunction<Row, Row>, Serializable{
        List<Row> ReadsAndIndices = new ArrayList<Row>();

        Row lastKmer=null;

        List<Row> ReadsKmerBuffer= new ArrayList<Row>();

        public Iterator<Row> call(Iterator<Row> sIterator) throws Exception{

            while (sIterator.hasNext()){
                Row s = sIterator.next();

                System.out.println("Findmatch new entry: " + BinaryBlocksToString( seq2array(s.getSeq(0)) ) + " ID " + s.getLong(1) + " index/mark " + s.getLong(2));
                // ------- k-mer
                // ------- read
                // ------- read
                // ------- k-mer
                if (s.getLong(2) == -1L){ // a more than 2x coverage k-mer
                    if (ReadsKmerBuffer.size()>0){
                        for (int i =0; i< ReadsKmerBuffer.size();i++){
                            if (dynamicSubKmerComparator(ReadsKmerBuffer.get(i).getSeq(0), s.getSeq(0)) == true){
                                System.out.println("Findmatch buffered difference compared: " + BinaryBlocksToString( seq2array(ReadsKmerBuffer.get(ReadsKmerBuffer.size() - 1).getSeq(0)) ) + " ID " + ReadsKmerBuffer.get(ReadsKmerBuffer.size() - 1).getLong(1) + " index/mark " + ReadsKmerBuffer.get(ReadsKmerBuffer.size() - 1).getLong(2));
                                System.out.println("Findmatch buffered to current compared: " + BinaryBlocksToString( seq2array(s.getSeq(0)) ) + " ID " + s.getLong(1) + " index/mark " + s.getLong(2));
                                ReadsAndIndices.add(
                                        RowFactory.create(ReadsKmerBuffer.get(i).getLong(1), ReadsKmerBuffer.get(i).getLong(2))
                                );
                            }else{
                                System.out.println("Findmatch buffered did not match current: " + BinaryBlocksToString( seq2array(s.getSeq(0)) ) + " ID " + s.getLong(1) + " index/mark " + s.getLong(2));
                            }
                        }

                        ReadsKmerBuffer = new ArrayList<Row>();
                    }

                    lastKmer = s;
                    System.out.println("Findmatch lastKmer: " + BinaryBlocksToString( seq2array(lastKmer.getSeq(0)) ) + " ID " + lastKmer.getLong(1) + " index/mark " + lastKmer.getLong(2));
                }else{ // a read k-mer
                    if (lastKmer !=null){
                        if (dynamicSubKmerComparator(lastKmer.getSeq(0), s.getSeq(0)) == true){
                            System.out.println("Findmatch lastKmer compared: " + BinaryBlocksToString( seq2array(lastKmer.getSeq(0)) ) + " ID " + lastKmer.getLong(1) + " index/mark " + lastKmer.getLong(2));
                            System.out.println("Findmatch current to last directly compared: " + BinaryBlocksToString( seq2array(s.getSeq(0)) ) + " ID " + s.getLong(1) + " index/mark " + s.getLong(2));
                            ReadsAndIndices.add(
                                    RowFactory.create(s.getLong(1), s.getLong(2))
                            );

                        }else {
                            // --------- read 1 k-mer
                            // --------- read 2 k-mer different
                            // -----
                            if (ReadsKmerBuffer.size()>0) {
                                if (dynamicSubKmerComparator(ReadsKmerBuffer.get(ReadsKmerBuffer.size() - 1).getSeq(0), s.getSeq(0)) == true) {
                                    System.out.println("Findmatch buffered difference compared: " + BinaryBlocksToString( seq2array(ReadsKmerBuffer.get(ReadsKmerBuffer.size() - 1).getSeq(0)) ) + " ID " + ReadsKmerBuffer.get(ReadsKmerBuffer.size() - 1).getLong(1) + " index/mark " + ReadsKmerBuffer.get(ReadsKmerBuffer.size() - 1).getLong(2));
                                    System.out.println("Findmatch current to buffered compared: " + BinaryBlocksToString( seq2array(s.getSeq(0)) ) + " ID " + s.getLong(1) + " index/mark " + s.getLong(2));
                                    ReadsKmerBuffer.add(s);
                                } else {
                                    System.out.println("Findmatch read k-mer did not match lastKmer :" + BinaryBlocksToString(seq2array(s.getSeq(0)) ) + " ID " + s.getLong(1) + " index/mark " + s.getLong(2));
                                    ReadsKmerBuffer = new ArrayList<Row>();
                                    ReadsKmerBuffer.add(s);
                                }
                            }else{
                                System.out.println("Findmatch read k-mer buffer empty :" + BinaryBlocksToString(seq2array(s.getSeq(0)) ) + " ID " + s.getLong(1) + " index/mark " + s.getLong(2));
                                ReadsKmerBuffer.add(s);
                            }
                        }
                    }else{
                        System.out.println("Findmatch read without k-mer yet: " + BinaryBlocksToString( seq2array(s.getSeq(0)) ) + " ID " + s.getLong(1) + " index/mark " + s.getLong(2));
                        ReadsKmerBuffer.add(s);
                    }
                }

            }

            for (int i=0; i< ReadsAndIndices.size(); i++){
                System.out.println("Matched 2x k-mer read ID: " + ReadsAndIndices.get(i).getLong(0) + " and its index: " + ReadsAndIndices.get(i).getLong(1) );
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

        int kmerResidue = param.subKmerSizeResidue + 1;
        List<Row> kmerList = new ArrayList<Row>();
        long maxKmerBits = ~((~0L) << (2 * kmerResidue));
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
                long[] nucleotideBinarySlot = new long[param.subKmerBinarySlots];
                long[] nucleotideBinaryReverseComplementSlot = new long[param.subKmerBinarySlots];

                System.out.println("subKmerBinarySlots: " + param.subKmerBinarySlots);
                System.out.println("subKmerSizeResidue: " + kmerResidue);

                for (int i = param.frontClip; i < readLength - param.endClip; i++) {
                    nucleotide = read.charAt(i);
                    if (nucleotide >= 256) nucleotide = 255;
                    nucleotideInt = nucleotideValue(nucleotide);

                    // forward kmer in bits
                    if (i - param.frontClip <= param.kmerSize - 1) {
                        nucleotideBinary <<= 2;
                        nucleotideBinary |= nucleotideInt;

                        if ((i - param.frontClip + 1) % 31 == 0) { // each 31 nucleotides fill a slot
                            nucleotideBinarySlot[(i - param.frontClip + 1) / 31 - 1] = nucleotideBinary;
                            nucleotideBinary = 0L;
                        }else if (i - param.frontClip == param.kmerSize - 1) { // start completing the first kmer
                            nucleotideBinary &= maxKmerBits;
                            nucleotideBinarySlot[nucleotideBinarySlot.length -1] = nucleotideBinary; // (i-param.frontClip+1)/32 == nucleotideBinarySlot.length -1
                            nucleotideBinary = 0L;

                            // reverse complement

                        }
                    } else {
                        // the last block, which is shorter than 32 mer
                        Long transitBit1 = nucleotideBinarySlot[param.subKmerBinarySlots - 1] >>> 2 * (kmerResidue - 1);  // 0000**----------  -> 000000000000**
                        // for the next block
                        Long transitBit2; // for the next block

                        // update the last block of kmer binary array
                        nucleotideBinarySlot[param.subKmerBinarySlots - 1] <<= 2;    // 0000-------------  -> 00------------00
                        nucleotideBinarySlot[param.subKmerBinarySlots - 1] |= nucleotideInt;  // 00------------00  -> 00------------**
                        nucleotideBinarySlot[param.subKmerBinarySlots - 1] &= maxKmerBits; // 00------------**  -> 0000----------**

                        // the rest
                        for (int j = param.subKmerBinarySlots - 2; j >= 0; j--) {
                            transitBit2 = nucleotideBinarySlot[j] >>> (2 * 31);   // **---------------  -> 0000000000000**
                            nucleotideBinarySlot[j] <<= 2;    // ---------------  -> --------------00
                            nucleotideBinarySlot[j] |= transitBit1;  // -------------00 -> -------------**
                            transitBit1 = transitBit2;
                        }
                    }

                    // reverse kmer binarizationalitivities :) non English native speaking people making fun of English
                    nucleotideIntComplement = nucleotideInt ^ 3;  // 3 is binary 11; complement: 11(T) to 00(A), 10(G) to 01(C)

                    if (i - param.frontClip <= param.kmerSize - 1) {
                        if (i - param.frontClip < kmerResidue - 1) {
                            nucleotideIntComplement <<= 2 * (i - param.frontClip);   //
                            nucleotideBinaryReverseComplement |= nucleotideIntComplement;
                        } else if (i - param.frontClip == kmerResidue - 1) {
                            nucleotideIntComplement <<= 2 * (i - param.frontClip);
                            nucleotideBinaryReverseComplement |= nucleotideIntComplement;
                            nucleotideBinaryReverseComplementSlot[param.subKmerBinarySlots - 1] = nucleotideBinaryReverseComplement; // param.kmerBinarySlot-1 = nucleotideBinaryReverseComplementSlot.length -1
                            nucleotideBinaryReverseComplement = 0L;

                            /**
                             * param.kmerSizeResidue is the last block length;
                             * i-param.frontClip is the index of the nucleotide on the sequence;
                             * +1 change index to length
                             */
                        } else if ((i - param.frontClip - kmerResidue + 1) % 31 == 0) {  //

                            nucleotideIntComplement <<= 2 * ((i - param.frontClip - kmerResidue) % 31); // length (i- param.frontClip-param.kmerSizeResidue +1) -1 shift
                            nucleotideBinaryReverseComplement |= nucleotideIntComplement;

                            // filling the blocks in a reversed order
                            nucleotideBinaryReverseComplementSlot[param.subKmerBinarySlots - ((i - param.frontClip - kmerResidue + 1) / 31) - 1] = nucleotideBinaryReverseComplement;
                            nucleotideBinaryReverseComplement = 0L;
                        } else {
                            nucleotideIntComplement <<= 2 * ((i - param.frontClip - kmerResidue) % 31); // length (i- param.frontClip-param.kmerSizeResidue +1) -1 shift
                            nucleotideBinaryReverseComplement |= nucleotideIntComplement;
                        }
                    } else {
                        // the first transition bit from the first block
                        long transitBit1 = nucleotideBinaryReverseComplementSlot[0] << 2 * 31;
                        long transitBit2;

                        nucleotideBinaryReverseComplementSlot[0] >>>= 2;
                        if (param.subKmerBinarySlots>1) {
                            nucleotideIntComplement <<= 2 * 30;
                        }else{
                            nucleotideIntComplement <<= 2 * (kmerResidue -1 );
                        }
                        nucleotideBinaryReverseComplementSlot[0] |= nucleotideIntComplement;

                        for (int j = 1; j < param.subKmerBinarySlots - 1; j++) {
                            transitBit2 = nucleotideBinaryReverseComplementSlot[j] << 2 * 31;
                            nucleotideBinaryReverseComplementSlot[j] >>>= 2;
                            // transitBit1 <<= 2*31;
                            nucleotideBinaryReverseComplementSlot[j] |= transitBit1;
                            transitBit1 = transitBit2;
                        }

                        if (param.subKmerBinarySlots>1) {  // if param.subKmerBinarySlots =1 , then the above loop will not happen, nucleotideBinaryReverseComplementSlot[0] will leff shift twice
                            nucleotideBinaryReverseComplementSlot[param.subKmerBinarySlots - 1] >>>= 2;
                            transitBit1 >>>= 2 * (31 - kmerResidue + 1);
                            nucleotideBinaryReverseComplementSlot[param.subKmerBinarySlots - 1] |= transitBit1;
                        }
                    }

                    if (i - param.frontClip >= param.kmerSize - 1) {
                        long[] nucleotideBinarySlotPreRow = new long[param.subKmerBinarySlots];
                        for (int j = 0; j < nucleotideBinarySlot.length; j++) {
                            nucleotideBinarySlotPreRow[j] = nucleotideBinarySlot[j];
                            nucleotideBinarySlotPreRow[j] <<= 2;// --*************** -> ****************--
                        }

                        nucleotideBinarySlotPreRow[nucleotideBinarySlotPreRow.length - 1] >>>= 2; // above in the loop, the last slot left shitted 2 bits
                        nucleotideBinarySlotPreRow[nucleotideBinarySlotPreRow.length - 1] <<= 2 * (32 - kmerResidue);
                        nucleotideBinarySlotPreRow[nucleotideBinarySlotPreRow.length - 1] |= (1L << 2 * (32 - kmerResidue - 1)); // add C marker
                        // kmerList.add(RowFactory.create(nucleotideBinarySlotPreRow, ID, (long) i));  // the number does not matter, as the count is based on units

                        long[] nucleotideBinaryReverseComplementSlotPreRow = new long[param.subKmerBinarySlots];
                        for (int j = 0; j < nucleotideBinarySlot.length; j++) {
                            nucleotideBinaryReverseComplementSlotPreRow[j] = nucleotideBinaryReverseComplementSlot[j];
                            nucleotideBinaryReverseComplementSlotPreRow[j] <<= 2;   // --*************** -> ****************--
                        }

                        nucleotideBinaryReverseComplementSlotPreRow[nucleotideBinaryReverseComplementSlotPreRow.length - 1] >>>= 2;  // above in the loop, the last slot left shitted 2 bits
                        nucleotideBinaryReverseComplementSlotPreRow[nucleotideBinaryReverseComplementSlotPreRow.length - 1] <<= 2 * (32 - kmerResidue);
                        nucleotideBinaryReverseComplementSlotPreRow[nucleotideBinaryReverseComplementSlotPreRow.length - 1] |= (1L << 2 * (32 - kmerResidue - 1)); // add C marker
                        // kmerList.add(RowFactory.create(nucleotideBinaryReverseComplementSlotPreRow, -ID, (long) 1));

                        String kmerReadExtracted = BinaryBlocksToString(nucleotideBinarySlotPreRow);
                        String kmerRcReadExtracted = BinaryBlocksToString(nucleotideBinaryReverseComplementSlotPreRow);

                        // update no longer marking forward and rc from read,  negative not necessary
                        // 0 negative is still 0 , switch to long max_value
                       // if (ID==0){
                        //    ID=Long.MAX_VALUE;
                       // }

                        System.out.println("forward k-mer extracted from Read: " + kmerReadExtracted + " ID " + ID + " index: " + ((long) i - param.kmerSize + 1));
                        long b = -ID;
                        // long c = -b;
                       //  System.out.println("reverse k-mer extracted from Read: " + kmerRcReadExtracted + " ID " + b + " index: " + (long) (readLength - i - 1) + " test negative: " + c);
                        System.out.println("reverse k-mer extracted from Read: " + kmerRcReadExtracted + " ID " + ID + " index: " + ((long) i - param.kmerSize + 1));

                        if (compareLongArrayBlocks(nucleotideBinarySlotPreRow, nucleotideBinaryReverseComplementSlotPreRow) == true) {
                            System.out.println("Choose : " + BinaryBlocksToString(nucleotideBinarySlotPreRow));
                            kmerList.add(RowFactory.create(nucleotideBinarySlotPreRow, ID, (long) (i - param.kmerSize + 1)));  // the number does not matter, as the count is based on units
                        }else{
                            System.out.println("Choose : " + BinaryBlocksToString(nucleotideBinaryReverseComplementSlotPreRow));
                            kmerList.add(RowFactory.create(nucleotideBinaryReverseComplementSlotPreRow, ID, (long) (i - param.kmerSize + 1)));
                            // update 2, index is set to the same forward index
                            // update,  set reverse complement to the same read, as only low bit k-mer is used now. Later both forward and reverse complement mercy k-mer are used
                            // only choose on between forward and RC, so index stay the same (long) (readLength - param.kmerSize - i + param.kmerSize - 1)));
                        }
                    }

                    // reach the first complete K-mer
                   // if (i - param.frontClip >= param.kmerSize - 1) {
                    //    kmerList.add(RowFactory.create(nucleotideBinarySlot, ID, (long) (i - param.kmerSize + 1) ));  // the number does not matter, as the count is based on units
                    //    kmerList.add(RowFactory.create(nucleotideBinaryReverseComplementSlot, -ID, (long) (readLength - param.kmerSize - i + param.kmerSize - 1) ));
                    // }
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
                    // ***C--------
                    for (int j = 0; j < param.kmerSizeResidue; j++) {
                        long shiftedBinary1 = forward[i] >>> (2 * (32 - j));
                        shiftedBinary1 &= 3L;
                        long shiftedBinary2 = reverse[i] >>> (2 * (32 - j));
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
     *
     * @param param
     */
    public void setParam(DefaultParam param) {
        this.param = param;
    }
}
