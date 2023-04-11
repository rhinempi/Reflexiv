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
import org.apache.spark.storage.StorageLevel;
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
public class ReflexivDSDynamicKmerDedup implements Serializable {
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

        StructType kmerBinaryCountTupleLongStruct = new StructType();
        kmerBinaryCountTupleLongStruct = kmerBinaryCountTupleLongStruct.add("kmer", DataTypes.createArrayType(DataTypes.LongType), false);
        kmerBinaryCountTupleLongStruct = kmerBinaryCountTupleLongStruct.add("count", DataTypes.LongType, false);
        ExpressionEncoder<Row> KmerBinaryCountLongEncoder = RowEncoder.apply(kmerBinaryCountTupleLongStruct);


        StructType kmerFixingStruct = new StructType();
        kmerFixingStruct = kmerFixingStruct.add("kmer", DataTypes.LongType, false);
        kmerFixingStruct = kmerFixingStruct.add("count", DataTypes.IntegerType, false);
        ExpressionEncoder<Row> kmerFixingEncoder = RowEncoder.apply(kmerFixingStruct);

        Dataset<Row> markerKmer;
        StructType ReflexivKmerMarkerStruct = new StructType();
        ReflexivKmerMarkerStruct = ReflexivKmerMarkerStruct.add("kmerBinary", DataTypes.LongType, false);
        ReflexivKmerMarkerStruct = ReflexivKmerMarkerStruct.add("attribute", DataTypes.LongType, false);
        ExpressionEncoder<Row> ReflexivKmerMarkerEncoder = RowEncoder.apply(ReflexivKmerMarkerStruct);

        Dataset<Tuple2<Row, Long>> markerTuple;
        Dataset<Row> markerTupleRow;
        Dataset<Row> FixingFullKmer;
        Dataset<Row> MarkerShorterKmerID;
        Dataset<Row> MarkerIDCount;
        Dataset<Row> markerShortIDRow;
        StructType markerTupleStruct = new StructType();
        markerTupleStruct = markerTupleStruct.add("kmer", DataTypes.createArrayType(DataTypes.LongType), false);

        ExpressionEncoder<Row> markerTupleEncoder = RowEncoder.apply(markerTupleStruct);

        JavaPairRDD<Row, Long> ContigsRDDIndex;
        JavaPairRDD<String, Long> ContigsDSIndex;
        JavaRDD<String> ContigRDD;
        Dataset<String> ContigDS;



        /**
         * loading Kmer counts
         */
        KmerCountDS = spark.read().csv(param.inputKmerPath);

        if (param.partitions > 0) {
            KmerCountDS = KmerCountDS.repartition(param.partitions);
        }

        DynamicKmerBinarizerFromReducedToSubKmer ReducedKmerToSubKmer= new DynamicKmerBinarizerFromReducedToSubKmer();
        markerTupleRow = KmerCountDS.mapPartitions(ReducedKmerToSubKmer, KmerBinaryCountLongEncoder);

        markerTupleRow.persist(StorageLevel.DISK_ONLY());

        ReverseComplementKmerMarkerExtraction MarkerKmerExtract = new ReverseComplementKmerMarkerExtraction();
        markerKmer = markerTupleRow.mapPartitions(MarkerKmerExtract, ReflexivKmerMarkerEncoder);

        markerKmer= markerKmer.sort("kmerBinary");


        DSMarkerKmerSelection MarkerKmerRC = new DSMarkerKmerSelection();
        MarkerShorterKmerID = markerKmer.mapPartitions(MarkerKmerRC, kmerFixingEncoder);

        MarkerIDCount = MarkerShorterKmerID.groupBy("kmer")
                .count()
                .toDF("id","count");

        MarkerIDCount = MarkerIDCount.filter(col("count")
                        .geq(2) // minimal 2 k-mer seed matches
        );

        DSMarkerKmerShorterID getShorterKmerID = new DSMarkerKmerShorterID();
        markerShortIDRow = MarkerIDCount.mapPartitions(getShorterKmerID, KmerBinaryCountLongEncoder );

        markerTupleRow = markerTupleRow.union(markerShortIDRow);
        markerTupleRow.persist(StorageLevel.DISK_ONLY());

        markerTupleRow = markerTupleRow.sort("count");

        DSShorterRCContigSeqAndTargetExtraction RCContigSeqAndTarget = new DSShorterRCContigSeqAndTargetExtraction();

        markerTupleRow = markerTupleRow.mapPartitions(RCContigSeqAndTarget, KmerBinaryCountLongEncoder);

        markerTupleRow = markerTupleRow.sort("count");

        DSShorterRCContigRemoval shorterRemoval=new DSShorterRCContigRemoval();

        FixingFullKmer = markerTupleRow.mapPartitions(shorterRemoval, markerTupleEncoder);

        /**
         * round two, ready go
         */


        ContigsRDDIndex = FixingFullKmer.toJavaRDD().zipWithIndex();

        markerTuple = spark.createDataset(ContigsRDDIndex.rdd(), Encoders.tuple(markerTupleEncoder, Encoders.LONG()));

        DSTupleToDataset tuple2Dataset = new DSTupleToDataset();
        markerTupleRow = markerTuple.mapPartitions(tuple2Dataset, KmerBinaryCountLongEncoder);

        markerTupleRow.persist(StorageLevel.DISK_ONLY());

        ForwardAndReverseComplementKmerMarkerExtraction bothMarkerKmerExtract = new ForwardAndReverseComplementKmerMarkerExtraction();
        markerKmer = markerTupleRow.mapPartitions(bothMarkerKmerExtract, ReflexivKmerMarkerEncoder);

        markerKmer= markerKmer.sort("kmerBinary");

        MarkerShorterKmerID = markerKmer.mapPartitions(MarkerKmerRC, kmerFixingEncoder);

        MarkerIDCount = MarkerShorterKmerID.groupBy("kmer")
                .count()
                .toDF("id","count");

        MarkerIDCount = MarkerIDCount.filter(col("count")
                .geq(2) // minimal 2 k-mer seed matches
        );

        markerShortIDRow = MarkerIDCount.mapPartitions(getShorterKmerID, KmerBinaryCountLongEncoder );

        markerTupleRow = markerTupleRow.union(markerShortIDRow);
        markerTupleRow.persist(StorageLevel.DISK_ONLY());

        markerTupleRow = markerTupleRow.sort("count");

        markerTupleRow = markerTupleRow.mapPartitions(RCContigSeqAndTarget, KmerBinaryCountLongEncoder);

        markerTupleRow = markerTupleRow.sort("count");

        DSShorterForwardAndRCContigRemoval shorterForwardAndRCRemoval=new DSShorterForwardAndRCContigRemoval();
        ContigDS = markerTupleRow.mapPartitions(shorterForwardAndRCRemoval, Encoders.STRING());

        ContigDS.persist(StorageLevel.DISK_ONLY());

        ContigsDSIndex = ContigDS.toJavaRDD().zipWithIndex();

        TagRowContigDSID DSIDLabel = new TagRowContigDSID();
        ContigRDD = ContigsDSIndex.flatMap(DSIDLabel);

        if (param.gzip) {
            ContigRDD.saveAsTextFile(param.outputPath + "/Assembly", GzipCodec.class);
        }else{
            ContigRDD.saveAsTextFile(param.outputPath + "/Assembly");
        }

        spark.stop();
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
                        RowFactory.create( extensionBinarySlot, attribute)
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

    class DSShorterForwardAndRCContigRemoval implements MapPartitionsFunction<Row, String>, Serializable{
        List<String> longerRCContig = new ArrayList<String>();
        List<Row> shortKmer = new ArrayList<Row>();
        Row longestKmer;
        boolean marker =false;
        int currentLength;
        int longestLength;

        public Iterator<String> call(Iterator<Row> sIterator) throws Exception {
            while (sIterator.hasNext()){
                Row s= sIterator.next();

                if (longestKmer==null){
                    longestKmer=s;
                    longestLength=currentKmerSizeFromBinaryBlockArray(seq2array(s.getSeq(0)));
                    continue;
                }

                if (s.getLong(1) == longestKmer.getLong(1)) {
                    currentLength = currentKmerSizeFromBinaryBlockArray(seq2array(s.getSeq(0)));
                    if (currentLength>longestLength){

                        shortKmer.add(longestKmer);
                        longestKmer = s;
                        longestLength=currentLength;
                    }else{
                        shortKmer.add(s);
                    }
                }else{
                    if (shortKmer.size()>0){
                        long[] longestContigBlock = seq2array(longestKmer.getSeq(0));
                        for (int i=0;i<shortKmer.size();i++){
                            longestContigBlock=merge2RCContigs(longestContigBlock,seq2array(shortKmer.get(i).getSeq(0)));
                        }

                        longerRCContig.add(BinaryBlocksToString(longestContigBlock));

                        shortKmer = new ArrayList<Row>();
                    }else {
                        longerRCContig.add(BinaryBlocksToString(seq2array(longestKmer.getSeq(0))));
                    }

                    longestKmer = s;
                    longestLength= currentKmerSizeFromBinaryBlockArray(seq2array(s.getSeq(0)));

                }

            }

            if (shortKmer.size()>0){
                long[] longestContigBlock = seq2array(longestKmer.getSeq(0));
                for (int i=0;i<shortKmer.size();i++){
                    merge2RCContigs(longestContigBlock,seq2array(shortKmer.get(i).getSeq(0)));
                }
                longerRCContig.add(BinaryBlocksToString(longestContigBlock));
            }else{
                longerRCContig.add(BinaryBlocksToString(seq2array(longestKmer.getSeq(0))));
            }

            return longerRCContig.iterator();
        }

        private long[] merge2RCContigs(long[] longContig, long[] RCshortContig) throws Exception {
            List<Integer> distanceList = new ArrayList<Integer>();
            HashMap<Integer, List<Integer>> kmerCoordinates = new HashMap<Integer, List<Integer>>();
            int kmerSeedLength = 15;
            List<Integer> coordinates;


            int longContigLength = currentKmerSizeFromBinaryBlockArray(longContig);

            for (int i=0; i<=longContigLength; i+=kmerSeedLength){
                long[] kmerSeed = leftShiftArray(longContig,i);
                kmerSeed= leftShiftOutFromArray(kmerSeed, kmerSeedLength);
                int kmerSeedInt = (int) (kmerSeed[0] >>> 2*(32-kmerSeedLength));  // remove C marker in the end

                if (kmerCoordinates.containsKey(kmerSeed)){
                    kmerCoordinates.get(kmerSeed).add(i+1);
                }else{
                    coordinates=new ArrayList<Integer>();
                    coordinates.add(i+1);
                    kmerCoordinates.put(kmerSeedInt, coordinates);
                }

            }

            // for forward strand



            int forwardContigLength = currentKmerSizeFromBinaryBlockArray(RCshortContig);

            for (int i=0;i < forwardContigLength; i++){
                long[] kmerQuery = leftShiftArray(RCshortContig, i);
                kmerQuery = leftShiftOutFromArray(kmerQuery, kmerSeedLength);
                int kmerQueryInt= (int) (kmerQuery[0] >>> 2*(32-kmerSeedLength));

                if (kmerCoordinates.containsKey(kmerQueryInt)){
                    for (int seedLocus : kmerCoordinates.get(kmerQueryInt)) {
                        distanceList.add(i+1-seedLocus);
                    }
                }
            }

            Collections.sort(distanceList);

            int totalMatches = distanceList.size();
            int lastDistance=0;
            int lastFrequency=0;
            int finalDistance=-1;

            for (int distance : distanceList){
                if (distance - lastDistance >=-1 && distance - lastDistance <=1  ){
                    lastFrequency++;
                    if ((double)lastFrequency/totalMatches>=0.7 && lastFrequency >=7){ // at least 8 15-mer matches
                        finalDistance=distance;
                        break;
                    }
                }else{
                    lastFrequency=1;
                    lastDistance=distance;
                }
            }

            if (finalDistance==-1){ // not enough 15mer match was found, adding short Contig back to the pool
                // skip and check RC
            }else if (finalDistance == 0){ // one side perfectly aligned
                // return longContig;

            }else if (finalDistance <0){
                //  |--------------------------|            longLength
                //   xxxxxxxxxxxx                           -finalDistance
                //               |------------------|       shortLength

                int shortRightSideFlank = forwardContigLength - (longContigLength + finalDistance);
                if (shortRightSideFlank >0){
                    long[] rightFlankBlock = leftShiftArray(RCshortContig,forwardContigLength-shortRightSideFlank);
                    longContig= combineTwoLongBlocks(longContig, rightFlankBlock);
                    return longContig; // stop and without RC
                }else{
                    // return longContig
                }
            }else{ // finalDistance >0
                //       |--------------------------|       longLength
                //  |----------------|                      shortLength

                long[] leftFlankBlock = leftShiftOutFromArray(RCshortContig, finalDistance);
                longContig= combineTwoLongBlocks(leftFlankBlock, longContig);
                return longContig;
            }


            // for reverse complement

            long[] shortContig = binaryBlockReverseComplementary(RCshortContig);
            int shortContigLength = currentKmerSizeFromBinaryBlockArray(shortContig);

            for (int i=0;i < shortContigLength; i++){
                long[] kmerQuery = leftShiftArray(shortContig, i);
                kmerQuery = leftShiftOutFromArray(kmerQuery, kmerSeedLength);
                int kmerQueryInt= (int) (kmerQuery[0] >>> 2*(32-kmerSeedLength));

                if (kmerCoordinates.containsKey(kmerQueryInt)){
                    for (int seedLocus : kmerCoordinates.get(kmerQueryInt)) {
                        distanceList.add(i+1-seedLocus);
                    }
                }
            }

            Collections.sort(distanceList);

            totalMatches = distanceList.size();
            lastDistance=0;
            lastFrequency=0;
            finalDistance=-1;

            for (int distance : distanceList){
                if (distance - lastDistance >=-1 && distance - lastDistance <=1  ){
                    lastFrequency++;
                    if ((double)lastFrequency/totalMatches>=0.7 && lastFrequency >=7){ // at least 8 15-mer matches
                        finalDistance=distance;
                        break;
                    }
                }else{
                    lastFrequency=1;
                    lastDistance=distance;
                }
            }

            if (finalDistance==-1){ // not enough 15mer match was found, adding short Contig back to the pool
                longerRCContig.add(BinaryBlocksToString(RCshortContig));
                // return longContig;
            }else if (finalDistance == 0){ // one side perfectly aligned
                // return longContig;

            }else if (finalDistance <0){
                //  |--------------------------|            longLength
                //   xxxxxxxxxxxx                           -finalDistance
                //               |------------------|       shortLength

                int shortRightSideFlank = shortContigLength - (longContigLength + finalDistance);
                if (shortRightSideFlank >0){
                    long[] rightFlankBlock = leftShiftArray(shortContig,shortContigLength-shortRightSideFlank);
                    longContig= combineTwoLongBlocks(longContig, rightFlankBlock);

                }else{
                    // return longContig
                }
            }else{ // finalDistance >0
                //       |--------------------------|       longLength
                //  |----------------|                      shortLength

                long[] leftFlankBlock = leftShiftOutFromArray(shortContig, finalDistance);
                longContig= combineTwoLongBlocks(leftFlankBlock, longContig);
            }




            return longContig;
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

    class DSShorterRCContigRemoval implements MapPartitionsFunction<Row, Row>, Serializable{
        List<Row> longerRCContig = new ArrayList<Row>();
        List<Row> shortKmer = new ArrayList<Row>();
        Row longestKmer;
        boolean marker =false;
        int currentLength;
        int longestLength;

        public Iterator<Row> call(Iterator<Row> sIterator) throws Exception {
            while (sIterator.hasNext()){
                Row s= sIterator.next();

                if (longestKmer==null){
                    longestKmer=s;
                    longestLength=currentKmerSizeFromBinaryBlockArray(seq2array(s.getSeq(0)));
                    continue;
                }

                if (s.getLong(1) == longestKmer.getLong(1)) {
                    currentLength = currentKmerSizeFromBinaryBlockArray(seq2array(s.getSeq(0)));
                    if (currentLength>longestLength){

                        shortKmer.add(longestKmer);
                        longestKmer = s;
                        longestLength=currentLength;
                    }else{
                        shortKmer.add(s);
                    }
                }else{
                    if (shortKmer.size()>0){
                        long[] longestContigBlock = seq2array(longestKmer.getSeq(0));
                        for (int i=0;i<shortKmer.size();i++){
                            longestContigBlock=merge2RCContigs(longestContigBlock,seq2array(shortKmer.get(i).getSeq(0)));
                        }

                        longerRCContig.add(RowFactory.create(longestContigBlock));

                        shortKmer = new ArrayList<Row>();
                    }else {
                        longerRCContig.add(RowFactory.create(seq2array(longestKmer.getSeq(0))));
                    }

                    longestKmer = s;
                    longestLength= currentKmerSizeFromBinaryBlockArray(seq2array(s.getSeq(0)));

                }

            }

            if (shortKmer.size()>0){
                long[] longestContigBlock = seq2array(longestKmer.getSeq(0));
                for (int i=0;i<shortKmer.size();i++){
                    merge2RCContigs(longestContigBlock,seq2array(shortKmer.get(i).getSeq(0)));
                }
                longerRCContig.add(RowFactory.create(longestContigBlock));
            }else{
                longerRCContig.add(RowFactory.create(seq2array(longestKmer.getSeq(0))));
            }

            return longerRCContig.iterator();
        }

        private long[] merge2RCContigs(long[] longContig, long[] RCshortContig) throws Exception {
            List<Integer> distanceList = new ArrayList<Integer>();
            HashMap<Integer, List<Integer>> kmerCoordinates = new HashMap<Integer, List<Integer>>();
            int kmerSeedLength = 15;
            List<Integer> coordinates;


            int longContigLength = currentKmerSizeFromBinaryBlockArray(longContig);

            for (int i=0; i<=longContigLength; i+=kmerSeedLength){
                long[] kmerSeed = leftShiftArray(longContig,i);
                kmerSeed= leftShiftOutFromArray(kmerSeed, kmerSeedLength);
                int kmerSeedInt = (int) (kmerSeed[0] >>> 2*(32-kmerSeedLength));  // remove C marker in the end

                if (kmerCoordinates.containsKey(kmerSeed)){
                    kmerCoordinates.get(kmerSeed).add(i+1);
                }else{
                    coordinates=new ArrayList<Integer>();
                    coordinates.add(i+1);
                    kmerCoordinates.put(kmerSeedInt, coordinates);
                }

            }

            long[] shortContig = binaryBlockReverseComplementary(RCshortContig);
            int shortContigLength = currentKmerSizeFromBinaryBlockArray(shortContig);

            for (int i=0;i < shortContigLength; i++){
                long[] kmerQuery = leftShiftArray(shortContig, i);
                kmerQuery = leftShiftOutFromArray(kmerQuery, kmerSeedLength);
                int kmerQueryInt= (int) (kmerQuery[0] >>> 2*(32-kmerSeedLength));

                if (kmerCoordinates.containsKey(kmerQueryInt)){
                    for (int seedLocus : kmerCoordinates.get(kmerQueryInt)) {
                        distanceList.add(i+1-seedLocus);
                    }
                }
            }

            Collections.sort(distanceList);

            int totalMatches = distanceList.size();
            int lastDistance=0;
            int lastFrequency=0;
            int finalDistance=-1;

            for (int distance : distanceList){
                if (distance - lastDistance >=-1 && distance - lastDistance <=1  ){
                    lastFrequency++;
                    if ((double)lastFrequency/totalMatches>=0.7 && lastFrequency >=7){ // at least 8 15-mer matches
                        finalDistance=distance;
                        break;
                    }
                }else{
                    lastFrequency=1;
                    lastDistance=distance;
                }
            }

            if (finalDistance==-1){ // not enough 15mer match was found, adding short Contig back to the pool
                longerRCContig.add(RowFactory.create(RCshortContig));
                // return longContig;
            }else if (finalDistance == 0){ // one side perfectly aligned
                // return longContig;

            }else if (finalDistance <0){
                //  |--------------------------|            longLength
                //   xxxxxxxxxxxx                           -finalDistance
                //               |------------------|       shortLength

                int shortRightSideFlank = shortContigLength - (longContigLength + finalDistance);
                if (shortRightSideFlank >0){
                    long[] rightFlankBlock = leftShiftArray(shortContig,shortContigLength-shortRightSideFlank);
                    longContig= combineTwoLongBlocks(longContig, rightFlankBlock);

                }else{
                    // return longContig
                }
            }else{ // finalDistance >0
                //       |--------------------------|       longLength
                //  |----------------|                      shortLength

                long[] leftFlankBlock = leftShiftOutFromArray(shortContig, finalDistance);
                longContig= combineTwoLongBlocks(leftFlankBlock, longContig);
            }




            return longContig;
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

    class DSMarkerKmerSelection implements MapPartitionsFunction<Row, Row>, Serializable{
        Row LongestKmer = RowFactory.create(1L, 1L);
        List<Row> shorterKmer=new ArrayList<Row>();
        List<Row> contigID = new ArrayList<Row>();
        List<Row> kmerList = new ArrayList<Row>();
        long[] kmerArray = new long[1];


        public Iterator<Row> call(Iterator<Row> sIterator) throws Exception {
            while (sIterator.hasNext()) {
                Row s = sIterator.next();

                int reflexivMarker = getReflexivMarker(s.getLong(1));

                if (reflexivMarker==1){
                    if (s.getLong(0) == LongestKmer.getLong(0)){
                        if (getLeftMarker(s.getLong(1)) > getLeftMarker(LongestKmer.getLong(1))){
                            LongestKmer =s; // new kmer is longer
                        }
                    }else { // a new kmer
                        for (int i = 0; i < shorterKmer.size(); i++) {
                            if (shorterKmer.get(i).getLong(0) == LongestKmer.getLong(0)) {
                                if (getLeftMarker(shorterKmer.get(i).getLong(1)) < getLeftMarker(LongestKmer.getLong(1)) ) {
                                   // kmerArray[0] = shorterKmer.get(i).getLong(0);
                                   // kmerList.add(RowFactory.create(BinaryBlocksToString(kmerArray), String.valueOf(getLeftMarker(shorterKmer.get(i).getLong(1))), String.valueOf(getLeftMarker(LongestKmer.getLong(1))), String.valueOf(getRightMarker(shorterKmer.get(i).getLong(1))),String.valueOf(getRightMarker(LongestKmer.getLong(1)))));

                                    contigID.add(RowFactory.create(buildingAlongFromTwoInt(getRightMarker(shorterKmer.get(i).getLong(1)), getRightMarker(LongestKmer.getLong(1)) ), 1));
                                } else if (getLeftMarker(shorterKmer.get(i).getLong(1)) == getLeftMarker(LongestKmer.getLong(1))) { // equal length, choose the earlier one
                                    if (getRightMarker(shorterKmer.get(i).getLong(1)) > getRightMarker(LongestKmer.getLong(1))) {
                                        //kmerArray[0] = shorterKmer.get(i).getLong(0);
                                       // kmerList.add(RowFactory.create(BinaryBlocksToString(kmerArray), String.valueOf(getLeftMarker(shorterKmer.get(i).getLong(1))), String.valueOf(getLeftMarker(LongestKmer.getLong(1))), String.valueOf(getRightMarker(shorterKmer.get(i).getLong(1))),String.valueOf(getRightMarker(LongestKmer.getLong(1)))));
                                        contigID.add(RowFactory.create(buildingAlongFromTwoInt(getRightMarker(shorterKmer.get(i).getLong(1)), getRightMarker(LongestKmer.getLong(1)) ), 1));
                                    }
                                }
                            } else if (shorterKmer.get(i).getLong(0) == s.getLong(0)) {
                                if (getLeftMarker(shorterKmer.get(i).getLong(1)) < getLeftMarker(s.getLong(1)) ) {
                                   // kmerArray[0] = shorterKmer.get(i).getLong(0);
                                   // kmerList.add(RowFactory.create(BinaryBlocksToString(kmerArray), String.valueOf(getLeftMarker(shorterKmer.get(i).getLong(1))), String.valueOf(getLeftMarker(s.getLong(1))), String.valueOf(getRightMarker(shorterKmer.get(i).getLong(1))),String.valueOf(getRightMarker(s.getLong(1)))));
                                    contigID.add(RowFactory.create(buildingAlongFromTwoInt(getRightMarker(shorterKmer.get(i).getLong(1)), getRightMarker(s.getLong(1)) ), 1));
                                } else if (getLeftMarker(shorterKmer.get(i).getLong(1)) == getLeftMarker(s.getLong(1))) {
                                    if (getRightMarker(shorterKmer.get(i).getLong(1)) > getRightMarker(s.getLong(1))) {
                                     //   kmerArray[0] = shorterKmer.get(i).getLong(0);
                                     //   kmerList.add(RowFactory.create(BinaryBlocksToString(kmerArray), String.valueOf(getLeftMarker(shorterKmer.get(i).getLong(1))), String.valueOf(getLeftMarker(s.getLong(1))), String.valueOf(getRightMarker(shorterKmer.get(i).getLong(1))),String.valueOf(getRightMarker(s.getLong(1)))));
                                        contigID.add(RowFactory.create(buildingAlongFromTwoInt(getRightMarker(shorterKmer.get(i).getLong(1)), getRightMarker(s.getLong(1)) ), 1));
                                    }
                                }
                            }
                        }
                        LongestKmer = s;
                        shorterKmer = new ArrayList<Row>();

                    }
                }else{ // RC k-mer
                    shorterKmer.add(s);

                }

            }

            for (int i=0; i<shorterKmer.size(); i++){
                if (shorterKmer.get(i).getLong(0) == LongestKmer.getLong(0) ) {
                    if (getLeftMarker(shorterKmer.get(i).getLong(1)) < getLeftMarker(LongestKmer.getLong(1)) ) {
                      //  kmerArray[0]= shorterKmer.get(i).getLong(0);
                      //  kmerList.add(RowFactory.create(BinaryBlocksToString(kmerArray), String.valueOf(getLeftMarker(shorterKmer.get(i).getLong(1))), String.valueOf(getLeftMarker(LongestKmer.getLong(1))), String.valueOf(getRightMarker(shorterKmer.get(i).getLong(1))),String.valueOf(getRightMarker(LongestKmer.getLong(1)))));
                        contigID.add(RowFactory.create(buildingAlongFromTwoInt(getRightMarker(shorterKmer.get(i).getLong(1)), getRightMarker(LongestKmer.getLong(1)) ),1) );
                    }else if (getLeftMarker(shorterKmer.get(i).getLong(1)) == getLeftMarker(LongestKmer.getLong(1))){
                        if (getRightMarker(shorterKmer.get(i).getLong(1)) > getRightMarker(LongestKmer.getLong(1))){
                           // kmerArray[0]= shorterKmer.get(i).getLong(0);
                           // kmerList.add(RowFactory.create(BinaryBlocksToString(kmerArray), String.valueOf(getLeftMarker(shorterKmer.get(i).getLong(1))), String.valueOf(getLeftMarker(LongestKmer.getLong(1))), String.valueOf(getRightMarker(shorterKmer.get(i).getLong(1))),String.valueOf(getRightMarker(LongestKmer.getLong(1)))));

                            contigID.add(RowFactory.create(buildingAlongFromTwoInt(getRightMarker(shorterKmer.get(i).getLong(1)), getRightMarker(LongestKmer.getLong(1)) ), 1) );
                            //contigID.add(getRightMarker((int) shorterKmer.get(i).getLong(1)));
                        }
                    }
                }
            }

    //        return kmerList.iterator();
            return contigID.iterator();
        }


        private long buildingAlongFromTwoInt(int leftCover, int rightCover){
            /**
             * shorten the int and change negative to positive to avoid two's complementary
             */
            long info;

            info = ((long) leftCover << 32) ; // move one integer (32 bits) to the left
            info |= ((long) rightCover); //  01--LeftCover---RightCover

            return info;
        }

        private int getLeftMarker(long attribute){
            int leftMarker = (int) (attribute >>> 2*(16)); // 01--xxxx-----xxxx -> 01--xxxx shift out right marker
            int leftMarkerBinaryBits= ~(3 << 30) ; // ---------11 -> 11---------- -> 0011111111111
            leftMarker &= leftMarkerBinaryBits; // remove reflexivMarker

            if (leftMarker>500000000){
                leftMarker=500000000-leftMarker;
            }

            return leftMarker;
        }

        private int getRightMarker(long attribute){
            int rightMarker = (int) attribute;

            if (rightMarker>1000000000){
                rightMarker=1000000000-rightMarker;
            }

            return rightMarker;
        }

        private long binaryLongReverseComplementary(long forward, int currentKmerSize){
            int currentKmerResidue =31;
            long reverseComplement=0;
            long lastTwoBits;

            for (int i = 0; i < currentKmerSize; i++) {
                int RCindex = currentKmerSize - i - 1; //  ------------- ------------- ---------**-- RC index goes reverse
                //  ------------- ------------- -------**----  <--
                // reverseComplement[i / 31] <<= 2;

                if (RCindex >= currentKmerSize - currentKmerResidue) {
                    lastTwoBits = forward >>> 2 * (32-(RCindex % 31)-1);    //  ------------- ------------- ------|----**
                    lastTwoBits &= 3L;
                    lastTwoBits ^= 3L;
                } else { // the same
                    lastTwoBits =  forward >>> 2 * (32 - (RCindex % 31) - 1);
                    lastTwoBits &= 3L;
                    lastTwoBits ^= 3L;
                }

                reverseComplement |= lastTwoBits;
                reverseComplement <<=2; // the order of these two lines are very important

            }
            reverseComplement <<= 2*(32-currentKmerResidue-1); //  ---xxxxxxx -> xxxxxxx--- extra -1 because there are a vacancy from the step above
            reverseComplement|=(1L<<2*(32-currentKmerResidue-1)); // adding ending marker C

            return reverseComplement;

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

    class DSTupleToDataset implements  MapPartitionsFunction<Tuple2<Row, Long>, Row>, Serializable{
        List<Row> MarkerKmerList = new ArrayList<Row>();
        long[] fullKmerArray;

        public Iterator<Row> call(Iterator<Tuple2<Row, Long>> sIterator) throws Exception {
            while (sIterator.hasNext()){
                Tuple2<Row, Long> sTuple =sIterator.next();
                Row s = sTuple._1;
                fullKmerArray = (long[])s.get(0);

                MarkerKmerList.add(RowFactory.create(fullKmerArray,sTuple._2));
            }

            return MarkerKmerList.iterator();
        }

        private long[] seq2array(Seq a){
            long[] array =new long[a.length()];
            for (int i = 0; i < a.length(); i++) {
                array[i] = (Long) a.apply(i);
            }
            return array;
        }
    }

    class ForwardAndReverseComplementKmerMarkerExtraction implements MapPartitionsFunction<Row, Row>, Serializable{
        List<Row> RCMarkerKmerList = new ArrayList<Row>();
        long maxKmerBinary =(~0L) << 2;
        long[] fullKmerArray;
        int kmerLength;
        int MarkerKmerSize =31;

        public Iterator<Row> call(Iterator<Row> sIterator) throws Exception {
            while (sIterator.hasNext()) {
                Row s = sIterator.next();
                fullKmerArray =  seq2array(s.getSeq(0));
                kmerLength = currentKmerSizeFromBinaryBlockArray(fullKmerArray);
                // if (kmerLength >= 300 && kmerLength >= param.minContig) {
                if (kmerLength>=300){
                // if (kmerLength>= 2*param.maxKmerSize){
                    getForwardKmerBinary(fullKmerArray, s.getLong(1), kmerLength);
                    getRCKmerProbBinary(fullKmerArray, s.getLong(1), kmerLength);
                }
            }

            return RCMarkerKmerList.iterator();
        }

        private void getForwardKmerBinary(long[] kmerArray, long ID, int length){
            long seedKmer;

            long attribute = buildingAlongFromThreeInt(1,length, (int) ID); // 1 is forward seeding k-mer


            for (int i= 0; i<kmerArray.length-1; i++){
                seedKmer = ( kmerArray[i] & maxKmerBinary);
                seedKmer|=1L; // add C marker

                RCMarkerKmerList.add(RowFactory.create(seedKmer, attribute));
            }

            if (length % 31==0) {
                seedKmer = kmerArray[kmerArray.length - 1];
                RCMarkerKmerList.add(RowFactory.create(seedKmer, attribute));
            }
        }

        private void getRCKmerProbBinary(long[] kmerArray, long ID, int length) throws Exception {
            long seedKmer;
            long[] seedKmerArray;

            long attribute = buildingAlongFromThreeInt(2, length, (int) ID); // 2 is shorter RC prob k-mer


            if (length>=4000){

                for (int i = 0; i < MarkerKmerSize; i++) {
                    seedKmerArray = leftShiftArray(kmerArray, i);
                    seedKmerArray = leftShiftOutFromArray(seedKmerArray, MarkerKmerSize);
                    seedKmer = seedKmerArray[0];

                    // forward binary
                    RCMarkerKmerList.add(RowFactory.create(seedKmer, attribute));

                    // reverse complement binary
                    seedKmer = binaryLongReverseComplementary(seedKmer, MarkerKmerSize);

                    RCMarkerKmerList.add(RowFactory.create(seedKmer, attribute));
                }


                for (int i = 1000 - MarkerKmerSize+1; i < 1000; i++) {
                    seedKmerArray = leftShiftArray(kmerArray, i);
                    seedKmerArray = leftShiftOutFromArray(seedKmerArray, MarkerKmerSize);
                    seedKmer = seedKmerArray[0];

                    // forward binary
                    RCMarkerKmerList.add(RowFactory.create(seedKmer, attribute));

                    // reverse complement binary
                    seedKmer = binaryLongReverseComplementary(seedKmer, MarkerKmerSize);
                    RCMarkerKmerList.add(RowFactory.create(seedKmer, attribute));
                }


                for (int i = (length - 2 * MarkerKmerSize) / 2; i < (length - 2 * MarkerKmerSize) / 2 + MarkerKmerSize; i++) {
                    seedKmerArray = leftShiftArray(kmerArray, i);
                    seedKmerArray = leftShiftOutFromArray(seedKmerArray, MarkerKmerSize);
                    seedKmer = seedKmerArray[0];

                    // forward binary
                    RCMarkerKmerList.add(RowFactory.create(seedKmer, attribute));

                    // reverse complement binary
                    seedKmer = binaryLongReverseComplementary(seedKmer, MarkerKmerSize);
                    RCMarkerKmerList.add(RowFactory.create(seedKmer, attribute));
                }

                for (int i = length -1000 - MarkerKmerSize +1; i < length - 1000; i++) {
                    seedKmerArray = leftShiftArray(kmerArray, i);
                    seedKmerArray = leftShiftOutFromArray(seedKmerArray, MarkerKmerSize);
                    seedKmer = seedKmerArray[0];

                    // forward binary
                    RCMarkerKmerList.add(RowFactory.create(seedKmer, attribute));

                    // reverse complement binary
                    seedKmer = binaryLongReverseComplementary(seedKmer, MarkerKmerSize);
                    RCMarkerKmerList.add(RowFactory.create(seedKmer, attribute));
                }

                for (int j = length - 2 * MarkerKmerSize; j < length - MarkerKmerSize; j++) {
                    seedKmerArray = leftShiftArray(kmerArray, j);
                    seedKmerArray = leftShiftOutFromArray(seedKmerArray, MarkerKmerSize);
                    seedKmer = seedKmerArray[0];

                    // forward binary
                    RCMarkerKmerList.add(RowFactory.create(seedKmer, attribute));

                    seedKmer = binaryLongReverseComplementary(seedKmer, MarkerKmerSize);
                    RCMarkerKmerList.add(RowFactory.create(seedKmer, attribute));
                }


            }else {

                // --------------------------------
                //                                -------------------------------- // 61 nt
                for (int i = 0; i < MarkerKmerSize; i++) {
                    seedKmerArray = leftShiftArray(kmerArray, i);
                    seedKmerArray = leftShiftOutFromArray(seedKmerArray, MarkerKmerSize);
                    seedKmer = seedKmerArray[0];

                    // forward binary
                    RCMarkerKmerList.add(RowFactory.create(seedKmer, attribute));

                    // reverse complement binary
                    seedKmer = binaryLongReverseComplementary(seedKmer, MarkerKmerSize);


                    RCMarkerKmerList.add(RowFactory.create(seedKmer, attribute));
                }

                for (int i = (length - 2 * MarkerKmerSize) / 3; i < (length - 2 * MarkerKmerSize) / 3 + MarkerKmerSize; i++) {
                    seedKmerArray = leftShiftArray(kmerArray, i);
                    seedKmerArray = leftShiftOutFromArray(seedKmerArray, MarkerKmerSize);
                    seedKmer = seedKmerArray[0];

                    // forward binary
                    RCMarkerKmerList.add(RowFactory.create(seedKmer, attribute));

                    // reverse complement binary
                    seedKmer = binaryLongReverseComplementary(seedKmer, MarkerKmerSize);
                    RCMarkerKmerList.add(RowFactory.create(seedKmer, attribute));
                }

                for (int i = (length - 2 * MarkerKmerSize) * 2 / 3; i < (length - 2 * MarkerKmerSize) * 2 / 3 + MarkerKmerSize; i++) {
                    seedKmerArray = leftShiftArray(kmerArray, i);
                    seedKmerArray = leftShiftOutFromArray(seedKmerArray, MarkerKmerSize);
                    seedKmer = seedKmerArray[0];

                    // forward binary
                    RCMarkerKmerList.add(RowFactory.create(seedKmer, attribute));

                    // reverse complement binary
                    seedKmer = binaryLongReverseComplementary(seedKmer, MarkerKmerSize);
                    RCMarkerKmerList.add(RowFactory.create(seedKmer, attribute));
                }

                for (int j = length - 2 * MarkerKmerSize; j < length - MarkerKmerSize; j++) {
                    seedKmerArray = leftShiftArray(kmerArray, j);
                    seedKmerArray = leftShiftOutFromArray(seedKmerArray, MarkerKmerSize);
                    seedKmer = seedKmerArray[0];

                    // forward binary
                    RCMarkerKmerList.add(RowFactory.create(seedKmer, attribute));

                    seedKmer = binaryLongReverseComplementary(seedKmer, MarkerKmerSize);
                    RCMarkerKmerList.add(RowFactory.create(seedKmer, attribute));
                }
            }




        }

        private long binaryLongReverseComplementary(long forward, int currentKmerSize){
            int currentKmerResidue =31;
            long reverseComplement=0;
            long lastTwoBits;

            for (int i = 0; i < currentKmerSize; i++) {
                int RCindex = currentKmerSize - i - 1; //  ------------- ------------- ---------**-- RC index goes reverse
                //  ------------- ------------- -------**----  <--
                // reverseComplement[i / 31] <<= 2;

                if (RCindex >= currentKmerSize - currentKmerResidue) {
                    lastTwoBits = forward >>> 2 * (32-(RCindex % 31)-1);    //  ------------- ------------- ------|----**
                    lastTwoBits &= 3L;
                    lastTwoBits ^= 3L;
                } else { // the same
                    lastTwoBits =  forward >>> 2 * (32 - (RCindex % 31) - 1);
                    lastTwoBits &= 3L;
                    lastTwoBits ^= 3L;
                }

                reverseComplement |= lastTwoBits;
                reverseComplement <<=2; // the order of these two lines are very important

            }
            reverseComplement <<= 2*(32-currentKmerResidue-1); //  ---xxxxxxx -> xxxxxxx--- extra -1 because there are a vacancy from the step above
            reverseComplement|=(1L<<2*(32-currentKmerResidue-1)); // adding ending marker C

            return reverseComplement;

        }

        private long buildingAlongFromThreeInt(int ReflexivMarker, int leftCover, int rightCover){
            long info = (long) ReflexivMarker <<2*(32-1);  //move to the left most

            /**
             * shorten the int and change negative to positive to avoid two's complementary
             */
            if (leftCover>=500000000){
                leftCover=500000000;
            }else if (leftCover<=-500000000){
                leftCover=500000000-(-500000000);
            }else if (leftCover<0){
                leftCover=500000000-leftCover;
            }

            if (rightCover>=1000000000){
                rightCover=1000000000;
            }else if (rightCover<=-1000000000){
                rightCover=2000000000;
            }else if (rightCover<0){
                rightCover=1000000000-rightCover;
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


        private long[] seq2array(Seq a){
            long[] array =new long[a.length()];
            for (int i = 0; i < a.length(); i++) {
                array[i] = (Long) a.apply(i);
            }
            return array;
        }

    }

    class ReverseComplementKmerMarkerExtraction implements MapPartitionsFunction<Row, Row>, Serializable{
        List<Row> RCMarkerKmerList = new ArrayList<Row>();
        long maxKmerBinary =(~0L) << 2;
        long[] fullKmerArray;
        int kmerLength;
        int MarkerKmerSize =31;

        public Iterator<Row> call(Iterator<Row> sIterator) throws Exception {
            while (sIterator.hasNext()) {
                Row s = sIterator.next();
                fullKmerArray =  seq2array(s.getSeq(0));
                kmerLength = currentKmerSizeFromBinaryBlockArray(fullKmerArray);
                //if (kmerLength >= 2*param.maxKmerSize){
                if (kmerLength>=300){
                // if (kmerLength >= 300 && kmerLength >= param.minContig) {
                    getForwardKmerBinary(fullKmerArray, s.getLong(1), kmerLength);
                    getRCKmerProbBinary(fullKmerArray, s.getLong(1), kmerLength);
                }
            }

            return RCMarkerKmerList.iterator();
        }

        private void getForwardKmerBinary(long[] kmerArray, long ID, int length){
            long seedKmer;

            long attribute = buildingAlongFromThreeInt(1,length, (int) ID); // 1 is forward seeding k-mer


            for (int i= 0; i<kmerArray.length-1; i++){
                seedKmer = ( kmerArray[i] & maxKmerBinary);
                seedKmer|=1L; // add C marker

                RCMarkerKmerList.add(RowFactory.create(seedKmer, attribute));
            }

            if (length % 31==0) {
                seedKmer = kmerArray[kmerArray.length - 1];
                RCMarkerKmerList.add(RowFactory.create(seedKmer, attribute));
            }
        }

        private void getRCKmerProbBinary(long[] kmerArray, long ID, int length) throws Exception {
            long seedKmer;
            long[] seedKmerArray;

            long attribute = buildingAlongFromThreeInt(2, length, (int) ID); // 2 is shorter RC prob k-mer

            // --------------------------------
            //                                -------------------------------- // 61 nt
            for (int i = 0; i < MarkerKmerSize; i++) {
                seedKmerArray = leftShiftArray(kmerArray, i);
                seedKmerArray = leftShiftOutFromArray(seedKmerArray, MarkerKmerSize);
                seedKmer = seedKmerArray[0];

                // reverse complement binary
                seedKmer = binaryLongReverseComplementary(seedKmer, MarkerKmerSize);


                RCMarkerKmerList.add(RowFactory.create(seedKmer, attribute));
            }

            for (int i=(length-2*MarkerKmerSize)/3; i<(length-2*MarkerKmerSize)/3 + MarkerKmerSize ; i++){
                seedKmerArray = leftShiftArray(kmerArray, i);
                seedKmerArray = leftShiftOutFromArray(seedKmerArray, MarkerKmerSize);
                seedKmer = seedKmerArray[0];

                // reverse complement binary
                seedKmer = binaryLongReverseComplementary(seedKmer, MarkerKmerSize);
                RCMarkerKmerList.add(RowFactory.create(seedKmer, attribute));
            }

            for (int i=(length-2*MarkerKmerSize)*2/3; i<(length-2*MarkerKmerSize)*2/3+MarkerKmerSize; i++){
                seedKmerArray = leftShiftArray(kmerArray, i);
                seedKmerArray = leftShiftOutFromArray(seedKmerArray, MarkerKmerSize);
                seedKmer = seedKmerArray[0];

                // reverse complement binary
                seedKmer = binaryLongReverseComplementary(seedKmer, MarkerKmerSize);
                RCMarkerKmerList.add(RowFactory.create(seedKmer, attribute));
            }

            for (int j= length-2*MarkerKmerSize; j<length -MarkerKmerSize; j++){
                seedKmerArray = leftShiftArray(kmerArray, j);
                seedKmerArray =leftShiftOutFromArray(seedKmerArray, MarkerKmerSize);
                seedKmer = seedKmerArray[0];

                seedKmer = binaryLongReverseComplementary(seedKmer, MarkerKmerSize);
                RCMarkerKmerList.add(RowFactory.create(seedKmer, attribute));
            }




        }

        private long binaryLongReverseComplementary(long forward, int currentKmerSize){
            int currentKmerResidue =31;
            long reverseComplement=0;
            long lastTwoBits;

            for (int i = 0; i < currentKmerSize; i++) {
                int RCindex = currentKmerSize - i - 1; //  ------------- ------------- ---------**-- RC index goes reverse
                //  ------------- ------------- -------**----  <--
                // reverseComplement[i / 31] <<= 2;

                if (RCindex >= currentKmerSize - currentKmerResidue) {
                    lastTwoBits = forward >>> 2 * (32-(RCindex % 31)-1);    //  ------------- ------------- ------|----**
                    lastTwoBits &= 3L;
                    lastTwoBits ^= 3L;
                } else { // the same
                    lastTwoBits =  forward >>> 2 * (32 - (RCindex % 31) - 1);
                    lastTwoBits &= 3L;
                    lastTwoBits ^= 3L;
                }

                reverseComplement |= lastTwoBits;
                reverseComplement <<=2; // the order of these two lines are very important

            }
            reverseComplement <<= 2*(32-currentKmerResidue-1); //  ---xxxxxxx -> xxxxxxx--- extra -1 because there are a vacancy from the step above
            reverseComplement|=(1L<<2*(32-currentKmerResidue-1)); // adding ending marker C

            return reverseComplement;

        }

        private long buildingAlongFromThreeInt(int ReflexivMarker, int leftCover, int rightCover){
            long info = (long) ReflexivMarker <<2*(32-1);  //move to the left most

            /**
             * shorten the int and change negative to positive to avoid two's complementary
             */
            if (leftCover>=500000000){
                leftCover=500000000;
            }else if (leftCover<=-500000000){
                leftCover=500000000-(-500000000);
            }else if (leftCover<0){
                leftCover=500000000-leftCover;
            }

            if (rightCover>=1000000000){
                rightCover=1000000000;
            }else if (rightCover<=-1000000000){
                rightCover=2000000000;
            }else if (rightCover<0){
                rightCover=1000000000-rightCover;
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


        private long[] seq2array(Seq a){
            long[] array =new long[a.length()];
            for (int i = 0; i < a.length(); i++) {
                array[i] = (Long) a.apply(i);
            }
            return array;
        }

    }

    class DSShorterRCContigSeqAndTargetExtraction implements MapPartitionsFunction<Row, Row>, Serializable{
        List<Row> shorterContigTarget = new ArrayList<Row>();
        Row lastKmer;
        boolean marker =false;

        public Iterator<Row> call(Iterator<Row> sIterator) throws Exception {
            while (sIterator.hasNext()){
                Row s= sIterator.next();

                if (lastKmer==null){
                    lastKmer=s;
                    continue;
                }

                if (s.getLong(1) == lastKmer.getLong(1)) {

                    if (seq2array(s.getSeq(0))[0] == -1L) {
                        shorterContigTarget.add(RowFactory.create(lastKmer.getSeq(0),seq2array(s.getSeq(0))[1]));
                    }else if (seq2array(lastKmer.getSeq(0))[0] == -1L){
                        shorterContigTarget.add(RowFactory.create(s.getSeq(0), seq2array(lastKmer.getSeq(0))[1])); // using target's ID for next step
                    }
                    lastKmer=null;
                }else{
                    shorterContigTarget.add(lastKmer);
                    lastKmer=s;
                }

            }

            if (lastKmer!=null){
                shorterContigTarget.add(lastKmer);
            }

            return shorterContigTarget.iterator();
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

    }


    class DSMarkerKmerShorterID implements MapPartitionsFunction<Row, Row>, Serializable{
        List<Row> kmerList= new ArrayList<Row>();
        int shorterID;
        long[] subKmerArray;


        public Iterator<Row> call(Iterator<Row> s) throws Exception {

            while (s.hasNext()){
                Row subKmer=s.next();

                subKmerArray= new long[2];
                subKmerArray[0]=-1L;

                shorterID = getLeftMarker(subKmer.getLong(0));
                subKmerArray[1]= (long) getRightMarker(subKmer.getLong(0));
                kmerList.add(RowFactory.create(subKmerArray, (long) shorterID));
            }

            return kmerList.iterator();
        }

        private int getRightMarker(long attribute){
            int rightMarker = (int) attribute;

            return rightMarker;
        }

        private int getLeftMarker(long attribute){
            int leftMarker = (int) (attribute >>> 2*(16)); // 01--xxxx-----xxxx -> 01--xxxx shift out right marker

            return leftMarker;
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
        private int currentKmerSizeFromBinaryBlockArray(long[] binaryBlocks){
            int kmerSize;
            int blockSize = binaryBlocks.length;
            kmerSize= (blockSize-1) *31;
            final int suffix0s = Long.numberOfTrailingZeros(binaryBlocks[blockSize - 1]); // ATCG...01---
            int lastMers = Long.SIZE/2-suffix0s/2-1;

            kmerSize+=lastMers;
            return kmerSize;

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

    class TagRowContigDSID implements FlatMapFunction<Tuple2<String, Long>, String>, Serializable {

        List<String> contigList = new ArrayList<String>();

        public Iterator<String> call(Tuple2<String, Long> s) {

            contigList = new ArrayList<String>();

            String contig = s._1();
            int length = contig.length();
            if (length >= param.minContig) {
                String ID = ">Contig-" + length + "-" + s._2();
                String formatedContig = changeLine(contig, length, 10000000);
                contigList.add(ID + "\n" + formatedContig);
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
     *
     * @param param
     */
    public void setParam(DefaultParam param) {
        this.param = param;
    }
}
