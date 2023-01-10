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

        markerTupleRow.persist(StorageLevel.MEMORY_AND_DISK());

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
        markerTupleRow.persist(StorageLevel.MEMORY_AND_DISK());

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

        markerTupleRow.persist(StorageLevel.MEMORY_AND_DISK());

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
        markerTupleRow.persist(StorageLevel.MEMORY_AND_DISK());

        markerTupleRow = markerTupleRow.sort("count");

        markerTupleRow = markerTupleRow.mapPartitions(RCContigSeqAndTarget, KmerBinaryCountLongEncoder);

        markerTupleRow = markerTupleRow.sort("count");

        DSShorterForwardAndRCContigRemoval shorterForwardAndRCRemoval=new DSShorterForwardAndRCContigRemoval();
        ContigDS = markerTupleRow.mapPartitions(shorterForwardAndRCRemoval, Encoders.STRING());

        ContigDS.persist(StorageLevel.MEMORY_AND_DISK());

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
                if (kmerLength >= 300 && kmerLength >= param.minContig) {

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
                if (kmerLength >= 300 && kmerLength >= param.minContig) {

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
                                        }else if (getLeftMarker(s.getLong(1))< 0 && getRightMarker(tmpReflexivKmerExtendList.get(i).getLong(1))< 0) {
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
                                        }else if (getRightMarker(s.getLong(1)) < 0 && getLeftMarker(tmpReflexivKmerExtendList.get(i).getLong(1))< 0) {
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
                             //   System.out.println("first array extend. first leftMarker: " + getLeftMarker(s.getLong(1)) + " rightMarker: " + getRightMarker(s.getLong(1)) + " second leftMarker: " + getLeftMarker(tmpReflexivKmerExtendList.get(i).getLong(1)) + " rightMarker: " + getRightMarker(tmpReflexivKmerExtendList.get(i).getLong(1)));
                                if (getReflexivMarker(s.getLong(1)) == 1) {
                                    if (getReflexivMarker(tmpReflexivKmerExtendList.get(i).getLong(1)) == 2) {
                                        int tmpReflexivKmerSuffixLength = Long.SIZE / 2 - (Long.numberOfTrailingZeros(tmpReflexivKmerExtendList.get(i).getLong(2)) / 2 + 1);
                                        int currentReflexivKmerSuffixLength = Long.SIZE / 2 - (Long.numberOfTrailingZeros(s.getLong(2)) / 2 + 1);

                                        int lengthS = currentKmerSizeFromBinaryBlockArray(seq2array(s.getSeq(0)));
                                        int lengthTemp= currentKmerSizeFromBinaryBlockArray(seq2array(tmpReflexivKmerExtendList.get(i).getSeq(0)));

                                        int extraLength=0;
                                        if (lengthTemp< lengthS){
                                            extraLength=lengthS-lengthTemp;
                                        }

                                        if (lengthS<lengthTemp){
                                            singleKmerRandomizer(s);
                                            break;
                                        } else if (getLeftMarker(s.getLong(1))< 0 && getRightMarker(tmpReflexivKmerExtendList.get(i).getLong(1))< 0) {
                                            reflexivExtend(s, tmpReflexivKmerExtendList.get(i), -1);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else if (getLeftMarker(s.getLong(1))>= 0 && getRightMarker(tmpReflexivKmerExtendList.get(i).getLong(1))>= 0) {
                                            reflexivExtend(s, tmpReflexivKmerExtendList.get(i), -1);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else if (getLeftMarker(s.getLong(1))>= 0 && getLeftMarker(s.getLong(1))- tmpReflexivKmerSuffixLength >= 0) {
                                            reflexivExtend(s, tmpReflexivKmerExtendList.get(i), getLeftMarker(s.getLong(1))- tmpReflexivKmerSuffixLength);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else if (getRightMarker(tmpReflexivKmerExtendList.get(i).getLong(1))>= 0 && getRightMarker(tmpReflexivKmerExtendList.get(i).getLong(1)) - currentReflexivKmerSuffixLength -extraLength>= 0) {
                                            reflexivExtend(s, tmpReflexivKmerExtendList.get(i), getRightMarker(tmpReflexivKmerExtendList.get(i).getLong(1))- currentReflexivKmerSuffixLength);
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
                                        int tmpReflexivKmerSuffixLength = Long.SIZE / 2 - (Long.numberOfTrailingZeros(tmpReflexivKmerExtendList.get(i).getLong(2)) / 2 + 1);
                                        int currentReflexivKmerSuffixLength = Long.SIZE / 2 - (Long.numberOfTrailingZeros(s.getLong(2)) / 2 + 1);

                                        int lengthS = currentKmerSizeFromBinaryBlockArray(seq2array(s.getSeq(0)));
                                        int lengthTemp= currentKmerSizeFromBinaryBlockArray(seq2array(tmpReflexivKmerExtendList.get(i).getSeq(0)));

                                        int extraLength=0;
                                        if (lengthS< lengthTemp){
                                            extraLength=lengthTemp-lengthS;
                                        }

                                        if (lengthTemp<lengthS){ // longer kmer overlapped shorter kmer
                                            singleKmerRandomizer(s);
                                            break;
                                        }else if (getRightMarker(s.getLong(1)) < 0 && getLeftMarker(tmpReflexivKmerExtendList.get(i).getLong(1))< 0) {
                                            reflexivExtend(tmpReflexivKmerExtendList.get(i), s, -1);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else if (getRightMarker(s.getLong(1)) >= 0 && getLeftMarker(tmpReflexivKmerExtendList.get(i).getLong(1))>= 0) {
                                            reflexivExtend(tmpReflexivKmerExtendList.get(i), s, -1);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else if (getRightMarker(s.getLong(1)) >= 0 && getRightMarker(s.getLong(1)) - tmpReflexivKmerSuffixLength -extraLength>= 0) {
                                            reflexivExtend(tmpReflexivKmerExtendList.get(i), s, getRightMarker(s.getLong(1)) - tmpReflexivKmerSuffixLength);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else if (getLeftMarker(tmpReflexivKmerExtendList.get(i).getLong(1))>= 0 && getLeftMarker(tmpReflexivKmerExtendList.get(i).getLong(1))- currentReflexivKmerSuffixLength >= 0) {
                                            reflexivExtend(tmpReflexivKmerExtendList.get(i), s, getLeftMarker(tmpReflexivKmerExtendList.get(i).getLong(1)) - currentReflexivKmerSuffixLength);
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

        private boolean dynamicSubKmerComparator(Seq a, Seq b) throws Exception {
            long[] arrayA = seq2array(a);
            long[] arrayB = seq2array(b);

            int aLength= currentKmerSizeFromBinaryBlockArray(arrayA);
            int bLength= currentKmerSizeFromBinaryBlockArray(arrayB);

         //   String arrayAString = BinaryBlocksToString(arrayA);
         //   String arrayBString = BinaryBlocksToString(arrayB);

         //   System.out.println("different comparator: " + arrayAString + " B: " + arrayBString);

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

        private long[] seq2array(Seq a){
            long[] array =new long[a.length()];
            for (int i = 0; i < a.length(); i++) {
                array[i] = (Long) a.apply(i);
            }
            return array;
        }



        private boolean subKmerSlotComparator(Seq a, Seq b) {
            long[] arrayA = seq2array(a);
            long[] arrayB = seq2array(b);

            int aLength= currentKmerSizeFromBinaryBlockArray(arrayA);
            int bLength= currentKmerSizeFromBinaryBlockArray(arrayB);

         //   String arrayAString = BinaryBlocksToString(arrayA);
         //   String arrayBString = BinaryBlocksToString(arrayB);
            if (aLength==bLength){
       //         System.out.println("equal comparator: " + arrayAString + " B: " + arrayBString);

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


        /**
         *
         * @param currentSubKmer
         */
        public void singleKmerRandomizer(Row currentSubKmer) throws Exception {
            long[] currentSubKmerArray = seq2array(currentSubKmer.getSeq(0));

            if (getReflexivMarker(currentSubKmer.getLong(1)) == 1) {
                /**
                 * 00000000000000110010111010010   Long.SIZE
                 * --------------C-G-G-G-T-C-A-G   Long.SIZE - (Long.numberOfLeadingZeros / 2 + 1)
                 * --------------^-Length marker
                 */
         //   String subkmerString = BinaryBlocksToString(currentSubKmerArray);
               // System.out.println("subKmerString: " + subkmerString);


                int currentSuffixLength = Long.SIZE / 2 - (Long.numberOfTrailingZeros(currentSubKmer.getLong(2)) / 2 + 1); // xx01-------
                long[] newReflexivSubKmer; //= new long[param.subKmerBinarySlots];
               // long newReflexivLong;

                if (randomReflexivMarker == 2) {

                    long[] currentSuffixArray = new long[1];
                    currentSuffixArray[0]=currentSubKmer.getLong(2);
                    long[] combinedKmerArray = combineTwoLongBlocks(currentSubKmerArray, currentSuffixArray);

                    newReflexivSubKmer = leftShiftArray(combinedKmerArray, currentSuffixLength);

                    long[] newReflexivLongArray = leftShiftOutFromArray(combinedKmerArray, currentSuffixLength);

                    long attribute = onlyChangeReflexivMarker(currentSubKmer.getLong(1), randomReflexivMarker);

                    reflexivKmerConcatList.add(
                            RowFactory.create(newReflexivSubKmer, attribute, newReflexivLongArray)
                    );

                } else {
                    long[] newReflexivLongArray = new long[1];
                    newReflexivLongArray[0]= currentSubKmer.getLong(2);
                    reflexivKmerConcatList.add(
                            RowFactory.create(currentSubKmer.getSeq(0), currentSubKmer.getLong(1), newReflexivLongArray)
                    );
                }
            } else { /* currentSubKmer._2._1() == 2 */
                int currentPrefixLength = Long.SIZE / 2 - (Long.numberOfTrailingZeros(currentSubKmer.getLong(2)) / 2 + 1);
                long[] newReflexivSubKmer; // = new long[param.subKmerBinarySlots];
                Long newReflexivLong;
                int currentSubKmerSize= currentKmerSizeFromBinaryBlockArray(currentSubKmerArray);

                if (randomReflexivMarker == 2) {
                    long[] newReflexivLongArray = new long[1];
                    newReflexivLongArray[0]= currentSubKmer.getLong(2);
                    reflexivKmerConcatList.add(
                            RowFactory.create(currentSubKmer.getSeq(0), currentSubKmer.getLong(1), newReflexivLongArray)
                    );
                } else { /* randomReflexivMarker == 1 */

                    long[] currentPrefixArray = new long[1];
                    currentPrefixArray[0]= currentSubKmer.getLong(2);
                    long[] combinedKmerArray = combineTwoLongBlocks(currentPrefixArray, currentSubKmerArray);

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
            int forwardSubKmerLength = currentKmerSizeFromBinaryBlockArray(seq2array(forwardSubKmer.getSeq(0)));

            int reflexedPrefixLength = Long.SIZE / 2 - (Long.numberOfTrailingZeros(reflexedSubKmer.getLong(2)) / 2 + 1);
            int reflexedSubKmerLength = currentKmerSizeFromBinaryBlockArray(seq2array(reflexedSubKmer.getSeq(0)));

            long attribute=0;

            int newSubKmerLength;
            long[] longerSubKmer;

            int extraLength=0;
            if (forwardSubKmerLength>reflexedSubKmerLength){
                extraLength=forwardSubKmerLength-reflexedSubKmerLength;
            }

            if (forwardSubKmerLength >= reflexedSubKmerLength){ // In reality, it is always forwardSubKmer longer than or equal to reflexedSubKmer
                newSubKmerLength=forwardSubKmerLength;
                longerSubKmer= seq2array(forwardSubKmer.getSeq(0));
            }else{
                newSubKmerLength=reflexedSubKmerLength;
                longerSubKmer=seq2array(reflexedSubKmer.getSeq(0));
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
                            attribute = buildingAlongFromThreeInt(randomReflexivMarker, bubbleDistance, getRightMarker(reflexedSubKmer.getLong(1))-forwardSuffixLength-extraLength);
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

       //         System.out.println("Prefix " + newReflexivLongArrayString + " combined: " + newReflexivSubKmerString + " reflexivMarker: " + getReflexivMarker(attribute) + " leftMarker: " + getLeftMarker(attribute) + " rightMarker: " + getRightMarker(attribute));

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
                        if (getRightMarker(forwardSubKmer.getLong(1))>=0) {
                            attribute = buildingAlongFromThreeInt(randomReflexivMarker, bubbleDistance, getRightMarker(forwardSubKmer.getLong(1)));
                        }else{
                            attribute = buildingAlongFromThreeInt(randomReflexivMarker, bubbleDistance, getRightMarker(reflexedSubKmer.getLong(1))-forwardSuffixLength-extraLength);
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

            //    String newForwardSubKmerString = BinaryBlocksToString(newForwardSubKmer);
            //    String newForwardLongArrayString = BinaryBlocksToString(newForwardLongArray);

         //       System.out.println("After combine: " + newForwardSubKmerString + " suffix: " + newForwardLongArrayString + " reflexivMarker: " + getReflexivMarker(attribute) + " leftMarker: " + getLeftMarker(attribute) + " rightMarker: " + getRightMarker(attribute));

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

             //   String mergedKmer= BinaryBlocksToString(newBlocks);

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

    }

    /**MarkerKmerBinaryEncoder
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

                                long[] overhang1= new long[1];
                                overhang1[0]=s.getLong(2);
                                long[] overhang2= new long[1];
                                overhang2[0]=tmpReflexivKmerExtendList.get(i).getLong(2);
                            //    System.out.println("to see if it happens. first leftMarker: " + getLeftMarker(s.getLong(1)) + " rightMarker: " + getRightMarker(s.getLong(1)) + " reflexivMarker: " + getReflexivMarker(s.getLong(1)) + " overhang: " + BinaryBlocksToString(overhang1) + " second leftMarker: " + getLeftMarker(tmpReflexivKmerExtendList.get(i).getLong(1)) + " rightMarker: " + getRightMarker(tmpReflexivKmerExtendList.get(i).getLong(1)) + " reflexivMarker: " + getReflexivMarker(tmpReflexivKmerExtendList.get(i).getLong(1))+ " overhang2: "+ BinaryBlocksToString(overhang2));
                                if (getReflexivMarker(s.getLong(1)) == 1) {
                                    if (getReflexivMarker(tmpReflexivKmerExtendList.get(i).getLong(1)) == 2) {
                                        int tmpReflexivKmerSuffixLength = Long.SIZE / 2 - (Long.numberOfTrailingZeros(tmpReflexivKmerExtendList.get(i).getLong(2)) / 2 + 1);
                                        int currentReflexivKmerSuffixLength = Long.SIZE / 2 - (Long.numberOfTrailingZeros(s.getLong(2)) / 2 + 1);

                                        int lengthS = currentKmerSizeFromBinaryBlockArray(seq2array(s.getSeq(0)));
                                        int lengthTemp= currentKmerSizeFromBinaryBlockArray(seq2array(tmpReflexivKmerExtendList.get(i).getSeq(0)));

                                        int extraLength=0;
                                        if (lengthTemp<lengthS){
                                            extraLength=lengthS-lengthTemp;
                                        }

                                        if (lengthS<lengthTemp){  // longer one overlapped the shorter one, not extending until short kmer upgrades.
                                            singleKmerRandomizer(s);
                                            break;
                                        }else if (getLeftMarker(s.getLong(1)) < 0 && getRightMarker(tmpReflexivKmerExtendList.get(i).getLong(1)) < 0) {
                                            reflexivExtend(s, tmpReflexivKmerExtendList.get(i), -1);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else if (getLeftMarker(s.getLong(1))>= 0 && getRightMarker(tmpReflexivKmerExtendList.get(i).getLong(1)) >= 0) {
                                            reflexivExtend(s, tmpReflexivKmerExtendList.get(i), -1);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else if (getLeftMarker(s.getLong(1))>= 0 && getLeftMarker(s.getLong(1))- tmpReflexivKmerSuffixLength >= 0) {
                                            reflexivExtend(s, tmpReflexivKmerExtendList.get(i), getLeftMarker(s.getLong(1))- tmpReflexivKmerSuffixLength);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else if (getRightMarker(tmpReflexivKmerExtendList.get(i).getLong(1)) >= 0 && getRightMarker(tmpReflexivKmerExtendList.get(i).getLong(1)) - currentReflexivKmerSuffixLength -extraLength >= 0) {
                                            reflexivExtend(s, tmpReflexivKmerExtendList.get(i), getRightMarker(tmpReflexivKmerExtendList.get(i).getLong(1)) - currentReflexivKmerSuffixLength); // extraLength will be deducted inside the function
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

                                        int lengthS = currentKmerSizeFromBinaryBlockArray(seq2array(s.getSeq(0)));
                                        int lengthTemp= currentKmerSizeFromBinaryBlockArray(seq2array(tmpReflexivKmerExtendList.get(i).getSeq(0)));

                                        int extraLength=0;
                                        if (lengthS< lengthTemp){
                                            extraLength=lengthTemp-lengthS;
                                        }

                                        if (lengthTemp<lengthS){ // longer kmer overlapped shorter kmer
                                            singleKmerRandomizer(s);
                                            break;
                                        }else if (getRightMarker(s.getLong(1))< 0 && getLeftMarker(tmpReflexivKmerExtendList.get(i).getLong(1)) < 0) {
                                            reflexivExtend(tmpReflexivKmerExtendList.get(i), s, -1);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else if (getRightMarker(s.getLong(1))>= 0 && getLeftMarker(tmpReflexivKmerExtendList.get(i).getLong(1))>= 0) {
                                            reflexivExtend(tmpReflexivKmerExtendList.get(i), s, -1);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else if (getRightMarker(s.getLong(1))>= 0 && getRightMarker(s.getLong(1))- tmpReflexivKmerSuffixLength - extraLength >= 0) {
                                            reflexivExtend(tmpReflexivKmerExtendList.get(i), s, getRightMarker(s.getLong(1))- tmpReflexivKmerSuffixLength); // extraLength will be deducted inside the function
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else if (getLeftMarker(tmpReflexivKmerExtendList.get(i).getLong(1))>= 0 && getLeftMarker(tmpReflexivKmerExtendList.get(i).getLong(1))- currentReflexivKmerSuffixLength >= 0) {
                                            reflexivExtend(tmpReflexivKmerExtendList.get(i), s, getLeftMarker(tmpReflexivKmerExtendList.get(i).getLong(1)) - currentReflexivKmerSuffixLength);
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
            long[] arrayA = seq2array(a);
            long[] arrayB = seq2array(b);

            int aLength= currentKmerSizeFromBinaryBlockArray(arrayA);
            int bLength= currentKmerSizeFromBinaryBlockArray(arrayB);

          //  String arrayAString = BinaryBlocksToString(arrayA);
          //  String arrayBString = BinaryBlocksToString(arrayB);

         //   System.out.println("different comparator: " + arrayAString + " B: " + arrayBString);


            if (aLength>bLength){ // equal should not happen
                long[] shorterVersion = leftShiftOutFromArray(arrayA, bLength);
             //   String longer = BinaryBlocksToString(shorterVersion);
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


        private boolean subKmerSlotComparator(Seq a, Seq b) {
            long[] arrayA = seq2array(a);
            long[] arrayB = seq2array(b);

            int aLength= currentKmerSizeFromBinaryBlockArray(arrayA);
            int bLength= currentKmerSizeFromBinaryBlockArray(arrayB);

        //    String arrayAString = BinaryBlocksToString(arrayA);
        //    String arrayBString = BinaryBlocksToString(arrayB);
            if (aLength==bLength){
        //        System.out.println("equal comparator: " + arrayAString + " B: " + arrayBString);

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


        public void singleKmerRandomizer(Row currentSubKmer) throws Exception {
            long[] currentSubKmerArray = seq2array(currentSubKmer.getSeq(0));

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

                    newReflexivSubKmer = leftShiftArray(currentSubKmerArray, currentSuffixLength);
                    long[] newReflexivLongArray = leftShiftOutFromArray(currentSubKmerArray, currentSuffixLength);
                    newReflexivLong = newReflexivLongArray[0];

                    long[] suffixArray = new long[1];
                    suffixArray[0]= currentSubKmer.getLong(2);
                    newReflexivSubKmer = combineTwoLongBlocks(newReflexivSubKmer, suffixArray);

                    long attribute = onlyChangeReflexivMarker(currentSubKmer.getLong(1), randomReflexivMarker);

 //                   System.out.println("checking singleKmerRandomizer leftMarker 2: " + getLeftMarker(attribute));

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
                int currentSubKmerSize= currentKmerSizeFromBinaryBlockArray(currentSubKmerArray);

                if (randomReflexivMarker == 2) {
                    reflexivKmerConcatList.add(currentSubKmer);
                } else { /* randomReflexivMarker == 1 */

                    int rightShiftRemain = currentSubKmerSize-currentPrefixLength;
                    long[] rightShiftRemainBlocks = leftShiftOutFromArray(currentSubKmerArray, rightShiftRemain);
                    long[] prefixArray = new long[1];
                    prefixArray[0]=currentSubKmer.getLong(2);
                    newReflexivSubKmer = combineTwoLongBlocks(prefixArray, rightShiftRemainBlocks);

                    newReflexivLong = (leftShiftArray(currentSubKmerArray, rightShiftRemain))[0];

                    long attribute = onlyChangeReflexivMarker(currentSubKmer.getLong(1), randomReflexivMarker);

  //                  System.out.println("checking singleKmerRandomizer leftMarker 1: " + getLeftMarker(attribute));

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
            long attribute=0;

            int forwardSuffixLength = Long.SIZE / 2 - (Long.numberOfTrailingZeros(forwardSubKmer.getLong(2)) / 2 + 1);
            int forwardSubKmerLength = currentKmerSizeFromBinaryBlockArray(seq2array(forwardSubKmer.getSeq(0)));

            int reflexedPrefixLength = Long.SIZE / 2 - (Long.numberOfTrailingZeros(reflexedSubKmer.getLong(2)) / 2 + 1);
            int reflexedSubKmerLength = currentKmerSizeFromBinaryBlockArray(seq2array(reflexedSubKmer.getSeq(0)));

            int newSubKmerLength;
            long[] longerSubKmer;

            int extraLength=0;
            if (forwardSubKmerLength>reflexedSubKmerLength){
                extraLength=forwardSubKmerLength-reflexedSubKmerLength;
            }

            if (forwardSubKmerLength >= reflexedSubKmerLength){ // In reality, it is always forwardSubKmer longer than or equal to reflexedSubKmer
                newSubKmerLength=forwardSubKmerLength;
                longerSubKmer= seq2array(forwardSubKmer.getSeq(0));
            }else{
                newSubKmerLength=reflexedSubKmerLength;
                longerSubKmer=seq2array(reflexedSubKmer.getSeq(0));
            }

            long[] reflexedPrefixArray = new long[1];
            reflexedPrefixArray[0] = reflexedSubKmer.getLong(2);

            long[] forwardSuffixArray = new long[1];
            forwardSuffixArray[0] = forwardSubKmer.getLong(2);

            if (randomReflexivMarker == 2) {

                long[] newReflexivSubKmer = combineTwoLongBlocks(longerSubKmer, forwardSuffixArray); // xxxxx xxxxx xxx-- + xxx--- = xxxxx xxxxx xxxxx x----
                long[] newReflexivLongArray= leftShiftOutFromArray(newReflexivSubKmer, forwardSuffixLength); // xxx--  | ---xx xxxxx xxxxx x----

                newReflexivSubKmer = leftShiftArray(newReflexivSubKmer, forwardSuffixLength); // xxxxx xxxxx xxx---
           //     String newForwardSubKmerStringTemp = BinaryBlocksToString(newReflexivSubKmer);
           //     System.out.println("after leftshiftout: " + newForwardSubKmerStringTemp);
                newReflexivLongArray = combineTwoLongBlocks(reflexedPrefixArray, newReflexivLongArray); // xx--- + xxx--

                if (bubbleDistance < 0) {
         //           System.out.println("before: " + randomReflexivMarker + " left: " + getLeftMarker(reflexedSubKmer.getLong(1)) + " right: " + getRightMarker(forwardSubKmer.getLong(1)));
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
                            RowFactory.create(newReflexivSubKmer, attribute, newReflexivLongArray[0])
                    );
                } else {
                    if (getLeftMarker(forwardSubKmer.getLong(1)) > 0) {
                        if (getRightMarker(forwardSubKmer.getLong(1)) >=0) {
                            attribute = buildingAlongFromThreeInt(randomReflexivMarker, bubbleDistance, getRightMarker(forwardSubKmer.getLong(1)));
                        }else{
                            attribute = buildingAlongFromThreeInt(randomReflexivMarker, bubbleDistance, getRightMarker(reflexedSubKmer.getLong(1))-forwardSuffixLength-extraLength);
                        }
                        reflexivKmerConcatList.add(
                                RowFactory.create(newReflexivSubKmer, attribute, newReflexivLongArray[0])
                        );
                    } else {
                        if (getLeftMarker(reflexedSubKmer.getLong(1))>=0) {
                            attribute = buildingAlongFromThreeInt(randomReflexivMarker, getLeftMarker(reflexedSubKmer.getLong(1)), bubbleDistance-extraLength);
                        }else{
                            attribute = buildingAlongFromThreeInt(randomReflexivMarker, getLeftMarker(forwardSubKmer.getLong(1))-reflexedPrefixLength, bubbleDistance-extraLength);
                        }
                        reflexivKmerConcatList.add(
                                RowFactory.create(newReflexivSubKmer, attribute, newReflexivLongArray[0])
                        );
                    }
                }

             //   String newReflexivSubKmerString = BinaryBlocksToString(newReflexivSubKmer);
            //    String newReflexivLongArrayString = BinaryBlocksToString(newReflexivLongArray);

         //       System.out.println("Prefix " + newReflexivLongArrayString + " combined: " + newReflexivSubKmerString + " reflexivMarker: " + getReflexivMarker(attribute) + " leftMarker: " + getLeftMarker(attribute) + " rightMarker: " + getRightMarker(attribute));

                randomReflexivMarker = 1; /* an action of randomization */
            } else { /* randomReflexivMarker == 1 */

                long[] newForwardSubKmer = combineTwoLongBlocks(reflexedPrefixArray, longerSubKmer); // xx--- + xxxxx xxxxx xx--- = xxxxx xxxxx xxxx-
                long[] newForwardLongArray = leftShiftArray(newForwardSubKmer, newSubKmerLength);  // xxxxx xxxxx xxxx-  -> xx--

                newForwardSubKmer = leftShiftOutFromArray(newForwardSubKmer, newSubKmerLength); // xxxxx xxxxx xxxx- -> xxxxx xxxxx xx---|xx-
                newForwardLongArray = combineTwoLongBlocks(newForwardLongArray, forwardSuffixArray); // xx-- + xxx-- -> xxxxx


                if (bubbleDistance < 0) {
            //        System.out.println("before: " + randomReflexivMarker + " left: " + getLeftMarker(reflexedSubKmer.getLong(1)) + " right: " + getRightMarker(forwardSubKmer.getLong(1)));
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
                            RowFactory.create(newForwardSubKmer, attribute, newForwardLongArray[0])
                    );
                } else {
                    if (getLeftMarker(forwardSubKmer.getLong(1)) > 0) {
                        if (getRightMarker(forwardSubKmer.getLong(1))>=0) {
                            attribute = buildingAlongFromThreeInt(randomReflexivMarker, bubbleDistance, getRightMarker(forwardSubKmer.getLong(1)));
                        }else{
                            attribute = buildingAlongFromThreeInt(randomReflexivMarker, bubbleDistance, getRightMarker(reflexedSubKmer.getLong(1))-forwardSuffixLength-extraLength);
                        }
                        reflexivKmerConcatList.add(
                                RowFactory.create(newForwardSubKmer,
                                        attribute, newForwardLongArray[0]
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
                                        attribute, newForwardLongArray[0]
                                )
                        );
                    }
                }

            //    String newForwardSubKmerString = BinaryBlocksToString(newForwardSubKmer);
             //   String newForwardLongArrayString = BinaryBlocksToString(newForwardLongArray);

            //    System.out.println("After combine: " + newForwardSubKmerString + " suffix: " + newForwardLongArrayString + " reflexivMarker: " + getReflexivMarker(attribute) + " leftMarker: " + getLeftMarker(attribute) + " rightMarker: " + getRightMarker(attribute));

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
            int startingBlockIndex = (shiftingLength)/31; // xxxxxxxxxxx-- xxxxxxx-------
            int nucleotideLength = currentKmerSizeFromBinaryBlockArray(blocks);
            int residueLength = Long.SIZE / 2 - (Long.numberOfTrailingZeros(blocks[blocks.length-1])/2+1); // last block length

       //     String seq= BinaryBlocksToString(blocks);
 //           System.out.println("seq: " + seq + " shiftingLength: " + shiftingLength + " nucleotideLength: " + nucleotideLength + " residueLength: " + residueLength);

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

         //   System.out.println("relativeShiftSize: " + relativeShiftSize + " j: " + j);
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
            //        String rightShift= BinaryBlocksToString(rightBlocksLeftShiftedArray);
                    //  System.out.println("rightShift: " + rightShift);
                }

            //    String mergedKmer= BinaryBlocksToString(newBlocks);

 //               System.out.println(" left Blocks:" + leftBlocksString + " Right blocks: " + rightBlocksString + " rightLength: " + rightNucleotideLength + " leftNucleotideLength: " + leftNucleotideLength + " leftRelativeNTLength: " + leftRelativeNTLength + " leftVacancy: " + leftVacancy + " rightNucleotideLength: " + rightNucleotideLength + " combinedBlockSize: " + combinedBlockSize + " newBlock: " + mergedKmer);
            }

            return newBlocks;
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

            if (getReflexivMarker(currentSubKmer.getLong(1)) == 1) {
                /**
                 * 00000000000000110010111010010   Long.SIZE
                 * --------------C-G-G-G-T-C-A-G   Long.SIZE - (Long.numberOfLeadingZeros / 2 + 1)
                 * --------------^-Length marker
                 */

                int currentSuffixLength = Long.SIZE / 2 - (Long.numberOfTrailingZeros(currentSubKmer.getLong(2)) / 2 + 1); // xx01-------
                long[] newReflexivSubKmer; //= new long[param.subKmerBinarySlots];
                long newReflexivLong;

            //    String seq= BinaryBlocksToString((long[])currentSubKmer.get(0));
                long[] reflexivKmerArray = new long[1];
                reflexivKmerArray[0]=currentSubKmer.getLong(2);
            //    String reflexivKmerString= BinaryBlocksToString(reflexivKmerArray);
                int leftMarker = getLeftMarker(currentSubKmer.getLong(1));
                int rightMarker = getRightMarker(currentSubKmer.getLong(1));
                int reflexivMarker = getReflexivMarker(currentSubKmer.getLong(1));
              //  System.out.println("Seq: " + seq + " reflexivMarker: " + reflexivMarker + " leftMarker: " + leftMarker + " rightMarker: " + rightMarker + " extension: " + reflexivKmerString);

                if (randomReflexivMarker == 2) {

                    newReflexivSubKmer = leftShiftArray((long[])currentSubKmer.get(0), currentSuffixLength);
             //       String new1 = BinaryBlocksToString(newReflexivSubKmer);
               //     System.out.println("leftShiftArray: " + new1);
                    long[] newReflexivLongArray = leftShiftOutFromArray((long[])currentSubKmer.get(0), currentSuffixLength);
                    newReflexivLong = newReflexivLongArray[0];

                    long[] suffixArray = new long[1];
                    suffixArray[0]= currentSubKmer.getLong(2);
                    newReflexivSubKmer = combineTwoLongBlocks(newReflexivSubKmer, suffixArray);
            //        String new2 = BinaryBlocksToString(newReflexivSubKmer);
                 //   System.out.println("combineTwoLongBlocks: " + new2);

                    long attribute = onlyChangeReflexivMarker(currentSubKmer.getLong(1), randomReflexivMarker);

               //     String newSeq = BinaryBlocksToString(newReflexivSubKmer);
                    long[] newReflexivKmerArray = new long[1];
                    newReflexivKmerArray[0]=newReflexivLong;
             //       String newReflexivKmerString = BinaryBlocksToString(newReflexivKmerArray);

                //    System.out.println("new Seq: " + newSeq + " extension: " + newReflexivKmerString);

                    reflexivKmerConcatList.add(
                            RowFactory.create(newReflexivSubKmer, attribute, newReflexivLong)
                    );

                } else {
                    reflexivKmerConcatList.add(currentSubKmer);
                }
            } else { /* currentSubKmer._2._1() == 2 */
                int currentPrefixLength = Long.SIZE / 2 - (Long.numberOfTrailingZeros(currentSubKmer.getLong(2)) / 2 + 1);
                long[] newReflexivSubKmer; // = new long[param.subKmerBinarySlots];
                long newReflexivLong;
                int currentSubKmerSize= currentKmerSizeFromBinaryBlockArray((long[])currentSubKmer.get(0));

           //     String seq= BinaryBlocksToString((long[])currentSubKmer.get(0));
              //  System.out.println("Seq: " + seq + " attribute: " + currentSubKmer.getLong(1)  + " extension: " + currentSubKmer.getLong(2));

                if (randomReflexivMarker == 2) {
                    reflexivKmerConcatList.add(currentSubKmer);
                } else { /* randomReflexivMarker == 1 */

                    int rightShiftRemain = currentSubKmerSize-currentPrefixLength;
                    long[] rightShiftRemainBlocks = leftShiftOutFromArray((long[])currentSubKmer.get(0), rightShiftRemain);
                    long[] prefixArray = new long[1];
                    prefixArray[0]=currentSubKmer.getLong(2);
              //      String prefixArrayString = BinaryBlocksToString(prefixArray);
              //      String rightShiftRemainBlocksString = BinaryBlocksToString(rightShiftRemainBlocks);
                //    System.out.println("prefix: " + prefixArrayString + " rightsHIFTremainBlocks: " + rightShiftRemainBlocksString + " rightShiftRemain: " + rightShiftRemain + " currentSubKmerSize: "+ currentSubKmerSize + " currentPrefixLength: " + currentPrefixLength);

                    newReflexivSubKmer = combineTwoLongBlocks(prefixArray, rightShiftRemainBlocks);

                    newReflexivLong = (leftShiftArray((long[])currentSubKmer.get(0), rightShiftRemain))[0];

                    long attribute = onlyChangeReflexivMarker(currentSubKmer.getLong(1), randomReflexivMarker);

              //      String newseq= BinaryBlocksToString(newReflexivSubKmer);
                 //   System.out.println("new Seq: "  + newseq + " attribute: " + attribute + " extention:" + newReflexivLong);
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
            int leftRelativeNTLength = (leftNucleotideLength - 1) % 31 + 1;
            int leftVacancy = 31 - leftRelativeNTLength;
            int rightNucleotideLength = currentKmerSizeFromBinaryBlockArray(rightBlocks);
            int combinedBlockSize = (leftNucleotideLength + rightNucleotideLength - 1) / 31 + 1;
            long[] newBlocks = new long[combinedBlockSize];

            if (rightNucleotideLength==0){
                return leftBlocks;
            }

            if (leftNucleotideLength==0){
                return rightBlocks;
            }

            if (leftVacancy == 0) { // left last block is a perfect block
                for (int i = 0; i < leftBlocks.length; i++) {
                    newBlocks[i] = leftBlocks[i];
                }

                newBlocks[leftBlocks.length - 1] &= (~0L << 2); // remove the last block's C marker

                for (int j = leftBlocks.length; j < combinedBlockSize; j++) {
                    newBlocks[j] = rightBlocks[j - leftBlocks.length];
                }
            } else {
             //   String rightBlocksString = BinaryBlocksToString(rightBlocks);
             //   String leftBlocksString = BinaryBlocksToString(leftBlocks);

                long[] shiftOutBlocks = leftShiftOutFromArray(rightBlocks, leftVacancy); // right shift out for the left. here we only expect one block, because leftVacancy is relative to one block
                for (int i = 0; i < leftBlocks.length; i++) {
                    newBlocks[i] = leftBlocks[i];
                }

                newBlocks[leftBlocks.length - 1] &= (~0L << 2 * (leftVacancy + 1)); // leftVacancy = 32-leftRelativeNTLength-1. This is to remove the C marker
                newBlocks[leftBlocks.length - 1] |= (shiftOutBlocks[0] >>> 2 * (leftRelativeNTLength));
                if (leftBlocks.length < combinedBlockSize) { // this is not the end block, the last 2 bits (C marker) of shift out needs to be removed  ----------C
                    newBlocks[leftBlocks.length - 1] &= (~0L << 2); // remove shift out blocks C marker. apparently, if there is a C marker, this is the last block anyway
                }

                long[] rightBlocksLeftShifted = leftShiftArray(rightBlocks, leftVacancy);

                int k = 0; // rightBlocksLeftShifted index
                for (int j = leftBlocks.length; j < combinedBlockSize; j++) { // including the last blocks.
                    newBlocks[j] = rightBlocksLeftShifted[k];
                    k++;
                    long[] rightBlocksLeftShiftedArray = new long[1];
                    rightBlocksLeftShiftedArray[0] = rightBlocksLeftShifted[k - 1];
               //     String rightShift = BinaryBlocksToString(rightBlocksLeftShiftedArray);
                    //  System.out.println("rightShift: " + rightShift);
                }

              //  String mergedKmer = BinaryBlocksToString(newBlocks);

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
    }

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
                    reverseComplement[i / 31] <<=2;
                    reverseComplement[i / 31] |= lastTwoBits;
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
            return Long.SIZE/2 - suffix0s/2 -1; // minus one for ending marker
        }

        private int currentKmerSizeFromBinaryBlock(Seq binaryBlocks){
            int kmerSize;
            int blockSize = binaryBlocks.length();
            kmerSize= (blockSize-1) *31;
            final int suffix0s = Long.numberOfTrailingZeros((Long) binaryBlocks.apply(blockSize - 1)); // ATCG...01---
            int lastMers = Long.SIZE/2-suffix0s/2 -1;

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
