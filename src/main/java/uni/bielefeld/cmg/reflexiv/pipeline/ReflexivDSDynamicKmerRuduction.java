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
        // MixedReflexivSubkmerDS.unpersist();



       MixedFullKmerDS = MixedFullKmerDS.sort("k");

        ShorterKmerNeutralization SKNeutralizer = new ShorterKmerNeutralization();
        MixedFullKmerDS = MixedFullKmerDS.mapPartitions(SKNeutralizer, ReflexivFullKmerEncoder);

        MixedFullKmerDS.persist(StorageLevel.DISK_ONLY());

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


        spark.stop();
    }

    class DSBinaryFullKmerArrayToStringShort implements MapPartitionsFunction<Row, Row>, Serializable {
        List<Row> reflexivKmerStringList = new ArrayList<Row>();

        public Iterator<Row> call(Iterator<Row> sIterator) {

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

    class RightLongerToShorterComparisonAndNeutralizationPreparation implements MapPartitionsFunction<Row, Row>, Serializable{
        List<Row> kmerList= new ArrayList<Row>();

        public Iterator<Row> call(Iterator<Row> s) throws Exception {
            long[] seqBlocks;
            int subKmerLength;
            long[] subKmer;
            long[] extension=new long[1];
            long attribute;
            long[] combinedBlock;

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

    /**
     *
     * @param param
     */
    public void setParam(DefaultParam param) {
        this.param = param;
    }
}
