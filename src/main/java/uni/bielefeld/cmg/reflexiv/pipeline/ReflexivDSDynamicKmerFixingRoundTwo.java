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
public class ReflexivDSDynamicKmerFixingRoundTwo implements Serializable {
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

        Dataset<Row> FixingKmerDSCount;
        StructType ReflexivFixingKmerStruct = new StructType();
        ReflexivFixingKmerStruct= ReflexivFixingKmerStruct.add("k-1", DataTypes.LongType, false);
        ReflexivFixingKmerStruct= ReflexivFixingKmerStruct.add("attribute", DataTypes.LongType, false);
        ReflexivFixingKmerStruct= ReflexivFixingKmerStruct.add("extension", DataTypes.createArrayType(DataTypes.LongType), false);
        ExpressionEncoder<Row> ReflexivFixingKmerEndocer = RowEncoder.apply(ReflexivFixingKmerStruct);

        StructType ContigLongKmerStringStruct = new StructType();
        ContigLongKmerStringStruct = ContigLongKmerStringStruct.add("ID", DataTypes.StringType, false);
        ContigLongKmerStringStruct = ContigLongKmerStringStruct.add("contig", DataTypes.StringType, false);
        ExpressionEncoder<Row> ContigStringEncoder = RowEncoder.apply(ContigLongKmerStringStruct);

        Dataset<Row> ReflexivLongSubKmerStringDS;
        StructType ReflexivLongKmerStringStruct = new StructType();
        ReflexivLongKmerStringStruct = ReflexivLongKmerStringStruct.add("k-1", DataTypes.StringType, false);
        ReflexivLongKmerStringStruct = ReflexivLongKmerStringStruct.add("attribute", DataTypes.StringType, false);
        ReflexivLongKmerStringStruct = ReflexivLongKmerStringStruct.add("extension", DataTypes.StringType, false);
        ExpressionEncoder<Row> ReflexivLongKmerStringEncoder = RowEncoder.apply(ReflexivLongKmerStringStruct);

        Dataset<Tuple2<Row, Long>> markerTuple;
        Dataset<Tuple2<String, Long>> markerTupleString;

        Dataset<Row> FixingFullKmer;
        Dataset<String> FixingFullKmerString;

        StructType markerTupleStruct = new StructType();
        markerTupleStruct = markerTupleStruct.add("kmer", DataTypes.createArrayType(DataTypes.LongType), false);

        ExpressionEncoder<Row> markerTupleEncoder = RowEncoder.apply(markerTupleStruct);

        JavaPairRDD<Row, Long> ContigsRDDIndex;

        Dataset<Row> ContigDS;



        /**
         * loading Kmer counts
         */
        KmerCountDS = spark.read().csv(param.inputKmerPath);

        if (param.partitions > 0) {
            KmerCountDS = KmerCountDS.repartition(param.partitions);
        }

        DynamicKmerBinarizerFromReducedToSubKmer ReducedKmerToSubKmer= new DynamicKmerBinarizerFromReducedToSubKmer();
        FixingKmerDSCount = KmerCountDS.mapPartitions(ReducedKmerToSubKmer, ReflexivFixingKmerEndocer);

        DSExtendFixingKmerLoop FixingKmerExtensionLoop = new DSExtendFixingKmerLoop();

        int iterations=0;
        while (iterations <= param.maximumIteration) {
            iterations++;

            if (iterations >= 30) {
                break;
            } else{
                FixingKmerDSCount  = FixingKmerDSCount.sort("k-1");
                FixingKmerDSCount = FixingKmerDSCount.mapPartitions(FixingKmerExtensionLoop, ReflexivFixingKmerEndocer);
            }
        }
/*
        DSBinaryFixingKmerToFullKmer FixingKmer2FullKmer = new DSBinaryFixingKmerToFullKmer();

        FixingFullKmer = FixingKmerDSCount.mapPartitions(FixingKmer2FullKmer,markerTupleEncoder );
        FixingFullKmer.persist(StorageLevel.DISK_ONLY());

        ContigsRDDIndex = FixingFullKmer.toJavaRDD().zipWithIndex();

        markerTuple = spark.createDataset(ContigsRDDIndex.rdd(), Encoders.tuple(markerTupleEncoder, Encoders.LONG()));

        TagRowContigRDDID DSContigIDLabel = new TagRowContigRDDID();
        ContigDS = markerTuple.flatMap(DSContigIDLabel, ContigStringEncoder);

        ContigDS.write().
                mode(SaveMode.Overwrite).
                format("csv").
                option("compression", "gzip").save(param.outputPath + "/Assembly_intermediate/03FixingAgain");

        spark.stop();
*/

        DSBinaryFixingKmerWithLongExtensionToString SubKmerToString = new DSBinaryFixingKmerWithLongExtensionToString();
        FixingFullKmerString  = FixingKmerDSCount.mapPartitions(SubKmerToString, Encoders.STRING());
        FixingFullKmerString.persist(StorageLevel.DISK_ONLY());

        JavaPairRDD<String, Long> ContigsRDDStringIndex = FixingFullKmerString.toJavaRDD().zipWithIndex();

        markerTupleString = spark.createDataset(ContigsRDDStringIndex.rdd(), Encoders.tuple(Encoders.STRING(), Encoders.LONG()));

        TagStringContigRDDID DSContigIDLabel = new TagStringContigRDDID();
        ContigDS = markerTupleString.flatMap(DSContigIDLabel, ContigStringEncoder);

        ContigDS.persist(StorageLevel.DISK_ONLY());

        ContigDS.write().
                mode(SaveMode.Overwrite).
                format("csv").
                option("compression", "gzip").save(param.outputPath + "/Assembly_intermediate/05FixingAgain");

        DSExtractContigEndsForAlignment extractContigEnds = new DSExtractContigEndsForAlignment();
        Dataset<String> ContigEnds = ContigDS.mapPartitions(extractContigEnds, Encoders.STRING());

        // ContigEnds = ContigEnds.coalesce(1);

        JavaRDD<String> ContigEndsRDD = ContigEnds.toJavaRDD();

        ContigEndsRDD.saveAsTextFile(param.outputPath + "/Assembly_intermediate/06ContigEnds", GzipCodec.class);

        spark.stop();
    }

    class DSExtractContigEndsForAlignment implements MapPartitionsFunction<Row, String>, Serializable{
        List<String> ContigEnds = new ArrayList<>();

        public Iterator<String> call(Iterator<Row> sIterator) throws Exception{
            while (sIterator.hasNext()){
                Row s= sIterator.next();

                String ID = s.getString(0);
                String Seq = s.getString(1);

                int Length = Seq.length();

                if (Length >= 400){
                    String prefix = Seq.substring(0,200);
                    String suffix = Seq.substring(Length-200);

                    ContigEnds.add(ID+"-L\n"+prefix);
                    ContigEnds.add(ID+"-R\n"+suffix);
                }else{
                    ContigEnds.add(ID+"\n"+Seq);
                }
            }


            return ContigEnds.iterator();
        }
    }

    class DSBinaryFixingKmerWithLongExtensionToString implements MapPartitionsFunction<Row, String>, Serializable{
        List<String> reflexivKmerStringList = new ArrayList<String>();

        long[] subKmerArray;
        String attributeString;


        public Iterator<String> call(Iterator<Row> sIterator) throws Exception {
            while (sIterator.hasNext()) {
                String subKmer = "";
                String extension ="";
                Row s = sIterator.next();

                subKmerArray=new long[1];
                subKmerArray[0]=s.getLong(0);
                /*
                if (getReflexivMarker(s.getLong(1)) ==1){
                    combinedArray = combineTwoLongBlocks( subKmerArray, seq2array(s.getSeq(2)));
                }else{
                    combinedArray = combineTwoLongBlocks( seq2array(s.getSeq(2)), subKmerArray );
                }
*/

                subKmer = BinaryBlocksToString(subKmerArray);


                if (s.get(2) instanceof  Seq) {
                    extension = BinaryBlocksToString(seq2array(s.getSeq(2)));
                }else{
                    extension = BinaryBlocksToString((long[]) s.get(2));
                }

                String Contig="";
                if (getReflexivMarker(s.getLong(1)) ==1){
                    Contig = subKmer + extension;
                }else{
                    Contig = extension + subKmer;
                }

                if (Contig.length()<2*param.maxKmerSize){
                    continue;
                }

                attributeString = "Contig_"+ Contig.length() + "_" + getLeftMarker(s.getLong(1)) + "_" + getRightMarker(s.getLong(1));
                Contig = attributeString + "," + Contig;

                reflexivKmerStringList.add(
                        Contig
                );
            }
            return reflexivKmerStringList.iterator();
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

    class DynamicKmerBinarizerFromReducedToSubKmer implements MapPartitionsFunction<Row, Row>, Serializable{
        List<Row> kmerList = new ArrayList<Row>();
        Row units;
        String kmer;
        String extension;
        int currentExtensionSize;
        int currentExtensionBlockSize;
        int currentSubKmerSize;
        int currentSubKmerBlockSize;
        long attribute;
        char nucleotide;
        long nucleotideInt;
        //     Long suffixBinary;
        //     Long[] suffixBinaryArray;


        public Iterator<Row> call(Iterator<Row> s) {

            while (s.hasNext()) {
                units = s.next();

                kmer = units.getString(0);
                extension = units.getString(2);

                if (kmer.startsWith("(")) {
                    kmer = kmer.substring(1);
                }

                currentSubKmerSize= kmer.length();
                currentSubKmerBlockSize = (currentSubKmerSize-1)/31+1;

                currentExtensionSize = extension.length();
                currentExtensionBlockSize = (currentExtensionSize-1)/31+1;

            //    if (!kmerSizeCheck(kmer, param.kmerListHash)){continue;} // the kmer length does not fit into any of the kmers in the list.

                if (units.getString(1).endsWith(")")) {
                    String[] attributeStringArray = StringUtils.chop(units.getString(1)).split("\\|");
                    attribute = buildingAlongFromThreeInt(
                            Integer.parseInt(attributeStringArray[0]),Integer.parseInt(attributeStringArray[1]),Integer.parseInt(attributeStringArray[2])
                    );
                    // attribute = Long.parseLong(StringUtils.chop(units.getString(1)));
                } else {
                    String[] attributeStringArray = units.getString(1).split("\\|");
                    attribute = buildingAlongFromThreeInt(
                            Integer.parseInt(attributeStringArray[0]),Integer.parseInt(attributeStringArray[1]),Integer.parseInt(attributeStringArray[2])
                    );
                    // attribute = Long.parseLong(units.getString(1));
                }

                long[] nucleotideBinarySlot = new long[currentSubKmerBlockSize];
                //       Long nucleotideBinary = 0L;

                for (int i = 0; i < currentSubKmerSize; i++) {
                    nucleotide = kmer.charAt(i);
                    if (nucleotide >= 256) nucleotide = 255;
                    nucleotideInt = nucleotideValue(nucleotide);
                    // forward kmer in bits
                    nucleotideInt <<= 2*(32-1-(i%31)); // shift to the left   [ATCGGATCC-,ATCGGATCC-]

                    nucleotideBinarySlot[i / 31] |= nucleotideInt;
                }

                // marking the end of the kmer
                long kmerEndMark = 1L;
                kmerEndMark <<= 2*(32-1-((currentSubKmerSize-1)%31+1));
                nucleotideBinarySlot[currentSubKmerBlockSize-1] |= kmerEndMark; // param.kmerListHash.get(currentKmerSize)] == currentKmerBlockSize


                long[] extensionBinarySlot = new long[currentExtensionBlockSize];

                for (int i = 0; i < currentExtensionSize; i++) {
                    nucleotide = extension.charAt(i);
                    if (nucleotide >= 256) nucleotide = 255;
                    nucleotideInt = nucleotideValue(nucleotide);
                    // forward kmer in bits
                    nucleotideInt <<= 2*(32-1-(i%31)); // shift to the left   [ATCGGATCC-,ATCGGATCC-]

                    extensionBinarySlot[i / 31] |= nucleotideInt;
                }

                kmerEndMark=1L;
                kmerEndMark <<= 2*(32-1-((currentExtensionSize-1)%31+1));
                extensionBinarySlot[currentExtensionBlockSize-1] |= kmerEndMark; // param.kmerListHash.get(currentKmerSize)] == currentKmerBlockSize

                // attribute= onlyChangeReflexivMarker(attribute,1);
                kmerList.add(
                        RowFactory.create(nucleotideBinarySlot[0], attribute, extensionBinarySlot)
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

                //if (currentKmerSizeFromBinaryBlockArray(combinedArray) < 200){
                if (currentKmerSizeFromBinaryBlockArray(combinedArray) < 2*param.maxKmerSize){
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

    class TagStringContigRDDID implements FlatMapFunction<Tuple2<String, Long>, Row>, Serializable {

        List<Row> contigList;

        public Iterator<Row> call(Tuple2<String, Long> s) {

            contigList = new ArrayList<Row>();

            String[] contig = s._1().split(",");

            int length = contig[1].length();

            if (length >= 2*param.maxKmerSize) {
                contigList.add(RowFactory.create(">" + contig[0] + "_" + s._2(), contig[1]));
            }
            //if (length>=200){

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

    class DSExtendFixingKmerLoop implements MapPartitionsFunction<Row, Row>, Serializable {

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
                            if (s.getLong(0) == tmpReflexivKmerExtendList.get(i).getLong(0)) {
                                //   System.out.println("loop array extend. first leftMarker: " + getLeftMarker(s.getLong(1)) + " rightMarker: " + getRightMarker(s.getLong(1)) + " second leftMarker: " + getLeftMarker(tmpReflexivKmerExtendList.get(i).getLong(1)) + " rightMarker: " + getRightMarker(tmpReflexivKmerExtendList.get(i).getLong(1)));
                                if (getReflexivMarker(s.getLong(1))== 1) {
                                    if (getReflexivMarker(tmpReflexivKmerExtendList.get(i).getLong(1)) == 2) {
                                        // residue length
                                        int tmpReflexivKmerSuffixLength = Long.SIZE / 2 - (Long.numberOfTrailingZeros((Long) tmpReflexivKmerExtendList.get(i).getSeq(2).apply(tmpReflexivKmerExtendList.get(i).getSeq(2).size()-1)) / 2 + 1);
                                        // extended overall length
                                        int tmpBlockSize = (tmpReflexivKmerExtendList.get(i).getSeq(2).length() - 1) * 31 + tmpReflexivKmerSuffixLength;
                                        int currentReflexivKmerSuffixLength = Long.SIZE / 2 - (Long.numberOfTrailingZeros((Long) s.getSeq(2).apply(s.getSeq(2).size()-1)) / 2 + 1);
                                        int currentBlockSize = (s.getSeq(2).length() - 1) * 31 + currentReflexivKmerSuffixLength;

                                       if (getLeftMarker(s.getLong(1))< 0 && getRightMarker(tmpReflexivKmerExtendList.get(i).getLong(1))< 0) {
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
                                        } else if (getRightMarker(tmpReflexivKmerExtendList.get(i).getLong(1))>= 0 && getRightMarker(tmpReflexivKmerExtendList.get(i).getLong(1))- currentBlockSize>= 0) {
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

                                       if (getRightMarker(s.getLong(1)) < 0 && getLeftMarker(tmpReflexivKmerExtendList.get(i).getLong(1))< 0) {
                                            reflexivExtend(tmpReflexivKmerExtendList.get(i), s, -1);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else if (getRightMarker(s.getLong(1)) >= 0 && getLeftMarker(tmpReflexivKmerExtendList.get(i).getLong(1)) >= 0) {
                                            reflexivExtend(tmpReflexivKmerExtendList.get(i), s, -1);
                                            tmpReflexivKmerExtendList.remove(i); /* already extended */
                                            break;
                                        } else if (getRightMarker(s.getLong(1))>= 0 && getRightMarker(s.getLong(1))- tmpBlockSize>= 0) {
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
            long[] currentSubKmerArray = new long[1];
            currentSubKmerArray[0]=currentSubKmer.getLong(0);
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
                            RowFactory.create(newReflexivSubKmer[0], attribute, newReflexivLongArray)
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
                            RowFactory.create(newReflexivSubKmer[0], attribute, newReflexivLongArray)
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
            int reflexedPrefixLength = currentKmerSizeFromBinaryBlockArray(seq2array(reflexedSubKmer.getSeq(2)));

            int newSubKmerLength;
            long[] longerSubKmer= new long[1];
            longerSubKmer[0]=forwardSubKmer.getLong(0);
            newSubKmerLength=currentKmerSizeFromBinaryBlockArray(longerSubKmer);

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
                        right = getRightMarker(reflexedSubKmer.getLong(1))-forwardSuffixLength;
                    }

                    attribute = buildingAlongFromThreeInt(randomReflexivMarker, left, right);
                    reflexivKmerConcatList.add(
                            RowFactory.create(newReflexivSubKmer[0],
                                    attribute, newReflexivLongArray
                            )
                    );
                } else {
                    if (getLeftMarker(forwardSubKmer.getLong(1)) > 0) {
                        if (getRightMarker(forwardSubKmer.getLong(1)) >=0) {
                            attribute = buildingAlongFromThreeInt(randomReflexivMarker, bubbleDistance, getRightMarker(forwardSubKmer.getLong(1)));
                        }else{
                            attribute= buildingAlongFromThreeInt(randomReflexivMarker, bubbleDistance, getRightMarker(reflexedSubKmer.getLong(1))-forwardSuffixLength);
                        }
                        reflexivKmerConcatList.add(
                                RowFactory.create(newReflexivSubKmer[0],
                                        attribute, newReflexivLongArray
                                )
                        );
                    } else { // reflexedSubKmer right >0
                        if (getLeftMarker(reflexedSubKmer.getLong(1))>=0) {
                            attribute = buildingAlongFromThreeInt(randomReflexivMarker, getLeftMarker(reflexedSubKmer.getLong(1)), bubbleDistance);
                        }else{
                            attribute = buildingAlongFromThreeInt(randomReflexivMarker, getLeftMarker(forwardSubKmer.getLong(1))-reflexedPrefixLength, bubbleDistance);
                        }
                        reflexivKmerConcatList.add(
                                RowFactory.create(newReflexivSubKmer[0],
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
                        right = getRightMarker(reflexedSubKmer.getLong(1))-forwardSuffixLength;
                    }

                    attribute = buildingAlongFromThreeInt(randomReflexivMarker, left, right);
                    reflexivKmerConcatList.add(
                            RowFactory.create(newForwardSubKmer[0],
                                    attribute, newForwardLongArray
                            )
                    );
                } else {

                    if (getLeftMarker(forwardSubKmer.getLong(1)) > 0) {
                        if (getRightMarker(forwardSubKmer.getLong(1)) >=0) {
                            attribute = buildingAlongFromThreeInt(randomReflexivMarker, bubbleDistance, getRightMarker(forwardSubKmer.getLong(1)));
                        }else{
                            attribute= buildingAlongFromThreeInt(randomReflexivMarker, bubbleDistance, getRightMarker(reflexedSubKmer.getLong(1))-forwardSuffixLength);
                        }
                        reflexivKmerConcatList.add(
                                RowFactory.create(newForwardSubKmer[0],
                                        attribute, newForwardLongArray
                                )
                        );
                    } else { // reflexedSubKmer.getInt(4) >0
                        if (getLeftMarker(reflexedSubKmer.getLong(1))>=0) {
                            attribute = buildingAlongFromThreeInt(randomReflexivMarker, getLeftMarker(reflexedSubKmer.getLong(1)), bubbleDistance);
                        }else{
                            attribute = buildingAlongFromThreeInt(randomReflexivMarker, getLeftMarker(forwardSubKmer.getLong(1))-reflexedPrefixLength, bubbleDistance);
                        }
                        reflexivKmerConcatList.add(
                                RowFactory.create(newForwardSubKmer[0],
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
     * @param param
     */
    public void setParam(DefaultParam param) {
        this.param = param;
    }
}
