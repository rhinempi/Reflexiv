package uni.bielefeld.cmg.reflexiv.pipeline;


import com.oracle.jrockit.jfr.DataType;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import uni.bielefeld.cmg.reflexiv.util.DefaultParam;
import uni.bielefeld.cmg.reflexiv.util.InfoDumper;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.spark.sql.functions.col;


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
 * Returns an object for running the Reflexiv counter pipeline.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
 */
public class ReflexivDataFrameCounter64 implements Serializable{
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

    /**
     *
     * @return
     */
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
        Dataset<Row> KmerBinaryDS;
        Dataset<Row> DFKmerBinaryCount;
        Dataset<Row> DFKmerCount;

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

        StructType kmerBinaryStruct = new StructType();
        kmerBinaryStruct = kmerBinaryStruct.add("kmerBlocks", DataTypes.createArrayType(DataTypes.LongType), false);
        kmerBinaryStruct = kmerBinaryStruct.add("count", DataTypes.IntegerType, false);
        ExpressionEncoder<Row> kmerBinaryEncoder = RowEncoder.apply(kmerBinaryStruct);

        ReverseComplementKmerBinaryExtractionFromDataset64 DSExtractRCKmerBinaryFromFastq = new ReverseComplementKmerBinaryExtractionFromDataset64();
        KmerBinaryDS = FastqDS.mapPartitions(DSExtractRCKmerBinaryFromFastq, kmerBinaryEncoder);

       // PrintElement ElementPrinter = new PrintElement();
      //  KmerBinaryDS = KmerBinaryDS.mapPartitions(ElementPrinter, kmerBinaryEncoder);


      //  KmerBinaryDS = KmerBinaryDS.sort("kmerBlocks");
      //  KmerBinaryDS.cache();

     //   KmerBinaryDS = KmerBinaryDS.mapPartitions(ElementPrinter, kmerBinaryEncoder);

        DFKmerBinaryCount = KmerBinaryDS.groupBy("kmerBlocks")
                .count()
                .toDF("kmerBlocks","count");

        if (param.minKmerCoverage >1) {
            DFKmerBinaryCount = DFKmerBinaryCount.filter(col("count")
                    .geq(param.minKmerCoverage));
        }

        if (param.maxKmerCoverage < 10000000){
            DFKmerBinaryCount = DFKmerBinaryCount.filter(col("count")
                    .leq(param.maxKmerCoverage));
        }

        DSBinaryKmerToString BinaryKmerToString = new DSBinaryKmerToString();

        StructType kmerCountTupleStruct = new StructType();
        kmerCountTupleStruct= kmerCountTupleStruct.add("kmer", DataTypes.StringType, false);
        kmerCountTupleStruct= kmerCountTupleStruct.add("count", DataTypes.LongType, false);
        ExpressionEncoder<Row> kmerCountEncoder = RowEncoder.apply(kmerCountTupleStruct);

        DFKmerCount = DFKmerBinaryCount.mapPartitions(BinaryKmerToString, kmerCountEncoder);

        if (param.gzip) {
            DFKmerCount.write().
                    mode(SaveMode.Overwrite).
                    format("csv").
                    option("codec", "org.apache.hadoop.io.compress.GzipCodec").
                    save(param.outputPath + "/Count_" + param.kmerSize);
        }else{
            DFKmerCount.write().
                    mode(SaveMode.Overwrite).
                    format("csv").
                    save(param.outputPath + "/Count_" + param.kmerSize);
        }

        spark.stop();
    }

    /**
     *
     */
    class DSBinaryKmerToString implements MapPartitionsFunction<Row, Row>, Serializable{
        List<Row> reflexivKmerStringList = new ArrayList<Row>();

        public Iterator<Row> call(Iterator<Row> sIterator){
            while (sIterator.hasNext()){
                String subKmer = "";
                Row s = sIterator.next();

                for (int i=0; i<(param.kmerSize / 32) *32;i++){
                    Long currentNucleotideBinary = (Long)s.getSeq(0).apply(i/32) >>> 2*(31-i%32);

                    currentNucleotideBinary &= 3L;
                    char currentNucleotide =  BinaryToNucleotide(currentNucleotideBinary);
                    subKmer += currentNucleotide;
                }

                for (int i=(param.kmerSize /32)*32; i<param.kmerSize; i++){
                    Long currentNucleotideBinary = (Long)s.getSeq(0).apply(i/32) >>> 2*(param.kmerSizeResidue-1-i%32);

                    currentNucleotideBinary &= 3L;
                    char currentNucleotide =  BinaryToNucleotide(currentNucleotideBinary);
                    subKmer += currentNucleotide;
                }

                reflexivKmerStringList.add (
                        RowFactory.create(subKmer, s.getLong(1))
                        // new Row(); Tuple2<String, Integer>(subKmer, s._2)
                );
            }
            return reflexivKmerStringList.iterator();
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

    /**
     *
     */
    class DSFastqUnitFilter implements FilterFunction<String>, Serializable{
        public boolean call(String s){
            return s != null;
        }
    }

    /**
     *
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

    class PrintElement implements MapPartitionsFunction<Row, Row>, Serializable{
        List<Row> theSameList = new ArrayList<Row>();

        public Iterator<Row> call(Iterator<Row> s){
            while (s.hasNext()){
                Row sIterator = s.next();
              //  Long a = (Long)sIterator.getSeq(0).apply(0);
              //  Long b = (Long)sIterator.getSeq(0).apply(1);

            //    System.out.println(sIterator.getList(0).get(0));
            //    System.out.println(sIterator.getInt(1));

                theSameList.add(sIterator);
            }
            return theSameList.iterator();
        }
    }

    /**
     *
     */

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


    /**
     *
     */
    /*
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
    */

    /**
     *
     * @param param
     */
    public void setParam(DefaultParam param) {
        this.param = param;
    }
}
