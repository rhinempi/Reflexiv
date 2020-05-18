package uni.bielefeld.cmg.reflexiv.pipeline;


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
public class ReflexivDataFrameReAssembleCounter implements Serializable{
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
        Dataset<String> ContigDS;
        Dataset<Row> KmerBinaryDS;
        Dataset<Row> ContigBinaryDS;
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

        ContigDS = spark.read().text(param.inputContigPath).as(Encoders.STRING());

        LoadContigFromText ContigReader= new LoadContigFromText();
        ContigDS = ContigDS.mapPartitions(ContigReader, Encoders.STRING());

        StructType kmerBinaryStruct = new StructType();
        kmerBinaryStruct = kmerBinaryStruct.add("kmerBlocks", DataTypes.LongType, false);
        kmerBinaryStruct = kmerBinaryStruct.add("count", DataTypes.IntegerType, false);
        ExpressionEncoder<Row> kmerBinaryEncoder = RowEncoder.apply(kmerBinaryStruct);

        ReverseComplementKmerBinaryExtractionFromContig DSExtractRCKmerBinaryFromFasta = new ReverseComplementKmerBinaryExtractionFromContig();
        ContigBinaryDS = ContigDS.mapPartitions(DSExtractRCKmerBinaryFromFasta, kmerBinaryEncoder);

        ReverseComplementKmerBinaryExtractionFromDataset DSExtractRCKmerBinaryFromFastq = new ReverseComplementKmerBinaryExtractionFromDataset();
        KmerBinaryDS = FastqDS.mapPartitions(DSExtractRCKmerBinaryFromFastq, kmerBinaryEncoder);

        KmerBinaryDS = KmerBinaryDS.union(ContigBinaryDS);

        DFKmerBinaryCount = KmerBinaryDS.groupBy("kmerBlocks")
                .sum("count")
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

        DFKmerCount.write().
                mode(SaveMode.Overwrite).
                format("csv").
                option("codec", "org.apache.hadoop.io.compress.GzipCodec").
                save(param.outputPath + "/Count_" + param.kmerSize);

        spark.stop();
    }

    class LoadContigFromText implements MapPartitionsFunction<String, String>, Serializable{
        List<String> contigList = new ArrayList<String>();
        String contig="";
        String contigID="";
        int ContigNum=0;

        public Iterator<String> call(Iterator<String> s){
            if (!s.hasNext()){
                return contigList.iterator();
            }

            while (s.hasNext()){
                String line = s.next();

                if (line.startsWith(">") && ContigNum==0){
                    ContigNum++;

                    String[] head = line.split("\\s+");
                    contigID = head[0];
                    contig="";

                }else if (line.startsWith(">") && ContigNum !=0){
                    ContigNum++;
                    if (contig.length() < param.kmerSize){
                        info.readMessage("Contig : " + contigID + "is shorter than a Kmer, skip it.");
                        info.screenDump();
                    }else{
                        contigList.add(contigID + "\n" + contig);
                    }

                    String[] head = line.split("\\s+");
                    contigID = head[0];
                    contig="";
                }else{
                    contig+=line;
                }
            }

            if (ContigNum >0) {
                contigList.add(contigID + "\n" + contig);
                return contigList.iterator();
            }else{
                return contigList.iterator();
            }
        }
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
                for (int i=1; i<=param.kmerSize;i++){
                    Long currentNucleotideBinary = s.getLong(0) >>> 2*(param.kmerSize - i);
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

    class ReverseComplementKmerBinaryExtractionFromContig implements MapPartitionsFunction<String, Row>, Serializable{
        long maxKmerBits= ~((~0L) << (2*param.kmerSize));

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

                if (readLength - param.kmerSize <= 1 ) {
                    continue;
                }

                Long nucleotideBinary = 0L;
                Long nucleotideBinaryReverseComplement = 0L;

                for (int i = 0; i < readLength; i++) {
                    nucleotide = read.charAt(i);
                    if (nucleotide >= 256) nucleotide = 255;
                    nucleotideInt = nucleotideValue(nucleotide);
                    // forward kmer in bits
                    nucleotideBinary <<= 2;
                    nucleotideBinary |= nucleotideInt;
                    if (i >= param.kmerSize) {
                        nucleotideBinary &= maxKmerBits;
                    }

                    // reverse kmer binarizationalitivities :) non English native speaking people making fun of English
                    nucleotideIntComplement = nucleotideInt ^ 3;  // 3 is binary 11; complement: 11(T) to 00(A), 10(G) to 01(C)

                    if (i >= param.kmerSize) {
                        nucleotideBinaryReverseComplement >>>= 2;
                        nucleotideIntComplement <<= 2 * (param.kmerSize - 1);
                    } else {
                        nucleotideIntComplement <<= 2 * (i);
                    }
                    nucleotideBinaryReverseComplement |= nucleotideIntComplement;

                    // reach the first complete K-mer
                    if (i >= param.kmerSize - 1) {
                        if (nucleotideBinary.compareTo(nucleotideBinaryReverseComplement) < 0) {
                            kmerList.add(RowFactory.create(nucleotideBinary, param.minKmerCoverage));
                        } else {
                            kmerList.add(RowFactory.create(nucleotideBinaryReverseComplement, param.minKmerCoverage));
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
     *
     */
    class ReverseComplementKmerBinaryExtractionFromDataset implements MapPartitionsFunction<String, Row>, Serializable{
        long maxKmerBits= ~((~0L) << (2*param.kmerSize));

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
                if (units.length <=1){
                    continue;
                }
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
                            kmerList.add(RowFactory.create(nucleotideBinary, 1));
                        } else {
                            kmerList.add(RowFactory.create(nucleotideBinaryReverseComplement, 1));
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
     *
     * @param param
     */
    public void setParam(DefaultParam param) {
        this.param = param;
    }
}
