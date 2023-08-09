package uni.bielefeld.cmg.reflexiv.pipeline;


import com.fing.mapreduce.FourMcTextInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
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
import scala.Tuple2;
import uni.bielefeld.cmg.reflexiv.util.DefaultParam;
import uni.bielefeld.cmg.reflexiv.util.InfoDumper;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.shuffle;


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
public class ReflexivDataFrameCounter implements Serializable{
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
                .config("spark.cleaner.referenceTracking.cleanCheckpoints", true)
                .config("spark.checkpoint.compress",true)
                .config("spark.sql.shuffle.partitions", String.valueOf(shufflePartitions))
                .config("spark.sql.files.maxPartitionBytes", "6000000")
                .config("spark.sql.adaptive.advisoryPartitionSizeInBytes","8mb")
                .config("spark.driver.maxResultSize","1000g")
                .config("spark.memory.fraction","06")
                .config("spark.network.timeout","60000s")
                .config("spark.executor.heartbeatInterval","20000s")
                .getOrCreate();

        return spark;
    }

    private SparkConf setSparkConfiguration(){
        SparkConf conf = new SparkConf().setAppName("Reflexiv");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.referenceTracking", "false");
        conf.set("spark.kryo.registrator", "uni.bielefeld.cmg.reflexiv.serializer.SparkKryoRegistrator");
        conf.set("spark.cleaner.referenceTracking.cleanCheckpoints", "true");
        conf.set("spark.checkpoint.compress", "true");
        conf.set("spark.hadoop.mapred.max.split.size", "6000000");
        conf.set("spark.sql.files.maxPartitionBytes", "6000000");
        conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes","8mb");
        conf.set("spark.driver.maxResultSize","1000g");
        conf.set("spark.memory.fraction","0.6");
        conf.set("spark.network.timeout","60000s");
        conf.set("spark.executor.heartbeatInterval","20000s");

        return conf;
    }

    /**
     *
     */
    public void assembly() throws IOException {
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


        Dataset<String> FastqDS;
        Dataset<Long> KmerBinaryDS;
        Dataset<Row> DFKmerBinaryCount;
        Dataset<Row> DFKmerCount;

        if (param.inputFormat.equals("4mc")){
            Configuration baseConfiguration = new Configuration();
            // baseConfiguration.setInt("mapred.min.split.size", 6000000);
            // baseConfiguration.setInt("mapred.max.split.size", 6000000);
            Job jobConf = Job.getInstance(baseConfiguration);
            //  sc.hadoopConfiguration().setInt("mapred.max.split.size", 6000000);
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

            /*
            DSFastqFilterWithQual DSFastqFilter = new DSFastqFilterWithQual();
            FastqDS = FastqDS.map(DSFastqFilter, Encoders.STRING());

            DSFastqUnitFilter FilterDSUnit = new DSFastqUnitFilter();

            FastqDS = FastqDS.filter(FilterDSUnit);
            */
        }


        if (param.cache) {
            FastqDS.cache();
        }

        ReverseComplementKmerBinaryExtractionFromDataset DSExtractRCKmerBinaryFromFastq = new ReverseComplementKmerBinaryExtractionFromDataset();
        KmerBinaryDS = FastqDS.mapPartitions(DSExtractRCKmerBinaryFromFastq, Encoders.LONG());

        DFKmerBinaryCount = KmerBinaryDS.groupBy("value")
                .count()
                .toDF("kmer","count");

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

                if (seq.length() >= param.maxReadLength){
                    continue;
                }else if (!checkSeq(seq.charAt(0))){
                    continue;
                }
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

    /**
     *
     */
    class DSBinaryKmerToString implements MapPartitionsFunction<Row, Row>, Serializable{
        List<Row> reflexivKmerStringList = new ArrayList<Row>();
        StringBuilder sb;

        public Iterator<Row> call(Iterator<Row> sIterator){
        //    Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        //    System.out.println(timestamp+"RepeatCheck DSBinaryKmerToString: " + param.kmerSize1);

            while (sIterator.hasNext()){
                String subKmer;
                sb= new StringBuilder();
                Row s = sIterator.next();
                for (int i=1; i<=param.kmerSize;i++){
                    Long currentNucleotideBinary = s.getLong(0) >>> 2*(param.kmerSize - i);
                    currentNucleotideBinary &= 3L;
                    char currentNucleotide =  BinaryToNucleotide(currentNucleotideBinary);
                    sb.append(currentNucleotide);
                }

                subKmer =sb.toString();

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
    }

    /**
     *
     */
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
          //  Timestamp timestamp = new Timestamp(System.currentTimeMillis());
           // System.out.println(timestamp+"RepeatCheck ReverseComplementKmerBinaryExtractionFromDataset: " + param.kmerSize1);

            while (s.hasNext()) {
              //  units = s.next().split("\\n");
              //  if (units.length <=1){
              //      continue;
              //  }
                read = s.next();
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
     *
     * @param param
     */
    public void setParam(DefaultParam param) {
        this.param = param;
    }
}
