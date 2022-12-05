package uni.bielefeld.cmg.reflexiv.pipeline;

import com.fing.compression.fourmc.FourMcCodec;
import com.fing.fourmc.elephantbird.adapter.FourMzEbProtoInputFormat;
import com.fing.mapreduce.FourMcTextInputFormat;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import scala.collection.Seq;
import uni.bielefeld.cmg.reflexiv.util.DefaultParam;
import uni.bielefeld.cmg.reflexiv.util.InfoDumper;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
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
public class ReflexivDataFrameDecompresser implements Serializable{
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

    private SparkConf setSparkConfiguration(){
        SparkConf conf = new SparkConf().setAppName("SparkHit");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.referenceTracking", "false");
        conf.set("spark.kryo.registrator", "uni.bielefeld.cmg.reflexiv.serializer.SparkKryoRegistrator");

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

        info.readMessage("Initiating Spark context ...");
        info.screenDump();
        info.readMessage("Start Spark framework");
        info.screenDump();

        Dataset<String> FastqDS;

        Dataset<Row> FastqBinaryDS;
        StructType readBinaryStruct = new StructType();
        readBinaryStruct = readBinaryStruct.add("read", DataTypes.createArrayType(DataTypes.LongType), false);
        ExpressionEncoder<Row> readBinaryEncoder = RowEncoder.apply(readBinaryStruct);
/*
        Configuration baseConfiguration = new Configuration();
        Job jobConf = Job.getInstance(baseConfiguration);
        JavaPairRDD<LongWritable, Text> FastqPairRDD= sc.newAPIHadoopFile(param.inputFqPath, FourMcTextInputFormat.class, LongWritable.class, Text.class, jobConf.getConfiguration());

        DSInputTupleToString tupleToString= new DSInputTupleToString();

        JavaRDD<String> FastqRDD= FastqPairRDD.mapPartitions(tupleToString);
*/

        FastqDS = spark.read().text(param.inputFqPath).as(Encoders.STRING());



//        FastqDS= spark.createDataset(FastqRDD.rdd(), Encoders.STRING());

        DSFastqFilterWithQual DSFastqFilter = new DSFastqFilterWithQual(); // for sparkhit
        DSFastqFilterOnlySeq DSFastqFilterToSeq = new DSFastqFilterOnlySeq(); // for reflexiv
        FastqDS = FastqDS.map(DSFastqFilter, Encoders.STRING());

        DSFastqUnitFilter FilterDSUnit = new DSFastqUnitFilter();

        FastqDS = FastqDS.filter(FilterDSUnit);

        FirstNFastq extractFirstN = new FirstNFastq();
        FastqDS=FastqDS.mapPartitions(extractFirstN, Encoders.STRING());

//        ReadBinarizer binarizerRead = new ReadBinarizer();

 //       FastqBinaryDS = FastqDS.mapPartitions(binarizerRead, readBinaryEncoder);

        if (param.partitions > 0) {
            FastqDS = FastqDS.repartition(param.partitions);
        }

 //       DSBinaryReadToString readBinary2String = new DSBinaryReadToString();
 //       FastqDS = FastqBinaryDS.mapPartitions(readBinary2String, Encoders.STRING());
        if (param.gzip){
            FastqDS.write().mode(SaveMode.Overwrite).format("text").option("compression", "gzip").save(param.outputPath + "/Read_Repartitioned");
        }else {
            JavaRDD<String> FastqRDD = FastqDS.toJavaRDD();
            FastqRDD.saveAsTextFile(param.outputPath + "/Read_Repartitioned", FourMcCodec.class);
        }


        spark.stop();
    }

    /**
     *
     */
    class DSFastqUnitFilter implements FilterFunction<String>, Serializable{
        public boolean call(String s){
            return s != null;
        }
    }

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
     *
     */
    class DSFastqFilterOnlySeq implements MapFunction<String, String>, Serializable{
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

    class FirstNFastq implements MapPartitionsFunction<String,String>, Serializable{
        List<String> fastLines = new ArrayList<String>();
        String line;
        int readcount=0;
        int lineMark=0;

        public Iterator<String> call(Iterator<String> s) {
            while (s.hasNext() && readcount <= param.readLimit) {
                line = s.next();
                fastLines.add(line);
                readcount++;
            }
            //System.out.println("how many times did you went through me");
            return fastLines.iterator();
        }
    }


    /**
     *
     */
    class ReadBinarizer implements MapPartitionsFunction<String, Row>, Serializable{
        List<Row> kmerList = new ArrayList<Row>();
        String units;
        String kmer;
        int currentKmerSize;
        int currentKmerBlockSize;
        int currentSubKmerSize;
        int currentSubKmerBlockSize;
        char nucleotide;
        long nucleotideInt;
        //     Long suffixBinary;
        //     Long[] suffixBinaryArray;

        public Iterator<Row> call(Iterator<String> s) {

            while (s.hasNext()) {

                units = s.next();

                kmer = units.split("\\n")[1];

                if (kmer.startsWith("(")) {
                    kmer = kmer.substring(1);
                }

                currentKmerSize= kmer.length();
                currentSubKmerSize = currentKmerSize-1;
                currentKmerBlockSize = (currentKmerSize-1)/31+1; // each 31 mer is a block
                currentSubKmerBlockSize = (currentSubKmerSize-1)/31+1;

                long[] nucleotideBinarySlot = new long[currentKmerBlockSize];
                //       Long nucleotideBinary = 0L;

                for (int i = 0; i < currentSubKmerSize; i++) {
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
                nucleotideBinarySlot[currentKmerBlockSize-1] |= kmerEndMark; // param.kmerListHash.get(currentKmerSize)] == currentKmerBlockSize

                kmerList.add(
                        RowFactory.create(nucleotideBinarySlot)
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

    class DSInputTupleToString implements FlatMapFunction<Iterator<Tuple2<LongWritable, Text>>, String>, Serializable {
        List<String> reflexivKmerStringList = new ArrayList<String>();
        String seq;

        public Iterator<String> call(Iterator<Tuple2<LongWritable, Text>> sIterator) throws Exception {
            while (sIterator.hasNext()) {

                Tuple2<LongWritable, Text> s = sIterator.next();
                seq = s._2().toString();

                reflexivKmerStringList.add(
                        seq
                );
            }
            return reflexivKmerStringList.iterator();
        }
    }

    class DSBinaryReadToString implements MapPartitionsFunction<Row, String>, Serializable{
        List<String> reflexivKmerStringList = new ArrayList<String>();
        long[] subKmerArray;

        public Iterator<String> call(Iterator<Row> sIterator) throws Exception {
            while (sIterator.hasNext()) {
                String subKmer = "";

                Row s = sIterator.next();

                subKmerArray= seq2array(s.getSeq(0));

                subKmer = BinaryBlocksToString(subKmerArray);

                reflexivKmerStringList.add(
                        subKmer
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

        private long[] seq2array(Seq a){
            long[] array =new long[a.length()];
            for (int i = 0; i < a.length(); i++) {
                array[i] = (Long) a.apply(i);
            }
            return array;
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
