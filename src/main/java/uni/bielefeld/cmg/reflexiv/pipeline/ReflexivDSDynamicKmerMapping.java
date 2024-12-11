package uni.bielefeld.cmg.reflexiv.pipeline;


import com.fing.mapreduce.FourMcTextInputFormat;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
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

import java.io.*;
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
public class ReflexivDSDynamicKmerMapping implements Serializable {
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
        conf.set("spark.kryo.referenceTracking", "false");
        conf.set("spark.kryo.registrator", "uni.bielefeld.cmg.reflexiv.serializer.SparkKryoRegistrator");
        conf.set("spark.driver.maxResultSize","1000G");

        return conf;
    }

    private SparkSession setSparkSessionConfiguration(int shufflePartitions) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Reflexiv")
                .config("spark.executor.cores", "1")
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
    public void assemblyFromKmer() throws IOException, InterruptedException {
        info.readMessage("Start Spark framework");
        info.screenDump();
        info.readMessage("Initiating Spark context ...");
        info.screenDump();

        SparkConf conf = setSparkConfiguration();
        JavaSparkContext jsc = new JavaSparkContext(conf);
        jsc.addFile(param.inputContigEnds, true);

        info.readMessage("Initiating Spark Session ...");
        info.screenDump();
        SparkSession spark = setSparkSessionConfiguration(param.shufflePartition);

        String partitionRef = SparkFiles.get("06ContigEnds");
        String minimap2 = SparkFiles.get("minimap2");

        List<String> concatRefCommands = Arrays.asList("/bin/bash",
                "-c",
                String.join(" ",
                        "cat",
                        partitionRef + "/part*",
                        ">",
                        partitionRef + "/contigEnds.fa.gz"
                )
        );

        List<String> buildIndexCommand = Arrays.asList(
                minimap2,
                "-x",
                "sr",
                "-I",
                "55G",
                "-d",
                partitionRef + "/contigEnds.mmi",
                partitionRef + "/contigEnds.fa.gz"
        );

        /**
         * concatenate reference
         */
        ProcessBuilder pb1 = new ProcessBuilder();
        pb1.command(concatRefCommands);

        try {
            Process process1 = pb1.start();
            int exitCode = process1.waitFor();
            if (exitCode != 0) {
                try (BufferedReader errorReader = new BufferedReader(new InputStreamReader(process1.getErrorStream()))) {
                    String errorLine;
                    while ((errorLine = errorReader.readLine()) != null) {
                        System.err.println("Error: " + errorLine);
                    }
                }
                throw new RuntimeException("Process failed with exit code: " + exitCode);
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

        /**
         * build index
         */
        ProcessBuilder pb2 = new ProcessBuilder();
        pb2.command(buildIndexCommand);

        try {
            Process process2 = pb2.start();
            int exitCode = process2.waitFor();
            if (exitCode != 0) {
                try (BufferedReader errorReader = new BufferedReader(new InputStreamReader(process2.getErrorStream()))) {
                    String errorLine;
                    while ((errorLine = errorReader.readLine()) != null) {
                        System.err.println("Error: " + errorLine);
                    }
                }
                throw new RuntimeException("Process failed with exit code: " + exitCode);
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

        jsc.addFile(partitionRef+"/contigEnds.mmi", true);


        // SparkContext sc = spark.sparkContext();
         //   sc.addFile(param.inputContigEnds, true);

       // sc.setCheckpointDir("/tmp/checkpoints");
       // String checkpointDir= sc.getCheckpointDir().get();

        Dataset<Row> KmerCountDS;
        Dataset<String> FastqWithQualDS;
        JavaRDD<String> DSSAMresult;
        Dataset<String> DSStringSAM;
        Dataset<Row> DSSAM;
        Dataset<Row> DSExtendedConsensus;

        StructType SAMSimpleStruct = new StructType();
        SAMSimpleStruct=SAMSimpleStruct.add("contig", DataTypes.StringType, false);
        SAMSimpleStruct=SAMSimpleStruct.add("start", DataTypes.IntegerType, false);
        SAMSimpleStruct=SAMSimpleStruct.add("tag", DataTypes.StringType, false);
        SAMSimpleStruct=SAMSimpleStruct.add("seq", DataTypes.StringType, false);
        ExpressionEncoder<Row> SAMSimpleEncoder = RowEncoder.apply(SAMSimpleStruct);

        StructType ContigLongKmerStringStruct = new StructType();
        ContigLongKmerStringStruct = ContigLongKmerStringStruct.add("ID", DataTypes.StringType, false);
        ContigLongKmerStringStruct = ContigLongKmerStringStruct.add("contig", DataTypes.StringType, false);
        ExpressionEncoder<Row> ContigStringEncoder = RowEncoder.apply(ContigLongKmerStringStruct);

        Dataset<Tuple2<String, Long>> markerTupleString;

        Dataset<String> FixingFullKmerString;

        JavaPairRDD<Row, Long> ContigsRDDIndex;

        Dataset<Row> OldContigDS;
        Dataset<Row> ContigDS;

        Dataset<String> FastqDS;

        if (param.inputFormat.equals("4mc")){
            Configuration baseConfiguration = new Configuration();

            Job jobConf = Job.getInstance(baseConfiguration);
            //  sc.hadoopConfiguration().setInt("mapred.max.split.size", 6000000);
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

        DSInputSeqToFastq seq2Fastq = new DSInputSeqToFastq();
        FastqWithQualDS = FastqDS.mapPartitions(seq2Fastq, Encoders.STRING());

        DSJavaPipeMinimap2 myPipeMinimap2 = new DSJavaPipeMinimap2();
        DSSAMresult=FastqWithQualDS.toJavaRDD().mapPartitions(myPipeMinimap2);

        DSStringSAM = spark.createDataset(DSSAMresult.rdd(), Encoders.STRING());

        SAMString2ROW String2Row = new SAMString2ROW();

        DSSAM = DSStringSAM.mapPartitions(String2Row, SAMSimpleEncoder );

        DSSAM = DSSAM.sort("contig");

        /*
        DSSAM.persist(StorageLevel.DISK_ONLY());

        DSSAM.write().
                mode(SaveMode.Overwrite).
                format("csv").
                option("compression", "gzip").save(param.outputPath + "/Assembly_intermediate/007EndExtend");
*/

        OldContigDS = spark.read().csv(param.inputKmerPath);

        OldContig2Row Contig2Row = new OldContig2Row();
        OldContigDS = OldContigDS.mapPartitions(Contig2Row, ContigStringEncoder);

        DSProcessSAMandExtendContigs SAMandExtend = new DSProcessSAMandExtendContigs();
        DSExtendedConsensus = DSSAM.mapPartitions(SAMandExtend, ContigStringEncoder);

        OldContigDS = DSExtendedConsensus.union(OldContigDS);
        OldContigDS.persist(StorageLevel.DISK_ONLY());

        OldContigDS = OldContigDS.sort("ID");


        DSExtendContigs extendContigs = new DSExtendContigs();
        FixingFullKmerString = OldContigDS.mapPartitions(extendContigs, Encoders.STRING());
        FixingFullKmerString.persist(StorageLevel.DISK_ONLY());

        JavaPairRDD<String, Long> ContigsRDDStringIndex = FixingFullKmerString.toJavaRDD().zipWithIndex();
        markerTupleString = spark.createDataset(ContigsRDDStringIndex.rdd(), Encoders.tuple(Encoders.STRING(), Encoders.LONG()));

        TagStringContigRDDID DSContigIDLabel = new TagStringContigRDDID();
        ContigDS = markerTupleString.flatMap(DSContigIDLabel, ContigStringEncoder);

        ContigDS.write().
                mode(SaveMode.Overwrite).
                format("csv").
                option("compression", "gzip").save(param.outputPath + "/Assembly_intermediate/07EndExtend");

        spark.stop();
    }

    class OldContig2Row implements MapPartitionsFunction<Row, Row>, Serializable{
        List<Row> ContigRow  = new ArrayList<>();
        public Iterator<Row> call(Iterator<Row> sIterator) throws Exception{
            while (sIterator.hasNext()){

                Row s = sIterator.next();

                ContigRow.add(
                        RowFactory.create(s.getString(0), s.getString(1))
                );
            }


            return ContigRow.iterator();
        }
    }

    class SAMString2ROW implements MapPartitionsFunction<String, Row>, Serializable{
        List<Row> SAMcolumns  = new ArrayList<>();
        public Iterator<Row> call(Iterator<String> sIterator) throws Exception{
            while (sIterator.hasNext()){

                String[] SAM = sIterator.next().split("\t");

                if (SAM.length<4){
                    continue;
                }

                SAMcolumns.add(
                        RowFactory.create(SAM[0], Integer.parseInt(SAM[1]), SAM[2], SAM[3])
                );
            }


            return SAMcolumns.iterator();
        }
    }

    class DSInputSeqToFastq implements MapPartitionsFunction<String, String>, Serializable {
        List<String> reflexivKmerStringList = new ArrayList<String>();
        int readID=0;

        public Iterator<String> call(Iterator<String> sIterator) throws Exception {
            while (sIterator.hasNext()) {

                String s = sIterator.next();

                readID++;

                reflexivKmerStringList.add("@readID" + readID);
                reflexivKmerStringList.add(s);
                reflexivKmerStringList.add("+");
                reflexivKmerStringList.add(s);// second seq is the sequencing quality

            }

            return reflexivKmerStringList.iterator();
        }

    }

    class DSExtendContigs implements MapPartitionsFunction<Row, String>, Serializable {
        List<String> ContigList = new ArrayList<>();
        String lastID;
        String leftConsen="";
        String rightConsen="";
        String OriginalContig="";

        public Iterator<String> call(Iterator<Row> sIterator) throws Exception {
            while (sIterator.hasNext()){
                Row s = sIterator.next();

                String currentID= s.getString(0);

                if ( lastID ==null){

                }else if (currentID.equals(lastID)){

                }else{ // next contig
                    if (OriginalContig.length() >1) {

                        leftConsen = reverseString(leftConsen);
                        OriginalContig = leftConsen + OriginalContig;
                        OriginalContig = OriginalContig + rightConsen;
                        lastID = lastID + "_" + leftConsen.length() + "_" + rightConsen.length();
                        OriginalContig = lastID + "," + OriginalContig;

                        ContigList.add(OriginalContig);
                    }else{

                    }

                    leftConsen="";
                    rightConsen="";
                    OriginalContig="";
                }

                if (s.getString(1).startsWith("L-")){ // left extension consensus sequence
                    leftConsen = s.getString(1).substring(2);
                }else if (s.getString(1).startsWith("R-")){ // right extension consensus sequence
                    rightConsen= s.getString(1).substring(2);
                }else{
                    OriginalContig = s.getString(1);
                }

                lastID = currentID;
            }

            if (OriginalContig.length() >1) {

                leftConsen = reverseString(leftConsen);
                OriginalContig = leftConsen + OriginalContig;
                OriginalContig = OriginalContig + rightConsen;
                lastID = lastID + "_" + leftConsen.length() + "_" + rightConsen.length();
                OriginalContig = lastID + "," + OriginalContig;

                ContigList.add(OriginalContig);
            }else{
            }

            return ContigList.iterator();
        }

        private String reverseString(String forward){
            char[] nt = forward.toCharArray();
            String rev = "";
            
            for (int i = nt.length - 1; i >= 0; i--){
                rev += nt[i];
            }
            
            return rev;
        }
    }

    class DSProcessSAMandExtendContigs implements MapPartitionsFunction<Row, Row>, Serializable {
        List<Row> ExtensionList = new ArrayList<Row>();
        String lastContig = "first";
        int longestExtend = 300; // two reads with 150nt overlaps maximum 290
        int[][] consensus = new int[300][4];// = new int[300][4];
        int[][] consensusRight = new int[300][4];

        public Iterator<Row> call(Iterator<Row> sIterator) throws Exception {
            while (sIterator.hasNext()) {
                Row s = sIterator.next();

                String contigID = s.getString(0);
                contigID= contigID.replaceAll("-L$", "");
                contigID= contigID.replaceAll("-R$", "");;

                if (lastContig.equals("first")){
                    lastContig = contigID;
                }else if (lastContig.equals(contigID)){

                }else{
                    String consen = "";
                    for (int i=0; i<longestExtend; i++){
                        int highCharInt =-1;
                        int highCover =0;

                        for (int ntInt=0; ntInt <=3; ntInt++){
                            if (consensus[i][ntInt]!=0){
                                if (consensus[i][ntInt] > highCover){
                                    highCharInt= ntInt;
                                    highCover=consensus[i][ntInt];
                                }else if(consensus[i][ntInt] == highCover){
                                    if (ntInt > highCharInt){
                                        highCharInt = ntInt;
                                    } // else keep the old highCharInt
                                }
                            }
                        }

                        if (highCharInt>=0) {
                            consen += int2char(highCharInt);
                        }
                    }

                    ExtensionList.add(RowFactory.create(lastContig, "L-" + consen));

                    consen = "";
                    for (int i=0; i<longestExtend; i++){
                        int highCharInt = -1;
                        int highCover =0;

                        for (int ntInt=0; ntInt <=3; ntInt++){
                            if (consensusRight[i][ntInt]!=0){
                                if (consensusRight[i][ntInt] > highCover){
                                    highCharInt= ntInt;
                                    highCover=consensusRight[i][ntInt];
                                }else if(consensusRight[i][ntInt] == highCover){
                                    if (ntInt > highCharInt){
                                        highCharInt = ntInt;
                                    } // else keep the old highCharInt
                                }
                            }
                        }

                        if (highCharInt>=0) {
                            consen += int2char(highCharInt);
                        }
                    }

                    ExtensionList.add(RowFactory.create(lastContig, "R-" + consen));

                    lastContig=contigID;

                    consensus=new int[300][4];
                    consensusRight = new int[300][4];
                }

                String Len= s.getString(2).replaceAll("\\D+", "-");
                Len= Len.trim();
                String[] Lengths = Len.split("-");  // 10S20M30I  but the last element is not empty, interesting
                String tag= s.getString(2).replaceAll("\\d+", "-");
                tag= tag.trim();
                String[] tags_extra = tag.split("-"); // 10S20M30I      tags split by number always have an empty string in the begining
                String[] tags = Arrays.copyOfRange(tags_extra, 1, tags_extra.length);

                if (s.getString(0).endsWith("-L")){
                    int refStart = s.getInt(1);

                    if (refStart >=10){
                        continue;
                    }

                    int start=0;
                    int overhang=0;

                    if (!tags[0].equals("S")){
                        continue;
                    }

                    overhang = Integer.parseInt(Lengths[0]);

                    if (overhang - refStart >0){
                        int overhangLength= overhang - refStart;

                        int j=-1;
                        for (int i = overhangLength; i>=0; i--){
                            j++;
                            int readLength = s.getString(3).length();
                            char nucleotide = s.getString(3).charAt(i);

                            if (j>=longestExtend){
                                continue;
                            }
                            int charInt = char2int(nucleotide);
                            consensus[j][charInt]++;
                        }
                    }

                }else if (s.getString(0).endsWith("-R")){
                    int start=0;
                    int alignLength =0;
                    int overhang=0;

                    int refStart = s.getInt(1);

                    if (!tags[tags.length-1].equals("S")){
                        continue;
                    }

                    overhang = Integer.parseInt(Lengths[Lengths.length-1]);

                    for (int i=0; i<Lengths.length; i++){
                        if (tags[i].equals("M")){
                            start=i;
                        }
                    }

                    for (int i=start; i<tags.length-1; i++){
                        if (tags[i].equals("I")){
                            alignLength -= Integer.parseInt(Lengths[i]);
                        }else if (tags[i].equals("D")){
                            alignLength += Integer.parseInt(Lengths[i]);
                        }else {
                            alignLength += Integer.parseInt(Lengths[i]);
                        }
                    }

                    int refLength = 200;
                    int refTailLength = refLength - (alignLength + refStart);
                    if(refTailLength >= 10){
                        continue;
                    }

                    if (overhang - refTailLength >0){
                        int overhangLength = overhang - refTailLength;
                        int readLength = s.getString(3).length();

                        int j=-1;
                        for (int i = readLength - overhangLength; i<readLength ; i++){
                            j++;
                            char nucleotide = s.getString(3).charAt(i);

                            if (j>=longestExtend){
                                continue;
                            }
                            int charInt = char2int(nucleotide);
                            consensusRight[j][charInt]++;
                        }
                    }


                }else{
                    int start =0;
                    int alignLength = 0;
                    int overhang = 0;

                    int refStart = s.getInt(1);


                    if (tags[0].equals("S") && refStart < 10){
                        overhang = Integer.parseInt(Lengths[0]);

                        if (overhang - refStart > 0){
                            int overhangLength = overhang - refStart; // refStart is not index based, it starts from 1

                            int j = -1;
                            for (int i = overhangLength; i>=0; i--){
                                j++;
                                int readLength = s.getString(3).length();
                                char nucleotide = s.getString(3).charAt(i);

                                if (j>=longestExtend){
                                    continue;
                                }
                                int charInt = char2int(nucleotide);
                                consensus[j][charInt]++;
                            }
                        }

                    }

                    if (tags[tags.length-1].equals("S")){
                        overhang = Integer.parseInt(Lengths[Lengths.length-1]);

                        for (int i=0; i<Lengths.length;i++){
                            if(tags[i].equals("M")){
                                start = i;
                            }
                        }

                        for (int i=start; i<tags.length-1;i++){
                            if (tags[i].equals("I")) {
                                alignLength -= Integer.parseInt(Lengths[i]);
                            }else if (tags[i].equals("D")){
                                alignLength += Integer.parseInt(Lengths[i]);
                            }else{
                                alignLength += Integer.parseInt(Lengths[i]);
                            }
                        }

                        String[] contig = s.getString(0).split("_");
                        int refLength = Integer.parseInt(contig[1]);
                        int refTailLength = refLength - (alignLength + refStart);

                        if (refTailLength >=10){
                            continue;
                        }

                        if (overhang - refTailLength >0){
                            int overhangLength = overhang - refTailLength;
                            int readLength = s.getString(3).length();

                            int j=-1;
                            for (int i = readLength - overhangLength; i<readLength; i++){
                                j++;
                                char nucleotide = s.getString(3).charAt(i);

                                if (j>=longestExtend){
                                    continue;
                                }
                                int charInt = char2int(nucleotide);
                                consensusRight[j][charInt]++;
                            }
                        }
                    }

                }

            }

            if (longestExtend > 1) { // ?
                String consen = "";
                for (int i = 0; i < longestExtend; i++) {
                    int highCharInt = -1;
                    int highCover = 0;

                    for (int ntInt = 0; ntInt <= 3; ntInt++) {
                        if (consensus[i][ntInt] != 0) {
                            if (consensus[i][ntInt] > highCover) {
                                highCharInt = ntInt;
                                highCover = consensus[i][ntInt];
                            } else if (consensus[i][ntInt] == highCover) {
                                if (ntInt > highCharInt) {
                                    highCharInt = ntInt;
                                } // else keep the old highCharInt
                            }
                        }
                    }

                    if (highCharInt>=0) {
                        consen += int2char(highCharInt);
                    }
                }

                ExtensionList.add(RowFactory.create(lastContig, "L-" + consen));

                consen = "";
                for (int i = 0; i < longestExtend; i++) {
                    int highCharInt = -1;
                    int highCover = 0;

                    for (int ntInt = 0; ntInt <= 3; ntInt++) {
                        if (consensusRight[i][ntInt] != 0) {
                            if (consensusRight[i][ntInt] > highCover) {
                                highCharInt = ntInt;
                                highCover = consensusRight[i][ntInt];
                            } else if (consensusRight[i][ntInt] == highCover) {
                                if (ntInt > highCharInt) {
                                    highCharInt = ntInt;
                                } // else keep the old highCharInt
                            }
                        }
                    }

                    if (highCharInt>=0) {
                        consen += int2char(highCharInt);
                    }
                }

                ExtensionList.add(RowFactory.create(lastContig, "R-" + consen));
            }

            return ExtensionList.iterator();
        }

        private int char2int(char nt){
            if (nt == 'a'){
                return 0;
            }else if (nt == 'A'){
                return 0;
            }else if (nt == 'c'){
                return 1;
            }else if (nt == 'C'){
                return 1;
            }else if (nt == 'g'){
                return 2;
            }else if (nt == 'G'){
                return 2;
            }else if (nt == 't'){
                return 3;
            }else if (nt == 'T'){ //
                return 3;
            }else if (nt == 'N'){
                return 3;
            }else{ // other unexpected char
                return 3;
            }
        }

        private char int2char(int i){
            if (i == 0){
                return 'A';
            }else if (i == 1){
                return 'C';
            }else if (i == 2){
                return 'G';
            }else if (i == 3){
                return 'T';
            }else{ // others consider as T
                return 'T';
            }

        }

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


    class DSJavaPipeMinimap2 implements FlatMapFunction<Iterator<String>, String>, Serializable {
        List<String> reflexivKmerStringList = new ArrayList<String>();


        public Iterator<String> call(Iterator<String> sIterator) throws Exception {
            String executable = SparkFiles.get("minimap2");
            String indexPath = SparkFiles.get("contigEnds.mmi");

            int CPUs= Runtime.getRuntime().availableProcessors();
            if (CPUs <=1){
                CPUs=1;
            }


            List<String> executeCommands = Arrays.asList(
                "/bin/bash",
                "-c",
                String.join(" ",
                        executable,
                    "-x", "sr",
                    "-a",
                    "-Y",
                    "-t", "" + CPUs,
                    "-I", "55G",
                        indexPath,
                    "/dev/stdin",
                    "|",
                    "awk '{if ($3!=\"*\" && $1!~/^@/){print $3\"\\t\"$4\"\\t\"$6\"\\t\"$10}}' "
                )
            );

            /**
             * start 3rd process
             */

            ProcessBuilder pb2 = new ProcessBuilder();
            pb2.command(executeCommands);
            final Process process2 = pb2.start();

            // Get the output stream of the process
            OutputStream outputStream = process2.getOutputStream();

            // Start a separate thread to read the output of the external program
            final List<String> output = new ArrayList<String>();

            Thread outputThread = new Thread(() -> {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(process2.getInputStream()))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        reflexivKmerStringList.add(line);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            outputThread.start();


            while (sIterator.hasNext()) {
                String s = sIterator.next();
                if (s==null){
                    continue;
                }
                outputStream.write(s.getBytes());
                outputStream.write("\n".getBytes());
                // outputStream.flush();
            }

            outputStream.close();

            // Wait for the output thread to finish
            try {
                outputThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }


            try {
                int exitCode = process2.waitFor();
                // System.out.println("External process finished with exit code " + exitCode);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }


            /**
             * start last process
             */
            /*
            pb.command(removeRefCommands);
            // Start the process
            final Process process3 = pb.start();

            try {
                process3.waitFor();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

             */

            return reflexivKmerStringList.iterator();
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

    /**
     *
     * @param param
     */
    public void setParam(DefaultParam param) {
        this.param = param;
    }
}
