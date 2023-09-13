package uni.bielefeld.cmg.reflexiv.pipeline;

import com.fing.compression.fourmc.FourMcCodec;
import com.fing.fourmc.elephantbird.adapter.FourMzEbProtoInputFormat;
import com.fing.mapreduce.FourMcTextInputFormat;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.collection.Seq;
import scala.io.Source;
import uni.bielefeld.cmg.reflexiv.util.DefaultParam;
import uni.bielefeld.cmg.reflexiv.util.InfoDumper;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

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
                .config("spark.cleaner.referenceTracking.cleanCheckpoints", true)
                .config("spark.checkpoint.compress",true)
                .config("spark.sql.shuffle.partitions", String.valueOf(shufflePartitions))
                .config("spark.sql.files.maxPartitionBytes", "6000000")
                .config("spark.sql.adaptive.advisoryPartitionSizeInBytes","6mb")
                .config("spark.driver.maxResultSize","1000G")
                .config("spark.memory.fraction","0.7")
                .config("spark.network.timeout","60000s")
                .config("spark.executor.heartbeatInterval","20000s")
                .getOrCreate();

        return spark;
    }

    private SparkConf setSparkConfiguration(){
        SparkConf conf = new SparkConf().setAppName("SparkHit");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.referenceTracking", "false");
        conf.set("spark.kryo.registrator", "uni.bielefeld.cmg.reflexiv.serializer.SparkKryoRegistrator");
        conf.set("spark.driver.maxResultSize","1000G");

        return conf;
    }

    private void cleanDiskStorage(String file) throws IOException{
        if (file.startsWith("hdfs")){
            Configuration conf = new Configuration();
            String header = file.substring(0,file.indexOf(":9000")+5);
            conf.set("fs.default.name", header);
            FileSystem hdfs = FileSystem.get(conf);

            Path fileHDFSPath= new Path(file.substring(file.indexOf(":9000")+5)+"/_SUCCESS");
            Path folderHDFSPath= new Path(file.substring(file.indexOf(":9000")+5));

            if (hdfs.exists(fileHDFSPath)){
                hdfs.delete(folderHDFSPath, true);
            }

        }else{
            File localFile= new File(file);
            if (localFile.exists()) {
                Runtime.getRuntime().exec("rm -r " + file);
            }
        }

    }

    private boolean checkOutputFile(String file) throws IOException {
        //  System.out.println("input path: " + file);

        if (file.startsWith("hdfs")){
            Configuration conf = new Configuration();
            String header = file.substring(0,file.indexOf(":9000")+5);

            //     System.out.println("input path header: " + header);

            conf.set("fs.default.name", header);
            FileSystem hdfs = FileSystem.get(conf);

            //      System.out.println("input path suffix: " + file.substring(file.indexOf(":9000")+5)+"/_SUCCESS");

            //      System.out.println("input path exists or not: " + hdfs.exists(new Path(file.substring(file.indexOf(":9000")+5)+"/_SUCCESS")));

            return hdfs.exists(new Path(file.substring(file.indexOf(":9000")+5)+"/_SUCCESS"));
        }else{
            return Files.exists(Paths.get(file+"/_SUCCESS"));
        }
    }

    /**
     *
     */
    public void assembly() throws IOException {

        SparkConf conf = setSparkConfiguration();
        info.readMessage("Start Spark framework");
        info.screenDump();
        info.readMessage("Initiating Spark context ...");
        info.screenDump();

        JavaSparkContext sc = new JavaSparkContext(conf);

        SparkSession spark = setSparkSessionConfiguration(param.shufflePartition);
        info.readMessage("Initiating Spark Session ...");
        info.screenDump();

        Dataset<String> FastqDS;

        FastqDS = spark.read().text(param.inputFqPath).as(Encoders.STRING());

        JavaRDD<String> FastqRDD = FastqDS.toJavaRDD();

        if (!param.inputFormat.equals("bzip2")){


            if (checkOutputFile(param.outputPath + "/Read_Repartitioned_4MC")){
                cleanDiskStorage(param.outputPath + "/Read_Repartitioned_4MC");
            }

            FastqRDD.saveAsTextFile(param.outputPath + "/Read_Repartitioned_4MC/", FourMcCodec.class);

            Configuration baseConfiguration = new Configuration();
            Job jobConf = Job.getInstance(baseConfiguration);
            JavaPairRDD<LongWritable, Text> FastqPairRDD = sc.newAPIHadoopFile(param.outputPath + "/Read_Repartitioned_4MC/part*", FourMcTextInputFormat.class, LongWritable.class, Text.class, jobConf.getConfiguration());

            DSInputTupleToString tupleToString = new DSInputTupleToString();

            FastqRDD = FastqPairRDD.mapPartitions(tupleToString);
            FastqDS = spark.createDataset(FastqRDD.rdd(), Encoders.STRING());

        }
        //  DSFastqFilterOnlySeq DSFastqFilterToSeq = new DSFastqFilterOnlySeq(); // for reflexiv

        DSFastqFilterWithOutSeq DSFastqFilterToSeqOnly = new DSFastqFilterWithOutSeq();
        Dataset<String> FastqDSLine = FastqDS.map(DSFastqFilterToSeqOnly, Encoders.STRING());

        DSFastqUnitFilter FilterDSUnit = new DSFastqUnitFilter();

        FastqDSLine = FastqDSLine.filter(FilterDSUnit);

        if (param.partitions > 0) {
            FastqDSLine = FastqDSLine.repartition(param.partitions);
        }

        if (param.pairing && param.inputSingleSwitch ){
            FastqRDD = FastqDSLine.toJavaRDD();
         //   FastqRDD.persist(StorageLevel.DISK_ONLY());
            FastqRDD.saveAsTextFile(param.outputPath + "/Read_Single", FourMcCodec.class);
         //   FastqRDD.saveAsTextFile(param.outputPath + "/Read_Repartitioned", FourMcCodec.class);
        }

        if (param.interleavedSwitch) {


            DSFastqFilterWithQual DSFastqFilterToFastq = new DSFastqFilterWithQual();
            FastqDS = FastqDS.map(DSFastqFilterToFastq, Encoders.STRING());

            FastqDS = FastqDS.filter(FilterDSUnit);

            DSInputFastqToTab FastqToTab = new DSInputFastqToTab();
            FastqDS = FastqDS.mapPartitions(FastqToTab, Encoders.STRING());

            JavaRDD<String> MergedSeq;

            DSJavaPipe myPipe = new DSJavaPipe();
            MergedSeq=FastqDS.toJavaRDD().mapPartitions(myPipe);

            DSFlashOutputToSeq FlashMergedTabToSeq = new DSFlashOutputToSeq();
            MergedSeq= MergedSeq.mapPartitions(FlashMergedTabToSeq);

            if (param.partitions > 0) {
                MergedSeq = MergedSeq.repartition(param.partitions);
            }

            MergedSeq.saveAsTextFile(param.outputPath + "/Read_Interleaved_Merged", FourMcCodec.class);
        }

        if (param.inputPairedSwitch){

            StructType ReadStringStruct = new StructType();
            ReadStringStruct = ReadStringStruct.add("ID", DataTypes.StringType, false);
            ReadStringStruct = ReadStringStruct.add("Read", DataTypes.StringType, false);
            ExpressionEncoder<Row> ReadStringEncoder = RowEncoder.apply(ReadStringStruct);

            // FastqDS = spark.createDataset(FastqRDD.rdd(), Encoders.STRING());

            DSFastqFilterWithQualPairs DSFastqFilterToFastqPair = new DSFastqFilterWithQualPairs();
            Dataset<Row> FastqPairDS = FastqDS.map(DSFastqFilterToFastqPair, ReadStringEncoder);

            DSFastqRowFilter FilterDSRow = new DSFastqRowFilter();

            FastqPairDS = FastqPairDS.filter(FilterDSRow);

            if (param.partitions > 0) {
                FastqPairDS = FastqPairDS.repartition(param.partitions);
            }

            FastqPairDS = FastqPairDS.sort("ID");

            DSInputTaggedFastqToString TaggedFastqToString = new DSInputTaggedFastqToString();

            FastqDS=  FastqPairDS.mapPartitions(TaggedFastqToString, Encoders.STRING());

            DSInputFastqToTab FastqToTab = new DSInputFastqToTab();
            FastqDS = FastqDS.mapPartitions(FastqToTab, Encoders.STRING());

            JavaRDD<String> MergedSeq;

            DSJavaPipe myPipe = new DSJavaPipe();
            MergedSeq=FastqDS.toJavaRDD().mapPartitions(myPipe);

            DSFlashOutputToSeq FlashMergedTabToSeq = new DSFlashOutputToSeq();
            MergedSeq= MergedSeq.mapPartitions(FlashMergedTabToSeq);

            MergedSeq.saveAsTextFile(param.outputPath + "/Read_Paired_Merged", FourMcCodec.class);

        }

        // FastqDS.write().mode(SaveMode.Overwrite).format("text").option("compression", "gzip").save(param.outputPath + "/Read_Repartitioned");

        cleanDiskStorage(param.outputPath + "/Read_Repartitioned_4MC");


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

    class DSFastqRowFilter implements FilterFunction<Row>, Serializable{
        public boolean call(Row s){
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

    class DSFastqFilterWithQualPairs implements MapFunction<String, Row>, Serializable {
        String line = "";
        String head = "";
        int lineMark = 0;

        public Row call(String s) {
            if (lineMark == 2) {
                lineMark++;
                line = line + "\n" + s;
                return null;
            } else if (lineMark == 3) {
                lineMark++;
                line = line + "\n" + s;
                return RowFactory.create(head, line);
            } else if (s.startsWith("@")) {
                line = s;
                lineMark = 1;
                head = s;
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

    class DSFastqFilterWithOutSeq implements MapFunction<String, String>, Serializable {
        String line = "";
        int lineMark = 0;

        public String call(String s) {
            if (lineMark == 2) {
                lineMark++;
              //  line = line + "\n" + s;
                return null;
            } else if (lineMark == 3) {
                lineMark++;
                // line = line + "\n" + s;
                return null;
            } else if (s.startsWith("@")) {
                // line = s;
                lineMark = 1;
                return null;
            } else if (lineMark == 1) {
                //line = line + "\n" + s;
                lineMark++;
                return s;
            } else {
                return null;
            }
        }
    }

    class DSInputFastqToTab implements MapPartitionsFunction<String, String>, Serializable {
        List<String> reflexivKmerStringList = new ArrayList<String>();
        String[] seq;
        String[] ID;
        String seqID;
        String lastSeqID;
        String[] lastSeq;

        public Iterator<String> call(Iterator<String> sIterator) throws Exception {
            while (sIterator.hasNext()) {

                String s = sIterator.next();
                seq = s.split("\\n");
                ID=seq[0].split("\\s+");
                seqID=ID[0].substring(0, ID[0].length()-2);

                if (lastSeq==null){
                    lastSeq=seq;
                    lastSeqID=seqID;
                    continue;
                }

                if (ID[0].endsWith("/1") || ID[0].endsWith("/2")){
                    if (seqID.equals(lastSeqID)){
                        reflexivKmerStringList.add(
                                ID[0] + "\t" + lastSeq[1] + "\t" + lastSeq[3] + "\t" + seq[1] + "\t" + seq[3]
                        );

                        lastSeq=null;
                        lastSeqID=null;

                    }else{
                        reflexivKmerStringList.add(
                                lastSeqID + "\t" + lastSeq[1] + "\t" + lastSeq[3]
                        );

                        lastSeqID= seqID;
                        lastSeq=seq;

                    //    System.out.println(reflexivKmerStringList.get(reflexivKmerStringList.size()-1).split("\\t")[0] + " and " + lastSeqID);
                    }
                }else{
                    reflexivKmerStringList.add(
                            lastSeqID + "\t" + lastSeq[1] + "\t" + lastSeq[3]
                    );

                    lastSeqID= seqID;
                    lastSeq=seq;
                }
            }

            if (lastSeq!=null){
                reflexivKmerStringList.add(
                        lastSeqID + "\t" + lastSeq[1] + "\t" + lastSeq[3]
                );
            }

            return reflexivKmerStringList.iterator();
        }
    }

    class DSJavaPipe implements FlatMapFunction<Iterator<String>, String>, Serializable {
        List<String> reflexivKmerStringList = new ArrayList<String>();


        public Iterator<String> call(Iterator<String> sIterator) throws Exception {
            String executable = SparkFiles.get("flash");
            List<String> executeCommands = Arrays.asList(
                    executable,
                    "-t", "1",
                    "--tab-delimited-input",
                    "--tab-delimited-output",
                    "--allow-outies",
                    "--max-overlap", "85",
                    "-c", "/dev/stdin"
            );

            ProcessBuilder pb = new ProcessBuilder();
            pb.command(executeCommands);
            // Start the process
            final Process process = pb.start();

            // Get the output stream of the process
            OutputStream outputStream = process.getOutputStream();

            // Start a separate thread to read the output of the external program
            final List<String> output = new ArrayList<String>();

            Thread outputThread = new Thread(() -> {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
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
                outputStream.write(s.getBytes());
                outputStream.write("\n".getBytes());
              //  outputStream.flush();
            }

            outputStream.close();

            // Wait for the output thread to finish
            try {
                outputThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }


            try {
                int exitCode = process.waitFor();
               // System.out.println("External process finished with exit code " + exitCode);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            return reflexivKmerStringList.iterator();
        }
    }

    class DSInputTaggedFastqToString implements MapPartitionsFunction<Row, String>, Serializable {
        List<String> reflexivKmerStringList = new ArrayList<String>();
        String seq;

        public Iterator<String> call(Iterator<Row> sIterator) throws Exception {
            while (sIterator.hasNext()) {

                Row s = sIterator.next();
                seq = s.getString(1);

                reflexivKmerStringList.add(
                        seq
                );
            }
            return reflexivKmerStringList.iterator();
        }
    }

    class DSFlashOutputToSeq implements FlatMapFunction<Iterator<String>, String>, Serializable {
        List<String> reflexivKmerStringList = new ArrayList<String>();
        String[] seq;

        public Iterator<String> call(Iterator<String> sIterator) throws Exception {
            while (sIterator.hasNext()) {

                String s = sIterator.next();
                seq = s.split("\\s+");

                if (seq.length>=4){
                    reflexivKmerStringList.add(
                            seq[1]
                    );
                 //   reflexivKmerStringList.add(
                 //           seq[1]
                 //   );
                    reflexivKmerStringList.add(
                            seq[3]
                    );
                //    reflexivKmerStringList.add(
                //            seq[3]
                //    );
                }else{
                    /*
                    String addition;

                    String addition2;
                    int mergedLength= seq[1].length();
                    int middle = mergedLength/2;

                    if (mergedLength>=500 ){
                        int readLength=300;
                        int radius=(2*readLength-mergedLength)/2;
                        addition=seq[1].substring(0, middle+radius);
                        addition2=seq[1].substring(middle-radius, seq[1].length());
                    }else if (mergedLength>=300 && mergedLength<500){
                        int readLength=250;
                        int radius=(2*readLength-mergedLength)/2;
                        addition=seq[1].substring(0, middle+radius);
                        addition2=seq[1].substring(middle-radius, seq[1].length());
                    }else if (mergedLength>188 && mergedLength<300){
                        int readLength=150;
                        int radius=(2*readLength-mergedLength)/2;
                        addition=seq[1].substring(0, middle+radius);
                        addition2=seq[1].substring(middle-radius, seq[1].length());
                    }else if (mergedLength>=120 && mergedLength<=188){
                        int readLength=100;
                        int radius=(2*readLength-mergedLength)/2;
                        addition=seq[1].substring(0, middle+radius);
                        addition2=seq[1].substring(middle-radius, seq[1].length());
                    }else if (mergedLength>=75 && mergedLength<120){
                        int readLength=75;
                        int radius=(2*readLength-mergedLength)/2;
                        addition=seq[1].substring(0, middle+radius);
                        addition2=seq[1].substring(middle-radius, seq[1].length());
                    }else{
                        addition=seq[1];
                        addition2=seq[1];
                    }
*/
                    reflexivKmerStringList.add(
                            seq[1]
                    );
/*                    reflexivKmerStringList.add(
                            seq[1]
                    );

                    int margin250=50;
                    int margin150=1;
                    int margin100=1;
                    if (mergedLength>=490-margin250 && mergedLength<490){ // 490 because the paired-end overlap is min 10nt
                        reflexivKmerStringList.add(
                                seq[1]
                        );
                    }else if (mergedLength>=290-margin150 && mergedLength<=290){
                        reflexivKmerStringList.add(
                                seq[1]
                        );
                    }else if (mergedLength>=190-margin100 && mergedLength<=190){
                        reflexivKmerStringList.add(
                                seq[1]
                        );
                    }

                    reflexivKmerStringList.add(
                            addition
                    );
                    reflexivKmerStringList.add(
                            addition2
                    );

                     */
                }

            }
            return reflexivKmerStringList.iterator();
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

    /**
     *
     * @param param
     */
    public void setParam(DefaultParam param) {
        this.param = param;
    }
}
