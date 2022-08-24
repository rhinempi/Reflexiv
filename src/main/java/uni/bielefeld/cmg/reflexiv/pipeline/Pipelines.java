package uni.bielefeld.cmg.reflexiv.pipeline;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import uni.bielefeld.cmg.reflexiv.util.DefaultParam;
import uni.bielefeld.cmg.reflexiv.util.InfoDumper;

import java.io.*;
import java.nio.file.Files;
import org.apache.hadoop.fs.Path;
import java.nio.file.Paths;

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
 * Returns an object for managing different pipelines of each Sparkhit
 * application.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
 */
public class Pipelines implements Pipeline, Serializable{
    private DefaultParam param;

    private InfoDumper info = new InfoDumper();

    private BufferedWriter outputBufferedWriter;

    private long time;

    private void clockStart() {
        time = System.currentTimeMillis();

    }

    private long clockCut () {
        long tmp = time;
        time = System.currentTimeMillis();
        return time -tmp;
    }

    /**
     * A constructor that construct an object of {@link Pipelines} class.
     */
    public Pipelines () {
    }


    /**
     * This method starts the Reflexiv run pipeline
     */
    public void reflexivMainPipe(){
        ReflexivMain rflPipe = new ReflexivMain();
        rflPipe.setParam(param);
        if (param.inputKmerPath != null){
            rflPipe.assemblyFromKmer();
        }else {
            rflPipe.assembly();
        }
    }

    public void reflexivDSMainPipe(){
        ReflexivDSMain rflPipe = new ReflexivDSMain();
        rflPipe.setParam(param);
        if (param.inputKmerPath != null){
            rflPipe.assemblyFromKmer();
        }else {
            rflPipe.assembly();
        }
    }

    public void reflexivDSMercyPipe(){
        ReflexivDSMainMercy rflPipe = new ReflexivDSMainMercy();
        rflPipe.setParam(param);

            rflPipe.assembly();

    }

    public void reflexivDSMainPipe64(){
        ReflexivDSMain64 rflPipe = new ReflexivDSMain64();
        rflPipe.setParam(param);
        if (param.inputKmerPath != null){
            rflPipe.assemblyFromKmer();
        }else {
            rflPipe.assembly();
        }
    }

    public void reflexivDSMainMetaPipe64() {
        ReflexivDSMainMeta64 rflPipe = new ReflexivDSMainMeta64();
        rflPipe.setParam(param);
        if (param.inputKmerPath != null) {
            rflPipe.assemblyFromKmer();
        } else {
            rflPipe.assembly();
        }
    }

    public void reflexivDSMergerPipe(){
        ReflexivDSMerger rflPipe = new ReflexivDSMerger();
        rflPipe.setParam(param);
        if (param.inputKmerPath != null){
            rflPipe.assemblyFromKmer();
        }else {
            rflPipe.assembly();
        }
    }


    /**
     * This method starts the Reflexiv counter pipeline
     */
    public void reflexivCounterPipe(){
        ReflexivCounter rflPipe = new ReflexivCounter();
        rflPipe.setParam(param);
        rflPipe.assembly();
    }

    public void reflexivDSCounterPipe(){
        ReflexivDataFrameCounter rflPipe = new ReflexivDataFrameCounter();
        rflPipe.setParam(param);
        rflPipe.assembly();
    }

    public void reflexivDS64CounterPipe(){
        ReflexivDataFrameCounter64 rfPipe = new ReflexivDataFrameCounter64();
        rfPipe.setParam(param);
        rfPipe.assembly();
    }

    public void reflexivDS64ReAssembleCounterPipe(){
        ReflexivDataFrameReAssembleCounter64 rfPipe = new ReflexivDataFrameReAssembleCounter64();
        rfPipe.setParam(param);
        rfPipe.assembly();
    }

    public void reflexivDSReAssembleCounterPipe(){
        ReflexivDataFrameReAssembleCounter rfPipe = new ReflexivDataFrameReAssembleCounter();
        rfPipe.setParam(param);
        rfPipe.assembly();
    }


    /**
     * This method starts the Reflexiv reassembler pipeline
     */
    public void reflexivDSReAssemblerPipe(){
        ReflexivDSReAssembler rfPipe = new ReflexivDSReAssembler();
        rfPipe.setParam(param);
        if (param.inputKmerPath != null){
            rfPipe.assemblyFromKmer();
        }else {
            rfPipe.assembly();
        }
    }

    public void reflexivDSReAssemblerPipe64(){
        ReflexivDSReAssembler64 rfPipe = new ReflexivDSReAssembler64();
        rfPipe.setParam(param);
        if (param.inputKmerPath != null){
            rfPipe.assemblyFromKmer();
        }else {
            rfPipe.assembly();
        }
    }

    public void reflexivDSDecompresserPipe(){
        ReflexivDataFrameDecompresser rfPipe = new ReflexivDataFrameDecompresser();
        rfPipe.setParam(param);
        rfPipe.assembly();
    }

    public void reflexivDSDynamicKmerReductionPipe(){
        ReflexivDSDynamicKmerRuduction rfPipe = new ReflexivDSDynamicKmerRuduction();
        rfPipe.setParam(param);
        rfPipe.assemblyFromKmer();
    }

    private boolean checkOutputFile(String file) throws IOException {
        if (file.startsWith("hdfs")){
            Configuration conf = new Configuration();
            String header = file.substring(0,file.indexOf(":9000")+5);
            conf.set("fs.default.name", header);
            FileSystem hdfs = FileSystem.get(conf);

            return hdfs.exists(new Path(file.substring(file.indexOf(":9000")+5)));
        }else{
            return Files.exists(Paths.get(file));
        }
    }

    private void renameDiskStorage(String oldFile, String newFile) throws IOException {
        if (oldFile.startsWith("hdfs")){
            Runtime.getRuntime().exec("hadoop fs mv " + oldFile + " " + newFile);

        }else{
            Runtime.getRuntime().exec("mv -v " + oldFile + " " + newFile);
        }
    }

    public void reflexivDSDynamicAssemblyPipe() throws IOException{
        ReflexivDSDynamicKmer64 rfPipe = new ReflexivDSDynamicKmer64();
        rfPipe.setParam(param);
        rfPipe.assemblyFromKmer();
    }

    public void reflexivDSDynamicReductionPipe() throws IOException {



        // step 1 decompress
        if (!checkOutputFile(param.outputPath + "/Read_Repartitioned")) {
            reflexivDSDecompresserPipe();
        }
        param.setGzip(true);

        // step 2 smallest kmer count
        param.setInputFqPath(param.outputPath + "/Read_Repartitioned/part*.txt.gz");
        param.kmerSize1= param.kmerListInt[0];
        if (!checkOutputFile(param.outputPath + "/Count_" + param.kmerSize1)) {
            param.setKmerSize(param.kmerSize1); // set kmer Size for counter
            param.setAllbyKmerSize(param.kmerSize1);
            if (param.kmerSize <= 31) {
                reflexivDSCounterPipe();
            } else {
                reflexivDS64CounterPipe();
            }
        }

        for (int i=1;i<param.kmerListInt.length;i++){ // longer kmer

            param.kmerSize1=param.kmerListInt[i-1];
            param.kmerSize2=param.kmerListInt[i];

            if (!checkOutputFile(param.outputPath + "/Count_" + param.kmerSize2)) {  // the next kmer increment
                param.setKmerSize(param.kmerSize2); // set kmer size for counter
                param.setAllbyKmerSize(param.kmerSize2);
                if (param.kmerSize <= 31) {
                    reflexivDSCounterPipe();
                } else {
                    reflexivDS64CounterPipe();
                }
            }

            if (i==1) {
                param.inputKmerPath1 = param.outputPath + "/Count_" + param.kmerSize1 + "/part*.csv.gz";
            }else{ // after the first iteration
                param.inputKmerPath1 = param.outputPath + "/Count_" + param.kmerSize1 + "_sorted/part*.csv.gz";
            }
            param.inputKmerPath2 = param.outputPath + "/Count_" + param.kmerSize2 + "/part*.csv.gz";

            reflexivDSDynamicKmerReductionPipe();

            // cleanDiskStorage(param.outputPath + "/Count_" + param.kmerSize1);
            // cleanDiskStorage(param.outputPath + "/Count_" + param.kmerSize2);
            // cleanDiskStorage(param.outputPath + "/Count_" + param.kmerSize1 + "_sorted");


        }

        // rename the longest kmer directory name with "reduced" ending
        renameDiskStorage(param.outputPath + "/Count_" + param.kmerSize2 + "_sorted", param.outputPath + "/Count_" + param.kmerSize2 + "_reduced");

    }

    /**
     *
     */
    public void reflexivDSIterativeAssemblerPipe() throws IOException {



        // step 1 decompress
        if (!checkOutputFile(param.outputPath + "/Read_Repartitioned")) {
            reflexivDSDecompresserPipe();
        }
        param.setGzip(true);

        // step 2 k-mer counting with smallest kmer size
        param.setInputFqPath(param.outputPath + "/Read_Repartitioned/part*.txt.gz");
        if (!checkOutputFile(param.outputPath + "/Count_" + param.kmerSize)) {
            if (param.kmerSize <= 31) {
                reflexivDSCounterPipe();
            } else {
                reflexivDS64CounterPipe();
            }
        }

        param.setInputKmerPath(param.outputPath + "/Count_" + param.kmerSize + "/part*.csv.gz");
        param.setInputContigPath(param.outputPath + "/Assemble_" + param.kmerSize);
 //       param.setInputFqPath(param.outputPath + "/Read_Repartitioned/part*.txt.gz");
        param.setMinContig(param.kmerSize + param.kmerIncrease);

 //       param.setPartitions(param.partitions/10);
 //       param.setShufflePartition(param.shufflePartition/10);

        // step 3 assemble smallest kmer size normally
        if (!checkOutputFile(param.outputPath + "/Assemble_" + param.kmerSize)){
            if (param.kmerSize <= 31) {
                reflexivDSMainPipe();
            } else {
                reflexivDSMainPipe64();
            }
        }

        // step 4 repeat each kmer size
        while(param.kmerSize + param.kmerIncrease < param.maxKmerSize){
            param.kmerSize += param.kmerIncrease;

            param.setAllbyKmerSize(param.kmerSize);
            param.setRCmerge(false);

        //    param.setPartitions(param.partitions*10);
        //    param.setShufflePartition(param.shufflePartition*10);

            if (!checkOutputFile(param.outputPath + "/Count_" + param.kmerSize)) {
                if (param.kmerSize <= 31) {
                    reflexivDSReAssembleCounterPipe();
                } else {
                    reflexivDS64ReAssembleCounterPipe();
                }
            }


            param.setInputKmerPath(param.outputPath + "/Count_" + param.kmerSize + "/part*.csv.gz");
            param.setMinContig(param.kmerSize + param.kmerIncrease);

            if (param.kmerSize >31 && param.kmerSize <61){
                param.setMinErrorCoverage(param.minKmerCoverage * 4);
            } else if (param.kmerSize >= 61 && param.kmerSize <=81 ){
                param.setMinErrorCoverage(param.minKmerCoverage * 3);
            }else if (param.kmerSize >81){
                param.setMinErrorCoverage(param.minKmerCoverage * 2);
            }

       //     param.setPartitions(param.partitions/10);
       //     param.setShufflePartition(param.shufflePartition/10);

            if (!checkOutputFile(param.outputPath + "/Assemble_" + param.kmerSize)) {
                if (param.kmerSize <= 31) {
                    reflexivDSMainPipe();
                } else {
                    reflexivDSMainPipe64();
                }
            }

            param.setInputContigPath(param.outputPath + "/Assemble_" + param.kmerSize);
        }

        param.kmerSize = param.maxKmerSize;
        param.setAllbyKmerSize(param.kmerSize);

   //     param.setPartitions(param.partitions*10);
   //     param.setShufflePartition(param.shufflePartition*10);

        if (!checkOutputFile(param.outputPath + "/Count_" + param.kmerSize )){
            if (param.kmerSize <= 31) {
                reflexivDSReAssembleCounterPipe();
            } else {
                reflexivDS64ReAssembleCounterPipe();
            }
        }
  //      param.setPartitions(param.partitions/10);
  //      param.setShufflePartition(param.shufflePartition/10);

        param.setInputKmerPath(param.outputPath + "/Count_" + param.kmerSize + "/part*.csv.gz");
        param.setGzip(false);
        param.setRCmerge(true);

        if (checkOutputFile(param.outputPath + "/Assemble_" + param.kmerSize )){
            if (param.kmerSize <= 31) {
                reflexivDSReAssemblerPipe();
            } else {
                reflexivDSReAssemblerPipe64();
            }
        }else{
            info.readMessage("Result already exist! Choose a new directory to restart a new assembly");
            info.screenDump();
        }
    }

    /**
     * This method sets correspond parameters.
     *
     * @param param {@link DefaultParam} is the object for command line parameters.
     */
    public void setParameter (DefaultParam param) {
        this.param = param;
    }

    /**
     * This method sets input buffer reader.
     *
     * @param inputBufferedReader a {@link BufferedReader} to read input data.
     */
    public void setInput (BufferedReader inputBufferedReader){

    }

    /**
     * This method sets output buffer writer.
     *
     * @param outputBufferedWriter a {@link BufferedWriter} to write to an output file.
     */
    public void setOutput(BufferedWriter outputBufferedWriter) {
        this.outputBufferedWriter = outputBufferedWriter;
    }
}
