package uni.bielefeld.cmg.reflexiv.pipeline;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import uni.bielefeld.cmg.reflexiv.util.DefaultParam;
import uni.bielefeld.cmg.reflexiv.util.InfoDumper;

import java.io.*;
import java.nio.file.Files;
import org.apache.hadoop.fs.Path;
import java.nio.file.Paths;

import static java.lang.System.exit;

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

    public void reflexivDSCounterPipe() throws IOException {
        ReflexivDataFrameCounter rflPipe = new ReflexivDataFrameCounter();
        rflPipe.setParam(param);
        rflPipe.assembly();
    }

    public void reflexivDS64CounterPipe() throws IOException {
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

    public void reflexivDSPatchingPipe() throws IOException {
        ReflexivDSDynamicKmerPatching rfPipe = new ReflexivDSDynamicKmerPatching();
        rfPipe.setParam(param);
        rfPipe.assemblyFromKmer();
    }

    public void reflexivDSStitchingPipe() throws IOException {
        ReflexivDSStitching rfPipe = new ReflexivDSStitching();
        rfPipe.setParam(param);
        ReflexivDSStitchingLonger rfPipeLong= new ReflexivDSStitchingLonger();
        rfPipeLong.setParam(param);

        param.stitchKmerLength=21;
        reflexivDSLowCoverageCountingPipe();

        param.inputKmerPath1 = param.outputPath + "/Stitch_kmer/Count_" + param.stitchKmerLength + "_sorted/part*.csv.gz";
        param.inputKmerPath2 = param.outputPath + "/Assembly_intermediate/03FixingAgain/part*";

        rfPipe.assemblyFromKmer();

        param.stitchKmerLength=31;
        reflexivDSLowCoverageCountingPipe();

        param.inputKmerPath1 = param.outputPath + "/Stitch_kmer/Count_" + param.stitchKmerLength + "_sorted/part*.csv.gz";
        param.inputKmerPath2 = param.outputPath + "/Assembly_intermediate/Assembly_stitched_" + 21 + "/part*";

        rfPipe.assemblyFromKmer();

        param.stitchKmerLength=61;
        reflexivDSLowCoverageCountingPipe();

        param.inputKmerPath1 = param.outputPath + "/Stitch_kmer/Count_" + param.stitchKmerLength + "_sorted/part*.csv.gz";
//        param.inputKmerPath2 = param.outputPath + "/Assembly_intermediate/03FixingAgain/part*";
        param.inputKmerPath2 = param.outputPath + "/Assembly_intermediate/Assembly_stitched_" + 31 + "/part*";
        rfPipeLong.assemblyFromKmer();
    }

    public void reflexivDSLowCoverageCountingPipe() throws IOException {
        int stitchSize=param.stitchKmerLength;
        info.readMessage("-------- Starting stitch k-mer checking: stitch k-mer size " + stitchSize + " --------");
        info.screenDump();

        String oldPath= param.outputPath ;
        param.outputPath= param.outputPath + "/Stitch_kmer";
        param.maxKmerCoverage=1; // stitch k-mer has only 1 coverage
        param.minKmerCoverage=1;

        if (!checkOutputFile(param.outputPath + "/Count_" + stitchSize + "_sorted")) {
            info.readMessage("Checking existing stitch k-mer counts: Count_" + stitchSize + "_sorted does not exist");
            info.screenDump();

            if (!checkOutputFile(param.outputPath + "/Count_" + stitchSize)) {
                info.readMessage("Checking existing stitch k-mer counts: Count_" + stitchSize+ " does not exist");
                info.screenDump();

                param.setKmerSize(stitchSize); // set kmer size for counter
                param.setAllbyKmerSize(stitchSize);

                info.readMessage("Start counting stitch k-mer" + stitchSize);
                info.screenDump();

                if (param.kmerSize <= 31) {
                    reflexivDSCounterPipe();
                } else {
                    reflexivDS64CounterPipe();
                }


            }else{
                info.readMessage("Checking existing stitch k-mer counts: Count_" + stitchSize + " succeeded");
                info.screenDump();
                info.readMessage("Skip counting stitch k-mer" + stitchSize);
                info.screenDump();
            }

            param.inputKmerPath = param.outputPath + "/Count_" + stitchSize + "/part*.csv.gz";

            param.setKmerSize(stitchSize); // set kmer size for counter
            param.setAllbyKmerSize(stitchSize);

            info.readMessage("Start sorting stitch k-mer" + stitchSize);
            info.screenDump();

            param.maxKmerCoverage=1000000;
            reflexivLeftAndRightSortingPipe();


            if (checkOutputFile(param.outputPath + "/Count_" + stitchSize+ "_sorted")) {
                info.readMessage("Finished stitch k-mer sorting : " + stitchSize+ " succeeded");
                info.screenDump();

                info.readMessage("Removing : stitch k-mer Count_" + stitchSize);
                info.screenDump();

                cleanDiskStorage(param.outputPath + "/Count_" + stitchSize);
            } else {
                info.readMessage("Failed stitch k-mer sorting : " + stitchSize + " failed:");
                info.screenDump();
                info.readMessage("The process is finished. However, one or more results are not complete");
            }

            param.outputPath=oldPath;

        }else{
            info.readMessage("Checking existing stitch k-mer counts: Count_" + stitchSize + "_sorted succeeded");
            info.screenDump();
        }
    }

    public void reflexivDSDecompresserPipe() throws IOException {
        ReflexivDataFrameDecompresser rfPipe = new ReflexivDataFrameDecompresser();
        rfPipe.setParam(param);

        if (param.interleaved != null) {
            if (!checkOutputFile(param.outputPath + "/Read_Interleaved") || !checkOutputFile(param.outputPath + "/Read_Interleaved_Merged")) {
                info.readMessage("Starting to process interleaved input reads");
                info.screenDump();

                cleanDiskStorage(param.outputPath + "/Read_Interleaved");
                cleanDiskStorage(param.outputPath + "/Read_Interleaved_Merged");

                param.interleavedSwitch = true;
                param.inputFqPath = param.interleaved;
                rfPipe.assembly();
            } else {
                info.readMessage("interleaved input reads has already been processed");
                info.screenDump();
                param.pairing=true;
            }
        }

        if (param.inputPaired !=null){
            if (!checkOutputFile(param.outputPath + "/Read_Paired") || !checkOutputFile(param.outputPath + "/Read_Paired_Merged")) {
                info.readMessage("Starting to process paired-end input");
                info.screenDump();

                cleanDiskStorage(param.outputPath + "/Read_Paired");
                cleanDiskStorage(param.outputPath + "/Read_Paired_Merged");

                param.inputSingleSwitch = false;
                param.interleavedSwitch = false;
                param.inputPairedSwitch = true;
                param.inputFqPath = param.inputPaired;
                rfPipe.assembly();
            }else{
                info.readMessage("paired-end input has already been processed");
                info.screenDump();
                param.pairing=true;
            }
        }

        if (param.inputSingle !=null) {
            if (param.pairing){
                if (!checkOutputFile(param.outputPath + "/Read_Single") || ! checkOutputFile(param.outputPath + "/Read_Repartitioned")) {
                    info.readMessage("Starting to process single end input");
                    info.screenDump();

                    cleanDiskStorage(param.outputPath + "/Read_Single");
                    cleanDiskStorage(param.outputPath + "/Read_Repartitioned");

                    param.interleavedSwitch = false;
                    param.inputPairedSwitch = false;  // however param.pairing is on
                    param.inputSingleSwitch = true;
                    param.inputFqPath=param.inputSingle;
                    rfPipe.assembly();
                } else{
                    info.readMessage("Single end input has already been processed");
                    info.screenDump();
                }
            }else{
                if (! checkOutputFile(param.outputPath + "/Read_Repartitioned")) {
                    info.readMessage("Starting to process single end input");
                    info.screenDump();

                    cleanDiskStorage(param.outputPath + "/Read_Single");
                    cleanDiskStorage(param.outputPath + "/Read_Repartitioned");

                    param.interleavedSwitch = false;
                    param.inputPairedSwitch = false;  // however param.pairing is on
                    param.inputSingleSwitch = true;
                    param.inputFqPath=param.inputSingle;
                    rfPipe.assembly();
                }else{
                    info.readMessage("Single end input has already been processed");
                    info.screenDump();
                }
            }

        }
    }

    public void reflexivDSDynamicKmerReductionPipe(){
        ReflexivDSDynamicKmerRuduction rfPipe = new ReflexivDSDynamicKmerRuduction();
        rfPipe.setParam(param);
        rfPipe.assemblyFromKmer();
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

    private void cleanDiskStorage(String file) throws IOException{
        if (file.startsWith("hdfs")){
            Configuration conf = new Configuration();
            String header = file.substring(0,file.indexOf(":9000")+5);
            conf.set("fs.default.name", header);
            FileSystem hdfs = FileSystem.get(conf);

            Path fileHDFSPath= new Path(file.substring(file.indexOf(":9000")+5)+"/_SUCCESS");
            Path folderHDFSPath= new Path(file.substring(file.indexOf(":9000")+5));

            if (hdfs.exists(folderHDFSPath)){
                hdfs.delete(folderHDFSPath, true);
            }

        }else{
            File localFile= new File(file);
            if (localFile.exists()) {
                Runtime.getRuntime().exec("rm -r " + file);
            }
        }

    }

    private void renameDiskStorage(String oldFile, String newFile) throws IOException {
        if (oldFile.startsWith("hdfs")) {

            Configuration conf = new Configuration();
            String header = oldFile.substring(0, oldFile.indexOf(":9000") + 5);
            conf.set("fs.default.name", header);
            FileSystem hdfs = FileSystem.get(conf);

            Path oldFileHDFSPath= new Path(oldFile.substring(oldFile.indexOf(":9000")+5)+"/_SUCCESS");
            Path oldFolderHDFSPath= new Path(oldFile.substring(oldFile.indexOf(":9000")+5));

            Path newFileHDFSPath= new Path(newFile.substring(newFile.indexOf(":9000")+5)+"/_SUCCESS");
            Path newFolderHDFSPath= new Path(newFile.substring(newFile.indexOf(":9000")+5));

            if (hdfs.exists(oldFileHDFSPath)){
                if (hdfs.exists(newFileHDFSPath)){
                    info.readMessage("Destination folder: " + newFolderHDFSPath + " already exist");
                    info.screenDump();
                }else {
                    hdfs.rename(oldFolderHDFSPath, newFolderHDFSPath);
                }
            }else{
                info.readMessage("Targeted rename folder: " + oldFolderHDFSPath + " does not exist");
                info.screenDump();
            }

        }else{
            Runtime.getRuntime().exec("mv -v " + oldFile + " " + newFile);
        }
    }

    public void reflexivDSDynamicKmerFirstFourPipe() throws IOException{

        ReflexivDSDynamicKmerFirstFour rfPipe = new ReflexivDSDynamicKmerFirstFour();
        rfPipe.setParam(param);
        rfPipe.assemblyFromKmer();
    }

    public void reflexivDSDynamicKmerIterationPipe() throws IOException{

        ReflexivDSDynamicKmerIteration rfPipe = new ReflexivDSDynamicKmerIteration();
        rfPipe.setParam(param);
        rfPipe.assemblyFromKmer();
    }

    public void reflexivDSDynamicKmerFixingPipe() throws IOException{

        ReflexivDSDynamicKmerFixing rfPipe = new ReflexivDSDynamicKmerFixing();
        rfPipe.setParam(param);
        rfPipe.assemblyFromKmer();
    }

    public void reflexivDSDynamicKmerFixingRoundTwoPipe() throws IOException{

        ReflexivDSDynamicKmerFixingRoundTwo rfPipe = new ReflexivDSDynamicKmerFixingRoundTwo();
        rfPipe.setParam(param);
        rfPipe.assemblyFromKmer();
    }

    public void reflexivDSDynamicKmerDedupPipe() throws IOException{

        ReflexivDSDynamicKmerDedup rfPipe = new ReflexivDSDynamicKmerDedup();
        rfPipe.setParam(param);
        rfPipe.assemblyFromKmer();
    }


    private int checkStepsForDynamicAssemblyPipe() throws IOException {
        /*
        if (checkOutputFile(param.outputPath + "/Assembly_intermediate/04Patching")){
            info.readParagraphedMessages("04Patching succeed, will use existing results:\n"+ param.outputPath + "/Assembly_intermediate/04Patching");
            info.screenDump();

            info.readMessage("Removing: " + param.outputPath + "/Assembly_intermediate/00firstFour");
            info.screenDump();
            cleanDiskStorage(param.outputPath + "/Assembly_intermediate/00firstFour");

            info.readMessage("Removing: " + param.outputPath + "/Assembly_intermediate/01Iteration15_19");
            info.screenDump();
            cleanDiskStorage(param.outputPath + "/Assembly_intermediate/01Iteration15_19");

            info.readMessage("Removing: " + param.outputPath + "/Assembly_intermediate/01Iteration61_70");
            info.screenDump();
            cleanDiskStorage(param.outputPath + "/Assembly_intermediate/01Iteration61_70");

            info.readMessage("Removing: " + param.outputPath + "/Assembly_intermediate/02Fixing");
            info.screenDump();
            cleanDiskStorage(param.outputPath + "/Assembly_intermediate/02Fixing");

            info.readMessage("Removing: " + param.outputPath + "/Assembly_intermediate/03FixingAgain");
            info.screenDump();
            cleanDiskStorage(param.outputPath + "/Assembly_intermediate/03FixingAgain");

            return 6;
        }*/
        if (checkOutputFile(param.outputPath + "/Assembly")){
            info.readParagraphedMessages("An assembly already exist: \nUse a new output directory or delete the existing one at:\n\t"+ param.outputPath + "/Assembly");
            info.screenDump();
            return 6; // 6 as kill
        } else if (checkOutputFile(param.outputPath + "/Assembly_intermediate/03FixingAgain")){

            info.readParagraphedMessages("03FixingAgain succeed, will use existing results:\n"+ param.outputPath + "/Assembly_intermediate/03FixingAgain");
            info.screenDump();

            info.readMessage("Removing: " + param.outputPath + "/Assembly_intermediate/00firstFour");
            info.screenDump();
            cleanDiskStorage(param.outputPath + "/Assembly_intermediate/00firstFour");

            info.readMessage("Removing: " + param.outputPath + "/Assembly_intermediate/01Iteration15_19");
            info.screenDump();
            cleanDiskStorage(param.outputPath + "/Assembly_intermediate/01Iteration15_19");

            info.readMessage("Removing: " + param.outputPath + "/Assembly_intermediate/01Iteration61_70");
            info.screenDump();
           // cleanDiskStorage(param.outputPath + "/Assembly_intermediate/01Iteration61_70");

            info.readMessage("Removing: " + param.outputPath + "/Assembly_intermediate/02Fixing");
            info.screenDump();
            cleanDiskStorage(param.outputPath + "/Assembly_intermediate/02Fixing");

            return 5;

        }else if (checkOutputFile(param.outputPath + "/Assembly_intermediate/02Fixing")){
            info.readParagraphedMessages("02Fixing succeed, will use existing results:\n"+ param.outputPath + "/Assembly_intermediate/02Fixing");
            info.screenDump();

            info.readMessage("Removing: " + param.outputPath + "/Assembly_intermediate/00firstFour");
            info.screenDump();
            cleanDiskStorage(param.outputPath + "/Assembly_intermediate/00firstFour");

            info.readMessage("Removing: " + param.outputPath + "/Assembly_intermediate/01Iteration15_19");
            info.screenDump();
            cleanDiskStorage(param.outputPath + "/Assembly_intermediate/01Iteration15_19");

            info.readMessage("Removing: " + param.outputPath + "/Assembly_intermediate/01Iteration61_70");
            info.screenDump();
            // cleanDiskStorage(param.outputPath + "/Assembly_intermediate/01Iteration61_70");

            return 4;

        }else if (checkOutputFile(param.outputPath + "/Assembly_intermediate/01Iteration61_70")){
            info.readParagraphedMessages("01Iteration61_70 succeed, will use existing results:\n"+ param.outputPath + "/Assembly_intermediate/01Iteration61_70");
            info.screenDump();

            info.readMessage("Removing: " + param.outputPath + "/Assembly_intermediate/00firstFour");
            info.screenDump();
            cleanDiskStorage(param.outputPath + "/Assembly_intermediate/00firstFour");

            info.readMessage("Removing: " + param.outputPath + "/Assembly_intermediate/01Iteration15_19");
            info.screenDump();
            cleanDiskStorage(param.outputPath + "/Assembly_intermediate/01Iteration15_19");

            return 3;

        }else if (checkOutputFile(param.outputPath + "/Assembly_intermediate/01Iteration15_19")){
            info.readParagraphedMessages("01Iteration15_19 succeed, will use existing results:"+ param.outputPath + "/Assembly_intermediate/01Iteration15_19");
            info.screenDump();

            info.readMessage("Removing: " + param.outputPath + "/Assembly_intermediate/00firstFour");
            info.screenDump();
            cleanDiskStorage(param.outputPath + "/Assembly_intermediate/00firstFour");

            return 2;

        }else if (checkOutputFile(param.outputPath + "/Assembly_intermediate/00firstFour")){
            info.readParagraphedMessages("00firstFour succeed, will use existing results:\n"+ param.outputPath + "/Assembly_intermediate/00firstFour");
            info.screenDump();

            return 1;
        }else{
            info.readParagraphedMessages("Start from the k-mers :\n"+ param.outputPath + "/Count_*_reduced/part*");
            info.screenDump();

            return 0;
        }
    }

    /**
     * Small chucks of steps for dynamic genome assembly
     *
     * Spark keeping shuffle intermediate results to preserve the linage for fault tolerance
     * Splitting the long pipeline to steps reduces storage for intermediate results
     *
     * @throws IOException
     */
    public void reflexivDSDynamicAssemblyStepsPipe() throws IOException{

        info.readMessage("-------- Starting Assembly pipeline --------");
        info.screenDump();

        int OriginalShufflePartition = param.shufflePartition;
        int OriginalPartition = param.partitions;

        param.setGzip(true);

        int step = checkStepsForDynamicAssemblyPipe();

        if (step ==6){
            exit(1);
        }

        if (step<1) {
            param.inputKmerPath = param.outputPath + "/Count_*_reduced/part*.csv.gz";

            info.readMessage("Starting first 4 iterations");
            info.screenDump();
            reflexivDSDynamicKmerFirstFourPipe();

            info.readMessage("First 4 iterations finished");
            info.screenDump();

            if (checkOutputFile(param.outputPath + "/Assembly_intermediate/00firstFour")){
                info.readMessage("00firstFour succeed");
                info.screenDump();
            }else{
                info.readMessage("Failed first four iterations : ");
                info.screenDump();
                info.readMessage("The process is finished. However, one or more results are not complete");
                info.screenDump();
            }
        }

        if (step<2) {
            if (OriginalPartition>=10){
                param.partitions=param.partitions*80/100;
            }
            if (OriginalShufflePartition>=10){
                param.shufflePartition=param.shufflePartition*80/100;
            }

            param.inputKmerPath = param.outputPath + "/Assembly_intermediate/00firstFour/part*";
            for (int i = 5; i < 20; i += 5) {
                param.startIteration = i;
                param.endIteration = i + 4;

                info.readMessage("Starting " + param.startIteration + " -> " + param.endIteration + " iterations");
                info.screenDump();
                reflexivDSDynamicKmerIterationPipe();

                info.readMessage("Iterations from " + param.startIteration + " -> " + param.endIteration + " finished");
                info.screenDump();

                if (checkOutputFile(param.outputPath + "/Assembly_intermediate/01Iteration" + param.startIteration + "_" +  param.endIteration)){
                    info.readMessage("Removing: " + param.inputKmerPath.substring(0,param.inputKmerPath.length()-6));
                    info.screenDump();
                    cleanDiskStorage(param.inputKmerPath.substring(0,param.inputKmerPath.length()-6));
                }else{
                    info.readMessage("Failed " + param.startIteration + " -> " + param.endIteration + " iterations : ");
                    info.screenDump();
                    info.readMessage("The process is finished. However, one or more results are not complete");
                    info.screenDump();
                }

                param.inputKmerPath = param.outputPath + "/Assembly_intermediate/01Iteration" + param.startIteration + "_" + param.endIteration + "/part*";
            }


        }

        if (step <3) {

            if (OriginalPartition >=10){
                param.partitions=OriginalPartition*60/100;
            }
            if (OriginalShufflePartition >=10){
                param.shufflePartition=OriginalShufflePartition*60/100;
            }

            param.inputKmerPath = param.outputPath + "/Assembly_intermediate/01Iteration15_19/part*";

            for (int i = 21; i < 71; i += 10) {
                param.startIteration = i;
                param.endIteration = i + 9;

                info.readMessage("Starting " + param.startIteration + " -> " + param.endIteration + " iterations");
                info.screenDump();
                reflexivDSDynamicKmerIterationPipe();

                info.readMessage("Iterations from " + param.startIteration + " -> " + param.endIteration + " finished");
                info.screenDump();

                if (checkOutputFile(param.outputPath + "/Assembly_intermediate/01Iteration" + param.startIteration + "_" + param.endIteration)){
                    info.readMessage("Removing: " + param.inputKmerPath.substring(0,param.inputKmerPath.length()-6));
                    info.screenDump();

                    cleanDiskStorage(param.inputKmerPath.substring(0,param.inputKmerPath.length()-6));
                }else{
                    info.readMessage("Failed " + param.startIteration + " -> " + param.endIteration + " iterations : ");
                    info.screenDump();
                    info.readMessage("The process is finished. However, one or more results are not complete");
                    info.screenDump();
                }

                param.inputKmerPath = param.outputPath + "/Assembly_intermediate/01Iteration" + param.startIteration + "_" + param.endIteration + "/part*";
            }


        }

        if (step <4) {

            if (OriginalPartition>=10) {
                if (OriginalPartition>=1000 ){
                    param.partitions = OriginalPartition * 10 / 100;
                } else if (OriginalPartition>=100 && OriginalPartition<1000){
                    param.partitions = OriginalPartition * 20 / 100;
                }else {
                    param.partitions = OriginalPartition * 40 / 100;
                }
            }

            if (OriginalShufflePartition>=10) {
                if (OriginalShufflePartition>=1000 ){
                    param.shufflePartition = OriginalShufflePartition * 10 / 100;
                } else if (OriginalShufflePartition>=100 && OriginalShufflePartition<1000){
                    param.shufflePartition = OriginalShufflePartition * 20 / 100;
                } else {
                    param.shufflePartition = OriginalShufflePartition * 40 / 100;
                }
            }




            param.inputKmerPath = param.outputPath + "/Assembly_intermediate/01Iteration61_70/part*";

            info.readMessage("Start Fixing Contigs");
            info.screenDump();
            reflexivDSDynamicKmerFixingPipe();

            info.readMessage("Fixing Contigs finished");
            info.screenDump();

            if (checkOutputFile(param.outputPath + "/Assembly_intermediate/02Fixing")){
                info.readMessage("Removing: " + param.inputKmerPath.substring(0,param.inputKmerPath.length()-6));
                info.screenDump();
                // cleanDiskStorage(param.inputKmerPath.substring(0,param.inputKmerPath.length()-6));
            }else{
                info.readMessage("Failed Fixing contigs : ");
                info.screenDump();
                info.readMessage("The process is finished. However, one or more results are not complete");
                info.screenDump();
            }
        }

        if (step <5) {

            if (OriginalPartition>=10) {
                if (OriginalPartition>=1000 ){
                    param.partitions = OriginalPartition * 10 / 100;
                } else if (OriginalPartition>=100 && OriginalPartition<1000){
                    param.partitions = OriginalPartition * 20 / 100;
                }else {
                    param.partitions = OriginalPartition * 40 / 100;
                }
            }

            if (OriginalShufflePartition>=10) {
                if (OriginalShufflePartition>=1000 ){
                    param.shufflePartition = OriginalShufflePartition * 10 / 100;
                } else if (OriginalShufflePartition>=100 && OriginalShufflePartition<1000){
                    param.shufflePartition = OriginalShufflePartition * 20 / 100;
                } else {
                    param.shufflePartition = OriginalShufflePartition * 40 / 100;
                }
            }


            param.inputKmerPath = param.outputPath + "/Assembly_intermediate/02Fixing/part*";

            info.readMessage("Start Fixing Contigs round two");
            info.screenDump();
            reflexivDSDynamicKmerFixingRoundTwoPipe();

            info.readMessage("Fixing Contigs round two finished");
            info.screenDump();

            if (checkOutputFile(param.outputPath + "/Assembly_intermediate/03FixingAgain")){
                info.readMessage("Removing: " + param.inputKmerPath.substring(0,param.inputKmerPath.length()-6));
                info.screenDump();
                cleanDiskStorage(param.inputKmerPath.substring(0,param.inputKmerPath.length()-6));
            }else{
                info.readMessage("Failed Fixing contigs : ");
                info.screenDump();
                info.readMessage("The process is finished. However, one or more results are not complete");
                info.screenDump();
            }
        }

 //       if (step <6){
/*
            if (OriginalPartition>=10) {
                if (OriginalPartition>=20000){
                    param.partitions = OriginalPartition * 2 / 100;
                } else if (OriginalPartition>=5000 && OriginalPartition< 20000){
                    param.partitions = OriginalPartition * 4 / 100;
                } else if (OriginalPartition>=1000 && OriginalPartition <5000 ){
                    param.partitions = OriginalPartition * 6 / 100;
                } else if (OriginalPartition>=100 && OriginalPartition<1000){
                    param.partitions = OriginalPartition * 20 / 100;
                }else {
                    param.partitions = OriginalPartition * 40 / 100;
                }
            }


            if (OriginalShufflePartition>=10) {
                if (OriginalShufflePartition>=20000){
                    param.shufflePartition = OriginalShufflePartition * 2 / 100;
                } else if (OriginalShufflePartition>=5000 && OriginalShufflePartition< 20000){
                    param.shufflePartition = OriginalShufflePartition * 4 / 100;
                } else if (OriginalShufflePartition>=1000 && OriginalShufflePartition <5000 ){
                    param.shufflePartition = OriginalShufflePartition * 6 / 100;
                } else if (OriginalShufflePartition>=100 && OriginalShufflePartition<1000){
                    param.shufflePartition = OriginalShufflePartition * 20 / 100;
                } else {
                    param.shufflePartition = OriginalShufflePartition * 40 / 100;
                }
            }
*/
            /*
            param.partitions=0;



            info.readMessage("Start patching Contigs with reads");
            info.screenDump();

            reflexivDSPatchingPipe();

            info.readMessage("Contig patching finished");
            info.screenDump();

            if (checkOutputFile(param.outputPath + "/Assembly_intermediate/04Patching")){
                info.readMessage("Removing: " + param.inputKmerPath.substring(0,param.inputKmerPath.length()-6));
                info.screenDump();
                cleanDiskStorage(param.inputKmerPath.substring(0,param.inputKmerPath.length()-6));
            }else{
                info.readMessage("Failed Patching contigs : ");
                info.screenDump();
                info.readMessage("The process is finished. However, one or more results are not complete");
                info.screenDump();
            }
        }

        param.inputKmerPath = param.outputPath + "/Assembly_intermediate/04Patching/part*";
*/
        param.inputKmerPath = param.outputPath + "/Assembly_intermediate/03FixingAgain/part*";

        if (OriginalPartition>=10) {
            if (OriginalPartition>=20000){
                param.partitions = OriginalPartition * 2 / 100;
            } else if (OriginalPartition>=5000 && OriginalPartition< 20000){
                param.partitions = OriginalPartition * 4 / 100;
            } else if (OriginalPartition>=1000 && OriginalPartition <5000 ){
                param.partitions = OriginalPartition * 6 / 100;
            } else if (OriginalPartition>=100 && OriginalPartition<1000){
                param.partitions = OriginalPartition * 20 / 100;
            }else {
                param.partitions = OriginalPartition * 40 / 100;
            }
        }


        if (OriginalShufflePartition>=10) {
            if (OriginalShufflePartition>=20000){
                param.shufflePartition = OriginalShufflePartition * 2 / 100;
            } else if (OriginalShufflePartition>=5000 && OriginalShufflePartition< 20000){
                param.shufflePartition = OriginalShufflePartition * 4 / 100;
            } else if (OriginalShufflePartition>=1000 && OriginalShufflePartition <5000 ){
                param.shufflePartition = OriginalShufflePartition * 6 / 100;
            } else if (OriginalShufflePartition>=100 && OriginalShufflePartition<1000){
                param.shufflePartition = OriginalShufflePartition * 20 / 100;
            } else {
                param.shufflePartition = OriginalShufflePartition * 40 / 100;
            }
        }



        param.gzip=false;
        info.readMessage("Start removing duplication");
        info.screenDump();

        reflexivDSDynamicKmerDedupPipe();

        info.readMessage("Duplication removal finished");
        info.screenDump();


        if (checkOutputFile(param.outputPath + "/Assembly")){
            info.readParagraphedMessages("Assembly succeed at : \n\t" + param.outputPath + "/Assembly");
            info.screenDump();
            info.readMessage("\t\"Thank you for choosing Reflexiv\"");
            info.screenDump();

            /*   not removing for now
            info.readMessage("Removing: " + param.inputKmerPath.substring(0,param.inputKmerPath.length()-6));
            info.screenDump();
            cleanDiskStorage(param.inputKmerPath.substring(0,param.inputKmerPath.length()-6));
            */
        }else{
            info.readMessage("Failed Fixing contigs : ");
            info.screenDump();
            info.readMessage("The process is finished. However, one or more results are not complete");
            info.screenDump();
        }

    }

    public void reflexivDSDynamicAssemblyPipe() throws IOException{
        ReflexivDSDynamicKmer64 rfPipe = new ReflexivDSDynamicKmer64();
        rfPipe.setParam(param);
        rfPipe.assemblyFromKmer();
    }

    public void reflexivDSInputDataPreprocessing() throws IOException {
        if (!checkOutputFile(param.outputPath + "/Read_Repartitioned")) {
            reflexivDSDecompresserPipe();
        }else{
            info.readMessage("Output file: " + param.outputPath + "/Read_Repartitioned already exist!");
            info.screenDump();
        }
    }

    public void reflexivLeftAndRightSortingPipe() throws IOException {
        ReflexivDSKmerLeftAndRightSorting rfPipe = new ReflexivDSKmerLeftAndRightSorting();
        rfPipe.setParam(param);
        rfPipe.assemblyFromKmer();
    }

    public void reflexivDSDynamicReductionPipe() throws IOException {



        // step 1 decompress
        //      if (!checkOutputFile(param.outputPath + "/Read_Repartitioned")) {
        //          reflexivDSDecompresserPipe();
        //      }
        param.setGzip(true);
        param.inputFormat="4mc";

        // step 2 smallest kmer count

        //      if (param.inputFormat.equals("mc4")) {
        //          param.setInputFqPath(param.outputPath + "/Read_Repartitioned/part*.mc4");
        //      }else{
        //          param.setInputFqPath(param.inputFqPath);
        //      }

        param.setInputFqPath(param.inputFqPath);
        if (checkOutputFile(param.outputPath + "Read_Interleaved_Merged") || checkOutputFile(param.outputPath + "/Read_Paired_Merged")){
            param.minKmerCoverage=2*param.minKmerCoverage-1;
        }

        if (param.kmerListInt.length<2){ // only one kmer in the kmerlist
            info.readMessage("Only 1 kmer size has been provided: " + param.kmerSize1 + ". Require at least two");
            info.screenDump();
        }else {
            param.kmerSize2 = param.kmerListInt[1];
        }

        for (int i=1;i<param.kmerListInt.length;i++) {
            param.kmerSize1 = param.kmerListInt[i - 1];
            param.kmerSize2 = param.kmerListInt[i];

            /**
             * kmer1_count      ---
             * kmer0_reduced
             * kmer1_sorted     --
             * kmer2_count      ---
             * kmer1_reduced
             * kmer2_sorted     --
             * kmer3_count      ---
             * kmer2_reduced
             * kmer3_sorted     --
             */

            info.readMessage("-------- Starting from k-mer pairs: " + param.kmerSize1 + " and " + param.kmerSize2 + " --------");
            info.screenDump();

            if (!checkOutputFile(param.outputPath + "/Count_" + param.kmerSize1 + "_reduced")) {
                info.readMessage("Checking existing k-mer counts: Count_" + param.kmerSize1 + "_reduced does not exist");
                info.screenDump();

                if (!checkOutputFile(param.outputPath + "/Count_" + param.kmerSize1 + "_sorted")) {
                    info.readMessage("Checking existing k-mer counts: Count_" + param.kmerSize1 + "_sorted does not exist");
                    info.screenDump();

                    if (!checkOutputFile(param.outputPath + "/Count_" + param.kmerSize1)) {
                        info.readMessage("Checking existing k-mer counts: Count_" + param.kmerSize1 + " does not exist");
                        info.screenDump();

                        param.setKmerSize(param.kmerSize1); // set kmer size for counter
                        param.setAllbyKmerSize(param.kmerSize1);

                        info.readMessage("Start counting " + param.kmerSize1);
                        info.screenDump();
                        if (param.kmerSize <= 31) {
                            reflexivDSCounterPipe();
                        } else {
                            reflexivDS64CounterPipe();
                        }
                    } else {
                        info.readMessage("Checking existing k-mer counts: Count_" + param.kmerSize1 + " succeeded");
                        info.screenDump();
                        info.readMessage("Skip counting " + param.kmerSize1);
                        info.screenDump();
                    }

                    param.setKmerSize(param.kmerSize1); // set kmer size for counter
                    param.setAllbyKmerSize(param.kmerSize1);

                    param.inputKmerPath = param.outputPath + "/Count_" + param.kmerSize1 + "/part*.csv.gz";

                    if (param.kmerSize1>=61 && param.kmerSize2 <81 ){
                        param.minErrorCoverage=3*param.minKmerCoverage;
                    }else if (param.kmerSize1>=81) {
                        param.minErrorCoverage=3*param.minKmerCoverage;
                    }

                    // System.out.println("minErrorCoverage: " + param.minErrorCoverage);

                    info.readMessage("Start sorting " + param.kmerSize1);
                    info.screenDump();
                    reflexivLeftAndRightSortingPipe();

                    if (checkOutputFile(param.outputPath + "/Count_" + param.kmerSize1 + "_sorted")) {
                        info.readMessage("Finished k-mer sorting : " + param.kmerSize1 + " succeeded");
                        info.screenDump();

                        info.readMessage("Removing : Count_" + param.kmerSize1);
                        info.screenDump();

                        cleanDiskStorage(param.outputPath + "/Count_" + param.kmerSize1);
                    } else {
                        info.readMessage("Failed k-mer sorting : " + param.kmerSize1 + " failed:");
                        info.screenDump();
                        info.readMessage("The process is finished. However, one or more results are not complete");
                    }

                } else { // kmer1 sorted exist
                    info.readMessage("Checking existing k-mer counts: Count_" + param.kmerSize1 + "_sorted succeeded");
                    info.screenDump();

                    info.readMessage("Removing : Count_" + param.kmerSize1);
                    info.screenDump();

                    cleanDiskStorage(param.outputPath + "/Count_" + param.kmerSize1);
                }

                if (!checkOutputFile(param.outputPath + "/Count_" + param.kmerSize2 + "_sorted")) {
                    info.readMessage("Checking existing k-mer counts: Count_" + param.kmerSize2 + "_sorted does not exist");
                    info.screenDump();

                    if (!checkOutputFile(param.outputPath + "/Count_" + param.kmerSize2)) {
                        info.readMessage("Checking existing k-mer counts: Count_" + param.kmerSize2 + " does not exist");
                        info.screenDump();

                        param.setKmerSize(param.kmerSize2); // set kmer size for counter
                        param.setAllbyKmerSize(param.kmerSize2);

                        info.readMessage("Start counting " + param.kmerSize2);
                        info.screenDump();
                        if (param.kmerSize <= 31) {
                            reflexivDSCounterPipe();
                        } else {
                            reflexivDS64CounterPipe();
                        }
                    } else {
                        info.readMessage("Checking existing k-mer counts: Count_" + param.kmerSize2 + " succeeded");
                        info.screenDump();
                        info.readMessage("Skip counting " + param.kmerSize2);
                        info.screenDump();
                    }

                    param.setKmerSize(param.kmerSize2); // set kmer size for counter
                    param.setAllbyKmerSize(param.kmerSize2);

                    param.inputKmerPath = param.outputPath + "/Count_" + param.kmerSize2 + "/part*.csv.gz";

                    if (param.kmerSize2>=61 && param.kmerSize2 <81 ){
                        param.minErrorCoverage=3*param.minKmerCoverage;
                    }else if (param.kmerSize2>=81) {
                        param.minErrorCoverage=3*param.minKmerCoverage;
                    }

                    System.out.println("minErrorCoverage: " + param.minErrorCoverage);

                    info.readMessage("Start sorting " + param.kmerSize2);
                    info.screenDump();
                    reflexivLeftAndRightSortingPipe();

                    if (checkOutputFile(param.outputPath + "/Count_" + param.kmerSize2 + "_sorted")) {
                        info.readMessage("Finished k-mer sorting : " + param.kmerSize2 + " succeeded");
                        info.screenDump();

                        info.readMessage("Removing : Count_" + param.kmerSize2);
                        info.screenDump();

                        cleanDiskStorage(param.outputPath + "/Count_" + param.kmerSize2);
                    } else {
                        info.readMessage("Failed k-mer sorting : " + param.kmerSize2 + " failed:");
                        info.screenDump();
                        info.readMessage("The process is finished. However, one or more results are not complete");
                    }

                } else { // kmer1 sorted exist
                    info.readMessage("Checking existing k-mer counts: Count_" + param.kmerSize2 + "_sorted succeeded");
                    info.screenDump();

                    info.readMessage("Removing : Count_" + param.kmerSize2);
                    info.screenDump();

                    cleanDiskStorage(param.outputPath + "/Count_" + param.kmerSize2);
                }

                param.inputKmerPath1 = param.outputPath + "/Count_" + param.kmerSize1 + "_sorted/part*.csv.gz";
                param.inputKmerPath2 = param.outputPath + "/Count_" + param.kmerSize2 + "_sorted/part*.csv.gz";

                info.readMessage("Start k-mer reduction : " + param.kmerSize2 + " vs " + param.kmerSize1);
                info.screenDump();
                reflexivDSDynamicKmerReductionPipe();

                if (checkOutputFile(param.outputPath + "/Count_" + param.kmerSize1 + "_reduced")) {
                    info.readMessage("Finished k-mer reduction : " + param.kmerSize2 + " vs " + param.kmerSize1 + " succeeded");
                    info.screenDump();

                    info.readMessage("Removing: Count_" + param.kmerSize1 + "_sorted");
                    info.screenDump();

                    cleanDiskStorage(param.outputPath + "/Count_" + param.kmerSize1 + "_sorted");
                } else {
                    info.readMessage("Failed k-mer reduction : " + param.kmerSize2 + " vs " + param.kmerSize1 + " failed:");
                    info.screenDump();
                    info.readMessage("The process is finished. However, one or more results are not complete");
                    info.screenDump();
                }

                if (i== param.kmerListInt.length-1){
                    info.readMessage("This is the last k-mer reduction round");
                    info.screenDump();

                    // if (param.kmerSize2 <100) {
                        info.readMessage("Removing: Count_" + param.kmerSize2 + "_sorted");
                        info.screenDump();
                        cleanDiskStorage(param.outputPath + "/Count_" + param.kmerSize2 + "_sorted");
                   /* }else {
                        info.readMessage("Rename last k-mer sorted to k-mer reduced");
                        info.screenDump();
                        renameDiskStorage(param.outputPath + "/Count_" + param.kmerSize2 + "_sorted", param.outputPath + "/Count_" + param.kmerSize2 + "_reduced");
                    } */
                }
            }else{
                info.readMessage("Checking existing k-mer counts: Count_" + param.kmerSize1 + "_reduced succeeded");
                info.screenDump();

                info.readMessage("Removing: Count_" + param.kmerSize1 + "_sorted, Count_" + param.kmerSize1 + ", and Count_" + param.kmerSize2);
                info.screenDump();

                cleanDiskStorage(param.outputPath + "/Count_" + param.kmerSize1 + "_sorted");
                cleanDiskStorage(param.outputPath + "/Count_" + param.kmerSize1);
                cleanDiskStorage(param.outputPath + "/Count_" + param.kmerSize2);
            }
        }

        info.readMessage("-------- Starting last k-mer checking: " + param.kmerListInt[param.kmerListInt.length-1] + " --------");
        info.screenDump();

        if (!checkOutputFile(param.outputPath + "/Count_" + param.kmerListInt[param.kmerListInt.length-1] + "_reduced")) {
            info.readMessage("Checking existing k-mer counts: Count_" + param.kmerListInt[param.kmerListInt.length-1] + "_reduced does not exist");
            info.screenDump();
            info.readMessage("By the way, this is the last k-mer size");
            info.screenDump();

            if (!checkOutputFile(param.outputPath + "/Count_" + param.kmerListInt[param.kmerListInt.length-1] + "_sorted")) {
                info.readMessage("Checking existing k-mer counts: Count_" + param.kmerListInt[param.kmerListInt.length-1] + "_sorted does not exist");
                info.screenDump();

                if (!checkOutputFile(param.outputPath + "/Count_" + param.kmerListInt[param.kmerListInt.length-1] )) {
                    info.readMessage("Checking existing k-mer counts: Count_" + param.kmerListInt[param.kmerListInt.length-1] + " does not exist");
                    info.screenDump();

                    param.setKmerSize(param.kmerListInt[param.kmerListInt.length-1] ); // set kmer size for counter
                    param.setAllbyKmerSize(param.kmerListInt[param.kmerListInt.length-1]) ;

                    info.readMessage("Start counting " + param.kmerListInt[param.kmerListInt.length-1] );
                    info.screenDump();
                    if (param.kmerSize <= 31) {
                        reflexivDSCounterPipe();
                    } else {
                        reflexivDS64CounterPipe();
                    }
                } else {
                    info.readMessage("Checking existing k-mer counts: Count_" + param.kmerListInt[param.kmerListInt.length-1] + " succeeded");
                    info.screenDump();
                    info.readMessage("Skip counting " + param.kmerListInt[param.kmerListInt.length-1] );
                    info.screenDump();
                }

                param.setKmerSize(param.kmerListInt[param.kmerListInt.length - 1]); // set kmer size for counter
                param.setAllbyKmerSize(param.kmerListInt[param.kmerListInt.length - 1]);

                param.inputKmerPath = param.outputPath + "/Count_" + param.kmerListInt[param.kmerListInt.length - 1] + "/part*.csv.gz";

     //           if (param.kmerListInt[param.kmerListInt.length - 1]>=100){
                   // param.minErrorCoverage=4;
      //          }

                info.readMessage("Start sorting " + param.kmerListInt[param.kmerListInt.length - 1]);
                info.screenDump();
                reflexivLeftAndRightSortingPipe();

                if (checkOutputFile(param.outputPath + "/Count_" + param.kmerListInt[param.kmerListInt.length - 1] + "_sorted")) {
                    info.readMessage("Finished k-mer sorting : " + param.kmerListInt[param.kmerListInt.length - 1] + " succeeded");
                    info.screenDump();

                    info.readMessage("Removing : Count_" + param.kmerListInt[param.kmerListInt.length - 1]);
                    info.screenDump();

                    cleanDiskStorage(param.outputPath + "/Count_" + param.kmerListInt[param.kmerListInt.length - 1]);
                } else {
                    info.readMessage("Failed k-mer sorting : " + param.kmerListInt[param.kmerListInt.length - 1] + " failed:");
                    info.screenDump();
                    info.readMessage("The process is finished. However, one or more results are not complete");
                }
            }else{
                info.readMessage("Checking existing k-mer counts: Count_" + param.kmerListInt[param.kmerListInt.length-1] + "_sorted succeeded");
                info.screenDump();

                info.readMessage("Removing : Count_" + param.kmerListInt[param.kmerListInt.length-1] );
                info.screenDump();

                cleanDiskStorage(param.outputPath + "/Count_" + param.kmerListInt[param.kmerListInt.length-1] );

                info.readMessage("This is the last k-mer reduction round");
                info.screenDump();
                info.readMessage("Rename last k-mer sorted to k-mer reduced");
                info.screenDump();
                renameDiskStorage(param.outputPath + "/Count_" +param.kmerListInt[param.kmerListInt.length-1] + "_sorted", param.outputPath + "/Count_" + param.kmerListInt[param.kmerListInt.length-1] + "_reduced");

            }
        }else{
            info.readMessage("Checking existing k-mer counts: Count_" + param.kmerListInt[param.kmerListInt.length-1] + "_reduced succeeded");
            info.screenDump();
        }

        if (param.stitch){
            int stitchSize=param.stitchKmerLength;
            info.readMessage("-------- Starting stitch k-mer checking: stitch k-mer size " + stitchSize + " --------");
            info.screenDump();

            param.outputPath=param.outputPath + "/Stitch_kmer";
            param.maxKmerCoverage=1; // stitch k-mer has only 1 coverage
            param.minKmerCoverage=1;

            if (!checkOutputFile(param.outputPath + "/Count_" + stitchSize + "_sorted")) {
                info.readMessage("Checking existing stitch k-mer counts: Count_" + stitchSize + "_sorted does not exist");
                info.screenDump();

                if (!checkOutputFile(param.outputPath + "/Count_" + stitchSize)) {
                    info.readMessage("Checking existing stitch k-mer counts: Count_" + stitchSize+ " does not exist");
                    info.screenDump();

                    param.setKmerSize(stitchSize); // set kmer size for counter
                    param.setAllbyKmerSize(stitchSize);

                    info.readMessage("Start counting stitch k-mer" + stitchSize);
                    info.screenDump();

                    if (param.kmerSize <= 31) {
                        reflexivDSCounterPipe();
                    } else {
                        reflexivDS64CounterPipe();
                    }


                }else{
                    info.readMessage("Checking existing stitch k-mer counts: Count_" + stitchSize + " succeeded");
                    info.screenDump();
                    info.readMessage("Skip counting stitch k-mer" + stitchSize);
                    info.screenDump();
                }

                param.inputKmerPath = param.outputPath + "/Count_" + stitchSize + "/part*.csv.gz";

                param.setKmerSize(stitchSize); // set kmer size for counter
                param.setAllbyKmerSize(stitchSize);

                info.readMessage("Start sorting stitch k-mer" + stitchSize);
                info.screenDump();

                param.maxKmerCoverage=1000000;
                reflexivLeftAndRightSortingPipe();


                if (checkOutputFile(param.outputPath + "/Count_" + stitchSize+ "_sorted")) {
                    info.readMessage("Finished stitch k-mer sorting : " + stitchSize+ " succeeded");
                    info.screenDump();

                    info.readMessage("Removing : stitch k-mer Count_" + stitchSize);
                    info.screenDump();

                    cleanDiskStorage(param.outputPath + "/Count_" + stitchSize);
                } else {
                    info.readMessage("Failed stitch k-mer sorting : " + stitchSize + " failed:");
                    info.screenDump();
                    info.readMessage("The process is finished. However, one or more results are not complete");
                }

            }else{
                info.readMessage("Checking existing stitch k-mer counts: Count_" + stitchSize + "_sorted succeeded");
                info.screenDump();
            }

        }

        info.readMessage("-------- All k-mer counting finished --------");
        info.screenDump();
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
