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

    public void reflexivDSStitchingPipe() throws IOException {
        ReflexivDSStitching rfPipe = new ReflexivDSStitching();
        rfPipe.setParam(param);
        ReflexivDSStitchingLonger rfPipeLong= new ReflexivDSStitchingLonger();
        rfPipeLong.setParam(param);
/*
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
*/
        param.stitchKmerLength=61;
        reflexivDSLowCoverageCountingPipe();

        param.inputKmerPath1 = param.outputPath + "/Stitch_kmer/Count_" + param.stitchKmerLength + "_sorted/part*.csv.gz";
        param.inputKmerPath2 = param.outputPath + "/Assembly_intermediate/03FixingAgain/part*";
//        param.inputKmerPath2 = param.outputPath + "/Assembly_intermediate/Assembly_stitched_" + 31 + "/part*";
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
        rfPipe.assembly();
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

    public void reflexivDSDynamicAssemblyStepsPipe() throws IOException{
        param.inputKmerPath = param.outputPath + "/Count_*_reduced/part*.csv.gz";

        info.readMessage("Starting first 4 iterations");
        info.screenDump();
        reflexivDSDynamicKmerFirstFourPipe();

        info.readMessage("First 4 iterations finished");
        info.screenDump();

        param.inputKmerPath = param.outputPath + "/Assembly_intermediate/00firstFour/part*.gz";
        for (int i = 5;i<=20;i+=5) {
            param.startIteration=i;
            param.endIteration=i+5;

            info.readMessage("Starting " + param.startIteration + " -> " + param.endIteration + " iterations");
            info.screenDump();
            reflexivDSDynamicKmerIterationPipe();

            info.readMessage("Iterations from " + param.startIteration + " -> " + param.endIteration + " finished" );
            info.screenDump();

            param.inputKmerPath = param.outputPath + "/Assembly_intermediate/01Iteration"+ param.startIteration + "_" + param.endIteration + "/part*.gz";
        }

        for (int i =21 ;i<71;i+=10){
            param.startIteration=i;
            param.endIteration=i+10;

            info.readMessage("Starting " + param.startIteration + " -> " + param.endIteration + " iterations");
            info.screenDump();
            reflexivDSDynamicKmerIterationPipe();

            info.readMessage("Iterations from " + param.startIteration + " -> " + param.endIteration + " finished" );
            info.screenDump();

            param.inputKmerPath = param.outputPath + "/Assembly_intermediate/01Iteration"+ param.startIteration + "_" + param.endIteration + "/part*.gz";
        }

        info.readMessage("Start Fixing Contigs");
        info.screenDump();
        reflexivDSDynamicKmerFixingPipe();

        info.readMessage("Fixing Contigs finished");
        info.screenDump();

        param.inputKmerPath = param.outputPath + "/Assembly_intermediate/02Fixing/part*.gz";

        info.readMessage("Start Fixing Contigs round two");
        info.screenDump();
        reflexivDSDynamicKmerFixingRoundTwoPipe();

        info.readMessage("Fixing Contigs round two finished");
        info.screenDump();

        param.inputKmerPath = param.outputPath + "/Assembly_intermediate/03FixingAgain/part*.gz";

        param.gzip=false;
        info.readMessage("Start removing duplication");
        info.screenDump();
        reflexivDSDynamicKmerDedupPipe();

        info.readMessage("Duplication removal finished");
        info.screenDump();

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

        // step 2 smallest kmer count

        //      if (param.inputFormat.equals("mc4")) {
        //          param.setInputFqPath(param.outputPath + "/Read_Repartitioned/part*.mc4");
        //      }else{
        //          param.setInputFqPath(param.inputFqPath);
        //      }

        param.setInputFqPath(param.inputFqPath);
        //       param.kmerSize1= param.kmerListInt[0];

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

                    if (param.kmerSize1>=61){
                        param.minErrorCoverage=6;
                    }else if (param.kmerSize1>=81 && param.kmerSize2<100) {
                        param.minErrorCoverage=6;
                    }else if (param.kmerSize1>=100){
                        param.minErrorCoverage=2;
                    }

                    System.out.println("minErrorCoverage: " + param.minErrorCoverage);

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

                    if (param.kmerSize2>=61){
                        param.minErrorCoverage=6;
                    }else if (param.kmerSize2>=81 && param.kmerSize2<100) {
                        param.minErrorCoverage=6;
                    }else if (param.kmerSize2>=100){
                        param.minErrorCoverage=2;
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

                    if (param.kmerSize2 <100) {
                        info.readMessage("Removing: Count_" + param.kmerSize2 + "_sorted");
                        info.screenDump();
                        cleanDiskStorage(param.outputPath + "/Count_" + param.kmerSize2 + "_sorted");
                    }else {
                        info.readMessage("Rename last k-mer sorted to k-mer reduced");
                        info.screenDump();
                        renameDiskStorage(param.outputPath + "/Count_" + param.kmerSize2 + "_sorted", param.outputPath + "/Count_" + param.kmerSize2 + "_reduced");
                    }
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

    public void reflexivDSDynamicReductionPipe2() throws IOException {



        // step 1 decompress
  //      if (!checkOutputFile(param.outputPath + "/Read_Repartitioned")) {
  //          reflexivDSDecompresserPipe();
  //      }
        param.setGzip(true);

        // step 2 smallest kmer count

  //      if (param.inputFormat.equals("mc4")) {
  //          param.setInputFqPath(param.outputPath + "/Read_Repartitioned/part*.mc4");
  //      }else{
  //          param.setInputFqPath(param.inputFqPath);
  //      }

        param.setInputFqPath(param.inputFqPath);
 //       param.kmerSize1= param.kmerListInt[0];

        if (param.kmerListInt.length<2){ // only one kmer in the kmerlist
            info.readMessage("Only 1 kmer size has been provided: " + param.kmerSize1 + ". Require at least two");
            info.screenDump();
        }else {
            param.kmerSize2 = param.kmerListInt[1];
        }

        for (int i=1;i<param.kmerListInt.length;i++){
            param.kmerSize1=param.kmerListInt[i-1];
            param.kmerSize2=param.kmerListInt[i];

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

            if (!checkOutputFile(param.outputPath + "/Count_" + param.kmerSize2 + "_sorted")){
                info.readMessage("Checking existing k-mer counts: Count_" + param.kmerSize2 + "_sorted does not exist");
                info.screenDump();

                if (!checkOutputFile(param.outputPath + "/Count_" + param.kmerSize1 + "_reduced")){
                    info.readMessage("Checking existing k-mer counts: Count_" + param.kmerSize1 + "_reduced does not exist");
                    info.screenDump();

                    if (i==1) {
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
                        }else{
                            info.readMessage("Checking existing k-mer counts: Count_" + param.kmerSize1 + " succeeded");
                            info.screenDump();
                            info.readMessage("Skip counting " + param.kmerSize1);
                            info.screenDump();
                        }
                    }else{
                        if (!checkOutputFile(param.outputPath + "/Count_" + param.kmerSize1 + "_sorted")) { // should not happen
                            info.readMessage("Checking existing k-mer counts: Count_" + param.kmerSize1 + "_sorted does not exist.");
                            info.screenDump();

                            if (!checkOutputFile(param.outputPath + "/Count_" + param.kmerSize1)) {

                                param.setKmerSize(param.kmerSize1); // set kmer size for counter
                                param.setAllbyKmerSize(param.kmerSize1);

                                // only counting kmerSize1 for later comparison
                                info.readMessage("Start counting " + param.kmerSize1);
                                info.screenDump();
                                if (param.kmerSize <= 31) {
                                    reflexivDSCounterPipe();
                                } else {
                                    reflexivDS64CounterPipe();
                                }
                            }else{
                                info.readMessage("Checking existing k-mer counts: Count_" + param.kmerSize1 + " succeeded");
                                info.screenDump();
                                info.readMessage("Will use Count_" + param.kmerSize1);
                                info.screenDump();
                            }

                        }else{
                            info.readMessage("Checking existing k-mer counts: Count_" + param.kmerSize1 + "_sorted succeeded");
                            info.screenDump();
                            info.readMessage("Will use Count_" + param.kmerSize1 + "_sorted");
                            info.screenDump();
                        }
                    }

                    if (!checkOutputFile(param.outputPath + "/Count_" + param.kmerSize2)){
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
                    }else{
                        info.readMessage("Checking existing k-mer counts: Count_" + param.kmerSize2 + " succeeded");
                        info.screenDump();
                        info.readMessage("Will use Count_" + param.kmerSize2);
                        info.screenDump();
                    }

                    if (i==1) {
                        param.inputKmerPath1 = param.outputPath + "/Count_" + param.kmerSize1 + "/part*.csv.gz";
                    }else if(!checkOutputFile(param.outputPath + "/Count_" + param.kmerSize1 + "_sorted") ){
                        param.inputKmerPath1 = param.outputPath + "/Count_" + param.kmerSize1 + "/part*.csv.gz";
                    } else{ // after the first iteration
                        param.inputKmerPath1 = param.outputPath + "/Count_" + param.kmerSize1 + "_sorted/part*.csv.gz";
                    }
                    param.inputKmerPath2 = param.outputPath + "/Count_" + param.kmerSize2 + "/part*.csv.gz";

                    info.readMessage("Start k-mer reduction : " + param.kmerSize2 + " vs " + param.kmerSize1);
                    info.screenDump();
                    reflexivDSDynamicKmerReductionPipe();

                    if (checkOutputFile(param.outputPath + "/Count_" + param.kmerSize1 + "_reduced") && checkOutputFile(param.outputPath + "/Count_" + param.kmerSize2 + "_sorted") ) {
                        info.readMessage("Finished k-mer reduction : " + param.kmerSize2 + " vs " + param.kmerSize1 + " succeeded");
                        info.screenDump();

                        info.readMessage("Removing: Count_" + param.kmerSize1 + ", Count_" + param.kmerSize2 + ", and Count_" + param.kmerSize1 + "_sorted");
                        info.screenDump();

                        /**
                         *
                         */
                        cleanDiskStorage(param.outputPath + "/Count_" + param.kmerSize1);
                        cleanDiskStorage(param.outputPath + "/Count_" + param.kmerSize2);
                        cleanDiskStorage(param.outputPath + "/Count_" + param.kmerSize1 + "_sorted");
                    }else if (checkOutputFile(param.outputPath + "/Count_" + param.kmerSize1 + "_reduced") && checkOutputFile(param.outputPath + "/Count_" + param.kmerSize2 + "_reduced") && param.kmerSize2==param.kmerListInt[param.kmerListInt.length-1]) {
                        info.readMessage("Finished k-mer reduction : " + param.kmerSize2 + " vs " + param.kmerSize1 + " succeeded");
                        info.screenDump();

                        info.readMessage("Removing: Count_" + param.kmerSize1 + ", Count_" + param.kmerSize2 + ", and Count_" + param.kmerSize1 + "_sorted");
                        info.screenDump();

                        cleanDiskStorage(param.outputPath + "/Count_" + param.kmerSize1);
                        cleanDiskStorage(param.outputPath + "/Count_" + param.kmerSize2);
                        cleanDiskStorage(param.outputPath + "/Count_" + param.kmerSize1 + "_sorted");
                    }else{
                        info.readMessage("Failed k-mer reduction : " + param.kmerSize2 + " vs " + param.kmerSize1 + " failed:");
                        info.screenDump();
                        info.readMessage("The process is finished. However, one or more results are not complete");
                        info.screenDump();
                    }

                }else{ // reduced exist
                    info.readMessage("Checking existing k-mer counts: Count_" + param.kmerSize1 + "_reduced succeeded");
                    info.screenDump();

                    if (!checkOutputFile(param.outputPath + "/Count_" + param.kmerSize2 + "_reduced")){ // next reduce does not exist

                        info.readMessage("Checking existing k-mer counts: Count_" + param.kmerSize2 + "_reduced does not exist");
                        info.screenDump();

                        if (param.kmerSize2==param.kmerListInt[param.kmerListInt.length-1]) { // last kmer
                            info.readMessage("Checking existing k-mer counts: Last k-mer size does not exist");

                            if (!checkOutputFile(param.outputPath + "/Count_" + param.kmerSize1 + "_sorted")) { // should not happen
                                info.readMessage("Checking existing k-mer counts: Count_" + param.kmerSize1 + "_sorted does not exist.");
                                info.screenDump();

                                if (!checkOutputFile(param.outputPath + "/Count_" + param.kmerSize1)) {

                                    param.setKmerSize(param.kmerSize1); // set kmer size for counter
                                    param.setAllbyKmerSize(param.kmerSize1);

                                    // only counting kmerSize1 for later comparison
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
                                    info.readMessage("Will use Count_" + param.kmerSize1);
                                    info.screenDump();
                                }
                            } else {
                                info.readMessage("Checking existing k-mer counts: Count_" + param.kmerSize1 + "_sorted succeeded");
                                info.screenDump();
                                info.readMessage("Will use Count_" + param.kmerSize1 + "_sorted");
                                info.screenDump();
                            }


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
                            }else{
                                info.readMessage("Checking existing k-mer counts: Count_" + param.kmerSize2 + " succeeded");
                                info.screenDump();
                                info.readMessage("Will use Count_" + param.kmerSize2);
                                info.screenDump();
                            }

                            if (i == 1) {
                                param.inputKmerPath1 = param.outputPath + "/Count_" + param.kmerSize1 + "/part*.csv.gz";
                            } else if (!checkOutputFile(param.outputPath + "/Count_" + param.kmerSize1 + "_sorted")) {
                                param.inputKmerPath1 = param.outputPath + "/Count_" + param.kmerSize1 + "/part*.csv.gz";
                            } else { // after the first iteration
                                param.inputKmerPath1 = param.outputPath + "/Count_" + param.kmerSize1 + "_sorted/part*.csv.gz";
                            }
                            param.inputKmerPath2 = param.outputPath + "/Count_" + param.kmerSize2 + "/part*.csv.gz";

                            info.readMessage("Start k-mer reduction : " + param.kmerSize2 + " vs " + param.kmerSize1);
                            info.screenDump();
                            reflexivDSDynamicKmerReductionPipe();

                            if (checkOutputFile(param.outputPath + "/Count_" + param.kmerSize1 + "_reduced") && checkOutputFile(param.outputPath + "/Count_" + param.kmerSize2 + "_sorted")) {
                                info.readMessage("Finished k-mer reduction : " + param.kmerSize2 + " vs " + param.kmerSize1 + " succeeded");
                                info.screenDump();

                                info.readMessage("Removing: Count_" + param.kmerSize1 + ", Count_" + param.kmerSize2 + ", and Count_" + param.kmerSize1 + "_sorted");
                                info.screenDump();

                                cleanDiskStorage(param.outputPath + "/Count_" + param.kmerSize1);
                                cleanDiskStorage(param.outputPath + "/Count_" + param.kmerSize2);
                                cleanDiskStorage(param.outputPath + "/Count_" + param.kmerSize1 + "_sorted");
                            }else if (checkOutputFile(param.outputPath + "/Count_" + param.kmerSize1 + "_reduced") && checkOutputFile(param.outputPath + "/Count_" + param.kmerSize2 + "_reduced") && param.kmerSize2==param.kmerListInt[param.kmerListInt.length-1]){
                                info.readMessage("Finished k-mer reduction : " + param.kmerSize2 + " vs " + param.kmerSize1 + " succeeded");
                                info.screenDump();

                                info.readMessage("Removing: Count_" + param.kmerSize1 + ", Count_" + param.kmerSize2 + ", and Count_" + param.kmerSize1 + "_sorted");
                                info.screenDump();

                                cleanDiskStorage(param.outputPath + "/Count_" + param.kmerSize1);
                                cleanDiskStorage(param.outputPath + "/Count_" + param.kmerSize2);
                                cleanDiskStorage(param.outputPath + "/Count_" + param.kmerSize1 + "_sorted");
                            } else {
                                info.readMessage("Failed k-mer reduction : " + param.kmerSize2 + " vs " + param.kmerSize1 + " failed:");
                                info.screenDump();
                                info.readMessage("The process is finished. However, one or more results are not complete");
                                info.screenDump();
                            }
                        }else{
                            info.readMessage("Removing: Count_" + param.kmerSize1 + " and Count_" + param.kmerSize1 + "_sorted");
                            info.screenDump();

                            cleanDiskStorage(param.outputPath + "/Count_" + param.kmerSize1);
                          //  cleanDiskStorage(param.outputPath + "/Count_" + param.kmerSize2);
                            cleanDiskStorage(param.outputPath + "/Count_" + param.kmerSize1 + "_sorted");
                        }
                            /**
                             *
                             */
                    }else{ // next reduced exist, remove this sorted and continue
                        info.readMessage("Checking existing k-mer counts: Count_" + param.kmerSize2 + "_reduced succeeded");
                        info.screenDump();
                        info.readMessage("Removing: Count_" + param.kmerSize1 + " and Count_" + param.kmerSize1 + "_sorted");
                        info.screenDump();

                        cleanDiskStorage(param.outputPath + "/Count_" + param.kmerSize1);
                        if (param.kmerSize2==param.kmerListInt[param.kmerListInt.length-1]) {
                            info.readMessage("Removing: Count_" + param.kmerSize2 + " as this is the last k-mer size");
                            info.screenDump();
                            cleanDiskStorage(param.outputPath + "/Count_" + param.kmerSize2);
                        }
                        cleanDiskStorage(param.outputPath + "/Count_" + param.kmerSize1 + "_sorted");
                        // continue
                    }
                    //
                }
            }else { // sorted exists
                info.readMessage("Checking existing k-mer counts: Count_" + param.kmerSize2 + "_sorted succeeded");
                info.screenDump();

                if (!checkOutputFile(param.outputPath + "/Count_" + param.kmerSize1 + "_reduced")) {
                    info.readMessage("Checking existing k-mer counts: Count_" + param.kmerSize1 + "_reduced does not exist");
                    info.screenDump();

                    if (i == 1) {
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
                        }else{
                            info.readMessage("Checking existing k-mer counts: Count_" + param.kmerSize1 + " succeeded");
                            info.screenDump();
                            info.readMessage("Skip counting " + param.kmerSize1);
                            info.screenDump();
                        }
                    } else {
                        if (!checkOutputFile(param.outputPath + "/Count_" + param.kmerSize1 + "_sorted")) { // should not happen
                            info.readMessage("Checking existing k-mer counts: Count_" + param.kmerSize1 + "_sorted does not exist.");
                            info.screenDump();

                            if (!checkOutputFile(param.outputPath + "/Count_" + param.kmerSize1)) {

                                param.setKmerSize(param.kmerSize1); // set kmer size for counter
                                param.setAllbyKmerSize(param.kmerSize1);

                                // only counting kmerSize1 for later comparison
                                info.readMessage("Start counting " + param.kmerSize1);
                                info.screenDump();
                                if (param.kmerSize <= 31) {
                                    reflexivDSCounterPipe();
                                } else {
                                    reflexivDS64CounterPipe();
                                }
                            }else{
                                info.readMessage("Checking existing k-mer counts: Count_" + param.kmerSize1 + " succeeded");
                                info.screenDump();
                                info.readMessage("Will use Count_" + param.kmerSize1);
                                info.screenDump();
                            }
                        }else{
                            info.readMessage("Checking existing k-mer counts: Count_" + param.kmerSize1 + "_sorted succeeded");
                            info.screenDump();
                            info.readMessage("Will use Count_" + param.kmerSize1 + "_sorted");
                            info.screenDump();
                        }
                    }

                    if (!checkOutputFile(param.outputPath + "/Count_" + param.kmerSize2)) {
                        info.readMessage("Checking existing k-mer counts: Count_" + param.kmerSize2 + " does not exist");
                        info.screenDump();

                        param.setKmerSize(param.kmerSize2); // set kmer size for counter
                        param.setAllbyKmerSize(param.kmerSize2);

                        /**
                         *
                         */
                        info.readMessage("Start counting " + param.kmerSize2);
                        info.screenDump();
                        if (param.kmerSize <= 31) {
                            reflexivDSCounterPipe();
                        } else {
                            reflexivDS64CounterPipe();
                        }
                    }else{
                        info.readMessage("Checking existing k-mer counts: Count_" + param.kmerSize2 + " succeeded");
                        info.screenDump();
                        info.readMessage("Will use Count_" + param.kmerSize2);
                        info.screenDump();
                    }

                    if (i == 1) {
                        param.inputKmerPath1 = param.outputPath + "/Count_" + param.kmerSize1 + "/part*.csv.gz";
                    } else if(!checkOutputFile(param.outputPath + "/Count_" + param.kmerSize1 + "_sorted") ){
                        param.inputKmerPath1 = param.outputPath + "/Count_" + param.kmerSize1 + "/part*.csv.gz";
                    } else { // after the first iteration
                        param.inputKmerPath1 = param.outputPath + "/Count_" + param.kmerSize1 + "_sorted/part*.csv.gz";
                    }
                    param.inputKmerPath2 = param.outputPath + "/Count_" + param.kmerSize2 + "/part*.csv.gz";

                    /**
                     *
                     */
                    info.readMessage("Start k-mer reduction : " + param.kmerSize2 + " vs " + param.kmerSize1);
                    info.screenDump();
                    reflexivDSDynamicKmerReductionPipe();

                    if (checkOutputFile(param.outputPath + "/Count_" + param.kmerSize1 + "_reduced") && checkOutputFile(param.outputPath + "/Count_" + param.kmerSize2 + "_sorted") ) {
                        info.readMessage("Finished k-mer reduction : " + param.kmerSize2 + " vs " + param.kmerSize1 + " succeeded");
                        info.screenDump();

                        info.readMessage("Removing: Count_" + param.kmerSize1 + ", Count_" + param.kmerSize2 + ", and Count_" + param.kmerSize1 + "_sorted");
                        info.screenDump();

                        /**
                         *
                         */
                        cleanDiskStorage(param.outputPath + "/Count_" + param.kmerSize1);
                        cleanDiskStorage(param.outputPath + "/Count_" + param.kmerSize2);
                        cleanDiskStorage(param.outputPath + "/Count_" + param.kmerSize1 + "_sorted");
                    }else if (checkOutputFile(param.outputPath + "/Count_" + param.kmerSize1 + "_reduced") && checkOutputFile(param.outputPath + "/Count_" + param.kmerSize2 + "_reduced") && param.kmerSize2==param.kmerListInt[param.kmerListInt.length-1]){
                        info.readMessage("Finished k-mer reduction : " + param.kmerSize2 + " vs " + param.kmerSize1 + " succeeded");
                        info.screenDump();

                        info.readMessage("Removing: Count_" + param.kmerSize1 + ", Count_" + param.kmerSize2 + ", and Count_" + param.kmerSize1 + "_sorted");
                        info.screenDump();

                        cleanDiskStorage(param.outputPath + "/Count_" + param.kmerSize1);
                        cleanDiskStorage(param.outputPath + "/Count_" + param.kmerSize2);
                        cleanDiskStorage(param.outputPath + "/Count_" + param.kmerSize1 + "_sorted");
                    } else{
                        info.readMessage("Failed k-mer reduction : " + param.kmerSize2 + " vs " + param.kmerSize1 + " failed:");
                        info.screenDump();
                        info.readMessage("The process is finished. However, one or more results are not complete");
                        info.screenDump();
                    }


                } else { // reduced exist
                    info.readMessage("Checking existing k-mer counts: Count_" + param.kmerSize1 + "_reduced succeeded");
                    info.screenDump();
                    info.readMessage("Removing: Count_" + param.kmerSize1 + ", Count_" + param.kmerSize2 + ", and Count_" + param.kmerSize1 + "_sorted");
                    info.screenDump();


                    /**
                     *
                     */
                    cleanDiskStorage(param.outputPath + "/Count_" + param.kmerSize1);
                    cleanDiskStorage(param.outputPath + "/Count_" + param.kmerSize2);
                    cleanDiskStorage(param.outputPath + "/Count_" + param.kmerSize1 + "_sorted");
                }
            }
        }

        info.readMessage("-------- All k-mer counting finished --------");
        info.screenDump();

/*
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
*/
            // cleanDiskStorage(param.outputPath + "/Count_" + param.kmerSize1);
            // cleanDiskStorage(param.outputPath + "/Count_" + param.kmerSize2);
            // cleanDiskStorage(param.outputPath + "/Count_" + param.kmerSize1 + "_sorted");


//        }

        // rename the longest kmer directory name with "reduced" ending
        // renameDiskStorage(param.outputPath + "/Count_" + param.kmerSize2 + "_sorted", param.outputPath + "/Count_" + param.kmerSize2 + "_reduced");

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
