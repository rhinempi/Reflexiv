package uni.bielefeld.cmg.reflexiv.pipeline;


import uni.bielefeld.cmg.reflexiv.util.DefaultParam;
import uni.bielefeld.cmg.reflexiv.util.InfoDumper;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Serializable;

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
        ReflexivDSReAssembler rflPipe = new ReflexivDSReAssembler();
        rflPipe.setParam(param);
        if (param.inputKmerPath != null){
            rflPipe.assemblyFromKmer();
        }else {
            rflPipe.assembly();
        }
    }

    public void reflexivDSReAssemblerPipe64(){
        ReflexivDSReAssembler64 rflPipe = new ReflexivDSReAssembler64();
        rflPipe.setParam(param);
        if (param.inputKmerPath != null){
            rflPipe.assemblyFromKmer();
        }else {
            rflPipe.assembly();
        }
    }

    /**
     *
     */
    public void reflexivDSIterativeAssemblerPipe(){
        param.setCacheLocal(true);
        param.setGzip(true);

        if (param.kmerSize <=31){
            reflexivDSCounterPipe();
        }else{
            reflexivDS64CounterPipe();
        }

        param.setInputKmerPath(param.outputPath + "/Count_" + param.kmerSize + "/part*.csv.gz");
        param.setInputContigPath(param.outputPath + "/Assemble_" + param.kmerSize);
        param.setCacheLocal(false);
        param.setInputFqPath(param.outputPath + "/Read_Repartitioned/part*.txt.gz");

        if (param.kmerSize <= 31){
            reflexivDSMainPipe();
        }else {
            reflexivDSMainPipe64();
        }

        while(param.kmerSize + param.kmerIncrease < param.maxKmerSize){
            param.kmerSize += param.kmerIncrease;
            param.setAllbyKmerSize(param.kmerSize);

            if (param.kmerSize <=31){
                reflexivDSReAssembleCounterPipe();
            }else {
                reflexivDS64ReAssembleCounterPipe();
            }

            param.setInputContigPath(param.outputPath + "/Assemble_" + param.kmerSize);
            param.setInputKmerPath(param.outputPath + "/Count_" + param.kmerSize + "/part*.csv.gz");

            if (param.kmerSize <= 31){
                reflexivDSMainPipe();
            }else {
                reflexivDSMainPipe64();
            }
        }

        param.kmerSize = param.maxKmerSize;
        param.setAllbyKmerSize(param.kmerSize);

        if (param.kmerSize <=31){
            reflexivDSReAssembleCounterPipe();
        }else {
            reflexivDS64ReAssembleCounterPipe();
        }

        param.setInputKmerPath(param.outputPath + "/Count_" + param.kmerSize + "/part*.csv.gz");
        param.setGzip(false);
        if (param.kmerSize <= 31){
            reflexivDSMainPipe();
        }else {
            reflexivDSMainPipe64();
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
