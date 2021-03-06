package uni.bielefeld.cmg.reflexiv.util;


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
 * A data structure class that stores all default parameters.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
 */
public class DefaultParam implements Serializable{


    /**
     * A constructor that construct an object of {@link DefaultParam} class.
     */
    public DefaultParam () {
        /**
         * This is a class of data structure which stores the default parameters
         */
    }

    public String mightyName = "Reflexiv";
    public String inputFqPath;
    public String inputKmerPath;
    public String inputFaPath;
    public String inputContigPath;
    public String outputPath;

    public int kmerSize = 31;
    public int subKmerSize = kmerSize - 1;

    // for kmer counting
    public int kmerSizeResidue = kmerSize % 32;
    public int kmerBinarySlots = kmerSize / 32 +1;   /* for kmer counting, each slot stores 32 mer */

    // for loading kmer in assembly
    public int kmerSizeResidueAssemble = kmerSize % 31;
    public int kmerBinarySlotsAssemble = (kmerSize-1) / 31 +1; /* for assembly, each slot stores 31 mer, load entire k-mer */

    // for sub kmer in assembly
    public int subKmerSizeResidue= (subKmerSize-1) % 31 +1;
    public int subKmerBinarySlots = (subKmerSize-1) / 31 +1; /* for assembly, each slot stores 31 mer, load k-1 mer */

    // metagenome assembly
    public int kmerIncrease = 10;
    public int maxKmerSize = 93;


    public int kmerBits = (1 << (kmerSize*2)) - 1;
    public int kmerOverlap = kmerSize - 1;
    public int minReadSize = kmerSize;
    public int minKmerCoverage = 2;
    public int maxKmerCoverage = 10000000;
    public int minErrorCoverage = minKmerCoverage * 6; // equal to minKmerCoverage
    public int minContig = 300;
    public boolean bubble= true;

    public boolean cache = false;
    public boolean cacheLocal = false;
    public boolean gzip = false;
    public int partitions = 0;
    public int maximumIteration = 100;
    public int minimumIteration = 15; // 20 - 4 (four Long iteration) -1 (one Long to LongArray)
    public int frontClip = 0;
    public int endClip = 0;

    public int shufflePartition=200;

    public float minLonger=60.0f;
    public float minIdentity=90.0f;
    public int searchableLength= 3000;

    public int scramble = 2;

    public boolean RCmerge = true;

    public boolean mercy = true;


    public void setAllbyKmerSize(int k){
        this.subKmerSize= k-1;
        setKmerSize(k);
        setSubKmerSize(subKmerSize);
        setKmerBinarySlots(k);
        setKmerSizeResidue(k);
        setSubKmerBinarySlots(subKmerSize);
        setSubKmerSizeResidue(subKmerSize);
        setKmerSizeResidueAssemble(k);
        setKmerBinarySlotsAssemble(k);
    }

    /**
     * This method initiates the K-mer size parameter.
     *
     * @param k the size of the k-mer.
     */
    public  void setKmerSize(int k){
        kmerSize = k;
        kmerBits = (1 << (kmerSize*2))-1;
    }

    /**
     * This method initiates the sub K-mer size parameter.
     *
     * @param s the size of the sub k-mer.
     */
    public void setSubKmerSize(int s){
        subKmerSize = s;
    }

    /**
     *
     */
    public void setKmerBinarySlots(int s){
        kmerBinarySlots = s / 32 +1;
    }

    public void setSubKmerBinarySlots(int s){
        subKmerBinarySlots = (s-1) / 31 +1;
    }

    public void setKmerSizeResidueAssemble(int s){
        kmerSizeResidueAssemble = s % 31;
    }

    public void setKmerBinarySlotsAssemble(int s){
        kmerBinarySlotsAssemble = (s-1) / 31 +1;
    }

    /**
     *
     */
    public void setKmerSizeResidue(int s){
        kmerSizeResidue = s % 32;
    }

    public void setSubKmerSizeResidue(int s){
        subKmerSizeResidue= (s -1) % 31 +1;
    }

    /**
     * This method initiates the minimal coverage for error correction.
     *
     * @param s the minimal size for error correction.
     */
    public void setMinErrorCoverage(int s){
        minErrorCoverage = s;
    }

    public void setMinKmerCoverage(int s){
        minKmerCoverage = s;
    }

    /**
     * This method initiates the overlap between k-mers.
     *
     * @param o the size of the overlap between k-mers.
     */
    public void setKmerOverlap(int o){
        kmerOverlap = o;
    }

    public void setInputKmerPath(String s){
        inputKmerPath = s;
    }

    public void setInputFqPath(String s){
        inputFqPath = s;
    }

    public void setInputContigPath(String s){
        inputContigPath = s;
    }

    public void setCacheLocal(boolean c){
        cacheLocal = c;
    }

    public void setGzip(boolean c){
        gzip = c;
    }

    public void setMinContig(int c){
        minContig =c;
    }

    public void setPartitions(int c){
        partitions = c;
    }

    public void setShufflePartition(int c){
        shufflePartition =c;
    }

    public void setRCmerge(boolean c){
        RCmerge = c;
    }
}
