package uni.bielefeld.cmg.reflexiv.util;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

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
    public String inputSingle =null;
    public boolean inputSingleSwitch=false;
    public String inputPaired = null ;
    public boolean inputPairedSwitch =false;
    public String interleaved = null;
    public boolean interleavedSwitch= false;
    public boolean pairing = false;
    public String inputKmerPath;
    public String inputKmerPath1; // for kmer reduction, shorter one
    public String inputKmerPath2; // for kmer reduction, longer one
    public String inputFaPath;
    public String inputContigPath;
    public String outputPath;
    public String inputFormat = "4mc"; // 4mc zipped file or gzip or bzip
    public long readLimit =Long.MAX_VALUE;

    public int kmerSize = 31;
    public int subKmerSize = kmerSize - 1;
    public int kmerSize1 = 21;
    public int kmerSize2 = 31;

    // for kmer counting
    public int kmerSizeResidue = kmerSize % 32;
    public int kmerBinarySlots = kmerSize / 32 +1;   /* for kmer counting, each slot stores 32 mer */

    // for loading kmer in assembly
    public int kmerSizeResidueAssemble = kmerSize % 31;
    public int kmerBinarySlotsAssemble = (kmerSize-1) / 31 +1; /* for assembly, each slot stores 31 mer, load entire k-mer */
    public int maxKmerBinarySlotsAssemble= 99;  /* for dynamic kmer length, the maximum kmer length */
    public String kmerList= "23,31,41,53,67,81,95" ; /* a list of kmers for dynamic kmer length assembly. 27,37,47,59,71,83,97 */
    public int[] kmerListInt;
    public HashMap<Integer, Integer> kmerListHash =new HashMap<Integer, Integer>(); /* a kmer list hashmap for checking valid k-mer length */
    /* Key: kmer size, Value: kmer binary slot size*/

    // for sub kmer in assembly
    public int subKmerSizeResidue= (subKmerSize-1) % 31 +1;
    public int subKmerBinarySlots = (subKmerSize-1) / 31 +1; /* for assembly, each slot stores 31 mer, load k-1 mer */

    // metagenome assembly
    public int kmerIncrease = 10;
    public int maxKmerSize = 99;


    public int kmerBits = (1 << (kmerSize*2)) - 1;
    public int kmerOverlap = kmerSize - 1;
    public int minReadSize = kmerSize;
    public int minKmerCoverage = 2;
    public int maxKmerCoverage = 10000000;
    public int minErrorCoverage = 4 * minKmerCoverage; // equal to minKmerCoverage
    public double minRepeatFold = 1.5;
    public int minContig = 500;
    public boolean bubble= true;

    public boolean cache = false;
    public boolean cacheLocal = false;
    public boolean gzip = false;
    public int partitions = 0;
    public int maximumIteration = 150;
    public int minimumIteration = 15; // 20 - 4 (four Long iteration) -1 (one Long to LongArray)
    public int numberIteration=5;
    public int startIteration=5;
    public int endIteration=9;
    public int frontClip = 0;
    public int endClip = 0;

    public int shufflePartition=200;

    public float minLonger=60.0f;
    public float minIdentity=90.0f;
    public int searchableLength= 600;

    public int maxReadLength=600;

    public int scramble = 2;

    public boolean RCmerge = true;
    public boolean sensitive = false;

    public boolean stitch = false;
    public int stitchKmerLength = 31;

    public String executable;

    public String mode="local";

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

    public void setKmerListArray (String KL){
        String[] kmers = KL.split(",");
        this.kmerListInt = new int[kmers.length];
        for (int i=0;i<kmers.length;i++){
            this.kmerListInt[i] = Integer.parseInt(kmers[i]);
        }
    }

    public void setKmerListHash (String KL){
        String[] kmers = KL.split(",");
        for (String kmer : kmers){
            int length = Integer.parseInt(kmer);
            int slots = (length-1) / 31 +1; /* for assembly, every 31 bases is a block */

            this.kmerListHash.put(length,slots);

        }
    }

    public void setMaxKmerSize (String KL){
        String[] kmers = KL.split(",");
        int maxLength = 0;
        for (String kmer : kmers){
            int length = Integer.parseInt(kmer);
            if (length >=maxLength){
                maxLength = length;
            }
        }
        this.maxKmerSize = maxLength;
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
