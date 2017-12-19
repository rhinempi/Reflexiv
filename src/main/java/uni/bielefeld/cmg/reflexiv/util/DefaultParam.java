package uni.bielefeld.cmg.reflexiv.util;


import java.io.Serializable;

/**
 * Created by Liren Huang on 24.08.17.
 *
 *      Reflexiv
 *
 * Copyright (c) 2015-2015
 *      Liren Huang      <huanglr at cebitec.uni-bielefeld.de>
 *
 * Reflexiv is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; Without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more detail.
 * 
 * You should have received a copy of the GNU General Public License along
 * with this program. If not, see <http://www.gnu.org/licenses>.
 *
 */


public class DefaultParam implements Serializable{

    public DefaultParam () {
        /**
         * This is a class of data structure which stores the default parameters
         */
    }

    public String mightyName = "Reflexiv";
    public String inputFqPath;
    public String inputFaPath;
    public String inputContigPath;
    public String outputPath;

    public int kmerSize = 63;
    public int subKmerSize = kmerSize - 1;
    public int kmerBits = (1 << (kmerSize*2)) - 1;
    public int kmerOverlap = kmerSize - 1;
    public int minReadSize = kmerSize;
    public int minKmerCoverage = 1;
    public int maxKmerCoverage = 100000;
    public int minContig = 100;
    public boolean bubble= true;

    public boolean cache = false;
    public int partitions = 0;
    public int maximumIteration = 100;
    public int minimumIteration = 15; // 20 - 4 (four Long iteration) -1 (one Long to LongArray)
    public int frontClip = 0;
    public int endClip = 0;


    public int[] initialAlphaCode(){
        int[] alphaCodeInitial = new int[256];
        for (int i=0; i<256; i++){
            alphaCodeInitial[i] = 0;         // 'a' 'A' ASCII code and all other Char
        }

        alphaCodeInitial['c']=alphaCodeInitial['C']=1; // 'c' is ASCII number of c
        alphaCodeInitial['g']=alphaCodeInitial['G']=2; // the same
        alphaCodeInitial['t']=alphaCodeInitial['T']=3; // the same

        return alphaCodeInitial;
    }

    public int[] initialAlphaCodeComplement(){
        int[] alphaCodeComplementInitial = new int[256];
        for (int i=0; i<256; i++){
            alphaCodeComplementInitial[i] = 3;
        }

        alphaCodeComplementInitial['c']=alphaCodeComplementInitial['C']=2;
        alphaCodeComplementInitial['g']=alphaCodeComplementInitial['G']=1;
        alphaCodeComplementInitial['t']=alphaCodeComplementInitial['T']=0;
        return alphaCodeComplementInitial;
    }

    public int[] initialNNNNNFilter(){
        int[] alphaCodeNNNNNInitial = new int[256];
        for (int i=0; i<256; i++){
            alphaCodeNNNNNInitial[i] = 1;
        }
        alphaCodeNNNNNInitial['c']=alphaCodeNNNNNInitial['C']=0;
        alphaCodeNNNNNInitial['g']=alphaCodeNNNNNInitial['G']=0;
        alphaCodeNNNNNInitial['t']=alphaCodeNNNNNInitial['T']=0;
        alphaCodeNNNNNInitial['a']=alphaCodeNNNNNInitial['A']=0;
        return alphaCodeNNNNNInitial;
    }

    /* change kmer length and maximum bit */
    public  void setKmerSize(int k){
        kmerSize = k;
        kmerBits = (1 << (kmerSize*2))-1;
    }

    public void setSubKmerSize(int s){
        subKmerSize = s;
    }

    public void setKmerOverlap(int o){
        kmerOverlap = o;
    }
}
