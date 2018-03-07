package uni.bielefeld.cmg.reflexiv.io;


import scala.Tuple2;
import scala.Tuple4;
import uni.bielefeld.cmg.reflexiv.util.DefaultParam;
import uni.bielefeld.cmg.reflexiv.util.InfoDumper;

import java.io.BufferedReader;
import java.io.IOException;
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
 * Returns an object for reading contigs in the Fasta format.
 * Providing Fasta unit buffer for multi-thread Fastq input stream.
 * This class is used in local mode only. For cluster mode,
 * Spark "textFile" function is used to access input Fastq file.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
 */
public class ContigReader implements Serializable{
    public DefaultParam param;
    public HashMap<Integer, String> contigIDTable = new HashMap<Integer, String>();
    public HashMap<String, String> contigPrimerTable= new HashMap<String, String>();
    public String[] contigIDArray;
    public String[] primerArray;

    public ArrayList<Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>> reflexivContigList = new ArrayList<Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>>();

    public int totalNum;

    private String name;
    private int contigId = 0;
    private String seq = "";
    private StringBuilder seqBuilder = new StringBuilder();


    /* info dumper */
    private InfoDumper info = new InfoDumper();

    /**
     * This method sets all correspond parameters for reference
     * data structure construction.
     *
     * @param param {@link DefaultParam}.
     */
    public void setParameter(DefaultParam param){
        this.param = param;
    }

    /**
     * This method loads the first line of a fasta file.
     *
     * @param fasta {@link BufferedReader}.
     */
    private void loadFirstLine(BufferedReader fasta){
        String line;
        try {
            if ((line = fasta.readLine()) != null){
                if (!line.startsWith(">")){ // first time encounter a ">"
                    info.readMessage("Input reference file is not fasta format");
                    info.screenDump();
                }else{
                    String[] head = line.split("\\s+");
                    name = head[0].substring(1);
                 //   contigIDTable.put(0, name);

                    contigId++;
                    totalNum++;
                }
            }else{
                info.readMessage("Input reference file empty!");
                info.screenDump();
                System.exit(0);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * This method loads the all contigs from a reference genome.
     *
     * @param fasta {@link BufferedReader}.
     */
    private void loadContig(BufferedReader fasta){
        String line;
        String forwardSubKmer;
        String reflectedSubKmer;
        String primer;
        try {
            while((line = fasta.readLine()) != null){
                if (line.startsWith(">")){
                    contigIDTable.put(contigId, name);     /* these are information of former contig */

                    seq = seqBuilder.toString();

                    if (seq.length() < param.kmerSize){
                        info.readMessage("Contig : " + name + "is shorter than a Kmer, skip it.");
                        info.screenDump();
                    }else{
                        forwardSubKmer = seq.substring(0,param.subKmerSize);
                        reflectedSubKmer =seq.substring(seq.length()-param.subKmerSize);
                        primer = forwardSubKmer+reflectedSubKmer;

                        if (contigPrimerTable.containsKey(primer)) {
                            contigPrimerTable.put(primer, contigPrimerTable.get(primer)+ "|" + name);
                        }else{
                            contigPrimerTable.put(primer, name);

                        }
                    }

                    Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>> subKmerSuffixBinary = getBinaryReflexivKmer(seq, contigId);
                    reflexivContigList.add(subKmerSuffixBinary);

                    seqBuilder = new StringBuilder();

                    contigId++;
                    totalNum++;

                    String[] head = line.split("\\s+");
                    name = head[0].substring(1);
                }else{
                    if (!line.isEmpty()) {
                        seqBuilder.append(line);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     *
     */
    private void logLastContig(){

        contigIDTable.put(contigId, name);

        seq = seqBuilder.toString();

        if (seq.length() < param.kmerSize){
            info.readMessage("Contig : " + name + "is shorter than a Kmer, skip it.");
            info.screenDump();
        }else{
            String forwardSubKmer = seq.substring(0,param.subKmerSize);
            String reflectedSubKmer =seq.substring(seq.length()-param.subKmerSize);
            String primer = forwardSubKmer+reflectedSubKmer;

            if (contigPrimerTable.containsKey(primer)) {
                contigPrimerTable.put(primer, contigPrimerTable.get(primer)+ "|" + name);
            }else{
                contigPrimerTable.put(primer, name);

            }

            Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>  subKmerSuffixBinary = getBinaryReflexivKmer(seq, contigId);
            reflexivContigList.add(subKmerSuffixBinary);
        }

      //  contigId++;
      //  totalNum++;
    }

    private Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>> getBinaryReflexivKmer(String contig, int Id){
        int length = contig.length();
        int suffixLength = length - param.subKmerSize;
        int suffixBlockSize = (suffixLength-1)/31 + 1;
        int firstSuffixLength =suffixLength%31;
        if (firstSuffixLength == 0){
            firstSuffixLength = 31;
        }

        Long[] suffixLongArray = new Long[suffixBlockSize];
        for (int i = 0; i< suffixBlockSize ; i++){
            suffixLongArray[i] = 0L;
        }
        Long subKmer=0L;

        char[] nucleotides = contig.toCharArray();

        for (int i=0; i< param.subKmerSize;i++) {
            long nucleotideBinary = nucleotideValue(nucleotides[i]);
            subKmer |= (nucleotideBinary << 2*(param.subKmerSize-i-1));
        }

        // 2nd and so on
        for (int i=length-1; i>=firstSuffixLength+param.subKmerSize; i--){
            long nucleotideBinary = nucleotideValue(nucleotides[i]);

            suffixLongArray[(i-firstSuffixLength-param.subKmerSize)/31+1] |= (nucleotideBinary << 2* ((length-i-1)%31));
        }

        // first block
        for (int i=firstSuffixLength+param.subKmerSize-1; i>=param.subKmerSize;i--){
            long nucleotideBinary = nucleotideValue(nucleotides[i]);

            suffixLongArray[0] |= (nucleotideBinary << 2*(firstSuffixLength+param.subKmerSize-i-1));
        }
        suffixLongArray[0] |= (1L << 2*firstSuffixLength); // add C marker


        return new Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>(
                subKmer, new Tuple4<Integer, Long[], Integer, Integer>(1, suffixLongArray, 1000000000 + Id, 1000000000 + Id)
        );
    }

    private long nucleotideValue(char a) {
        long value;
        if (a == 'A' || a == 'a') {
            value = 0L;
        } else if (a == 'C' || a == 'c') {
            value = 1L;
        } else if (a == 'G' || a == 'g') {
            value = 2L;
        } else { // T t N
            value = 3L;
        }
        return value;
    }

    private void buildContigIDArray(){
        contigIDArray = new String[totalNum];
        for (int i=0; i< totalNum; i++){
            contigIDArray[i] = contigIDTable.get(i+1);
        }
    }

    private void buildPrimerArray(){
        primerArray = new String[contigPrimerTable.size()];
        int i=0;
        for (String key : contigPrimerTable.keySet()){
            primerArray[i] = key + contigPrimerTable.get(key);
            i++;
        }
    }

    /**
     * This method load the genome sequences from the input reference file.
     *
     * @param inputFaPath the full path of the input file for reference genomes.
     */
    public void loadRef (String inputFaPath){
        ReadFasta refReader = new ReadFasta();
        refReader.bufferInputFile(inputFaPath);
        BufferedReader fasta = refReader.getFastaBufferedReader();

        loadFirstLine(fasta);
        loadContig(fasta);

        try {
            fasta.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        logLastContig();
  //      buildContigIDArray();
        buildPrimerArray();
    }

    /**
     * This method returns a List of Reflexible K-mers that represents contigs
     *
     * @return an ArrayList of Reflexible K-mers
     */
    public ArrayList<Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>> getReflexivContigList(){
        return reflexivContigList;
    }

    /**
     * under construction
     *
     * @return
     */
    public HashMap<Integer, String> getContigIDTable(){
        return contigIDTable;
    }

    /**
     * not ready yet
     *
     * @return
     */
    public String[] getContigIDArray () {
        return this.contigIDArray;
    }

    /**
     * This method returns an Array of String for searching re-assembled contigs
     *
     * @return
     */
    public String[] getPrimerArray(){
        return this.primerArray;
    }

}
