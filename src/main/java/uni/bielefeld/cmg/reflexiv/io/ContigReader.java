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
 * Created by Liren Huang on 18.12.17.
 *
 *      Reflexiv
 *
 * Copyright (c) 2015-2015
 * Liren Huang      <huanglr at cebitec.uni-bielefeld.de>
 *
 * SparkHit is free software: you can redistribute it and/or modify it
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

                        Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>> subKmerSuffixBinary = getBinaryReflexivKmer(seq, contigId);
                        reflexivContigList.add(subKmerSuffixBinary);
                    }

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

    public ArrayList<Tuple2<Long, Tuple4<Integer, Long[], Integer, Integer>>> getReflexivContigList(){
        return reflexivContigList;
    }

    public HashMap<Integer, String> getContigIDTable(){
        return contigIDTable;
    }

    public String[] getContigIDArray () {
        return this.contigIDArray;
    }

    public String[] getPrimerArray(){
        return this.primerArray;
    }

}
