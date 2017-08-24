package uni.bielefeld.cmg.reflexiv.io;


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


public interface InputFileManager extends FileManager{
    /**
     *  sub interface for managing input files
     */

    /**
     * BufferedReader for reading input textFile
     *
     * @param inputFile is the input text file in String
     */
    void bufferInputFile(String inputFile);

    /**
     * Please override this method in null
     *
     * @param outputFile
     */
    void bufferOutputFile(String outputFile);
}
