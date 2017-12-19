package uni.bielefeld.cmg.reflexiv.main;


import org.apache.commons.cli.ParseException;
import uni.bielefeld.cmg.reflexiv.pipeline.Pipelines;
import uni.bielefeld.cmg.reflexiv.util.DefaultParam;
import uni.bielefeld.cmg.reflexiv.util.InfoDumper;
import uni.bielefeld.cmg.reflexiv.util.Parameter;
import uni.bielefeld.cmg.reflexiv.util.ParameterOfReAssembler;

import java.io.IOException;

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


public class MainOfReAssembler {

    public static void main(String[] args){
        InfoDumper info = new InfoDumper();
        info.readParagraphedMessages("Reflexiv ReAssembler initiating ... \ninterpreting parameters.");
        info.screenDump();

        ParameterOfReAssembler parameter = null;
        try{
            parameter = new ParameterOfReAssembler(args);
        } catch (IOException e) {
            e.fillInStackTrace();
        } catch (ParseException e){
            e.fillInStackTrace();
        }
        DefaultParam param = parameter.importCommandLine();

        Pipelines pipes = new Pipelines();
        pipes.setParameter(param);
        pipes.reflexivReAssemblerPipe();
    }
}
