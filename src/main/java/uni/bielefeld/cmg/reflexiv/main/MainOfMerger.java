package uni.bielefeld.cmg.reflexiv.main;


import org.apache.commons.cli.ParseException;
import uni.bielefeld.cmg.reflexiv.pipeline.Pipelines;
import uni.bielefeld.cmg.reflexiv.util.DefaultParam;
import uni.bielefeld.cmg.reflexiv.util.InfoDumper;
import uni.bielefeld.cmg.reflexiv.util.Parameter;

import java.io.IOException;

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
 * Main Java method specified in the Manifest file for runnning
 * Reflexiv run. It can also be specified at the input options
 * of Spark cluster mode during submission.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
 */
public class MainOfMerger {

    /**
     * Main Java method specified in the Manifest file for Reflexiv run.
     * This is the main Java method that starts the program and receives input
     * arguments.
     *
     * @param args  a space separated command line arguments.
     */
    public static void main(String[] args){
        InfoDumper info = new InfoDumper();
        info.readParagraphedMessages("Reflexiv contig merger initiating ... \ninterpreting parameters.");
        info.screenDump();

        Parameter parameter = null;
        try{
            parameter = new Parameter(args);
        } catch (IOException e) {
            e.fillInStackTrace();
        } catch (ParseException e){
            e.fillInStackTrace();
        }
        DefaultParam param = parameter.importCommandLine();

        Pipelines pipes = new Pipelines();
        pipes.setParameter(param);
        pipes.reflexivDSMergerPipe();

    }
}
