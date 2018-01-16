package uni.bielefeld.cmg.reflexiv.pipeline;


import uni.bielefeld.cmg.reflexiv.util.DefaultParam;

import java.io.BufferedReader;
import java.io.BufferedWriter;

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
 * This is an interface for pipelines of each Sparkhit application.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
 */
public interface Pipeline {

    /**
     * Sets correspond parameter.
     *
     * @param param {@link DefaultParam} is the object for command line parameters.
     */
    void setParameter(DefaultParam param);

    /**
     * Sets input buffer reader.
     *
     * @param InputRead a {@link BufferedReader} to read input data.
     */
    void setInput(BufferedReader InputRead);

    /**
     * Sets output buffer writer.
     *
     * @param OutputWrite a {@link BufferedWriter} to write to an output file.
     */
    void setOutput(BufferedWriter OutputWrite);
}
