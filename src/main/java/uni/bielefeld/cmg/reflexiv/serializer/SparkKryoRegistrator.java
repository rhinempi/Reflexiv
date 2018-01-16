package uni.bielefeld.cmg.reflexiv.serializer;


import com.esotericsoftware.kryo.Kryo;
import org.apache.spark.serializer.KryoRegistrator;
import uni.bielefeld.cmg.reflexiv.util.DefaultParam;

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
 * An class used to register all classes for Spark kryo serializer.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
 */
public class SparkKryoRegistrator implements KryoRegistrator {

    /**
     * A constructor that construct an object of {@link SparkKryoRegistrator} class.
     * No constructor option needed.
     */
    public SparkKryoRegistrator (){
        /**
         * Registing all data structure for kryo serialization
         */
    }

    /**
     * This method registers all classes that are going to be serialized by kryo
     *
     * @param kryo {@link com.esotericsoftware.kryo.Kryo}.
     */
    public void registerClasses(Kryo kryo){
        kryo.register(DefaultParam.class);
    }
}
