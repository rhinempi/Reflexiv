package uni.bielefeld.cmg.reflexiv.serializer;


import com.esotericsoftware.kryo.Kryo;
import org.apache.spark.serializer.KryoRegistrator;
import uni.bielefeld.cmg.reflexiv.util.DefaultParam;

/**
 * Created by Liren Huang on 25.08.17.
 *
 *      SparkHit
 *
 * Copyright (c) 2015-2015
 *      Liren Huang      <huanglr at cebitec.uni-bielefeld.de>
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
 *
 */


public class SparkKryoRegistrator implements KryoRegistrator {

    public SparkKryoRegistrator (){
        /**
         * Registing all data structure for kryo serialization
         */
    }

    /**
     *
     * @param kryo
     */
    public void registerClasses(Kryo kryo){
        kryo.register(DefaultParam.class);
    }
}
