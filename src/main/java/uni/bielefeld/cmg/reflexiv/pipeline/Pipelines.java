package uni.bielefeld.cmg.reflexiv.pipeline;


import uni.bielefeld.cmg.reflexiv.util.DefaultParam;
import uni.bielefeld.cmg.reflexiv.util.InfoDumper;

import java.io.BufferedReader;
import java.io.BufferedWriter;
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


public class Pipelines implements Pipeline, Serializable{
    private DefaultParam param;

    private InfoDumper info = new InfoDumper();

    private BufferedWriter outputBufferedWriter;

    private long time;

    private void clockStart() {
        time = System.currentTimeMillis();

    }

    private long clockCut () {
        long tmp = time;
        time = System.currentTimeMillis();
        return time -tmp;
    }

    public Pipelines () {
    }

    public void reflexivMainPipe(){
        ReflexivMain rflPipe = new ReflexivMain();
        rflPipe.setParam(param);
        rflPipe.assembly();
    }

    public void reflexivCounterPipe(){
        ReflexivCounter rflPipe = new ReflexivCounter();
        rflPipe.setParam(param);
        rflPipe.assembly();
    }

    public void reflexivReAssemblerPipe(){
        ReflexivReAssembler rflPipe = new ReflexivReAssembler();
        rflPipe.setParam(param);
        rflPipe.assembly();
    }

    public void setParameter (DefaultParam param) {
        this.param = param;
    }

    public void setInput (BufferedReader inputBufferedReader){

    }

    public void setOutput(BufferedWriter outputBufferedWriter) {
        this.outputBufferedWriter = outputBufferedWriter;
    }
}
