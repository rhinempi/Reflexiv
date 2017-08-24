package uni.bielefeld.cmg.reflexiv.util;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;

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


public class InfoDumper implements Serializable {
    private String message;
    private String mightyName;

    public InfoDumper() {
        /**
         * format output information and message
         */
        this.mightyName = "Reflexiv"; /* the mighty name is your program name which will be used as the header */
    }

    /**
     * @param timeFormat
     * @return
     */
    private String headerMessage(String timeFormat) {
        String currentTime = headerTime(timeFormat);
        String mHeader = mightyName + " " + currentTime;
        return mHeader;
    }

    /**
     * @param timeFormat
     * @return
     */
    private String headerTime(String timeFormat) {
        SimpleDateFormat hourMinuteSecond = new SimpleDateFormat(timeFormat);
        String timeHeader = hourMinuteSecond.format(new Date());
        return timeHeader;
    }

    private String paragraphReader(String m) {
        String completedParagraph = "";
        String[] paragraphLines = m.split("\n");
        String mHeader = headerMessage("HH:mm:ss");
        for (String line : paragraphLines) {
            completedParagraph += mHeader + " " + line + "\n";
        }
        completedParagraph = completedParagraph.trim();
        return completedParagraph;
    }

    /**
     * @param m
     * @return
     */
    private String completeMessage(String m) {
        String mHeader = headerMessage("HH:mm:ss");
        String completedMessage = mHeader + " " + m;
        return completedMessage;
    }

    /**
     * out put formatted messages
     */
    public void screenDump() {
        System.out.println(message);
    }

    /**
     * @param m
     */
    public void readMessage(String m) {
        this.message = completeMessage(m);
    }

    /**
     * @param m
     */
    public void readParagraphedMessages(String m) {
        this.message = paragraphReader(m);
    }

    /**
     * @param e
     */
    public void readIOException(IOException e) {
        String m = e.getMessage();
        m = "IOException " + m;
        this.message = completeMessage(m);
    }

    /**
     * @param e
     */
    public void readFileNotFoundException(FileNotFoundException e) {
        String m = e.getMessage();
        m = "FileNotFoundException " + m;
        this.message = completeMessage(m);
    }

    public void readClassNotFoundException(ClassNotFoundException e) {
        String m = e.getMessage();
        m = "ClassNotFoundException " + m;
        this.message = completeMessage(m);
    }
}
