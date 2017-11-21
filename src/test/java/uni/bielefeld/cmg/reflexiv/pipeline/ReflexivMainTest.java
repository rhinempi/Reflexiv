package uni.bielefeld.cmg.reflexiv.pipeline;

import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by Liren Huang on 21.11.17.
 * <p/>
 * FragRec
 * Copyright (c) 2015-2015:
 * Liren Huang     <huanglr at cebitec.uni-bielefeld.de>
 * <p/>
 * FragRec is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option)
 * any later version.
 * <p/>
 * This program is distributed in the hope that it will be useful, but WITHOU
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 * <p/>
 * You should have received a copy of the GNU General Public License along
 * with this program. If not, see <http://www.gnu.org/licenses/>.
 */
public class ReflexivMainTest {
    protected String s;
    protected Integer i;
    protected Integer value;
    protected Integer expectedValue;

    @Test
    public void ObjectMemoryTest() throws Exception {
        long m0 = Runtime.getRuntime().freeMemory();
        String test = "A";
        long m1 = Runtime.getRuntime().freeMemory();
        System.out.println(m0 - m1);
        System.out.println(test);

        Assert.assertEquals(value, expectedValue);
    }

    @Before
    public void setUp(){
        s="A";
        i=1;
        value=1;
        expectedValue=1;
    }
}