/**
 *  Copyright (C) 2006 Kinetic Networks Inc. All rights reserved
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with this program; if not, write to the Free Software Foundation, Inc.,
 *  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *  
 *  Kinetic Networks Inc
 *  33 New Montgomery, Suite 1200
 *  San Francisco CA 94105
 *  http://www.kineticnetworks.com
 */
package com.kni.etl.ketl.lookup;

import java.io.File;

import com.kni.etl.EngineConstants;
import com.kni.etl.stringtools.NumberFormatter;

public class PerstIndexedMapTest extends IndexedMapTest {

    public static void main(String[] args) throws Exception {
        PerstIndexedMapTest ps = new PerstIndexedMapTest("Perst");
        ps.setUp();
        ps.testPutLarge();
    }

    public PerstIndexedMapTest(String name) {
        super(name);
        // TODO Auto-generated constructor stub
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        map = new CachedIndexedMap(new PerstIndexedMap("test", NumberFormatter.convertToBytes(EngineConstants
                .getDefaultCacheSize()), 0, System.getProperty("user.dir") + File.separator + "log", new Class[] {
                Long.class, Float.class }, new Class[] { Long.class, Float.class }, new String[] { "a", "b" }, false));
    }

}
