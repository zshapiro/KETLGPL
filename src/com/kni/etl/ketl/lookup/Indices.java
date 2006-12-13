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

import org.garret.perst.Index;
import org.garret.perst.Persistent;

@SuppressWarnings("serial")
public class Indices extends Persistent {

    Index[] indexes = new Index[0];
    String[] indexKey = new String[0];

    public synchronized Index getIndex(String name) {
        for (int i = 0; i < indexes.length; i++) {
            if (indexKey[i] != null && indexKey[i].equals(name))
                return (Index) indexes[i];
        }

        return null;
    }

    public synchronized void addIndex(String name, Index idx) {

        Object tmp = null;
        int freepos = -1;
        for (int i = 0; i < indexes.length; i++) {
            if (indexKey[i] != null && indexKey[i].equals(name)) {
                tmp = indexes[i];
                indexes[i] = idx;
            }
            else if (indexKey[i] == null) {
                freepos = i;
            }
        }

        if (tmp == null) {
            if (freepos == -1) {
                Index[] tmpIdx = new Index[indexes.length + 1];
                String[] tmpIdxKey = new String[indexes.length + 1];
                System.arraycopy(indexes, 0, tmpIdx, 0, indexes.length);
                System.arraycopy(indexKey, 0, tmpIdxKey, 0, indexes.length);
                indexes = tmpIdx;
                indexKey = tmpIdxKey;
                freepos = indexes.length - 1;
            }
            indexKey[freepos] = name;
            indexes[freepos] = idx;
        }

        if (tmp != null && tmp instanceof Index) {
            idx = (Index) tmp;
            idx.clear();
            idx.deallocate();
        }
    }

    public synchronized boolean removeIndex(String name) {
        Object tmp = null;
        boolean itemfound = false;
        for (int i = 0; i < indexes.length; i++) {
            if (indexKey[i] != null && indexKey[i].equals(name)) {
                tmp = indexes[i];
                indexKey[i] = null;
                indexes[i] = null;
            }
            else if (indexKey[i] != null)
                itemfound = true;
        }

        if (tmp != null && tmp instanceof Index) {
            Index idx = (Index) tmp;
            idx.clear();
            idx.deallocate();
        }

        return !itemfound;
    }
}