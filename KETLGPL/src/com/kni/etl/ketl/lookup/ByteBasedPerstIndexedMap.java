/*
 * Copyright (c)/**
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
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.garret.perst.Index;
import org.garret.perst.Key;
import org.garret.perst.Persistent;
import org.garret.perst.Storage;
import org.garret.perst.StorageError;
import org.garret.perst.StorageFactory;
import org.garret.perst.impl.Bytes;

import com.kni.etl.EngineConstants;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.exceptions.KETLError;
import com.kni.etl.stringtools.NumberFormatter;

/**
 * @author nwakefield To change the template for this generated type comment go to Window&gt;Preferences&gt;Java&gt;Code
 *         Generation&gt;Code and Comments
 */
final public class ByteBasedPerstIndexedMap implements PersistentMap {

    public void close() {
        // TODO Auto-generated method stub

    }

    private static Storage storage = null;

    String mCacheDir = null;

    Indices root;

    int DEFAULT_CACHE = 5;

    Index init() {

        if (storage == null) {
            Runtime r = Runtime.getRuntime();
            int mSize = (int) (((r.maxMemory() - (r.totalMemory() - r.freeMemory()))) * EngineConstants
                    .getCacheMemoryRatio());
            if (mSize < (64 * 4096))
                mSize = 10 * 1024 * 1024;
            ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE, "Setting cache size to "
                    + NumberFormatter.format(mSize));
            storage = StorageFactory.getInstance().createStorage();
            storage.setProperty("perst.background.gc", false);
            storage.setProperty("perst.extension.quantum", 48 * 1024);
            storage.setProperty("perst.file.noflush", true);

            try {
                storage.open(this.getCacheFileName(), mSize);
            } catch (StorageError e) {
                if (e.getErrorCode() == 18) {
                    File file = new File(this.getCacheFileName());

                    if (file.exists()) {
                        ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE,
                                "Deleting previous cache, record structure does not match cache");
                        file.delete();
                    }

                    if (storage.isOpened()) {
                        storage.close();
                    }

                    storage.open(this.getCacheFileName(), mSize);
                }
            }
        }
        root = (Indices) storage.getRoot();

        if (root == null) {
            root = new Indices();
        }

        Index index = root.getIndex(mName);

        if (index == null) {

            index = storage.createIndex(byte[].class, true);

            root.addIndex(mName, index);
            storage.setRoot(root);
        }

        ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.DEBUG_MESSAGE, "- Initializing lookup: "
                + this.mName + ", Key Type(s):" + java.util.Arrays.toString(this.mKeyTypes) + ", Key Field(s):"
                + java.util.Arrays.toString(this.mValueFields) + ", Result Type(s):"
                + java.util.Arrays.toString(this.mValueTypes));
        return index;
    }

    public void switchToReadOnlyMode() {

    }

    public void clear() {
        this.mMap.clear();
    }

    private Key wrapKey(Object pkey) {
        if (this.mKeyLen > 0)
            return new Key(LookupUtil.getKey((Object[]) pkey, mKeyTypes, mKeyLen));

        return new Key(LookupUtil.getKey(pkey, mKeyTypes[0]));
    }

    public boolean containsKey(Object key) {
        return mMap.get(wrapKey(key)) != null;
    }

    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException();
    }

    public Set entrySet() {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.lookup.KETLMap#get(java.lang.Object)
     */
    public Object get(Object key, String pField) {
        try {
            Object res;
            if ((res = this.getItem(key)) == null)
                return null;

            Integer idx;
            if (this.mValueIsSingle)
                idx = 0;
            else
                idx = (Integer) this.fieldIndex.get(pField);

            if (idx == null)
                throw new KETLError("Key " + pField + " does not exist in lookup");

            Object[] data = ((DatumArray) res).data;

            return unPack(data[idx], this.mValueTypes[idx]);

        } catch (ClassCastException e) {
            if (key instanceof Object[]) {
                Class[] tmp = new Class[((Object[]) key).length];
                for (int i = 0; i < ((Object[]) key).length; i++)
                    tmp[i] = ((Object[]) key)[i] == null ? null : ((Object[]) key)[i].getClass();

                String message = "[" + this.mName
                        + "]The key datatypes do not match the expected key datatypes, Expected key types:"
                        + java.util.Arrays.toString(this.mKeyTypes) + ", Incoming key types: "
                        + java.util.Arrays.toString(tmp);
                throw new ClassCastException(message);
            }
            throw new KETLError(e);
        } catch (Exception e) {
            throw new KETLError(e);
        }
    }

    private Object unPack(Object data, Class pClass) {
        if (data == null)
            return null;

        if (pClass == java.sql.Timestamp.class) {
            long t = Bytes.unpack8((byte[]) data, 0);
            java.sql.Timestamp res = new java.sql.Timestamp(t);
            res.setNanos(Bytes.unpack4((byte[]) data, 8));
            data = res;
        }
        else if (pClass == java.sql.Time.class) {
            data = new java.sql.Time((Long) data);
        }
        else if (pClass == java.sql.Date.class) {
            data = new java.sql.Date((Long) data);
        }
        return data;
    }

    private Object packObject(Object data, Class pClass) {
        if (data == null)
            return null;
        if (pClass == java.sql.Timestamp.class) {
            java.sql.Timestamp tmp = (java.sql.Timestamp) data;

            byte[] b = new byte[12];
            Bytes.pack8(b, 0, tmp.getTime());
            Bytes.pack4(b, 8, tmp.getNanos());
            data = b;
        }
        else if (pClass == java.sql.Date.class) {
            data = ((java.sql.Date) data).getTime();
        }
        else if (pClass == java.sql.Time.class) {
            data = ((java.sql.Time) data).getTime();
        }
        return data;
    }

    private Object[] packArray(Object[] data, Class[] pClass) {

        for (int i = data.length - 1; i >= 0; i--) {
            data[i] = packObject(data[i], pClass[i]);
        }
        return data;

    }

    public boolean isEmpty() {
        return mMap.size() == 0;
    }

    public Set keySet() {
        throw new UnsupportedOperationException();
    }

    static private int commitCount = 0;

    private static Object wait = new Object();

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.lookup.KETLMap#put(java.lang.Object, java.lang.Object)
     */
    public Object put(Object pkey, Object value) {
        if (commitCount++ == 500000) {
            Runtime r = Runtime.getRuntime();
            long free = (r.maxMemory() - (r.totalMemory() - r.freeMemory()));
            if (free < (1024 * 1024 * 10)) {
                this.commit(false);
            }
        }

        // if (this.mValueIsSingle)
        // value = new Object[] { value };
        /*
         * for (int i = 0; i < ((Object[]) value).length; i++) { if (((Object[]) value)[i] == null) throw new
         * Error("Components of the key result cannot be null, check key " + java.util.Arrays.toString(((Object[])
         * value))); }
         */
        Persistent datum = new DatumArray(packArray((Object[]) value, this.mValueTypes));

        Key key = wrapKey(pkey);

        synchronized (wait) {
            if (mMap.put(key, datum))
                count++;
        }
        return null;
    }

    public void putAll(Map t) {

    }

    public Object remove(Object pkey) {
        return mMap.remove(wrapKey(pkey));
    }

    public int size() {
        return mMap.size();
    }

    public Collection values() {
        return null;
    }

    Index mMap;

    private Integer mPersistanceID;

    private String mName;
    private int count = 0;

    private Class[] mKeyTypes;

    HashMap fieldIndex = new HashMap();

    private Class[] mValueTypes;

    private String[] mValueFields;

    private int mSize;

    private int mKeyLen;

    public ByteBasedPerstIndexedMap(String pName, int pSize, Integer pPersistanceID, String pCacheDir,
            Class[] pKeyTypes, Class[] pValueTypes, String[] pValueFields, boolean pPurgeCache) {
        super();

        mPersistanceID = pPersistanceID;
        mName = pName;
        mSize = pSize;
        this.mCacheDir = pCacheDir;
        this.mKeyTypes = pKeyTypes;
        this.mKeyLen = this.mKeyTypes.length;
        this.mValueTypes = pValueTypes;
        this.mValueFields = pValueFields;
        this.mValueIsSingle = pValueTypes.length == 1;

        for (int i = 0; i < pValueFields.length; i++) {
            fieldIndex.put(pValueFields[i], i);
        }

        if (pPurgeCache)
            this.deleteCache();
        mMap = init();
    }

    private boolean mValueIsSingle = false;

    public synchronized void commit(boolean force) {
        if (commitCount > 0) {
            commitCount = 0;
            ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE, "Lookup '" + mName + "' Size: "
                    + NumberFormatter.format(storage.getDatabaseSize()) + ", Count: " + mMap.size());
            storage.commit();
        }
    }

    public synchronized void deleteCache() {
        if (root != null && root.removeIndex(mName)) {

            storage.close();

            storage = null;
            File fl = new File(this.getCacheFileName());
            if (fl.exists())
                fl.delete();
        }
    }

    private String getCacheFileName() {
        if (this.mPersistanceID == null)
            return mCacheDir + File.separator + "KETL.cache";
        else
            return mCacheDir + File.separator + "KETL." + this.mPersistanceID + ".cache";

    }

    public Object get(Object key) {
        try {
            return this.get(key, null);
        } catch (Exception e) {
            throw new KETLError(e);
        }
    }

    public synchronized void delete() {
        this.deleteCache();
    }

    @Override
    public String toString() {
        String exampleValue = "N/A";
        if (mMap != null && mMap.size() > 0) {
            Iterator it = mMap.entryIterator();
            Map.Entry res = null;
            if (it != null) {
                res = (Map.Entry) it.next();
            }
            if (res != null && res.getKey() != null && res.getValue() != null) {

                Object tmp = (res.getValue() instanceof DatumArray) ? ((DatumArray) res.getValue()).data
                        : ((DatumObject) res.getValue()).data;
                exampleValue = (tmp instanceof Object[]) ? java.util.Arrays.toString((Object[]) tmp) : res.toString();
            }
        }
        // TODO Auto-generated method stub
        return "\n\tInternal Name: " + this.mName + "\n\tKey Type(s):" + java.util.Arrays.toString(this.mKeyTypes)
                + "\n\tKey Field(s):" + java.util.Arrays.toString(this.mValueFields) + "\n\tResult Type(s):"
                + java.util.Arrays.toString(this.mValueTypes) + "\n\t Example Value->" + exampleValue + "\n\tCount: "
                + this.mMap.size() + "\n\tConsolidated Cache Size: "
                + NumberFormatter.format(storage.getDatabaseSize());

    }

    public Object getItem(Object key) throws Exception {
        DatumArray res;

        if ((res = (DatumArray) mMap.get(wrapKey(key))) == null)
            return null;

        Object[] data = res.data;
        Object tmp[] = new Object[data.length];
        for (int i = this.mValueTypes.length - 1; i >= 0; i--)
            tmp[i] = unPack(data[i], this.mValueTypes[i]);

        return tmp;

    }

    public Class[] getKeyTypes() {
        return this.mKeyTypes;
    }

    public String[] getValueFields() {
        return this.mValueFields;
    }

    public Class[] getValueTypes() {
        return this.mValueTypes;
    }

    public int getCacheSize() {
        return this.mSize;
    }

    public String getName() {
        return this.mName;
    }

    public Class getStorageClass() {
        return this.getClass();
    }

}
