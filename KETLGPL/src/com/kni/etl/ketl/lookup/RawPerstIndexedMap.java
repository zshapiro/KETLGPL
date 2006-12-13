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
import java.math.BigDecimal;
import java.math.BigInteger;
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
final public class RawPerstIndexedMap implements PersistentMap {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    private byte[] mValueTypeMap;
    private static Storage storage = null;

    String mCacheDir = null;

    Indices root;

    int DEFAULT_CACHE = 5;

    Index init() {

        indexCount++;

        if (storage == null) {
            Runtime r = Runtime.getRuntime();
            System.gc();
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
                else
                    throw e;
            }
        }
        root = (Indices) storage.getRoot();
        if (!this.mKeyIsArray)
            c = mKeyTypes[0];

        if (root == null) {
            root = new Indices();
        }
        else {
            root.load();
        }

        Index index = root.getIndex(mName);

        if (index == null) {
            index = storage.createIndex(switchToStorageTypes(mKeyTypes), true); // unique

            root.addIndex(mName, index);
            storage.setRoot(root);
        }

        if (this.mKeyIsArray == false
                && (c == java.sql.Timestamp.class || c == java.sql.Time.class || c == java.sql.Date.class || c == java.math.BigDecimal.class)) {
            mKeyIsPacked = true;
        }

        ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.DEBUG_MESSAGE, "- Initializing lookup: "
                + this.mName + ", Key Type(s):" + java.util.Arrays.toString(this.mKeyTypes) + ", Key Field(s):"
                + java.util.Arrays.toString(this.mValueFields) + ", Result Type(s):"
                + java.util.Arrays.toString(this.mValueTypes));
        return index;
    }

    public Class getStorageClass() {
        return this.getClass();
    }

    private Class[] switchToStorageTypes(Class[] types) {
        Class[] tmp = new Class[types.length];
        for (int i = 0; i < types.length; i++) {
            Class pClass = types[i];
            if (pClass == java.sql.Timestamp.class || pClass == java.math.BigDecimal.class) {
                tmp[i] = byte[].class;
            }
            else if (pClass == java.sql.Date.class || pClass == java.sql.Time.class) {
                tmp[i] = Long.class;
            }
            else
                tmp[i] = pClass;
        }

        return tmp;
    }

    static private boolean objectIsPacked(Class[] types) {
        for (int i = 0; i < types.length; i++) {
            Class pClass = types[i];
            if (pClass == java.sql.Timestamp.class || pClass == java.sql.Date.class || pClass == java.sql.Time.class
                    || pClass == java.math.BigDecimal.class) {
                return true;
            }

        }

        return false;
    }

    private boolean mKeyIsPacked = false;

    private Class c;
    private byte[] mKeyTypeMap;

    public void switchToReadOnlyMode() {

    }

    public void clear() {
        this.mMap.clear();
    }

    final private Key wrapKey(Object pkey) {
        return new Key((Object[]) pkey);
    }

    public boolean containsKey(Object key) {
        return mMap.get(wrapKey(packArray((Object[]) key, this.mKeyTypes, this.mKeyIsPacked))) != null;
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

            Object[] data = (Object[]) res;

            return data[idx];

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

    @SuppressWarnings("unused")
    private Object unpackObject(Object data, Class pClass) {
        if (data == null)
            return null;

        if (pClass == java.sql.Timestamp.class) {
            long t = Bytes.unpack8((byte[]) data, 0);
            java.sql.Timestamp res = new java.sql.Timestamp(t);
            res.setNanos(Bytes.unpack4((byte[]) data, 8));
            data = res;
        }
        else if (pClass == java.math.BigDecimal.class) {
            byte[] bd = (byte[]) data;
            byte[] tmp = new byte[bd.length - 4];
            int scale = Bytes.unpack4(bd, 0);
            System.arraycopy(bd, 2, tmp, 0, bd.length - 4);
            BigInteger bi = new BigInteger(tmp);
            data = new BigDecimal(bi, scale);
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
        else if (pClass == java.math.BigDecimal.class) {
            BigDecimal bd = (BigDecimal) data;
            byte[] tmp = bd.unscaledValue().toByteArray();
            byte[] tmp2 = new byte[tmp.length + 4];
            Bytes.pack4(tmp2, 0, bd.scale());
            System.arraycopy(tmp, 0, tmp2, 4, tmp2.length - 4);
            data = tmp2;
        }
        else if (pClass == java.sql.Date.class) {
            data = ((java.sql.Date) data).getTime();
        }
        else if (pClass == java.sql.Time.class) {
            data = ((java.sql.Time) data).getTime();
        }
        return data;
    }

    private Object[] packArray(Object[] data, Class[] pClass, boolean copy) {

        if (copy) {
            Object[] tmp = new Object[data.length];
            System.arraycopy(data, 0, tmp, 0, data.length);
            data = tmp;
        }

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
        Persistent datum = new FastDatumArray((Object[]) value, this.mValueTypeMap);

        Key key = wrapKey(packArray((Object[]) pkey, this.mKeyTypes, this.mKeyIsPacked));

        synchronized (wait) {
            if (mMap.put(key, datum))
                count++;
        }
        return null;
    }

    public void putAll(Map t) {

    }

    public Object remove(Object pkey) {
        return mMap.remove(wrapKey(packArray((Object[]) pkey, this.mKeyTypes, this.mKeyIsPacked)));
    }

    public int size() {
        return mMap.size();
    }

    public Collection values() {
        return null;
    }

    Index mMap;

    private String mName;
    private int count = 0;

    private Class[] mKeyTypes;

    HashMap fieldIndex = new HashMap();

    private boolean mKeyIsArray;

    Class[] mValueTypes;

    private String[] mValueFields;

    private int mSize;

    public RawPerstIndexedMap(String pName, int pSize, Integer pPersistanceID, String pCacheDir, Class[] pKeyTypes,
            Class[] pValueTypes, String[] pValueFields, boolean pPurgeCache) {
        super();

        mName = pName;
        mSize = pSize;
        this.mCacheDir = pCacheDir;
        this.mKeyTypes = pKeyTypes;
        this.mValueTypes = pValueTypes;
        this.mKeyIsArray = true;// pKeyTypes.length == 1 ? false : true;
        this.mValueFields = pValueFields;
        this.mValueIsSingle = pValueTypes.length == 1;
        this.mValueTypeMap = new byte[pValueTypes.length];
        for (int i = 0; i < pValueFields.length; i++) {
            fieldIndex.put(pValueFields[i], i);
            this.mValueTypeMap[i] = RawPerstIndexedMap.getType(pValueTypes[i]);
        }
        this.mKeyTypeMap = new byte[pKeyTypes.length];
        for (int i = 0; i < pKeyTypes.length; i++) {
            this.mKeyTypeMap[i] = RawPerstIndexedMap.getType(pKeyTypes[i]);
        }

        this.mKeyIsPacked = objectIsPacked(this.mKeyTypes);

        if (pPurgeCache)
            this.deleteCache();
        mMap = init();
    }

    private boolean mValueIsSingle = false;
    static final byte cInt = 0;
    static final byte cDouble = 1;
    static final byte cLong = 2;
    static final byte cFloat = 3;
    static final byte cDate = 4;
    static final byte cSQLDate = 5;
    static final byte cSQLTime = 6;
    static final byte cSQLTimestamp = 7;
    static final byte cBoolean = 8;
    static final byte cByte = 9;
    static final byte cByteArray = 10;
    static final byte cChar = 12;
    static final byte cObject = 13;
    static final byte cString = 14;
    static final byte cShort = 15;

    public synchronized void commit(boolean force) {
        if (commitCount > 0 || force) {
            commitCount = 0;
            ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE, "Lookup '" + mName + "' Size: "
                    + NumberFormatter.format(storage.getDatabaseSize()) + ", Count: " + mMap.size());
            storage.commit();

        }
    }

    public synchronized void deleteCache() {
        if (root != null && root.removeIndex(mName)) {

            ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE, "Closing shared cache");
            // storage.gc();
            storage.commit();
            root.store();
            storage.close();
            

            storage = null;
            /*File fl = new File(this.getCacheFileName());
            if (fl.exists())
                if (fl.delete()) {
                    ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE, "Deleting cache file");
                }
             */                
        }
    }

    private String getCacheFileName() {
    	String sub;
		if (Thread.currentThread().getName().contains("ETLDaemon"))
			sub = "ETLDaemon";
		else if (Thread.currentThread().getName().contains("Console"))
			sub = "Console";
		else
			sub = "Default";

		return mCacheDir + File.separator + "KETL." + sub + ".cache";
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
        String exampleValue = "N/A", exampleKey = "N/A";
        if (mMap != null && mMap.size() > 0) {
            Iterator it = mMap.entryIterator();
            Map.Entry res = null;
            if (it != null) {
                res = (Map.Entry) it.next();
            }
            if (res != null && res.getKey() != null && res.getValue() != null) {
                Object ky = res.getKey();

                exampleKey = java.util.Arrays.toString((Object[]) ky);

                Object tmp = ((FastDatumArray) res.getValue()).data;
                exampleValue = (tmp instanceof Object[]) ? java.util.Arrays.toString((Object[]) tmp) : res.toString();
            }
        }
        
        return "\n\tInternal Name: " + this.mName + "\n\tKey Type(s):" + java.util.Arrays.toString(this.mKeyTypes)
                + "\n\tKey Field(s):" + java.util.Arrays.toString(this.mValueFields) + "\n\tResult Type(s):"
                + java.util.Arrays.toString(this.mValueTypes) + "\n\tExample: Key->" + exampleKey + " Value->"
                + exampleValue + "\n\tConsolidated Cache Size: " + NumberFormatter.format(storage.getDatabaseSize())
                + "\n\tOn Disk Count: " + this.mMap.size();

    }

    public Object getItem(Object key) throws Exception {
        key = packArray((Object[]) key, this.mKeyTypes, this.mKeyIsPacked);

        Object res;

        if ((res = mMap.get(wrapKey(key))) == null)
            return null;

        Object[] data = ((FastDatumArray) res).data;

        if (data == null)
            throw new KETLError("Critical error, DatumArray contains null data, lookup is corrupted");

        return data;

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

    static byte getType(Class cl) {
        if (cl == Double.class) {
            return cDouble;
        }
        else if (cl == Integer.class) {
            return cInt;

        }
        else if (cl == String.class) {
            return cString;
        }
        else if (cl == Long.class) {
            return cLong;
        }
        else if (cl == Short.class) {
            return cShort;
        }
        else if (cl == Float.class) {
            return cFloat;
        }
        else if (cl == Boolean.class) {
            return cBoolean;
        }
        else if (cl == java.util.Date.class) {
            return cDate;
        }
        else if (cl == java.sql.Date.class)
            return cSQLDate;
        else if (cl == java.sql.Time.class) {
            return cSQLTime;
        }
        else if (cl == java.sql.Timestamp.class) {
            return cSQLTimestamp;
        }
        else if (cl == byte[].class) {
            return cByteArray;
        }
        else if (cl == Character.class) {
            return cChar;

        }
        else {
            return cObject;
        }

    }

    private static int indexCount = 0;

    public void close() {
        indexCount--;
        if (indexCount == 0 && storage != null) {
            root.store();
            storage.close();
            storage = null;
        }
    }

}
