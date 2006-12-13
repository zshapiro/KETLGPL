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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.util.Date;

import org.garret.perst.Persistent;
import org.garret.perst.StorageError;
import org.garret.perst.impl.ByteBuffer;
import org.garret.perst.impl.Bytes;
import org.garret.perst.impl.FastSerializable;

import com.kni.etl.ketl.exceptions.KETLError;

final class FastDatumArray extends Persistent implements FastSerializable {

    transient byte[] mTypes;

    FastDatumArray(Object[] arg0, byte[] types) {
        super();
        this.data = arg0;
        this.mTypes = types;
    }

    transient Object[] data;

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    int packString(ByteBuffer buf, int dst, String value, String encoding) {

        int length = value.length();
        if (encoding == null) {
            buf.extend(dst + 4 + 2 * length);
            Bytes.pack4(buf.arr, dst, length);
            dst += 4;
            for (int i = 0; i < length; i++) {
                Bytes.pack2(buf.arr, dst, (short) value.charAt(i));
                dst += 2;
            }
        }
        else {
            try {
                byte[] bytes = value.getBytes(encoding);
                buf.extend(dst + 4 + bytes.length);
                Bytes.pack4(buf.arr, dst, -2 - bytes.length);
                System.arraycopy(bytes, 0, buf.arr, dst + 4, bytes.length);
                dst += 4 + bytes.length;
            } catch (UnsupportedEncodingException x) {
                throw new StorageError(StorageError.UNSUPPORTED_ENCODING);
            }
        }

        return dst;
    }

    public int pack(ByteBuffer buf, int offs, String encoding) {

        int dlen = this.data.length;

        buf.extend(offs + 2);
        Bytes.pack2(buf.arr, offs, (short) dlen);
        offs += 2;

        for (int i = 0; i < dlen; i++) {
            byte cl = mTypes[i];
            Object obj = data[i];
            // record if null
            if (obj == null) {
                buf.extend(offs + 1);
                buf.arr[offs++] = 0;
            }
            else {
                buf.extend(offs + 1);
                buf.arr[offs++] = 1;
                // record type
                buf.extend(offs + 1);
                buf.arr[offs++] = cl;

                switch (cl) {
                case RawPerstIndexedMap.cDouble:
                    buf.extend(offs + 8);
                    Bytes.pack8(buf.arr, offs, Double.doubleToLongBits((Double) obj));
                    offs += 8;
                    break;
                case RawPerstIndexedMap.cInt: {
                    buf.extend(offs + 4);
                    Bytes.pack4(buf.arr, offs, (Integer) obj);
                    offs += 4;

                }
                    break;
                case RawPerstIndexedMap.cString: {
                    offs = packString(buf, offs, (String) obj, encoding);
                }
                    break;
                case RawPerstIndexedMap.cLong: {
                    buf.extend(offs + 8);
                    Bytes.pack8(buf.arr, offs, (Long) obj);
                    offs += 8;
                }
                    break;
                case RawPerstIndexedMap.cShort: {
                    buf.extend(offs + 2);
                    Bytes.pack2(buf.arr, offs, (Short) obj);
                    offs += 2;
                }
                    break;
                case RawPerstIndexedMap.cFloat: {
                    buf.extend(offs + 4);
                    Bytes.pack4(buf.arr, offs, Float.floatToIntBits((Float) obj));
                    offs += 4;
                }
                    break;
                case RawPerstIndexedMap.cBoolean: {
                    buf.extend(offs + 1);
                    buf.arr[offs++] = (byte) ((Boolean) obj ? 1 : 0);

                }
                    break;
                case RawPerstIndexedMap.cDate:
                case RawPerstIndexedMap.cSQLDate: {
                    buf.extend(offs + 8);
                    Date d = (Date) obj;
                    Bytes.pack8(buf.arr, offs, d.getTime());
                    offs += 8;
                }
                    break;
                case RawPerstIndexedMap.cSQLTime: {
                    buf.extend(offs + 8);
                    Bytes.pack8(buf.arr, offs, ((java.sql.Time) obj).getTime());
                    offs += 8;
                }
                    break;
                case RawPerstIndexedMap.cSQLTimestamp: {
                    buf.extend(offs + 8);
                    Bytes.pack8(buf.arr, offs, ((java.sql.Timestamp) obj).getTime());
                    offs += 8;
                    buf.extend(offs + 4);
                    Bytes.pack4(buf.arr, offs, ((java.sql.Timestamp) obj).getNanos());
                    offs += 4;
                }
                    break;
                case RawPerstIndexedMap.cByteArray: {
                    byte[] arr = (byte[]) obj;

                    int len = arr.length;
                    buf.extend(offs + 4 + len);
                    Bytes.pack4(buf.arr, offs, len);
                    offs += 4;
                    System.arraycopy(arr, 0, buf.arr, offs, len);
                    offs += len;

                }
                    break;
                case RawPerstIndexedMap.cChar: {
                    buf.extend(offs + 2);
                    Bytes.pack2(buf.arr, offs, (short) (char) ((Character) obj));
                    offs += 2;

                }
                    break;
                default:
                    byte[] res = objectToByteArray(obj);
                    int len = res.length;
                    buf.extend(offs + 4 + len);
                    Bytes.pack4(buf.arr, offs, len);
                    offs += 4;
                    System.arraycopy(res, 0, buf.arr, offs, len);
                    offs += len;
                }
            }
        }

        return offs;
    }

    public int unpack(byte[] body, int offs, String encoding) {

        int dlen = (int) Bytes.unpack2(body, offs);
        offs += 2;

        data = new Object[dlen];

        for (int i = 0; i < dlen; i++) {
            if (body[offs++] == 1) {

                byte type = body[offs++];

                switch (type) {
                case RawPerstIndexedMap.cDouble: {
                    data[i] = Double.longBitsToDouble(Bytes.unpack8(body, offs));
                    offs += 8;
                }
                    break;
                case RawPerstIndexedMap.cInt: {
                    data[i] = Bytes.unpack4(body, offs);
                    offs += 4;

                }
                    break;
                case RawPerstIndexedMap.cString: {
                    int len = Bytes.unpack4(body, offs);
                    offs += 4;
                    char[] chars = new char[len];
                    for (int j = 0; j < len; j++) {
                        chars[j] = (char) Bytes.unpack2(body, offs);
                        offs += 2;
                    }
                    data[i] = new String(chars);
                }
                    break;
                case RawPerstIndexedMap.cLong: {
                    data[i] = Bytes.unpack8(body, offs);
                    offs += 8;
                }
                    break;
                case RawPerstIndexedMap.cShort: {
                    data[i] = Bytes.unpack2(body, offs);
                    offs += 2;
                }
                    break;
                case RawPerstIndexedMap.cFloat: {
                    data[i] = Float.intBitsToFloat(Bytes.unpack4(body, offs));
                    offs += 4;
                }
                    break;
                case RawPerstIndexedMap.cBoolean: {
                    data[i] = body[offs++] == 1;

                }
                    break;
                case RawPerstIndexedMap.cDate: {
                    data[i] = new java.util.Date(Bytes.unpack8(body, offs));
                    offs += 8;
                }
                    break;
                case RawPerstIndexedMap.cSQLDate: {
                    data[i] = new java.sql.Date(Bytes.unpack8(body, offs));
                    offs += 8;
                }
                    break;
                case RawPerstIndexedMap.cSQLTime: {
                    data[i] = new java.sql.Time(Bytes.unpack8(body, offs));
                    offs += 8;
                }
                    break;
                case RawPerstIndexedMap.cSQLTimestamp: {
                    java.sql.Timestamp tms = new java.sql.Timestamp(Bytes.unpack8(body, offs));
                    offs += 8;
                    tms.setNanos(Bytes.unpack4(body, offs));
                    offs += 4;
                    data[i] = tms;
                }
                    break;
                case RawPerstIndexedMap.cByteArray: {
                    int len = Bytes.unpack4(body, offs);
                    offs += 4;
                    byte[] tmp = new byte[len];
                    System.arraycopy(body, 0, tmp, offs, len);
                    offs += len;
                    data[i] = tmp;
                }
                    break;
                case RawPerstIndexedMap.cChar: {
                    short sc = Bytes.unpack2(body, offs);
                    data[i] = new Character((char) sc);
                    offs += 2;

                }
                    break;
                default:
                    int len = Bytes.unpack4(body, offs);
                    offs += 4;
                    data[i] = byteArrayToObject(body, offs, len);
                    offs += len;
                }
            }
        }

        return offs;
    }

    private static byte[] objectToByteArray(Object obj) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();

            ObjectOutputStream oos = new ObjectOutputStream(baos);

            oos.writeObject(obj);
            oos.flush();

            return baos.toByteArray();
        } catch (Exception e) {
            throw new KETLError(e);
        }
    }

    private static Object byteArrayToObject(byte[] serializedBytes, int offset, int len) {
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(serializedBytes, offset, len);
            ObjectInputStream ois = new ObjectInputStream(bais);

            return ois.readObject();
        } catch (Exception e) {
            throw new KETLError(e);
        }
    }

}