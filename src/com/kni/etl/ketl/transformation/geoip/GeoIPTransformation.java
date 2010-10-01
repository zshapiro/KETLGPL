/*
 * Created on Jul 13, 2005
 *
 * To change the template for this generated file go to
 * Window&gt;Preferences&gt;Java&gt;Code Generation&gt;/**
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
package com.kni.etl.ketl.transformation.geoip;

import java.io.IOException;
import java.text.Format;
import java.text.ParsePosition;

import org.w3c.dom.Element;
import org.w3c.dom.Node;

import com.kni.etl.ketl.ETLInPort;
import com.kni.etl.ketl.ETLOutPort;
import com.kni.etl.ketl.ETLPort;
import com.kni.etl.ketl.ETLStep;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.exceptions.KETLTransformException;
import com.kni.etl.ketl.smp.ETLThreadManager;
import com.kni.etl.ketl.transformation.ETLTransformation;
import com.kni.etl.util.XMLHelper;
import com.maxmind.geoip.Country;
import com.maxmind.geoip.Location;
import com.maxmind.geoip.LookupService;

// Create a parallel transformation. All thread management is done for you
// the parallism is within the transformation

public class GeoIPTransformation extends ETLTransformation {

    class GEOIPETLInPort extends ETLInPort {

        public GEOIPETLInPort(ETLStep esOwningStep, ETLStep esSrcStep) {
            super(esOwningStep, esSrcStep);
        }

        @Override
        public int initialize(Node xmlConfig) throws ClassNotFoundException, KETLThreadException {
            int res = super.initialize(xmlConfig);
            if (res != 0)
                return res;

            if (XMLHelper.getAttributeAsString(this.getXMLConfig().getAttributes(), "NAME", "").equalsIgnoreCase("IP")) {
                if (mSourceIPPort != null)
                    throw new KETLThreadException("Only one port can be assigned as the source IP", this);
                mSourceIPPort = this;
            }

            return 0;
        }

    }

    class GEOIPOutPort extends ETLOutPort {

        String fmt;

        Format formatter;

        int lookUpRef = -1, mField;
        ParsePosition position;

        public GEOIPOutPort(ETLStep esOwningStep, ETLStep esSrcStep) {
            super(esOwningStep, esSrcStep);
        }

        public String generateCode(int portReferenceIndex) throws KETLThreadException {

            if (this.lookUpRef == -1 || this.isUsed() == false)
                return super.generateCode(portReferenceIndex);

            // must be pure code then do some replacing

            return this.getCodeGenerationReferenceObject() + "[" + this.mesStep.getUsedPortIndex(this) + "] = (("
                    + this.mesStep.getClass().getCanonicalName() + ")this.getOwner()).getValue(" + portReferenceIndex
                    + ")";

        }

        @Override
        public ETLPort getAssociatedInPort() throws KETLThreadException {
            if (lookUpRef != -1)
                return mSourceIPPort;

            return super.getAssociatedInPort();
        }

        @Override
        public int initialize(Node xmlConfig) throws ClassNotFoundException, KETLThreadException {

            String tmpType = XMLHelper.getAttributeAsString(xmlConfig.getAttributes(), "TYPE", null);
            String tmp = XMLHelper.getAttributeAsString(xmlConfig.getAttributes(), FIELD_ATTRIB, null);

            if (tmpType != null) {
                if (tmp == null)
                    lookUpRef = -1;
                else if (tmpType.equals(COUNTRY)) {
                    lookUpRef = checkForLkup(COUNTRY_ID);
                }
                else if (tmpType.equals(ORG)) {
                    lookUpRef = checkForLkup(ORG_ID);
                }
                else if (tmpType.equals(CITY)) {
                    lookUpRef = checkForLkup(CITY_ID);
                }
                else if (tmpType.equals(ISP)) {
                    lookUpRef = checkForLkup(ISP_ID);
                }
                else
                    lookUpRef = -1;

                mField = getField(tmp, FIELDS);

            }

            int res = super.initialize(xmlConfig);

            if (res != 0)
                return res;

            return 0;

        }

        final public void setDataTypeFromPort(ETLPort in) throws KETLThreadException, ClassNotFoundException {
            if (this.lookUpRef != -1) {

                String required = null;
                switch (this.mField) {
                case CITY_DMA_ID:
                case CITY_AREA_ID:
                    required = "INTEGER";
                    break;
                case CITY_LATITUDE_ID:
                case CITY_LONGITUDE_ID:
                    required = "FLOAT";
                    break;
                default:
                    required = "STRING";
                }

                String declared = XMLHelper.getAttributeAsString(this.getXMLConfig().getAttributes(), "DATATYPE",
                        required);

                if (required.equalsIgnoreCase(declared) == false)
                    throw new KETLThreadException("Port " + this.mstrName + " requires datatype to be " + required,
                            this);
                ((Element) this.getXMLConfig()).setAttribute("DATATYPE", required);
                this.setPortClass();
            }
            else
                super.setDataTypeFromPort(in);
        }

    }

    private static final String CITY = "CITY";
    private static final int CITY_CITY_ID = 3;

    private static final String CITY_DB_PATH = "CITYDBPATH";
    private static final int CITY_ID = 1;
    private static final int CITY_LATITUDE_ID = 5;
    private static final int CITY_LONGITUDE_ID = 6;
    private static final int CITY_AREA_ID = 7;
    private static final int CITY_DMA_ID = 8;

    private static final int CITY_POSTALCODE_ID = 4;

    private static final int CITY_REGION_ID = 2;
    private static final String COUNTRY = "COUNTRY";
    private static final String COUNTRY_DB_PATH = "COUNTRYDBPATH";
    private static final int COUNTRY_ID = 0;

    private static final int COUNTRYCODE_ID = 0;
    private static final int COUNTRYNAME_ID = 1;
    private static final String FIELD_ATTRIB = "FIELD";
    private static final String[] FIELDS = { "COUNTRYCODE", "COUNTRYNAME", "REGION", "CITY", "POSTALCODE", "LATITUDE",
            "LONGITUDE" ,"AREACODE","DMA","ISP","ORG"};
    public static String FORMAT_STRING = "FORMATSTRING";
    private static final String ISP = "ISP";
    private static final String ISP_DB_PATH = "ISPDBPATH";

    private static final int ISP_ID = 3;

    private static final String ORG = "ORG";
    private static final String ORG_DB_PATH = "ORGDBPATH";

    private static final int ORG_ID = 2;

    private int checkForLkup(int id) throws KETLThreadException {
        loadCache[id] = true;
        return id;
    }

    private static void closeService(LookupService arg0) {
        if (arg0 == null)
            return;

        arg0.close();

    }

    private static LookupService getService(String dbpath) throws IOException {
        if (dbpath == null)
            return null;
        return new LookupService(dbpath);
    }

    Object[][] mCache = new Object[4][2];

    private LookupService mCountryDBPath, mCityDBPath, mOrgDBPath, mISPDBPath;

    private boolean[] loadCache = { false, false, false, false };
    String mCurrentIP;

    String mPreviousIP = null;

    GEOIPETLInPort mSourceIPPort = null;

    public GeoIPTransformation(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
            throws KETLThreadException {
        super(pXMLConfig, pPartitionID, pPartition, pThreadManager);

    }

    public Object checkCacheFirst(int type, String ip) {
        if (mCache[type][0] != null && ip != null && ip.equals(mCache[type][0]) && mCache[type][1] != null)
            return mCache[type][1];
        Object o = null;
        switch (type) {
        case ORG_ID:
        	 o = mOrgDBPath.getOrg(ip);
             break;
        case CITY_ID:
            o = mCityDBPath.getLocation(ip);
            if (o == null)
                o = new Location();
            break;
        case COUNTRY_ID:
            o = mCountryDBPath.getCountry(ip);
            if (o == null)
                o = new Country(null, null);

            break;
        case ISP_ID:
        	 o = mISPDBPath.getOrg(ip);
             break;
        }
        if (o != null && ip != null) {
            mCache[type][0] = ip;
            mCache[type][1] = o;
        }
        return o;
    }

    @Override
    protected void close(boolean success, boolean jobFailed) {
        closeService(this.mCountryDBPath);
        closeService(this.mOrgDBPath);
        closeService(this.mISPDBPath);
        closeService(this.mCityDBPath);
    }

    private int getField(String arg0, String[] arg1) throws KETLThreadException {

        if (arg0 == null)
            return -1;
        for (int i = 0; i < arg1.length; i++) {
            if (arg1[i].equalsIgnoreCase(arg0))
                return i;
        }
        throw new KETLThreadException("Invalid field " + arg0, this);
    }

    @Override
    protected ETLInPort getNewInPort(ETLStep srcStep) {
        return new GEOIPETLInPort(this, srcStep);
    }

    @Override
    protected ETLOutPort getNewOutPort(ETLStep srcStep) {
        return new GEOIPOutPort(this, srcStep);
    }

    @Override
    protected String getRecordExecuteMethodHeader() throws KETLThreadException {
        if (this.mSourceIPPort == null)
            return super.getRecordExecuteMethodHeader();

        return super.getRecordExecuteMethodHeader() + " ((" + this.getClass().getCanonicalName()
                + ")this.getOwner()).loadValue(" + this.mSourceIPPort.generateReference() + ");";
    }

    public Object getValue(int i) throws KETLTransformException {
        GEOIPOutPort port = (GEOIPOutPort) this.mOutPorts[i];

        switch (port.lookUpRef) {
        case ORG_ID:
        	String org = (String) checkCacheFirst(ORG_ID, this.mCurrentIP);
        	return org;
        case CITY_ID:
            Location loc = (Location) checkCacheFirst(CITY_ID, mCurrentIP);
            if (loc == null)
                loc = new Location();

            switch (port.mField) {
            case CITY_REGION_ID:
                return loc.region;

            case CITY_POSTALCODE_ID:
                return loc.postalCode;

            case CITY_LATITUDE_ID:
                return loc.latitude;

            case CITY_LONGITUDE_ID:
                return loc.longitude;

            case CITY_CITY_ID:
                return loc.city;

            case CITY_DMA_ID:
                return loc.dma_code;

            case CITY_AREA_ID:
                return loc.area_code;

            case COUNTRYCODE_ID:
                return loc.countryCode;
            case COUNTRYNAME_ID:
                return loc.countryName;
            }
            break;
        case COUNTRY_ID:
            Country cnty = (Country) checkCacheFirst(COUNTRY_ID, this.mCurrentIP);

            switch (port.mField) {

            case COUNTRYCODE_ID:
                return cnty.getCode();

            case COUNTRYNAME_ID:
                return cnty.getName();

            }
            break;
        case ISP_ID:
        	String isp = (String) checkCacheFirst(ISP_ID, this.mCurrentIP);
        	return isp;
        }

        throw new KETLTransformException("unexpected field type " + port.mstrName);

    }

    public void loadValue(Object pIP) throws KETLTransformException {
        mCurrentIP = (String) pIP;
    }

    @Override
    protected int initialize(Node xmlConfig) throws KETLThreadException {
        int res = super.initialize(xmlConfig);

        if (res != 0)
            return res;

        try {
            if (loadCache[COUNTRY_ID]) {
                mCountryDBPath = getService(getParameterValue(0, COUNTRY_DB_PATH));
                if (this.mCountryDBPath == null)
                    throw new KETLThreadException(COUNTRY_DB_PATH + " is missing from the parameter list", this);
            }
            if (loadCache[CITY_ID]) {
                mCityDBPath = getService(getParameterValue(0, CITY_DB_PATH));
                if (this.mCityDBPath == null)
                    throw new KETLThreadException(CITY_DB_PATH + " is missing from the parameter list", this);
            }

            if (loadCache[ORG_ID]) {
                mOrgDBPath = getService(getParameterValue(0, ORG_DB_PATH));
                if (this.mOrgDBPath == null)
                    throw new KETLThreadException(ORG_DB_PATH + " is missing from the parameter list", this);
            }

            if (loadCache[ISP_ID]) {
                mISPDBPath = getService(getParameterValue(0, ISP_DB_PATH));
                if (this.mISPDBPath == null)
                    throw new KETLThreadException(ISP_DB_PATH + " is missing from the parameter list", this);
            }

        } catch (IOException e) {
            throw new KETLThreadException(e, this);
        }

        return 0;
    }

}
