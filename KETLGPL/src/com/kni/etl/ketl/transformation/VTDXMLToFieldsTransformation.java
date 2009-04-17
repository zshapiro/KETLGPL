/*
 *  Copyright (C) Feb 18th, 2009 Kinetic Networks, Inc. All Rights Reserved. 
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *  
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *  
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307, USA.
 *  
 *  Kinetic Networks Inc
 *  33 New Montgomery, Suite 1200
 *  San Francisco CA 94105
 *  http://www.kineticnetworks.com
 */
package com.kni.etl.ketl.transformation;

import java.io.StringReader;
import java.io.StringWriter;
import java.lang.reflect.Constructor;
import java.text.Format;
import java.text.ParsePosition;
import java.util.ArrayList;
import java.util.List;

import com.ximpleware.*;
import com.ximpleware.xpath.*;
import java.io.*;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.ETLInPort;
import com.kni.etl.ketl.ETLOutPort;
import com.kni.etl.ketl.ETLPort;
import com.kni.etl.ketl.ETLStep;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.exceptions.KETLTransformException;
import com.kni.etl.ketl.smp.ETLThreadManager;
import com.kni.etl.ketl.transformation.ETLTransformation;
import com.kni.etl.stringtools.FastSimpleDateFormat;
import com.kni.etl.util.XMLHandler;
import com.kni.etl.util.XMLHelper;

// TODO: Auto-generated Javadoc
// Create a parallel transformation. All thread management is done for you
// the parallism is within the transformation

/**
 * The Class XMLToFieldsTransformation.
 */
public class VTDXMLToFieldsTransformation extends ETLTransformation {

	public interface XMLNodeListCreator {

		List<Document> getNodes(Document doc);

	}

	/** The Constant XPATH_EVALUATE_ATTRIB. */
	private static final String XPATH_EVALUATE_ATTRIB = "XPATHEVALUATE";

	/** The Constant DUMPXML_ATTRIB. */
	private static final String DUMPXML_ATTRIB = "DUMPXML";

	private VTDGen parser;

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.smp.ETLTransform#initialize(org.w3c.dom.Node)
	 */
	@Override
	protected int initialize(Node xmlConfig) throws KETLThreadException {
		int res = super.initialize(xmlConfig);

		if (res != 0)
			return res;

		if ((this.mRootXPath = XMLHelper.getAttributeAsString(xmlConfig.getAttributes(),
				VTDXMLToFieldsTransformation.XPATH_ATTRIB, null)) == null) {
			// No TABLE attribute listed...
			throw new KETLThreadException("ERROR: No root XPATH attribute specified in step '" + this.getName() + "'.",
					this);
		}

		boolean mbXPathEvaluateNodes = XMLHelper.getAttributeAsBoolean(xmlConfig.getAttributes(),
				VTDXMLToFieldsTransformation.XPATH_EVALUATE_ATTRIB, true);

		try {
			this.mXPath = new AutoPilot();
			this.mXPath.selectXPath(mbXPathEvaluateNodes ? this.mRootXPath : "//" + this.mRootXPath);

			this.parser = new VTDGen();
			this.bookMark = new BookMark();
		} catch (Exception e) {
			throw new KETLThreadException(e, this);
		}

		return 0;
	}

	/** The builder. */
	private DocumentBuilder mBuilder;

	/** The xml handler. */
	private XMLHandler xmlHandler = null;

	/** The namespace aware. */
	private boolean namespaceAware;

	/**
	 * Instantiates a new XML to fields transformation.
	 * 
	 * @param pXMLConfig
	 *            the XML config
	 * @param pPartitionID
	 *            the partition ID
	 * @param pPartition
	 *            the partition
	 * @param pThreadManager
	 *            the thread manager
	 * 
	 * @throws KETLThreadException
	 *             the KETL thread exception
	 */
	public VTDXMLToFieldsTransformation(Node pXMLConfig, int pPartitionID, int pPartition,
			ETLThreadManager pThreadManager) throws KETLThreadException {
		super(pXMLConfig, pPartitionID, pPartition, pThreadManager);

		try {

			this.namespaceAware = XMLHelper.getAttributeAsBoolean(pXMLConfig.getAttributes(), "NAMESPACEAWARE", false);

		} catch (Exception e) {
			throw new KETLThreadException(e, this);
		}

	}

	/** The Constant XPATH_ATTRIB. */
	public static final String XPATH_ATTRIB = "XPATH";

	/** The root X path. */
	private String mRootXPath;

	/** The xml src port. */
	XMLETLInPort xmlSrcPort = null;

	/**
	 * The Class XMLETLInPort.
	 */
	class XMLETLInPort extends ETLInPort {

		/*
		 * (non-Javadoc)
		 * 
		 * @see com.kni.etl.ketl.ETLInPort#initialize(org.w3c.dom.Node)
		 */
		@Override
		public int initialize(Node xmlConfig) throws ClassNotFoundException, KETLThreadException {
			int res = super.initialize(xmlConfig);
			if (res != 0)
				return res;

			if (XMLHelper.getAttributeAsBoolean(this.getXMLConfig().getAttributes(), "XMLDATA", false)) {
				if (VTDXMLToFieldsTransformation.this.xmlSrcPort != null)
					throw new KETLThreadException("Only one port can be assigned as XMLData", this);
				VTDXMLToFieldsTransformation.this.xmlSrcPort = this;
			}

			return 0;
		}

		/**
		 * Instantiates a new XMLETL in port.
		 * 
		 * @param esOwningStep
		 *            the es owning step
		 * @param esSrcStep
		 *            the es src step
		 */
		public XMLETLInPort(ETLStep esOwningStep, ETLStep esSrcStep) {
			super(esOwningStep, esSrcStep);
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.smp.ETLWorker#getNewOutPort(com.kni.etl.ketl.ETLStep)
	 */
	@Override
	protected ETLOutPort getNewOutPort(ETLStep srcStep) {
		return new XMLETLOutPort(this, srcStep);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.smp.ETLWorker#getNewInPort(com.kni.etl.ketl.ETLStep)
	 */
	@Override
	protected ETLInPort getNewInPort(ETLStep srcStep) {
		return new XMLETLInPort(this, srcStep);
	}

	/** The FORMA t_ STRING. */
	public static String FORMAT_STRING = "FORMATSTRING";

	/**
	 * The Class XMLETLOutPort.
	 */
	class XMLETLOutPort extends ETLOutPort {

		/*
		 * (non-Javadoc)
		 * 
		 * @see com.kni.etl.ketl.ETLPort#setDataTypeFromPort(com.kni.etl.ketl.ETLPort)
		 */
		@Override
		final public void setDataTypeFromPort(ETLPort in) throws KETLThreadException, ClassNotFoundException {
			if (this.xpath == null || this.getXMLConfig().hasAttribute("DATATYPE") == false)
				(this.getXMLConfig()).setAttribute("DATATYPE", in.getPortClass().getCanonicalName());
			this.setPortClass();
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see com.kni.etl.ketl.ETLPort#getAssociatedInPort()
		 */
		@Override
		public ETLPort getAssociatedInPort() throws KETLThreadException {
			if (this.xpath != null)
				return VTDXMLToFieldsTransformation.this.xmlSrcPort;

			return super.getAssociatedInPort();
		}

		/** The fetch attribute. */
		boolean fetchAttribute = false;

		/** The X path exp. */
		AutoPilot mXPathExp;

		/** The xpath. */
		String fmt, xpath;

		/** The formatter. */
		Format formatter;

		/** The attribute. */
		String attribute = null;

		
		/** The position. */
		ParsePosition position;

		/** The dump XML. */
		boolean mDumpXML = false;

		/** The null IF. */
		String nullIF = null;

		/*
		 * (non-Javadoc)
		 * 
		 * @see com.kni.etl.ketl.ETLPort#initialize(org.w3c.dom.Node)
		 */
		@Override
		public int initialize(Node xmlConfig) throws ClassNotFoundException, KETLThreadException {
			this.xpath = XMLHelper.getAttributeAsString(xmlConfig.getAttributes(),
					VTDXMLToFieldsTransformation.XPATH_ATTRIB, null);

			if (this.xpath == null)
				this.xpath = XMLHelper.getAttributeAsString(xmlConfig.getParentNode().getAttributes(),
						VTDXMLToFieldsTransformation.XPATH_ATTRIB, null);

			this.mDumpXML = XMLHelper.getAttributeAsBoolean(xmlConfig.getAttributes(),
					VTDXMLToFieldsTransformation.DUMPXML_ATTRIB, false);

			this.nullIF = XMLHelper.getAttributeAsString(xmlConfig.getAttributes(), "NULLIF", null);
			int res = super.initialize(xmlConfig);

			if (res != 0)
				return res;

			String content = XMLHelper.getTextContent(this.getXMLConfig());
			if (content != null && content.trim().length() > 0) {
				this.xpath = null;
			}

			this.fmt = XMLHelper.getAttributeAsString(this.getXMLConfig().getAttributes(),
					VTDXMLToFieldsTransformation.FORMAT_STRING, null);

			if (this.xpath != null) {
				try {
					this.mXPathExp = new AutoPilot();
					this.mXPathExp.selectXPath(this.xpath);
				} catch (Exception e) {
					throw new KETLThreadException(e.toString(), this);
				}
			}

			return 0;

		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see com.kni.etl.ketl.ETLOutPort#generateCode(int)
		 */
		@Override
		public String generateCode(int portReferenceIndex) throws KETLThreadException {

			if (this.xpath == null || this.isConstant() || this.isUsed() == false)
				return super.generateCode(portReferenceIndex);

			// must be pure code then do some replacing

			return this.getCodeGenerationReferenceObject() + "[" + this.mesStep.getUsedPortIndex(this) + "] = (("
					+ this.mesStep.getClass().getCanonicalName() + ")this.getOwner()).getXMLValue("
					+ portReferenceIndex + ")";

		}

		/**
		 * Instantiates a new XMLETL out port.
		 * 
		 * @param esOwningStep
		 *            the es owning step
		 * @param esSrcStep
		 *            the es src step
		 */
		public XMLETLOutPort(ETLStep esOwningStep, ETLStep esSrcStep) {
			super(esOwningStep, esSrcStep);
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.smp.ETLTransform#getRecordExecuteMethodFooter()
	 */
	@Override
	protected String getRecordExecuteMethodFooter() {
		if (this.xmlSrcPort == null)
			return super.getRecordExecuteMethodFooter();

		return " return ((" + this.getClass().getCanonicalName()
				+ ")this.getOwner()).noMoreNodes()?SUCCESS:REPEAT_RECORD;}";
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.smp.ETLTransform#getRecordExecuteMethodHeader()
	 */
	@Override
	protected String getRecordExecuteMethodHeader() throws KETLThreadException {
		if (this.xmlSrcPort == null)
			return super.getRecordExecuteMethodHeader();

		return super.getRecordExecuteMethodHeader() + " if(((" + this.getClass().getCanonicalName()
				+ ")this.getOwner()).loadNodeList(" + this.xmlSrcPort.generateReference()
				+ ") == false) return SKIP_RECORD;";
	}

	/**
	 * No more nodes.
	 * 
	 * @return true, if successful
	 * @throws NavException
	 * @throws XPathEvalException
	 */
	public boolean noMoreNodes() throws XPathEvalException, NavException {

		if (this.mXPath.evalXPath() == -1) {
			this.currentXMLString = null;
			return true;
		}
		return false;
	}

	/**
	 * Gets the XML dump.
	 * 
	 * @param o
	 *            the o
	 * 
	 * @return the XML dump
	 * 
	 * @throws Exception
	 *             the exception
	 */
	/*private String getXMLDump(Object o) throws Exception {

		if (o == null)
			return null;
		if (o instanceof Node)
			return XMLHelper.outputXML((Node) o);

		if (o instanceof Source) {
			StringWriter ws = new StringWriter();
			this.xmlTransformer.transform((Source) o, new StreamResult(ws));
			return ws.toString();
		}

		throw new Exception("Object could not be converted to xml " + o.getClass().getCanonicalName());
	}
*/
	/**
	 * Gets the XML value.
	 * 
	 * @param i
	 *            the i
	 * 
	 * @return the XML value
	 * 
	 * @throws KETLTransformException
	 *             the KETL transform exception
	 */
	public Object getXMLValue(int i) throws KETLTransformException {
		XMLETLOutPort port = (XMLETLOutPort) this.mOutPorts[i];

		try {
			String result = null;

			this.bookMark.setCursorPosition();
			VTDNav vn = this.bookMark.getNav();

			if(this.mXPath ==null){
				if (vn.getText() != -1)
					System.out.println(vn.toNormalizedString(vn.getText()));
			} else {
				if (port.mDumpXML) {
					ByteArrayOutputStream sw = new ByteArrayOutputStream();
					vn.dumpXML(sw);
					result = sw.toString();
				} else
					port.mXPathExp.bind(vn);
				result = port.mXPathExp.evalXPathToString();
			}
			if (result == null || result.length() == 0 || (port.nullIF != null && port.nullIF.equals(result)))
				return null;

			Class cl = port.getPortClass();
			if (cl == Float.class || cl == float.class)
				return Float.parseFloat(result);

			if (cl == String.class)
				return result;

			if (cl == Long.class || cl == long.class)
				return Long.parseLong(result);

			if (cl == Integer.class || cl == int.class)
				return Integer.parseInt(result);

			if (cl == java.util.Date.class) {
				if (port.formatter == null) {
					if (port.fmt != null)
						port.formatter = new FastSimpleDateFormat(port.fmt);
					else
						port.formatter = new FastSimpleDateFormat();

					port.position = new ParsePosition(0);
				}

				port.position.setIndex(0);
				return port.formatter.parseObject(result, port.position);
			}

			if (cl == Double.class || cl == double.class)
				return Double.parseDouble(result);

			if (cl == Character.class || cl == char.class)
				return new Character(result.charAt(0));

			if (cl == Boolean.class || cl == boolean.class)
				return Boolean.parseBoolean(result);

			if (cl == Byte[].class || cl == byte[].class)
				return result.getBytes();

			Constructor con;
			try {
				con = cl.getConstructor(new Class[] { String.class });
			} catch (Exception e) {
				throw new KETLTransformException("No constructor found for class " + cl.getCanonicalName()
						+ " that accepts a single string");
			}
			return con.newInstance(new Object[] { result });

		} catch (Exception e) {
			throw new KETLTransformException("XML parsing failed for port " + port.mstrName, e);
		}
	}

	private String getChildNodeValueAsString(VTDNav vn, String xpath, Object object, Object object2, Object object3) {
		// TODO Auto-generated method stub
		return null;
	}

	private String getAttributeAsString(VTDNav vn, String attribute, Object object) {
		// TODO Auto-generated method stub
		return null;
	}

	/** The current XML string. */
	private byte[] currentXMLString;

	/** The X path. */
	private AutoPilot mXPath;

	/** The length. */
	private int pos = 0, length;

	private XMLNodeListCreator customHandler;

	private BookMark bookMark;

	/**
	 * Load node list.
	 * 
	 * @param string
	 *            the string
	 * 
	 * @return true, if successful
	 * 
	 * @throws KETLTransformException
	 *             the KETL transform exception
	 */
	public boolean loadNodeList(String string) throws KETLTransformException {
		try {
			VTDNav vn = null;
			if (this.mXPath.evalXPath() == -1) {

				this.mXPath.resetXPath();

				if (string == null)
					return false;

				this.currentXMLString = string.getBytes();
				this.parser.setDoc(this.currentXMLString);
				this.parser.parse(this.namespaceAware);
				vn = this.parser.getNav();
				this.bookMark.bind(vn);
				this.mXPath.bind(vn);

				if (this.mXPath.evalXPath() == -1)
					return false;

			}

			this.bookMark.recordCursorPosition(vn);

			return true;
		} catch (Exception e) {
			if (e instanceof KETLTransformException)
				throw (KETLTransformException) e;

			throw new KETLTransformException(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.smp.ETLWorker#close(boolean)
	 */
	@Override
	protected void close(boolean success, boolean jobFailed) {
		// TODO Auto-generated method stub

	}

}
