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

package com.kni.etl.ketl.kernel;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.Vector;

import org.w3c.dom.Document;
import org.w3c.dom.Node;

import com.kni.etl.ETLJob;
import com.kni.etl.ETLJobManager;
import com.kni.etl.ETLJobManagerStatus;
import com.kni.etl.ETLJobStatus;
import com.kni.etl.EngineConstants;
import com.kni.etl.Metadata;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.scheduler.MetadataScheduler;
import com.kni.etl.util.XMLHelper;

public class KETLKernelImpl implements KETLKernel {

    private static int SleepTime = 500;
    public static boolean shutdown = false;
    public static boolean paused = false;
    
    /**
     * Insert the method's description here. Creation date: (4/21/2002 8:40:01 AM)
     * 
     * @return java.lang.String[]
     * @param pArg java.lang.String
     */
    public static String extractArguments(String pArg, String pVarName) {
        String result = null;
        int argPos = -1;

        argPos = pArg.indexOf(pVarName);

        if (argPos != -1) {
            String fields = pArg.substring(pVarName.length());

            if (fields.length() > 0) {
                result = fields;
            }
        }

        return (result);
    }

    /**
     * Insert the method's description here. Creation date: (4/21/2002 8:40:01 AM)
     * 
     * @return java.lang.String[]
     * @param pArg java.lang.String
     */
    public static String[] extractMultipleArguments(String pArg, String pVarName) {
        String[] result = null;
        int argPos = -1;

        argPos = pArg.indexOf(pVarName);

        if (argPos != -1) {
            String fields = pArg.substring(pVarName.length(), pArg.lastIndexOf(")"));

            if (fields.indexOf(',') != -1) {
                // string contains multiple files
                StringTokenizer st = new StringTokenizer(fields, ",");

                int nFields = st.countTokens();

                result = new String[nFields];

                int pos = 0;

                while (st.hasMoreTokens()) {
                    result[pos] = st.nextToken();
                    pos++;
                }
            }
            else if (fields.length() > 0) {
                result = new String[1];
                result[0] = fields;
            }
        }

        return (result);
    }

    private String displayVersionInfo() {
        EngineConstants.getVersion();
        return "KETL Server\n";
    }

    public void listCurrentThreads() {
        try {
            ThreadGroup currentGroup = Thread.currentThread().getThreadGroup();
            int numThreads = currentGroup.activeCount();
            Thread[] listOfThreads = new Thread[numThreads];

            currentGroup.enumerate(listOfThreads, true);

            for (int i = 0; i < numThreads; i++) {
                try {
                    ResourcePool.LogMessage(Thread.currentThread().getName(), ResourcePool.INFO_MESSAGE, "Thread #"
                            + (i + 1) + " = " + listOfThreads[i].getName() + " of " + Thread.activeCount());
                } catch (Exception e) {
                }
            }
        } catch (Exception e) {
        }
    }

    /**
     * Starts the application.
     * 
     * @param args an array of command-line arguments
     */
    public void run(java.lang.String[] args) {
        String[] mdUser = null;
        String mdServer = null;
        Vector submittedJobs = new Vector();
        Object[][] jobManagers = null;
        ETLJob baseJob;
        Thread.currentThread().setName("ETLDaemon");

        try {
            baseJob = new ETLJob();
        } catch (Exception e2) {
            e2.printStackTrace();

            return;
        }

        Document serverXMLConfig = null;
        String appPath = null;
        
        // this path is used to find important configuration files, eg. system.xml and KETLServers.xml; It must be initialized first.
        for (int index = 0; index < args.length; index++) {
            if (args[index].indexOf("APP_PATH=") != -1) {
                appPath = extractArguments(args[index], "APP_PATH=");
                ResourcePool.LogMessage("Using KETL Path = " + appPath);
            }
        }
        for (int index = 0; index < args.length; index++) {
            // load the KETLServers.xml (configurations for metadata database servers) - full path needed in filename
            if (args[index].indexOf("CONFIG=") != -1) {
                String filename = extractArguments(args[index], "CONFIG=");
                ResourcePool.LogMessage("Using config file " + filename + " to start server");
                serverXMLConfig = Metadata.LoadConfigFile(appPath, filename);
            }

            if ((mdUser == null) && (args[index].indexOf("MD_USER=(") != -1)) {
                mdUser = extractMultipleArguments(args[index], "MD_USER=(");
            }

            if ((mdServer == null) && (args[index].indexOf("SERVERNAME=") != -1)) {
                mdServer = extractArguments(args[index], "SERVERNAME=");
            }
        }

        if ((mdUser == null) & (mdServer == null) & (serverXMLConfig == null)) {
            ResourcePool.LogMessage("Using default config file to start server");
            serverXMLConfig = Metadata.LoadConfigFile(appPath, Metadata.CONFIG_FILE);
        }

        System.out.println(displayVersionInfo());

        String passphrase = null;
        if (serverXMLConfig != null) {
            Node serverNode = null;

            if (mdServer == null) {
                // try for localhost
                Node n = XMLHelper.findElementByName(serverXMLConfig, "SERVERS", null, null);
                if (n == null) {
                    ResourcePool.LogMessage("KETLServers.xml is missing the root node SERVERS, please review file");
                    System.exit(-1);
                }
                mdServer = XMLHelper.getAttributeAsString(n.getAttributes(), "DEFAULTSERVER", "localhost");
                serverNode = XMLHelper.findElementByName(serverXMLConfig, "SERVER", "NAME", mdServer);

                if (serverNode == null) {
                    mdServer = "LOCALHOST";
                    serverNode = XMLHelper.findElementByName(serverXMLConfig, "SERVER", "NAME", mdServer);
                }

                // look up via this machine's host name if no default server or "localhost" found
                if (serverNode == null) {
                    InetAddress thisIp;
                    try {
                        thisIp = InetAddress.getLocalHost();
                        mdServer = thisIp.getHostName();
                    } catch (UnknownHostException e) {
                        ResourcePool.LogMessage(Thread.currentThread().getName(), ResourcePool.ERROR_MESSAGE,
                                "Problems getting localhost name " + e.getMessage());
                        System.exit(com.kni.etl.EngineConstants.SERVER_NAME_ERROR_EXIT_CODE);
                    }
                }
            }

            // try for explicit name
            if (serverNode == null) {
                serverNode = XMLHelper.findElementByName(serverXMLConfig, "SERVER", "NAME", mdServer);
            }

            // At this point, if we do NOT have a serverNode then exit.
            if (serverNode == null) {
                ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.ERROR_MESSAGE,
                        "Problems getting server name, check config file");
                System.exit(com.kni.etl.EngineConstants.SERVER_NAME_ERROR_EXIT_CODE);
            }
            ResourcePool.LogMessage("Using metadata server: " + mdServer);

            mdUser = new String[5];

            mdUser[0] = XMLHelper.getChildNodeValueAsString(serverNode, "USERNAME", null, null, null);
            mdUser[1] = XMLHelper.getChildNodeValueAsString(serverNode, "PASSWORD", null, null, null);
            mdUser[2] = XMLHelper.getChildNodeValueAsString(serverNode, "URL", null, null, null);
            mdUser[3] = XMLHelper.getChildNodeValueAsString(serverNode, "DRIVER", null, null, null);
            mdUser[4] = XMLHelper.getChildNodeValueAsString(serverNode, "MDPREFIX", null, null, "");
            passphrase = XMLHelper.getChildNodeValueAsString(serverNode, "PASSPHRASE", null, null, null);

        }

        if (mdUser == null) {
            ResourcePool
                    .LogMessage("Wrong arguments:  [CONFIG=KETLServers.xml|MD_USER=(USER,PWD,JDBCURL,JDBCDriver,MDPrefix)] <SERVERNAME=KNI01>");
            ResourcePool
                    .LogMessage("example:  MD_USER=(ETLUSER,ETLPWD,jdbc:oracle:oci8:@DEV3ORA,oracle.jdbc.driver.OracleDriver,QA) SERVERNAME=KNI01");

            System.exit(com.kni.etl.EngineConstants.WRONG_ARGUMENT_EXIT_CODE);
        }

        MetadataScheduler md = null;

        String mdPrefix = null;

        if ((mdUser != null) && (mdUser.length == 5)) {
            mdPrefix = mdUser[4];
        }

        try {
            // use metadata repository now
            md = new MetadataScheduler(true, passphrase);

            md.setRepository(mdUser[0], mdUser[1], mdUser[2], mdUser[3], mdPrefix);
            // Place the repository in the resource pool for other threads to share...
            ResourcePool.setMetadata(md);

        } catch (Exception e1) {
            ResourcePool.LogMessage(Thread.currentThread().getName(), ResourcePool.ERROR_MESSAGE,
                    "Connecting to metadata - " + e1.getMessage());
            System.exit(com.kni.etl.EngineConstants.METADATA_ERROR_EXIT_CODE);
        }

        if (mdServer == null) {
            try {
                InetAddress thisIp = InetAddress.getLocalHost();
                mdServer = thisIp.getHostName();
            } catch (UnknownHostException e) {
                ResourcePool.LogMessage(Thread.currentThread().getName(), ResourcePool.ERROR_MESSAGE,
                        "Could not get system hostname please supply servername");

                return;
            }
        }

        int serverID;

        try {
            serverID = md.registerServer(mdServer);

            Object[][] serverExecutors = md.getServerExecutors(serverID);

            if (serverExecutors == null) {
                ResourcePool.LogMessage(Thread.currentThread().getName(), ResourcePool.ERROR_MESSAGE,
                        "Server has no job executors specified");
                shutdown = true;
            }
            else {
                jobManagers = new Object[serverExecutors.length][2];

                for (int i = 0; i < serverExecutors.length; i++) {
                    // get server executor
                    jobManagers[i][0] = new ETLJobManager((String) serverExecutors[i][0],
                            ((Integer) serverExecutors[i][1]).intValue(), ((Integer) serverExecutors[i][2]).intValue());

                    // get arraylist of associated job types
                    jobManagers[i][1] = md.getServerExecutorJobTypes((String) serverExecutors[i][0]);
                }
            }
        } catch (Exception e) {
            ResourcePool.LogException(e, null);

            return;
        }

        boolean completeShutdown = false;

        long mdCheckStatusTimer = System.currentTimeMillis();

        while (completeShutdown == false) {
            // listCurrentThreads();
            try {
                ETLJob job = null;

                ETLJob[] submittedJobsToCheck = new ETLJob[submittedJobs.size()];
                submittedJobs.toArray(submittedJobsToCheck);

                for (int pos = 0; pos < submittedJobsToCheck.length; pos++) {
                    switch (((ETLJob) submittedJobsToCheck[pos]).getStatus().getStatusCode()) {
                    case ETLJobStatus.PENDING_CLOSURE_FAILED:

                        ((ETLJob) submittedJobsToCheck[pos]).writeLog();
                        // set job for retry if retry limit not reached
                        if (((ETLJob) submittedJobsToCheck[pos]).getMaxRetries() > ((ETLJob) submittedJobsToCheck[pos])
                                .getRetryAttempts()) {
                            ((ETLJob) submittedJobsToCheck[pos]).getStatus().setStatusCode(
                                    ETLJobStatus.WAITING_TO_BE_RETRIED);
                            md.setJobStatus((ETLJob) submittedJobsToCheck[pos]);
                        }
                        else {
                            md.setJobStatus((ETLJob) submittedJobsToCheck[pos]);
                        }

                        submittedJobs.remove(submittedJobsToCheck[pos]);
                        ((ETLJob) submittedJobsToCheck[pos]).cleanup();

                        break;

                    case ETLJobStatus.PENDING_CLOSURE_CANCELLED:
                    case ETLJobStatus.PENDING_CLOSURE_SKIP:
                    case ETLJobStatus.PENDING_CLOSURE_SUCCESSFUL:
                        ((ETLJob) submittedJobsToCheck[pos]).writeLog();

                        md.setJobStatus((ETLJob) submittedJobsToCheck[pos]);
                        submittedJobs.remove(submittedJobsToCheck[pos]);
                        ((ETLJob) submittedJobsToCheck[pos]).cleanup();

                        break;

                    case ETLJobStatus.CRITICAL_FAILURE_PAUSE_LOAD:
                        ((ETLJob) submittedJobsToCheck[pos]).writeLog();

                        ((ETLJob) submittedJobsToCheck[pos]).getStatus().setStatusCode(
                                ETLJobStatus.PENDING_CLOSURE_FAILED);
                        md.setJobStatus((ETLJob) submittedJobsToCheck[pos]);
                        submittedJobs.remove(submittedJobsToCheck[pos]);
                        ((ETLJob) submittedJobsToCheck[pos]).cleanup();
                        md.pauseServer(mdServer, true);
                        paused = true;

                        break;

                    case ETLJobStatus.REJECTED:
                        ((ETLJob) submittedJobsToCheck[pos]).getStatus().setStatusCode(ETLJobStatus.READY_TO_RUN);
                        job = (ETLJob) submittedJobsToCheck[pos];

                        break;

                    case ETLJobStatus.EXECUTING:
                        // get the job status from the metadata, not the executors its says executing
                        // if current time is 1 second greater than last check then check metadata
                        // this prevents metadata from being thrashed
                        long currentTime = System.currentTimeMillis();
                        if (currentTime > (mdCheckStatusTimer + 1000)) {
                            mdCheckStatusTimer = currentTime;
                            int xStatus = md.getJobStatusByExecutionId((submittedJobsToCheck[pos]).getJobExecutionID());
                            if (xStatus == ETLJobStatus.ATTEMPT_CANCEL) {
                                (submittedJobsToCheck[pos]).getStatus().setStatusCode(ETLJobStatus.ATTEMPT_CANCEL);
                                if ((submittedJobsToCheck[pos]).isCancelled() == false)
                                    (submittedJobsToCheck[pos]).cancelJob();
                                submittedJobsToCheck[pos].getStatus().messageChanged = true;
                            }
                        }
                        // now fall through to the attempt to cancel
                        // DO NOT ADD 'BREAK'!!!!!
                    case ETLJobStatus.ATTEMPT_CANCEL:

                        // if message changed refresh status
                        if ((submittedJobsToCheck[pos]).getStatus().messageChanged) {
                            (submittedJobsToCheck[pos]).getStatus().messageChanged = false;

                            md.setJobStatus(submittedJobsToCheck[pos]);
                        }

                        break;

                    case ETLJobStatus.QUEUED_FOR_EXECUTION:
                        break;

                    default:
                        ResourcePool.LogMessage(Thread.currentThread().getName(), ResourcePool.ERROR_MESSAGE,
                                "Job in unmanaged status please contact support: Status ID = "
                                        + submittedJobsToCheck[pos].getStatus().getStatusCode() + ", Message = "
                                        + ((ETLJob) submittedJobsToCheck[pos]).getStatus().getStatusMessage());

                        break;
                    }
                }

                if (shutdown == false) {
                    paused = false;

                    switch (md.shutdownServer(serverID)) {
                    case com.kni.etl.ETLServerStatus.PAUSED:
                        paused = true;

                        break;

                    case com.kni.etl.ETLServerStatus.SERVER_ALIVE:
                        shutdown = false;

                        break;

                    case com.kni.etl.ETLServerStatus.SERVER_SHUTDOWN:
                        shutdown = true;

                        break;

                    case com.kni.etl.ETLServerStatus.SERVER_SHUTTING_DOWN:
                        shutdown = true;

                        break;

                    case com.kni.etl.ETLServerStatus.SERVER_KILLED:
                        ResourcePool
                                .LogMessage("WARNING: Server shutdown down immediately, jobs will not be shutdown gracefully! Exceptions will be shown");

                        // find a job types that can be serviced
                        for (int pos = 0; pos < jobManagers.length; pos++) {
                            ((ETLJobManager) jobManagers[pos][0]).kill();
                        }

                        ResourcePool.LogMessage("Shutdown complete");
                        md.closeMetadata();
                        System.exit(0);

                        break;
                    }

                    if (shutdown == true) {
                        ResourcePool.LogMessage(Thread.currentThread().getName(), ResourcePool.INFO_MESSAGE,
                                "Waiting for any executing jobs to end before shutdown.");
                    }
                }

                // only mark all jobs as finished if shutdown is requested
                if ((submittedJobsToCheck.length == 0) && (shutdown == true)) {
                    completeShutdown = true;
                }

                if ((shutdown == false) && (paused == false)) {
                    // int managerToUse = -1;
                    ArrayList jobTypesToRequest = new ArrayList();

                    // find a job types that can be serviced
                    for (int pos = 0; pos < jobManagers.length; pos++) {
                        if (((ETLJobManager) jobManagers[pos][0]).getStatus().getStatusCode() == ETLJobManagerStatus.READY) {
                            jobTypesToRequest.addAll((ArrayList) jobManagers[pos][1]);
                        }
                    }

                    if (job == null) {
                        job = md.getNextJobInQueue(jobTypesToRequest, serverID);
                    }

                    // if job not null give to executor
                    if (job != null) {
                        if (job.getClass().getName().compareTo(baseJob.getClass().getName()) == 0) {
                            // if job is a default pass through job set status to successfull, do not submit to
                            // executors and let it get picked up.
                            job.getStatus().setStatusCode(ETLJobStatus.PENDING_CLOSURE_SUCCESSFUL);
                            submittedJobs.add(job);
                        }
                        else {
                            // if non default job pass through to executors if executors accepting
                            job.getStatus().setStatusCode(ETLJobStatus.READY_TO_RUN);

                            for (int pos = 0; pos < jobManagers.length; pos++) {
                                if (((ETLJobManager) jobManagers[pos][0]).getStatus().getStatusCode() == ETLJobManagerStatus.READY) {
                                    if (((ETLJobManager) jobManagers[pos][0]).submitJob(job) == true) {
                                        pos = jobManagers.length;
                                        submittedJobs.add(job);
                                    }
                                    else {
                                        if (job.getStatus().getStatusCode() == ETLJobStatus.REJECTED) {
                                            job.getStatus().setStatusCode(ETLJobStatus.READY_TO_RUN);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                // ResourcePool release timed out connections
                ResourcePool.releaseTimedOutConnections();

                try {
                    // if job is null wait five seconds before executing checking for next job else wait 0.1 of a second
                    // don't want to hammer database night and day
                    if (job == null) {
                        if (shutdown) {
                            SleepTime = 100;
                        }
                        else if (SleepTime < 5000) {
                            SleepTime = SleepTime * 2;
                        }

                        Thread.sleep(SleepTime);
                    }
                    else {
                        if (SleepTime > 0) {
                            // quickly drop rate
                            if (SleepTime > 2000) {
                                SleepTime = 2000;
                            }
                            else if (SleepTime < 10) {
                                SleepTime = 10;
                            }
                            else {
                                SleepTime = SleepTime / 2;
                            }
                        }

                        Thread.sleep(SleepTime);
                    }
                } catch (InterruptedException e) {
                }
            } catch (SQLException e) {
                ResourcePool.LogException(e, null);
                md.closeMetadata();

                // int u = 1;
            } catch (Exception e) {
                ResourcePool.LogException(e, null);
            }
        }

        if (jobManagers != null) {
            // shutdown job managers
            for (int pos = 0; pos < jobManagers.length; pos++) {
                ((ETLJobManager) jobManagers[pos][0]).shutdown();
            }
        }

        ResourcePool.LogMessage("Flushing caches to disk");
        ResourcePool.releaseAllLookups();
        ResourcePool.LogMessage("Shutdown complete");

        md.closeMetadata();
    }

    /**
     * Insert the method's description here. Creation date: (6/3/2002 8:18:30 PM)
     */
    public final void shutdown() {
        shutdown = true;
    }

}
