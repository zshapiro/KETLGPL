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
package com.kni.etl.scheduler;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import com.kni.etl.ETLJob;
import com.kni.etl.ETLJobStatus;
import com.kni.etl.Metadata;
import com.kni.etl.dbutils.ResourcePool;

public class MetadataScheduler extends Metadata {

    public MetadataScheduler(boolean pEnableEncryption, String pPassphrase) throws Exception {
        super(pEnableEncryption, pPassphrase);
    }

    public MetadataScheduler(boolean pEnableEncryption) throws Exception {
        super(pEnableEncryption);
    }

    /**
     * Insert the method's description here. Creation date: (3/5/2002 3:18:54 PM)
     * 
     * @return jobscheduler.EtlJob
     */
    public ETLJob getNextJobInQueue(ArrayList pJobTypes, int pServerID) throws SQLException, java.lang.Exception {
        PreparedStatement clearJobLogStmt = null;
        PreparedStatement clearJobErrorStmt = null;
        PreparedStatement insJobLogHistStmt = null;
        PreparedStatement insJobErrorHistStmt = null;
        PreparedStatement getfinishedLoads = null;
        PreparedStatement setLoadEndDate = null;
        ResultSet rs = null;
        ResultSet m_rs = null;

        synchronized (this.oLock) {
            ETLJobStatus etlJobStatus = new ETLJobStatus();

            // Make metadata connection alive.
            refreshMetadataConnection();

            // clear job_log of all finished loads and move them to job_log_hist
            getfinishedLoads = this.metadataConnection.prepareStatement("SELECT LOAD_ID FROM  " + tablePrefix
                    + loadTableName() + " WHERE (START_JOB_ID,LOAD_ID) IN (SELECT JOB_ID,LOAD_ID FROM  " + tablePrefix
                    + "JOB_LOG WHERE STATUS_ID IN (?,?,?,?))");

            getfinishedLoads.setInt(1, ETLJobStatus.FAILED);
            getfinishedLoads.setInt(2, ETLJobStatus.CANCELLED);
            getfinishedLoads.setInt(3, ETLJobStatus.SUCCESSFUL);
            getfinishedLoads.setInt(4, ETLJobStatus.SKIPPED);

            m_rs = getfinishedLoads.executeQuery();

            // Get jobs if any
            while (m_rs.next()) {
                // update end_date of load
                if (setLoadEndDate == null) {
                    setLoadEndDate = this.metadataConnection.prepareStatement(" UPDATE  " + tablePrefix
                            + loadTableName() + " SET END_DATE = " + currentTimeStampSyntax + " WHERE LOAD_ID = ?");
                }

                ResourcePool.releaseLoadLookups(m_rs.getInt(1));

                setLoadEndDate.setInt(1, m_rs.getInt(1));
                setLoadEndDate.executeUpdate();

                // copy errors to job_error_hist
                if (insJobErrorHistStmt == null) {
                    insJobErrorHistStmt = this.metadataConnection
                            .prepareStatement("insert into  "
                                    + tablePrefix
                                    + "job_error_hist(dm_load_id,job_id,message,code,error_datetime) select dm_load_id,job_id,message,code,error_datetime from  "
                                    + tablePrefix + "job_error where dm_load_id in (select dm_load_id from  "
                                    + tablePrefix + "job_log where load_id = ?)");
                }

                insJobErrorHistStmt.setInt(1, m_rs.getInt(1));
                insJobErrorHistStmt.executeUpdate();

                // clear job_error for load
                if (clearJobErrorStmt == null) {
                    clearJobErrorStmt = this.metadataConnection.prepareStatement("delete from  " + tablePrefix
                            + "job_error where dm_load_id in (select dm_load_id from  " + tablePrefix
                            + "job_log where load_id = ?)");
                }

                clearJobErrorStmt.setInt(1, m_rs.getInt(1));
                clearJobErrorStmt.executeUpdate();

                // test if this column exist
                String JOB_LOG_LAST_UPDATE_COL = "";
                if (columnExists("JOB_LOG", "LAST_UPDATE_DATE"))
                    JOB_LOG_LAST_UPDATE_COL = ", LAST_UPDATE_DATE";
                String JOB_LOG_HIST_LAST_UPDATE_COL = "";
                if (columnExists("JOB_LOG_HIST", "LAST_UPDATE_DATE"))
                    JOB_LOG_HIST_LAST_UPDATE_COL = ", LAST_UPDATE_DATE";

                // copy job_log details to job_log_hist
                if (insJobLogHistStmt == null) {
                    insJobLogHistStmt = this.metadataConnection
                            .prepareStatement("insert into  "
                                    + tablePrefix
                                    + "job_log_hist(job_id,load_id,start_date,status_id,end_date,message,dm_load_id,retry_attempts,execution_date,server_id"
                                    + JOB_LOG_LAST_UPDATE_COL
                                    + ") "
                                    + "select job_id,load_id,start_date,status_id,end_date,message,dm_load_id,retry_attempts,execution_date,server_id"
                                    + JOB_LOG_HIST_LAST_UPDATE_COL + " from  " + tablePrefix
                                    + "job_log where load_id = ?");
                }

                insJobLogHistStmt.setInt(1, m_rs.getInt(1));
                insJobLogHistStmt.executeUpdate();

                // delete from job_log
                if (clearJobLogStmt == null) {
                    clearJobLogStmt = this.metadataConnection.prepareStatement("delete from  " + tablePrefix
                            + "job_log where load_id = ?");
                }

                clearJobLogStmt.setInt(1, m_rs.getInt(1));
                clearJobLogStmt.executeUpdate();
            }

            if (m_rs != null) {
                m_rs.close();
            }

            if (setLoadEndDate != null) {
                setLoadEndDate.close();
            }

            if (insJobErrorHistStmt != null) {
                insJobErrorHistStmt.close();
            }

            if (clearJobErrorStmt != null) {
                clearJobErrorStmt.close();
            }

            if (insJobLogHistStmt != null) {
                insJobLogHistStmt.close();
            }

            if (clearJobLogStmt != null) {
                clearJobLogStmt.close();
            }

            if (getfinishedLoads != null) {
                getfinishedLoads.close();
            }

            metadataConnection.commit();

            // Close open resources
            if (m_rs != null) {
                m_rs.close();
            }

            if (rs != null) {
                rs.close();
            }

            // Check scheduler to see if any jobs due to run
            // if so add jobs to job_log with waiting to be executed flag
            // SCHEDULE UNIT ...
            // MONTH e.g Type Month unit is 2 = every two months
            // MONTH_OF_YEAR e.g Type Month unit is 2 = every february
            // DAY e.g Type Day is 2 = every 2 days
            // DAY_OF_WEEK e.g Type Day of week is 1 = every monday
            // DAY_OF_MONTH e.g Type Day of month is 1 = every 1st of month
            // HOUR_OF_DAY e.g Type Hour of day is 18 = every 6pm
            // HOUR e.g Type Hour is 2 = every 2 hours
            //
            // MONTH = 1, DAY_OF_MONTH = 3 = 3rd of every month
            // MONTH = 1, DAY_OF_MONTH = 3, HOUR = 4 = 3rd of every month, every
            // 4 hours
            // MONTH = 1, DAY_OF_MONTH = 3, HOUR_OF_DAY = 4 = 3rd of every
            // month, at 4am
            // DAY_OF_MONTH = 3, HOUR = 4 = Every 3 days, every 4 hours
            // HOUR_OF_DAY = 6 = 6am everyday
            //
            // LIFESPAN ...
            // SCHEDULE START DATE
            // SCHEDULE END DATE
            // SELECT FOR UPDATE AND HOLD EVERYTHING ELSE
            PreparedStatement dueJobs = this.metadataConnection
                    .prepareStatement("SELECT A.JOB_ID, MONTH, MONTH_OF_YEAR, DAY, DAY_OF_WEEK, DAY_OF_MONTH, HOUR_OF_DAY,HOUR, NEXT_RUN_DATE, SCHEDULE_ID, PROJECT_ID, MINUTE, MINUTE_OF_HOUR FROM  "
                            + tablePrefix
                            + "JOB_SCHEDULE A, "
                            + tablePrefix
                            + "JOB B WHERE "
                            + currentTimeStampSyntax
                            + " >= NEXT_RUN_DATE AND A.JOB_ID = B.JOB_ID FOR UPDATE ");

            m_rs = dueJobs.executeQuery();

            String jobID = null;
            int month;
            int month_of_year;
            int day;
            int day_of_week;
            int day_of_month;
            int hour_of_day;
            int hour;
            int scheduleID;
            int projectID;
            int minute;
            int minute_of_hour;

            java.util.Date nextRunDate;
            java.util.Date lastRunDate;

            PreparedStatement updJobSched = null;

            // cycle through pending jobs setting next run date
            while (m_rs.next()) {
                jobID = m_rs.getString(1);
                month = m_rs.getInt(2);

                if (m_rs.wasNull()) {
                    month = -1;
                }

                month_of_year = m_rs.getInt(3);

                if (m_rs.wasNull()) {
                    month_of_year = -1;
                }

                day = m_rs.getInt(4);

                if (m_rs.wasNull()) {
                    day = -1;
                }

                day_of_week = m_rs.getInt(5);

                if (m_rs.wasNull()) {
                    day_of_week = -1;
                }

                day_of_month = m_rs.getInt(6);

                if (m_rs.wasNull()) {
                    day_of_month = -1;
                }

                hour_of_day = m_rs.getInt(7);

                if (m_rs.wasNull()) {
                    hour_of_day = -1;
                }

                hour = m_rs.getInt(8);

                if (m_rs.wasNull()) {
                    hour = -1;
                }

                scheduleID = m_rs.getInt(10);
                projectID = m_rs.getInt(11);
                minute = m_rs.getInt(12);

                if (m_rs.wasNull()) {
                    minute = -1;
                }

                minute_of_hour = m_rs.getInt(13);

                if (m_rs.wasNull()) {
                    minute_of_hour = -1;
                }

                // get as if timestamp so to not to loose time out of date
                lastRunDate = m_rs.getTimestamp(9);

                // Calculate next run date
                nextRunDate = getNextDate(lastRunDate, month, month_of_year, day, day_of_week, day_of_month, hour,
                        hour_of_day, minute, minute_of_hour);

                if (updJobSched == null) {
                    updJobSched = this.metadataConnection.prepareStatement(" UPDATE  " + tablePrefix
                            + "JOB_SCHEDULE SET NEXT_RUN_DATE = ? WHERE SCHEDULE_ID = ? AND JOB_ID = ?");
                }

                // if last run date is the same as the nextRunDate or the next
                // run date is null then set the next run
                // date to null
                if (nextRunDate == null || nextRunDate.equals(lastRunDate)) {
                    ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Disabling schedule [" + scheduleID
                            + "] next run date is null or the same as the previous date");

                    updJobSched.setNull(1, java.sql.Types.TIMESTAMP);
                }
                else
                    updJobSched.setTimestamp(1, new java.sql.Timestamp(nextRunDate.getTime()));

                updJobSched.setInt(2, scheduleID);
                updJobSched.setString(3, jobID);
                updJobSched.executeUpdate();

                executeJob(projectID, jobID, false, false);
            }

            // Close open resources
            if (m_rs != null) {
                m_rs.close();
            }

            if (updJobSched != null) {
                updJobSched.close();
            }

            if (dueJobs != null) {
                dueJobs.close();
            }

            // find jobs that have finished and set parent jobs to be waiting
            // for execution
            // *************** Need to improve this as the associated update is
            // slow ********/
            Calendar cal = Calendar.getInstance(TimeZone.getDefault());
            long ms = 0;
            Date tStop = null;
            Date tStart = cal.getTime();

            // mark jobs that have finished with appropiate status
            // -- actually, this block is only getting the next jobs ready to run (process WAITING statuses)
            // 20061010 daonguyen - incorporate new statuses
            // NOTE: PENDING_CLOSURE_CANCELLED stops the workflow so no update here
            PreparedStatement mReadyList = this.metadataConnection
                    .prepareStatement("SELECT load_id, job_id, status_id FROM " + tablePrefix
                            + "job_log WHERE STATUS_ID in (?, ?, ?) AND (load_id,job_id) IN "
                            + " (SELECT   load_id,parent_job_id FROM  " + tablePrefix + "job_log a,  " + tablePrefix
                            + "job_dependencie b " + " WHERE a.job_id = b.job_id GROUP BY load_id,parent_job_id "
                            + " HAVING MAX (CASE WHEN continue_if_failed = 'Y' "
                            + "   THEN (CASE status_id WHEN ? THEN 0 WHEN ? THEN 0 WHEN ? THEN 0 ELSE 1 END) "
                            + "   ELSE (CASE status_id WHEN ? THEN  0 WHEN ? THEN 0 ELSE 1 END)"
                            + " END) = 0) FOR UPDATE");
            mReadyList.setInt(1, ETLJobStatus.WAITING_FOR_CHILDREN);
            mReadyList.setInt(2, ETLJobStatus.WAITING_TO_SKIP);
            mReadyList.setInt(3, ETLJobStatus.WAITING_TO_PAUSE);
            mReadyList.setInt(4, (ETLJobStatus.PENDING_CLOSURE_SUCCESSFUL));
            mReadyList.setInt(5, (ETLJobStatus.PENDING_CLOSURE_FAILED));
            mReadyList.setInt(6, (ETLJobStatus.PENDING_CLOSURE_SKIP));
            mReadyList.setInt(7, (ETLJobStatus.PENDING_CLOSURE_SUCCESSFUL));
            mReadyList.setInt(8, (ETLJobStatus.PENDING_CLOSURE_SKIP));

            ResultSet rsReadyJobs = mReadyList.executeQuery();

            PreparedStatement updJobs = this.metadataConnection.prepareStatement(" UPDATE  " + tablePrefix
                    + "job_log SET status_id = ?, message = ? where job_id = ? and load_id = ?");

            while (rsReadyJobs.next()) {
                int wait_status = rsReadyJobs.getInt("status_id");
                switch (wait_status) {
                case ETLJobStatus.WAITING_FOR_CHILDREN:
                    updJobs.setInt(1, ETLJobStatus.READY_TO_RUN);
                    updJobs.setString(2, etlJobStatus.getStatusMessageForCode(ETLJobStatus.READY_TO_RUN));
                    break;
                case ETLJobStatus.WAITING_TO_SKIP:
                    updJobs.setInt(1, ETLJobStatus.PENDING_CLOSURE_SKIP);
                    updJobs.setString(2, etlJobStatus.getStatusMessageForCode(ETLJobStatus.PENDING_CLOSURE_SKIP));
                    break;
                case ETLJobStatus.WAITING_TO_PAUSE:
                    updJobs.setInt(1, ETLJobStatus.PAUSED);
                    updJobs.setString(2, etlJobStatus.getStatusMessageForCode(ETLJobStatus.PAUSED));
                    break;
                }
                updJobs.setString(3, rsReadyJobs.getString("job_id"));
                updJobs.setInt(4, rsReadyJobs.getInt("load_id"));
                updJobs.addBatch();
            }
            updJobs.executeBatch();

            tStop = cal.getTime();
            ms = tStop.getTime() - tStart.getTime();
            // System.out.println("-----");
            // System.out.println("WAITING: batch updated " + rsReadyJobs.getRow() + " rows in " + ms + "
            // milliseconds.");

            if (mReadyList != null)
                mReadyList.close();
            if (rsReadyJobs != null)
                rsReadyJobs.close();
            if (updJobs != null)
                updJobs.close();

            // mark jobs set for retry to be retried if time has passed
            tStart = cal.getTime();
            PreparedStatement mRetryList = this.metadataConnection
                    .prepareStatement("select a.job_id, "
                            + " case when (coalesce(a.retry_attempts,0)) < b.retry_attempts then ?  else  ?  end as status_id, "
                            + " case when (coalesce(a.retry_attempts,0)) < b.retry_attempts then ? else ? end as message, "
                            + " case when (coalesce(a.retry_attempts,0)) < b.retry_attempts then (coalesce(a.retry_attempts,0))+1 else a.retry_attempts end as retry_attempts "
                            + " from " + tablePrefix + "job_log a, " + tablePrefix + "job b "
                            + " where a.job_id = b.job_id " + " and status_id = ? " + " and "
                            + this.currentTimeStampSyntax
                            + " > a.end_date + ((interval '1' second) * seconds_before_retry)");

            mRetryList.setInt(1, ETLJobStatus.READY_TO_RUN);
            mRetryList.setInt(2, ETLJobStatus.PENDING_CLOSURE_FAILED);
            mRetryList.setString(3, etlJobStatus.getStatusMessageForCode(ETLJobStatus.READY_TO_RUN));
            mRetryList.setString(4, etlJobStatus.getStatusMessageForCode(ETLJobStatus.PENDING_CLOSURE_FAILED));
            mRetryList.setInt(5, ETLJobStatus.WAITING_TO_BE_RETRIED);

            ResultSet m_rsJobsToRetry = mRetryList.executeQuery();

            PreparedStatement updRetryJobs = this.metadataConnection.prepareStatement(" UPDATE  " + tablePrefix
                    + "job_log " + " SET status_id = ?, message = ?,retry_attempts = ? where job_id = ?");

            while (m_rsJobsToRetry.next()) {
                updRetryJobs.setInt(1, m_rsJobsToRetry.getInt(2));
                updRetryJobs.setString(2, m_rsJobsToRetry.getString(3));
                updRetryJobs.setInt(3, m_rsJobsToRetry.getInt(4));
                updRetryJobs.setString(4, m_rsJobsToRetry.getString(1));

                updRetryJobs.addBatch();
            }

            updRetryJobs.executeBatch();

            tStop = cal.getTime();
            ms = tStop.getTime() - tStart.getTime();
            // System.out.println("Retry: batch updated " + m_rsJobsToRetry.getRow() + " rows in " + ms + "
            // milliseconds.");

            if (mRetryList != null) {
                mRetryList.close();
            }

            // Close open resources
            if (m_rsJobsToRetry != null) {
                m_rsJobsToRetry.close();
            }

            // find jobs that have finished (process PENDING_CLOSURE statuses)
            // 20061010 daonguyen - update code for PENDING_CLOSURE_SKIP and PENDING_CLOSURE_CANCELLED
            tStart = cal.getTime();
            PreparedStatement selFinishedJobs = this.metadataConnection
                    .prepareStatement(" SELECT STATUS_ID, DM_LOAD_ID, LOAD_ID, JOB_ID FROM  " + tablePrefix
                            + "JOB_LOG WHERE STATUS_ID IN (?,?,?, ?) FOR UPDATE");
            selFinishedJobs.setInt(1, ETLJobStatus.PENDING_CLOSURE_SUCCESSFUL);
            selFinishedJobs.setInt(2, ETLJobStatus.PENDING_CLOSURE_FAILED);
            selFinishedJobs.setInt(3, ETLJobStatus.PENDING_CLOSURE_SKIP);
            selFinishedJobs.setInt(4, ETLJobStatus.PENDING_CLOSURE_CANCELLED);
            m_rs = selFinishedJobs.executeQuery();

            PreparedStatement pStmt = null;
            PreparedStatement pCancelStmt = null;

            while (m_rs.next()) {
                int statusID = m_rs.getInt(1);
                int dmLoadID = m_rs.getInt(2);
                int LoadID = m_rs.getInt(3);
                String cJobID = m_rs.getString(4);

                switch (statusID) {
                // -- now update finished jobs
                // -- only if all parent jobs have been processed (are not in WAITING state)
                case ETLJobStatus.PENDING_CLOSURE_SUCCESSFUL:
                case ETLJobStatus.PENDING_CLOSURE_FAILED:
                case ETLJobStatus.PENDING_CLOSURE_SKIP:
                    if (pStmt == null) {
                        pStmt = metadataConnection
                                .prepareStatement("UPDATE  "
                                        + tablePrefix
                                        + "JOB_LOG "
                                        + " SET STATUS_ID = (CASE STATUS_ID WHEN ? THEN ? WHEN ? THEN ? WHEN ? THEN ? END), "
                                        + "     MESSAGE = (CASE STATUS_ID WHEN ? THEN ? WHEN ? THEN ? WHEN ? THEN ? END) "
                                        + " WHERE DM_LOAD_ID = ? AND NOT EXISTS (SELECT 1 FROM  "
                                        + tablePrefix
                                        + "JOB_LOG B,  "
                                        + tablePrefix
                                        + "JOB_DEPENDENCIE C  "
                                        + " WHERE B.JOB_ID = C.PARENT_JOB_ID AND B.LOAD_ID = ? AND JOB_LOG.JOB_ID = C.JOB_ID AND B.STATUS_ID in (?, ?, ?))");
                    }
                    pStmt.setInt(1, (ETLJobStatus.PENDING_CLOSURE_FAILED));
                    pStmt.setInt(2, (ETLJobStatus.FAILED));
                    pStmt.setInt(3, (ETLJobStatus.PENDING_CLOSURE_SUCCESSFUL));
                    pStmt.setInt(4, (ETLJobStatus.SUCCESSFUL));
                    pStmt.setInt(5, (ETLJobStatus.PENDING_CLOSURE_SKIP));
                    pStmt.setInt(6, (ETLJobStatus.SKIPPED));
                    pStmt.setInt(7, (ETLJobStatus.PENDING_CLOSURE_FAILED));
                    pStmt.setString(8, etlJobStatus.getStatusMessageForCode(ETLJobStatus.FAILED));
                    pStmt.setInt(9, (ETLJobStatus.PENDING_CLOSURE_SUCCESSFUL));
                    pStmt.setString(10, etlJobStatus.getStatusMessageForCode(ETLJobStatus.SUCCESSFUL));
                    pStmt.setInt(11, (ETLJobStatus.PENDING_CLOSURE_SKIP));
                    pStmt.setString(12, etlJobStatus.getStatusMessageForCode(ETLJobStatus.SKIPPED));
                    pStmt.setInt(13, dmLoadID);
                    pStmt.setInt(14, LoadID);
                    pStmt.setInt(15, ETLJobStatus.WAITING_FOR_CHILDREN);
                    pStmt.setInt(16, ETLJobStatus.WAITING_TO_SKIP);
                    pStmt.setInt(17, ETLJobStatus.WAITING_TO_PAUSE);
                    pStmt.addBatch();
                    break;

                // -- now update cancelled jobs
                // -- for non-root jobs, ignore depends_on & waits_on
                // -- for root jobs, test that all children have completed/not started (nothing is running)
                case ETLJobStatus.PENDING_CLOSURE_CANCELLED:
                    if (pCancelStmt == null) {
                        pCancelStmt = metadataConnection.prepareStatement("UPDATE  " + tablePrefix
                                + "JOB_LOG SET STATUS_ID = ?, MESSAGE = ? "
                                + " WHERE DM_LOAD_ID = ? AND (EXISTS (SELECT 1 FROM " + tablePrefix
                                + "LOAD WHERE LOAD_ID = ? AND START_JOB_ID <> ?) " + " OR NOT EXISTS (SELECT 1 FROM  "
                                + tablePrefix + "LOAD B, " + tablePrefix + "JOB_LOG C "
                                + " WHERE B.LOAD_ID = ? AND B.START_JOB_ID = ? "
                                + " AND B.START_JOB_ID <> C.JOB_ID AND C.STATUS_ID in (?,?,?,?) ))");
                    }
                    pCancelStmt.setInt(1, (ETLJobStatus.CANCELLED));
                    pCancelStmt.setString(2, etlJobStatus.getStatusMessageForCode(ETLJobStatus.CANCELLED));
                    pCancelStmt.setInt(3, dmLoadID);
                    pCancelStmt.setInt(4, LoadID);
                    pCancelStmt.setString(5, cJobID);
                    pCancelStmt.setInt(6, LoadID);
                    pCancelStmt.setString(7, cJobID);
                    pCancelStmt.setInt(8, (ETLJobStatus.EXECUTING));
                    pCancelStmt.setInt(9, (ETLJobStatus.READY_TO_RUN));
                    pCancelStmt.setInt(10, (ETLJobStatus.ATTEMPT_PAUSE));
                    pCancelStmt.setInt(11, (ETLJobStatus.ATTEMPT_CANCEL));
                    // -- TODO: separate the pause execution and pause load statuses?
                    // we need to make sure a job in execution paused is stopped first/cleanly before cancelling the
                    // load
                    // pStmt.setInt(9, (ETLJobStatus.PAUSED)); //currently, this is a load pause
                    pCancelStmt.addBatch();
                    break;
                }

            }

            if (pStmt != null) {
                pStmt.executeBatch();
                pStmt.close();

                tStop = cal.getTime();
                ms = tStop.getTime() - tStart.getTime();
                // System.out.println("PENDING: batch updated " + m_rs.getRow() + " rows in " + ms + " milliseconds.");
            }

            if (pCancelStmt != null) {
                pCancelStmt.executeBatch();
                pCancelStmt.close();
            }
            // Close open resources
            if (m_rs != null) {
                m_rs.close();
            }

            metadataConnection.commit();

            // -- now process jobs that are ready_to_run --
            boolean ReturnNextJob = true;
            ETLJob job = null;

            if (ReturnNextJob == true) {
                Object[] jobTypes = pJobTypes.toArray();

                // create with job type of 0, empty job
                String jobTypesSQL = null;

                for (int a = 0; a < jobTypes.length; a++) {
                    if (jobTypesSQL == null) {
                        jobTypesSQL = "'" + (String) jobTypes[a] + "'";
                    }
                    else {
                        jobTypesSQL = jobTypesSQL + ",'" + (String) jobTypes[a] + "'";
                    }
                }

                PreparedStatement getNextJob = metadataConnection
                        .prepareStatement(" SELECT A.JOB_ID,A.DM_LOAD_ID,A.LOAD_ID,A.RETRY_ATTEMPTS FROM  "
                                + tablePrefix
                                + "JOB_LOG A,  "
                                + tablePrefix
                                + "JOB B  WHERE A.JOB_ID = B.JOB_ID AND A.STATUS_ID IN (?) AND ( B.JOB_TYPE_ID IN (SELECT JOB_TYPE_ID FROM  "
                                + tablePrefix + "JOB_TYPE WHERE CLASS_NAME IN (" + jobTypesSQL
                                + ") OR B.JOB_TYPE_ID = 0)) ORDER BY START_DATE FOR UPDATE");

                getNextJob.setInt(1, ETLJobStatus.READY_TO_RUN);

                m_rs = getNextJob.executeQuery();

                String jobToHandle = null;

                PreparedStatement updJobLog = null;

                // Get jobs if any -- actually, this is only updating the status for and returning 1 job
                while (m_rs.next() && (job == null)) {
                    if (jobToHandle == null) {
                        jobToHandle = m_rs.getString(1);

                        if (updJobLog == null) {
                            updJobLog = metadataConnection.prepareStatement(" UPDATE  " + tablePrefix
                                    + "JOB_LOG SET STATUS_ID = ?, MESSAGE = ?, EXECUTION_DATE = "
                                    + currentTimeStampSyntax + ", SERVER_ID= ? WHERE DM_LOAD_ID = ?");
                        }

                        updJobLog.setInt(1, ETLJobStatus.EXECUTING);
                        updJobLog.setString(2, etlJobStatus.getStatusMessageForCode(ETLJobStatus.EXECUTING));
                        updJobLog.setInt(3, pServerID);
                        updJobLog.setInt(4, m_rs.getInt(2));

                        updJobLog.executeUpdate();

                        // set job to executing
                        job = getJob(jobToHandle, m_rs.getInt(3), m_rs.getInt(2));
                        job.setRetryAttempts(m_rs.getInt(4));
                    }
                }

                if (getNextJob != null) {
                    getNextJob.close();
                }

                if (updJobLog != null) {
                    updJobLog.close();
                }

                if (m_rs != null) {
                    m_rs.close();
                }
            }

            metadataConnection.commit();

            // Close open resources
            if (m_rs != null) {
                m_rs.close();
            }

            if (updRetryJobs != null) {
                updRetryJobs.close();
            }

            if (updJobs != null) {
                updJobs.close();
            }

            if (selFinishedJobs != null) {
                selFinishedJobs.close();
            }

            if (rs != null) {
                rs.close();
            }

            return job;
        }
    }

}
