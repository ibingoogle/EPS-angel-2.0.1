package com.tencent.angel.psagent;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TaskIterReturnData {
    private static final Log LOG = LogFactory.getLog(TaskIterReturnData.class);
    private int trainDataStatus = 0;
    private boolean rmServerStatus = false;
    private int rmServerIndex = -1;

    public TaskIterReturnData() {
    }

    public TaskIterReturnData(int tDStatus, boolean rSStatus, int rSIndex) {
        trainDataStatus = tDStatus;
        rmServerStatus = rSStatus;
        rmServerIndex = rSIndex;
    }

    public void setTrainDataStatus(int status) {
        trainDataStatus = status;
    }

    public void setRmServerStatus(boolean status) {
        rmServerStatus = status;
    }

    public void setRmServerIndex(int index) {
        rmServerIndex = index;
    }

    public int getTrainDataStatus() {
        return trainDataStatus;
    }

    public boolean getRmServerStatus() {
        return rmServerStatus;
    }

    public int getRmServerIndex() {
        return rmServerIndex;
    }

    public void print_TaskIterReturnData(){
        LOG.info("trainDataStatus = " + trainDataStatus + ", rmServerStatus = " + rmServerStatus + ", rmServerIndex = " + rmServerIndex);
    }
}
