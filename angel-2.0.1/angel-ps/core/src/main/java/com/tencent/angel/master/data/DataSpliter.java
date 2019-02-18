/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in 
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */


package com.tencent.angel.master.data;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.master.app.AMContext;
import com.tencent.angel.split.SplitClassification;
import com.tencent.angel.utils.HdfsUtil;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Training date split tool, which splits the training data into blocks of roughly equal size
 * according to the number of workers and tasks
 */
public class DataSpliter {
  private static final Log LOG = LogFactory.getLog(DataSpliter.class);
  protected final AMContext context;

  /**
   * index to SplitClassification map
   */
  private final Map<Integer, SplitClassification> splitClassifications;

  /* new code */
  // workerGroup index to SplitClassifications map
  public Map<Integer, List<SplitClassification>> realSplitClassifications;
  public Map<Integer, List<Boolean>> realSCsStatus;
  public Map<Integer, List<Long>> realSCsLength;
  public Map<Integer, Long> realSCsTotalLength_all;
  public Map<Integer, Long> realSCsTotalLength_active;

  public List<Boolean> activeWGIndex;
  public int totalActiveWGNum = 0;
  public Map<Integer, Integer> activeSCsNum;

  // workergroup Index to trainDataStatus, 1 append, 0 normal, -1 delete
  public Map<Integer, Integer> trainDataStatus;

  public double averageSCsNum = 0.0;


  public Map<Integer, List<SplitClassification>> appendedSCs;
  public SplitClassification extraSplitClassification;

  public void print_all_SCs(){
    LOG.info("");
    LOG.info("");
    LOG.info("total active WG num = " + totalActiveWGNum);
    LOG.info("");
    LOG.info("");
    for (Integer wgIndex: realSplitClassifications.keySet()){
      print_wg_SCs(wgIndex);
    }
    LOG.info("");
    LOG.info("");
    LOG.info("average SCs per worker = " + averageSCsNum);
    LOG.info("");
    LOG.info("");
  }

  public void print_wg_SCs(int wgIndex) {
    LOG.info("SCs info of workergroup index = " + wgIndex);
    LOG.info("wg active status = " + activeWGIndex.get(wgIndex));
    LOG.info("realSCs size = " + realSplitClassifications.get(wgIndex).size());
    LOG.info("activeSCs size = " + activeSCsNum.get(wgIndex));
    for (int i = 0; i < realSplitClassifications.get(wgIndex).size(); i++) {
      LOG.info("    SC = " + realSplitClassifications.get(wgIndex).get(i).toString());
      LOG.info("    SCstatus = " + realSCsStatus.get(wgIndex).get(i));
      LOG.info("    SCsLength = " + realSCsStatus.get(wgIndex).get(i));
      LOG.info("~~~~~~");
    }
    LOG.info("totalLength = " + realSCsTotalLength_all.get(wgIndex));
    LOG.info("totalLength_active = " + realSCsTotalLength_active.get(wgIndex));
    LOG.info("^^^^^^^^^^^");
    LOG.info("appendedSCs in this workergroup:");
    if (appendedSCs != null && appendedSCs.get(wgIndex) != null) {
      LOG.info("appendedSCs.size = " + appendedSCs.get(wgIndex).size());
      for (int i = 0; i < appendedSCs.get(wgIndex).size(); i++){
        LOG.info("SC = " + appendedSCs.get(wgIndex).get(i).toString());
      }
    }
    LOG.info("#############");
  }


  /* code end */

  /**
   * use new version MapReduce API
   */
  private final boolean useNewAPI;

  private final static int maxLocationLimit = 10;
  private int splitNum;

  public int actualSplitNum;//////

  public DataSpliter(AMContext context) {
    this(context, new HashMap<Integer, SplitClassification>());
  }

  public DataSpliter(AMContext context, Map<Integer, SplitClassification> splits) {
    this.context = context;
    this.splitClassifications = splits;
    /* new code */
    this.realSplitClassifications = new HashMap<Integer, List<SplitClassification>>();
    this.realSCsStatus = new HashMap<Integer, List<Boolean>>();
    this.realSCsLength = new HashMap<Integer, List<Long>>();
    this.realSCsTotalLength_all = new HashMap<Integer, Long>();
    this.realSCsTotalLength_active = new HashMap<Integer, Long>();

    this.activeWGIndex = new ArrayList<Boolean>();
    this.trainDataStatus = new HashMap<Integer, Integer>();
    this.activeSCsNum = new HashMap<Integer, Integer>();

    this.appendedSCs = new HashMap<Integer, List<SplitClassification>>();
    /* code end */
    useNewAPI = context.getConf().getBoolean("mapred.mapper.new-api", false);
  }

  /**
   * Split training data in the input directory into roughly equal pieces, and then group them by
   * workergroup
   *
   * @return Map<Integer,SplitClassification> splits group map
   */
  public Map<Integer, SplitClassification> generateSplits()
    throws IOException, InterruptedException, ClassNotFoundException {
    Configuration conf = context.getConf();
    String trainDataPath = conf.get(AngelConf.ANGEL_JOB_INPUT_PATH);
    if (trainDataPath != null) {
      conf.set("mapreduce.input.fileinputformat.inputdir", trainDataPath);
    } else {
      throw new IOException("Angel input data directory is null");
    }

    // Calculate how many splits we need. As each task handles a separate split of data, so we want
    // the number of splits equal to the number of tasks
    int workergroupNumber =
      conf.getInt(AngelConf.ANGEL_WORKERGROUP_NUMBER, AngelConf.DEFAULT_ANGEL_WORKERGROUP_NUMBER);
    int taskNumInWorker =
      conf.getInt(AngelConf.ANGEL_WORKER_TASK_NUMBER, AngelConf.DEFAULT_ANGEL_WORKER_TASK_NUMBER);
    int splitNum = workergroupNumber * taskNumInWorker;
    LOG.info("expected split number=" + splitNum);

    if (!useNewAPI) {
      LOG.info("use old mapreduce api");
      // split data
      org.apache.hadoop.mapred.InputSplit[] splitArray = generateSplitsUseOldAPI(conf, splitNum);
      LOG.info("splits number=" + splitArray.length);

      if (LOG.isDebugEnabled()) {
        int num = splitArray.length;
        for (int i = 0; i < num; i++) {
          LOG.debug("split [" + i + "]=" + splitArray[i].getLength());
        }
      }

      // dispatch the splits to workergroups
      dispatchSplitsUseLocation(splitArray, workergroupNumber, taskNumInWorker);
    } else {
      LOG.info("use new mapreduce api");
      // split data
      List<org.apache.hadoop.mapreduce.InputSplit> splitsNewAPI =
        generateSplitsUseNewAPI(conf, splitNum);
      LOG.info("splits number=" + splitsNewAPI.size());
      if (LOG.isDebugEnabled()) {
        int num = splitsNewAPI.size();
        for (int i = 0; i < num; i++) {
          LOG.debug("split [" + i + "]=" + splitsNewAPI.get(i).getLength());
        }
      }

      // dispatch the splits to workergroups
      dispatchSplitsUseLocation(splitsNewAPI, workergroupNumber, taskNumInWorker);
    }

    return splitClassifications;
  }

  private List<org.apache.hadoop.mapreduce.InputSplit> generateSplitsUseNewAPI(Configuration conf,
    int expectedSplitNum) throws IOException, ClassNotFoundException, InterruptedException {
    String jobIDStr = conf.get(AngelConf.ANGEL_JOB_ID);
    JobID jobID = JobID.forName(jobIDStr);
    JobContext jobConf = new JobContextImpl(new JobConf(conf), jobID);

    jobConf.getCredentials().addAll(UserGroupInformation.getCurrentUser().getCredentials());
    // Set split minsize and maxsize to expected split size. We need to get the total size of data
    // first, then divided by expected split number
    long totalInputFileSize = HdfsUtil.getInputFileTotalSize(jobConf);
    LOG.info("totalInputFileSize=" + totalInputFileSize);

    jobConf.getConfiguration()
      .setLong(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.SPLIT_MINSIZE,
        totalInputFileSize / expectedSplitNum);
    jobConf.getConfiguration()
      .setLong(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.SPLIT_MAXSIZE,
        totalInputFileSize / expectedSplitNum);

    // get input format class from configuration and then instantiation a input format object
    String inputFormatClassName = jobConf.getConfiguration()
      .get(AngelConf.ANGEL_INPUTFORMAT_CLASS, AngelConf.DEFAULT_ANGEL_INPUTFORMAT_CLASS);
    org.apache.hadoop.mapreduce.InputFormat<?, ?> input =
      (org.apache.hadoop.mapreduce.InputFormat<?, ?>) ReflectionUtils
        .newInstance(Class.forName(inputFormatClassName), conf);
    LOG.debug("inputFormatClassName=" + inputFormatClassName);

    // split data
    LOG.info("input class = " + input.getClass());//////
    return input.getSplits(jobConf);
  }

  private org.apache.hadoop.mapred.InputSplit[] generateSplitsUseOldAPI(Configuration conf,
    int expectedSplitNum) throws ClassNotFoundException, IOException {
    // Set split minsize and maxsize to expected split size. We need to get the total size of data
    // first, then divided by expected split number
    JobConf jobConf = new JobConf(conf);
    long totalInputFileSize = HdfsUtil.getInputFileTotalSize(jobConf);
    LOG.info("totalInputFileSize=" + totalInputFileSize);

    jobConf.setLong(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.SPLIT_MINSIZE,
      totalInputFileSize / expectedSplitNum);
    jobConf.setLong(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.SPLIT_MAXSIZE,
      totalInputFileSize / expectedSplitNum);

    // get input format class from configuration and then instantiation a input format object
    String inputFormatClassName =
      jobConf.get(AngelConf.ANGEL_INPUTFORMAT_CLASS, AngelConf.DEFAULT_ANGEL_INPUTFORMAT_CLASS);
    InputFormat<?, ?> input =
      (InputFormat<?, ?>) ReflectionUtils.newInstance(Class.forName(inputFormatClassName), conf);
    LOG.debug("inputFormatClassName=" + inputFormatClassName);

    // split data
    return input.getSplits(jobConf, splitNum);
  }

  private void dispatchSplitsUseLocation(List<org.apache.hadoop.mapreduce.InputSplit> splitsNewAPI,
    int groupNumber, int groupItemNumber) throws IOException, InterruptedException {
    LOG.info("private void dispatchSplitsUseLocation~~~");//////
    splitNum = splitsNewAPI.size();
    actualSplitNum = splitNum;//////

    // Since the actual split size is sometimes not exactly equal to the expected split size, we
    // need to fine tune the number of workergroup and task based on the actual split number
    int estimatedGroupNum = (splitNum + groupItemNumber - 1) / groupItemNumber;
    int base = 0;

    // Dispatch data splits to workergroups, each SplitClassification corresponds to a workergroup.
    // Record the location information for the splits in order to data localized schedule
    for (int i = 0; i < estimatedGroupNum; i++) {
      List<String> locationList = new ArrayList<String>(maxLocationLimit);
      List<org.apache.hadoop.mapreduce.InputSplit> splitList =
        new ArrayList<org.apache.hadoop.mapreduce.InputSplit>();


      base = i * groupItemNumber;
      for (int j = 0; j < groupItemNumber && (base < splitNum); j++, base++) {
        splitList.add(splitsNewAPI.get(base));
        String[] locations = splitsNewAPI.get(base).getLocations();
        for (int k = 0; k < locations.length && locationList.size() < maxLocationLimit; k++) {
          locationList.add(locations[k]);
        }
      }


      SplitClassification splitClassification = new SplitClassification(null, splitList,
        locationList.toArray(new String[locationList.size()]), true);
      splitClassifications.put(i, splitClassification);

      /* new code */
      assignRealSplits(splitList, i);
      /*for (int k  = 0; k < realSplitClassifications.get(i).size(); k++){
        LOG.info("SC = " + realSplitClassifications.get(i).get(k).toString());
        LOG.info("Length = " + realSCsLength.get(i).get(k));
        LOG.info("Status = " + realSCsStatus.get(i).get(k));
      }*/
      updateWG_realSCsTotalLength_all(i);
      updateWG_realSCsTotalLength_active(i);
      //LOG.info("totalLength_all = " + realSCsTotalLength_all.get(i));
      //LOG.info("totalLength_active = " + realSCsTotalLength_active.get(i));
      /* code end */
    }
    update_avgSCsNum();//////
    print_all_SCs();//////
  }

  /* new code */
  public void dispatchExtraSplitsUseLocation(){
    LOG.info("private void dispatchExtraSplitsUseLocation~~~");
    if (extraSplitClassification != null && splitClassifications.size() < actualSplitNum){
      splitClassifications.put(actualSplitNum-1, extraSplitClassification);
    }
  }

  public void dispatchIdleSCs(List<SplitClassification> idleSCs) throws IOException, InterruptedException {
    LOG.info("private void dispatchIdleSCs(List<SplitClassification> idleSCs)~~~~");
    LOG.info("Before dispatch .......");
    print_all_SCs();

    // round robin
    int wgindex = 0;
    for (int i = 0; i < idleSCs.size(); i++){
      while (activeWGIndex.get(wgindex) == false){
        wgindex++;
        if (wgindex == activeWGIndex.size()){
          wgindex = wgindex%activeWGIndex.size();
        }
      }
      LOG.info("wgindex = " + wgindex);
      realSplitClassifications.get(wgindex).add(idleSCs.get(i));
      realSCsStatus.get(wgindex).add(true);
      realSCsLength.get(wgindex).add(idleSCs.get(i).getSplitNewAPI(0).getLength());
      activeSCsNum.put(wgindex, activeSCsNum.get(wgindex) + 1);

      SplitClassification idleSCCopy = new SplitClassification();
      idleSCCopy = idleSCs.get(i);
      appendedSCs.get(wgindex).add(idleSCCopy);
      wgindex++;
      if (wgindex == activeWGIndex.size()){
        wgindex = wgindex%activeWGIndex.size();
      }
    }
    update_realSCsTotalLength();
    LOG.info("after dispatch ......");
    update_avgSCsNum();
    print_all_SCs();

    for (Integer wgIndex : appendedSCs.keySet()){
      if (appendedSCs.get(wgIndex).size() > 0){
        trainDataStatus.put(wgIndex, 1);
      }
    }
  }

  public void assignRealSplits(List<org.apache.hadoop.mapreduce.InputSplit> splitList, int workergroupIndex)
          throws IOException, InterruptedException{
    LOG.info("workergroupIndex= " + workergroupIndex);
    LOG.info("original splitList = " + splitList.toString());

    for (int k = 0; k < splitList.size(); k++){
      CombineFileSplit inputList = (CombineFileSplit) splitList.get(k);
      for (int i = 0; i < inputList.getPaths().length; i++){
        // build CombineFileSplit for data in each path
        Path[] paths = new Path[1];
        long[] startoffset = new long[1];
        long[] lengths = new long[1];
        String[] locations = null;
        // get info from original inputList
        paths[0] = inputList.getPath(i);
        startoffset[0] = inputList.getOffset(i);
        lengths[0] = inputList.getLength(i);
        if (inputList.getLocations() != null && i < inputList.getLocations().length) {
          locations = new String[1];
          locations[0] = inputList.getLocations()[i];
        }
        // initialize
        CombineFileSplit newInputList = new CombineFileSplit(paths, startoffset, lengths, locations);
        List<org.apache.hadoop.mapreduce.InputSplit> newSplitList =
                new ArrayList<org.apache.hadoop.mapreduce.InputSplit>();
        newSplitList.add(newInputList);
        List<String> locationList = new ArrayList<String>(maxLocationLimit);
        if (locations != null) {
          locationList.add(locations[0]);
        }
        SplitClassification newSplitClassification = new SplitClassification(null, newSplitList,
                locationList.toArray(new String[locationList.size()]), true);
        // put into realSplitClassifications
        if (realSplitClassifications.containsKey(workergroupIndex)){
          realSplitClassifications.get(workergroupIndex).add(newSplitClassification);
          appendedSCs.get(workergroupIndex).add(newSplitClassification);
        }else {
          List<SplitClassification> SCsList = new ArrayList<SplitClassification>();
          SCsList.add(newSplitClassification);
          realSplitClassifications.put(workergroupIndex, SCsList);

          List<SplitClassification> appendedSCsList = new ArrayList<SplitClassification>();
          appendedSCsList.add(newSplitClassification);
          appendedSCs.put(workergroupIndex, appendedSCsList);

          activeWGIndex.add(true);
          totalActiveWGNum++;
          trainDataStatus.put(workergroupIndex, 0);
        }
        // put into realSCsStatus
        if (realSCsStatus.containsKey(workergroupIndex)){
          realSCsStatus.get(workergroupIndex).add(true);
          realSCsLength.get(workergroupIndex).add(lengths[0]);
          activeSCsNum.put(workergroupIndex, activeSCsNum.get(workergroupIndex) + 1);

        }else {
          List<Boolean> SCsStatus = new ArrayList<Boolean>();
          SCsStatus.add(true);
          realSCsStatus.put(workergroupIndex, SCsStatus);
          activeSCsNum.put(workergroupIndex, 1);

          List<Long> SCsLength = new ArrayList<Long>();
          SCsLength.add(lengths[0]);
          realSCsLength.put(workergroupIndex, SCsLength);
        }
      }
    }
  }

  public void update_avgSCsNum(){
    int num = 0;
    for (int i = 0; i < activeWGIndex.size(); i++){
      if (activeWGIndex.get(i)) num++;
    }
    totalActiveWGNum = num;
    for (int i = 0; i < realSCsStatus.size(); i++){
      num = 0;
      for (int j = 0; j < realSCsStatus.get(i).size(); j++){
        if (realSCsStatus.get(i).get(j)) num++;
      }
      activeSCsNum.put(i, num);
    }
    num = 0;
    for (int k = 0; k<activeSCsNum.size(); k++){
      if (activeWGIndex.get(k)) num += activeSCsNum.get(k);
    }
    averageSCsNum = (double) num/totalActiveWGNum;
  }

  public void update_realSCsTotalLength(){
    update_realSCsTotalLength_all();
    update_realSCsTotalLength_active();
  }

  public void update_realSCsTotalLength_all(){
    for (Integer workergroupIndex: realSplitClassifications.keySet()){
      updateWG_realSCsTotalLength_all(workergroupIndex);
    }
  }

  public void update_realSCsTotalLength_active(){
    for (Integer workergroupIndex: realSplitClassifications.keySet()){
      updateWG_realSCsTotalLength_active(workergroupIndex);
    }
  }

  public void updateWG_realSCsTotalLength_all(int workergroupIndex){
    long totalLength = 0;
    for(long length: realSCsLength.get(workergroupIndex)){
      totalLength += length;
    }
    realSCsTotalLength_all.put(workergroupIndex, totalLength);
  }

  public void updateWG_realSCsTotalLength_active(int workergroupIndex){
    long totalLength = 0;
    for (int i = 0; i < realSCsLength.get(workergroupIndex).size(); i++){
      if (realSCsStatus.get(workergroupIndex).get(i)) totalLength += realSCsLength.get(workergroupIndex).get(i);
    }
    realSCsTotalLength_active.put(workergroupIndex, totalLength);
  }
  /* code end */


  private void dispatchSplitsUseLocation(InputSplit[] splitArray, int groupNumber,
    int groupItemNumber) throws IOException {
    splitNum = splitArray.length;

    // Since the actual split size is sometimes not exactly equal to the expected split size, we
    // need to fine tune the number of workergroup and task based on the actual split number
    int estimatedGroupNum = (splitNum + groupItemNumber - 1) / groupItemNumber;
    int base = 0;

    // Dispatch data splits to workergroups, each SplitClassification corresponds to a workergroup.
    // Record the location information for the splits in order to data localized schedule
    for (int i = 0; i < estimatedGroupNum; i++) {
      List<String> locationList = new ArrayList<String>(maxLocationLimit);
      List<org.apache.hadoop.mapred.InputSplit> splitList =
        new ArrayList<org.apache.hadoop.mapred.InputSplit>();

      base = i * groupItemNumber;
      for (int j = 0; j < groupItemNumber && (base < splitNum); j++, base++) {
        splitList.add(splitArray[base]);
        String[] locations = splitArray[base].getLocations();
        for (int k = 0; k < locations.length && locationList.size() < maxLocationLimit; k++) {
          locationList.add(locations[k]);
        }
      }

      SplitClassification splitClassification = new SplitClassification(splitList, null,
        locationList.toArray(new String[locationList.size()]), useNewAPI);
      splitClassifications.put(i, splitClassification);
    }
  }

  public String[] getSplitLocations(int index) {
    return splitClassifications.get(index).getLocations();
  }

  public SplitClassification getSplits(int index) {
    return splitClassifications.get(index);
  }

  /* new code */
  public Map<Integer, SplitClassification> getSplitClassifications(){
    return splitClassifications;
  }
  public void  incrementActualSplitNum(){
    actualSplitNum++;
  }
  /* code end */

  public int getSplitNum() {
    return splitNum;
  }

  /**
   * write data splits to a output stream
   *
   * @param outputStream output stream
   * @throws IOException
   */
  public void serialize(FSDataOutputStream outputStream) throws IOException {
    outputStream.writeInt(splitNum);
    outputStream.writeInt(splitClassifications.size());
    LOG.info("this is to serialize splitClassifications!!!!!");//////
    for (Entry<Integer, SplitClassification> entry : splitClassifications.entrySet()) {
      outputStream.writeInt(entry.getKey());
      entry.getValue().serialize(outputStream);
    }
  }

  /**
   * read data splits from a input stream
   *
   * @param inputStream input stream
   * @throws IOException
   */
  public void deserialize(FSDataInputStream inputStream)
    throws IOException, ClassNotFoundException {
    splitNum = inputStream.readInt();
    int size = inputStream.readInt();
    LOG.info("this is to deserialize splitClassifications!!!!!");//////
    for (int i = 0; i < size; i++) {
      int index = inputStream.readInt();
      SplitClassification split = new SplitClassification();
      split.deserialize(inputStream);
      splitClassifications.put(index, split);
    }
    inputStream.close();
  }
}
