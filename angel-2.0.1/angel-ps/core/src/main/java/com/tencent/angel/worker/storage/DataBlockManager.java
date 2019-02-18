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


package com.tencent.angel.worker.storage;

import com.tencent.angel.split.SplitClassification;
import com.tencent.angel.worker.WorkerContext;
import com.tencent.angel.worker.task.TaskId;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * class for data block manager
 */
public class DataBlockManager {

  private static final Log LOG = LogFactory.getLog(DataBlockManager.class);//////

  private boolean useNewAPI;
  private final Map<TaskId, Integer> splitInfos; // taskId to splitId in SC
  private SplitClassification splitClassification;

  /* new code */
  public List<SplitClassification> realSplitClassifications = new ArrayList<SplitClassification>();
  public List<Boolean> realSCsStatus = new ArrayList<Boolean>();
  public List<Long> realSCsLength = new ArrayList<Long>();
  public Long realSCsTotalLength_all = 0L;
  public Long realSCsTotalLength_active = 0L;

  public List<Long> realSCsTotalSLength = new ArrayList<Long>();
  public Long realSCsTotalSTotalLength_all = 0L;
  public Long realSCsTotalSTotalLength_active = 0L;
  public List<Long> realSCsTrainSLength = new ArrayList<Long>();
  public Long realSCsTrainSTotalLength_all = 0L;
  public Long realSCsTrainSTotalLength_active = 0L;
  public List<Long> realSCsValidSLength = new ArrayList<Long>();
  public Long realSCsValidSTotalLength_all = 0L;
  public Long realSCsValidSTotalLength_active = 0L;

  public void appendRealSCs(List<SplitClassification> appendedSCs) throws IOException, InterruptedException {
    for (int i = 0; i < appendedSCs.size(); i++ ){
      realSplitClassifications.add(appendedSCs.get(i));
      realSCsStatus.add(true);
      realSCsLength.add(appendedSCs.get(i).getSplitNewAPI(0).getLength());
    }
    update_realSCsTotalLength();
  }

  public void update_realSCsTotalLength(){
    update_realSCsTotalLength_all();
    update_realSCsTotalLength_active();
  }

  public void update_realSCsTotalLength_all(){
    long totalLength = 0;
    for(long length: realSCsLength){
      totalLength += length;
    }
    realSCsTotalLength_all = totalLength;
  }

  public void update_realSCsTotalLength_active(){
    long totalLength = 0;
    for (int i = 0; i < realSCsLength.size(); i++){
      if (realSCsStatus.get(i)) totalLength += realSCsLength.get(i);
    }
    realSCsTotalLength_active = totalLength;
  }

  public void update_realSCsAllSTotalLength(){
    update_realSCsTotalSTotalLength();
    update_realSCsTrainSTotalLength();
    update_realSCsValidSTotalLength();
  }

  public void update_realSCsTotalSTotalLength(){
    update_realSCsTotalSTotalLength_all();
    update_realSCsTotalSTotalLength_active();
  }

  public void update_realSCsTotalSTotalLength_all(){
    long totalLength = 0;
    for(long length: realSCsTotalSLength){
      totalLength += length;
    }
    realSCsTotalSTotalLength_all = totalLength;
  }

  public void update_realSCsTotalSTotalLength_active(){
    long totalLength = 0;
    for (int i = 0; i < realSCsTotalSLength.size(); i++){
      if (realSCsStatus.get(i)) totalLength += realSCsTotalSLength.get(i);
    }
    realSCsTotalSTotalLength_active = totalLength;
  }

  public void update_realSCsTrainSTotalLength(){
    update_realSCsTrainSTotalLength_all();
    update_realSCsTrainSTotalLength_active();
  }

  public void update_realSCsTrainSTotalLength_all(){
    long totalLength = 0;
    for(long length: realSCsTrainSLength){
      totalLength += length;
    }
    realSCsTrainSTotalLength_all = totalLength;
  }

  public void update_realSCsTrainSTotalLength_active(){
    long totalLength = 0;
    for (int i = 0; i < realSCsTrainSLength.size(); i++){
      if (realSCsStatus.get(i)) totalLength += realSCsTrainSLength.get(i);
    }
    realSCsTrainSTotalLength_active = totalLength;
  }

  public void update_realSCsValidSTotalLength(){
    update_realSCsValidSTotalLength_all();
    update_realSCsValidSTotalLength_active();
  }

  public void update_realSCsValidSTotalLength_all(){
    long totalLength = 0;
    for(long length: realSCsValidSLength){
      totalLength += length;
    }
    realSCsValidSTotalLength_all = totalLength;
  }

  public void update_realSCsValidSTotalLength_active(){
    long totalLength = 0;
    for (int i = 0; i < realSCsValidSLength.size(); i++){
      if (realSCsStatus.get(i)) totalLength += realSCsValidSLength.get(i);
    }
    realSCsValidSTotalLength_active = totalLength;
  }

  public void print_realSCs_allSamples(){
    LOG.info("Total Samples = ");
    for (int i = 0; i < realSCsTotalSLength.size(); i++){
      LOG.info("      samples in SC["+i+"] = " + realSCsTotalSLength.get(i));
    }
    LOG.info("Total Samples size = " + realSCsTotalSTotalLength_all + ", active total samples size = " + realSCsTotalSTotalLength_active);
    LOG.info("");

    LOG.info("Train Samples = ");
    for (int i = 0; i < realSCsTrainSLength.size(); i++){
      LOG.info("      samples in SC["+i+"] = " + realSCsTrainSLength.get(i));
    }
    LOG.info("Train Samples size = " + realSCsTrainSTotalLength_all + ", active train samples size = " + realSCsTrainSTotalLength_active);
    LOG.info("");

    LOG.info("Valid Samples = ");
    for (int i = 0; i < realSCsValidSLength.size(); i++){
      LOG.info("      samples in SC["+i+"] = " + realSCsValidSLength.get(i));
    }
    LOG.info("Valid Samples size = " + realSCsValidSTotalLength_all + ", active valid samples size = " + realSCsValidSTotalLength_active);
    LOG.info("");
  }
  /* code end */

  /* new code */
  public Boolean IfAppendSC = false;
  public SplitClassification AppendSplitClassification;

  public Boolean ifNewAppendedSC = false;
  public List<Boolean> activeAppendedSCs;
  public List<SplitClassification> appendedSplitClassifications;
  /* code end */

  public DataBlockManager() {
    this.splitInfos = new ConcurrentHashMap<TaskId, Integer>();
  }

  public void init() {
    Configuration conf = WorkerContext.get().getConf();
    useNewAPI = conf.getBoolean("mapred.mapper.new-api", false);
  }

  public SplitClassification getSplitClassification() {
    return splitClassification;
  }

  public void setSplitClassification(SplitClassification splitClassification) {
    this.splitClassification = splitClassification;
  }


  /**
   * Assign split to tasks
   *
   * @param set the set
   */
  public void assignSplitToTasks(Set<TaskId> set) {
    if (splitClassification == null) {
      return;
    }

    assert (set.size() == splitClassification.getSplitNum());
    int index = 0;
    for (TaskId id : set) {
      splitInfos.put(id, index);
      index++;
    }
  }

  /**
   * Get the reader for given task
   *
   * @param <K>    the type parameter
   * @param <V>    the type parameter
   * @param taskId the task id
   * @return the reader
   * @throws IOException            the io exception
   * @throws InterruptedException   the interrupted exception
   * @throws ClassNotFoundException the class not found exception
   */
  @SuppressWarnings({"rawtypes", "unchecked"}) public <K, V> Reader<K, V> getReader(TaskId taskId)
    throws IOException, InterruptedException, ClassNotFoundException {
    /* new code */
    LOG.info("useNewAPI = " + useNewAPI);
    if (splitInfos.size() > 0){
      for(Map.Entry<TaskId, Integer> entry: splitInfos.entrySet()){
        LOG.info("taskId = " + entry.getKey());
        LOG.info("index = " + entry.getValue());
      }
    }
    /* code end */

    if (useNewAPI) {
      DFSStorageNewAPI storage =
        new DFSStorageNewAPI(splitClassification.getSplitNewAPI(splitInfos.get(taskId)));
      /* new code */
      LOG.info("InputSplit class = " + storage.getSplit().getClass());
      LOG.info("split length = " + storage.getSplit().getLength());
      LOG.info("split toString = " + storage.getSplit().toString());
      /* code end */
      storage.initReader();
      return storage.getReader();
    } else {
      DFSStorageOldAPI storage =
        new DFSStorageOldAPI(splitClassification.getSplitOldAPI(splitInfos.get(taskId)));
      storage.initReader();
      return storage.getReader();
    }
  }


  /* new code */
  /**
   * Get the reader of data in a real SC for given task
   *
   * @param <K>    the type parameter
   * @param <V>    the type parameter
   * @param taskId the task id
   * @return the reader
   * @throws IOException            the io exception
   * @throws InterruptedException   the interrupted exception
   * @throws ClassNotFoundException the class not found exception
   */
  @SuppressWarnings({"rawtypes", "unchecked"}) public <K, V> Reader<K, V> getReaderForRealSC(TaskId taskId, int SCIndex)
          throws IOException, InterruptedException, ClassNotFoundException {
    LOG.info("in getReaderForRealSC: useNewAPI = " + useNewAPI);

    if (useNewAPI) {
      DFSStorageNewAPI storage =
              new DFSStorageNewAPI(realSplitClassifications.get(SCIndex).getSplitNewAPI(splitInfos.get(taskId)));
      LOG.info("InputSplit class = " + storage.getSplit().getClass());
      LOG.info("split length = " + storage.getSplit().getLength());
      LOG.info("split toString = " + storage.getSplit().toString());
      storage.initReader();
      return storage.getReader();
    } else {
      DFSStorageOldAPI storage =
              new DFSStorageOldAPI(AppendSplitClassification.getSplitOldAPI(splitInfos.get(taskId)));
      storage.initReader();
      return storage.getReader();
    }
  }

  /* code end */

  public void stop() {
    // TODO Auto-generated method stub

  }
}
