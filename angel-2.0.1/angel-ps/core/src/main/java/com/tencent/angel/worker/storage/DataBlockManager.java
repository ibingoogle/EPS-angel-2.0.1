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

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * class for data block manager
 */
public class DataBlockManager {

  private static final Log LOG = LogFactory.getLog(DataBlockManager.class);//////

  private boolean useNewAPI;
  private final Map<TaskId, Integer> splitInfos;
  private SplitClassification splitClassification;

  /* new code */
  public Boolean IfAppendSC = false;
  public SplitClassification AppendSplitClassification;
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

  /* new code */
  public void setAppendSplitClassification(SplitClassification AppendSplitClassification) {
    this.AppendSplitClassification = AppendSplitClassification;
    this.IfAppendSC = true;
  }
  /* code end */

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
   * Get the reader of append data for given task
   *
   * @param <K>    the type parameter
   * @param <V>    the type parameter
   * @param taskId the task id
   * @return the reader
   * @throws IOException            the io exception
   * @throws InterruptedException   the interrupted exception
   * @throws ClassNotFoundException the class not found exception
   */
  @SuppressWarnings({"rawtypes", "unchecked"}) public <K, V> Reader<K, V> getReaderForAppendSplits(TaskId taskId)
          throws IOException, InterruptedException, ClassNotFoundException {
    LOG.info("in getReaderForAppendSplits: useNewAPI = " + useNewAPI);

    if (useNewAPI) {
      DFSStorageNewAPI storage =
              new DFSStorageNewAPI(AppendSplitClassification.getSplitNewAPI(splitInfos.get(taskId)));
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
