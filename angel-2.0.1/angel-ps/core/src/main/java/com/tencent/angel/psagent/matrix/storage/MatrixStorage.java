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


package com.tencent.angel.psagent.matrix.storage;

/**
 * The storage for a single matrix.
 */

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.psagent.matrix.transport.adapter.IndicesView;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MatrixStorage {
  private static final Log LOG = LogFactory.getLog(MatrixStorage.class); //////
  /**
   * row index to row map
   */
  private final ConcurrentHashMap<Integer, Vector> rowIndexToRowMap;

  /* new code */
  /**
   * row index to (PartitionKey to float[] in vectors) from different servers
   */
  public ConcurrentHashMap<Integer, HashMap<PartitionKey, float[]>> rowIdToPartKeyToFloats;
  /**
   * row index to (PartitionKey to IndicesView in vectors) from different servers
   */
  public ConcurrentHashMap<Integer, HashMap<PartitionKey, IndicesView>> rowIdToPartKeyToView;
  /* code end */

  private final ReentrantReadWriteLock lock;

  /**
   * Create a new MatrixStorage.
   */
  public MatrixStorage() {
    rowIndexToRowMap = new ConcurrentHashMap<>();
    rowIdToPartKeyToFloats = new ConcurrentHashMap<>(); //////
    rowIdToPartKeyToView = new ConcurrentHashMap<>(); //////
    lock = new ReentrantReadWriteLock();
  }

  /* new code */
  public void resetParameterServers_idle_MatrixStorage(){
    LOG.info("resetParameterServers_idle_MatrixStorage");
    Map<Integer, List<PartitionKey>> rowId2removedPartitionKeys = new HashMap<Integer, List<PartitionKey>>();
    // collect all partitionKey with false status
    for (Map.Entry<Integer, HashMap<PartitionKey, float[]>> entry : rowIdToPartKeyToFloats.entrySet()){
      rowId2removedPartitionKeys.put(entry.getKey(), new ArrayList<PartitionKey>());
      for (Map.Entry<PartitionKey, float[]> entry2 : entry.getValue().entrySet()){
        if (entry2.getKey().status == false){
          rowId2removedPartitionKeys.get(entry.getKey()).add(entry2.getKey());
        }
      }
    }
    // remove
    for (Map.Entry<Integer, List<PartitionKey>> entry: rowId2removedPartitionKeys.entrySet()){
      int rowIndex = entry.getKey();
      for (int i = 0; i < entry.getValue().size(); i++){
        rowIdToPartKeyToFloats.get(rowIndex).remove(entry.getValue().get(i));
        rowIdToPartKeyToView.get(rowIndex).remove(entry.getValue().get(i));
      }
    }
  }

  public void usePartitions_pre_MatrixStorage(){
    LOG.info("usePartitions_pre_MatrixStorage");
    Map<Integer, List<PartitionKey>> rowId2removedPartitionKeys = new HashMap<Integer, List<PartitionKey>>();
    // collect all partitionKey with false status
    for (Map.Entry<Integer, HashMap<PartitionKey, float[]>> entry : rowIdToPartKeyToFloats.entrySet()){
      rowId2removedPartitionKeys.put(entry.getKey(), new ArrayList<PartitionKey>());
      for (Map.Entry<PartitionKey, float[]> entry2 : entry.getValue().entrySet()){
        if (entry2.getKey().status == false){
          rowId2removedPartitionKeys.get(entry.getKey()).add(entry2.getKey());
        }
      }
    }
    // remove
    for (Map.Entry<Integer, List<PartitionKey>> entry: rowId2removedPartitionKeys.entrySet()){
      int rowIndex = entry.getKey();
      for (int i = 0; i < entry.getValue().size(); i++){
        rowIdToPartKeyToFloats.get(rowIndex).remove(entry.getValue().get(i));
        rowIdToPartKeyToView.get(rowIndex).remove(entry.getValue().get(i));
      }
    }
  }

  /* code end */


  /**
   * Get the row from the storage.
   *
   * @param rowIndex row index
   * @return TVector row
   */
  public Vector getRow(int rowIndex) {
    return rowIndexToRowMap.get(rowIndex);
  }

  /**
   * Add the row to the storage.
   *
   * @param rowIndex row index
   * @param row
   */
  public void addRow(int rowIndex, Vector row) {
    rowIndexToRowMap.put(rowIndex, row);
  }

  /**
   * Remove the row from the storage.
   *
   * @param rowIndex row index
   */
  public void removeRow(int rowIndex) {
    rowIndexToRowMap.remove(rowIndex);
  }

  public ReentrantReadWriteLock getLock() {
    return lock;
  }
}
