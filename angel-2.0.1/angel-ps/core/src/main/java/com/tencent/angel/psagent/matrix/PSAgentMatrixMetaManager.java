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


package com.tencent.angel.psagent.matrix;

import com.google.protobuf.ServiceException;
import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.MatrixMetaManager;
import com.tencent.angel.ml.matrix.PartitionLocation;
import com.tencent.angel.ml.matrix.PartitionMeta;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.ps.server.data.PSLocation;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.clock.ClockCache;
import com.tencent.angel.psagent.clock.MatrixClockCache;
import com.tencent.angel.psagent.matrix.transport.adapter.RowIndex;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * PSAgent matrix meta manager
 */
public class PSAgentMatrixMetaManager {
  private static final Log LOG = LogFactory.getLog(PSAgentMatrixMetaManager.class);
  /**
   * Matrix meta manager
   */
  private final MatrixMetaManager matrixMetaManager;

  /**
   * Matrix id -> the partitions of the matrix map
   */
  private final Map<Integer, List<PartitionKey>> matrixIdToPartsMap;

  /**
   * Matrix id -> (row index -> contain this row partitions map) map
   */
  private final Map<Integer, Map<Integer, List<PartitionKey>>> rowIndexToPartsMap;

  /**
   * Local clock cache
   */
  private final ClockCache partClockCache;


  /**
   * Create PSAgentMatrixMetaManager
   *
   * @param partClockCache clock cache
   */
  public PSAgentMatrixMetaManager(ClockCache partClockCache) {
    this.partClockCache = partClockCache;
    this.matrixMetaManager = new MatrixMetaManager();
    this.matrixIdToPartsMap = new ConcurrentHashMap<>();
    this.rowIndexToPartsMap = new ConcurrentHashMap<>();
  }


  /* new code */
  public void resetParameterServers_idle_PSAgentMatrixMetaManager(List<MatrixMeta> matrixMetas){
    LOG.info("resetParameterServers_idle_PSAgentMatrixMetaManager");
    // remove partitionMeta and partitionKey with false status
    for (Map.Entry<Integer, MatrixMeta>  entry : matrixMetaManager.getMatrixMetas().entrySet()) {
      List<Integer> removedPartitionIds = new ArrayList<>();
      for (Map.Entry<Integer, PartitionMeta> entry2: entry.getValue().getPartitionMetas().entrySet()){
        if (entry2.getValue().getPartitionKey().status == false){
          removedPartitionIds.add(entry2.getKey());
        }
      }
      for (int i = 0; i < removedPartitionIds.size(); i++){
        entry.getValue().getPartitionMetas().remove(removedPartitionIds.get(i));
      }
    }
    for (Map.Entry<Integer, List<PartitionKey>> entry : matrixIdToPartsMap.entrySet()){
      List<PartitionKey> partitionKeyList = entry.getValue();
      int partitionKeyListSize = partitionKeyList.size();
      int j = 0;
      while (j < partitionKeyListSize){
          if (partitionKeyList.get(j).status == false) {
          partitionKeyList.remove(partitionKeyList.get(j));
          partitionKeyListSize--;
        }else {
          j++;
        }
      }
    }
    for (Map.Entry<Integer, Map<Integer, List<PartitionKey>>> entry : rowIndexToPartsMap.entrySet()) {
      if (entry.getValue() != null){
        for (Map.Entry<Integer, List<PartitionKey>> entry2 : entry.getValue().entrySet()) {
          if (entry2.getValue() != null) {
            List<PartitionKey> partitionKeyList_row = entry2.getValue();
            int partitionKeyList_rowSize = partitionKeyList_row.size();
            int k = 0;
            while (k < partitionKeyList_rowSize){
              if (partitionKeyList_row.get(k).status == false) {
                partitionKeyList_row.remove(partitionKeyList_row.get(k));
                partitionKeyList_rowSize--;
              }else {
                k++;
              }
            }
          }
        }
      }
    }
    // add idle matrixMetas
    for (int i = 0; i < matrixMetas.size(); i++){
      MatrixMeta matrixMeta_idle = matrixMetas.get(i);
      int matrixIndex = matrixMeta_idle.getId();
      MatrixMeta matrixMeta_curr = matrixMetaManager.getMatrixMeta(matrixIndex);
      for (Map.Entry<Integer, PartitionMeta> entry : matrixMeta_idle.partitionMetas_idle.entrySet()){
        matrixMeta_curr.getPartitionMetas().put(entry.getKey(), entry.getValue());
        PartitionKey partitionKey_idle = entry.getValue().getPartitionKey();
        matrixIdToPartsMap.get(matrixIndex).add(partitionKey_idle);
        for (int rowIndex = partitionKey_idle.getStartRow(); rowIndex < partitionKey_idle.getEndRow(); rowIndex++){
          rowIndexToPartsMap.get(matrixIndex).get(rowIndex).add(partitionKey_idle);
        }
      }
    }
  }

  public void usePartitions_pre_PSAgentMatrixMetaManager(List<MatrixMeta> matrixMetas_pre){
    LOG.info("usePartitions_pre_PSAgentMatrixMetaManager");
    // remove partitionMeta and partitionKey with false status
    for (int i = 0; i < matrixMetas_pre.size(); i++) {
      // 1
      MatrixMeta matrixMeta_pre = matrixMetas_pre.get(i);
      MatrixMeta matrixMeta = matrixMetaManager.getMatrixMetas().get(matrixMeta_pre.getId());
      List<Integer> removedPartitionIds = new ArrayList<>();
      for (Map.Entry<Integer, PartitionMeta> entry : matrixMeta.getPartitionMetas().entrySet()) {
        LOG.info("entry.getValue().getPartitionKey().Id = " + entry.getValue().getPartitionKey().getPartitionId() + ", status = " + entry.getValue().getPartitionKey().status);
        if (entry.getValue().getPartitionKey().status == false) {
          removedPartitionIds.add(entry.getKey());
        }
      }
      LOG.info("removedPartitionIds size = " + removedPartitionIds.size());
      for (int j = 0; j < removedPartitionIds.size(); j++) {
        matrixMeta.getPartitionMetas().remove(removedPartitionIds.get(j));
      }
      // 2
      List<PartitionKey> partitionKeyList = matrixIdToPartsMap.get(matrixMeta_pre.getId());
      int partitionKeyListSize = partitionKeyList.size();
      int j = 0;
      while (j < partitionKeyListSize){
        LOG.info("partitionKeyList.get(j).getPartitionKey().Id = " + partitionKeyList.get(j).getPartitionId() + ", status = " + partitionKeyList.get(j).status);
        if (partitionKeyList.get(j).status == false) {
          partitionKeyList.remove(partitionKeyList.get(j));
          partitionKeyListSize--;
        }else {
          j++;
        }
      }
      // 3
      for (Map.Entry<Integer, List<PartitionKey>> entry : rowIndexToPartsMap.get(matrixMeta_pre.getId()).entrySet()) {
        List<PartitionKey> partitionKeyList_row = entry.getValue();
        int partitionKeyList_rowSize = partitionKeyList_row.size();
        int k = 0;
        while (k < partitionKeyList_rowSize){
          LOG.info("partitionKeyList_row.get(k).getPartitionKey().Id = " + partitionKeyList_row.get(k).getPartitionId() + ", status = " + partitionKeyList_row.get(k).status);
          if (partitionKeyList_row.get(k).status == false) {
            partitionKeyList_row.remove(partitionKeyList_row.get(k));
            partitionKeyList_rowSize--;
          }else {
            k++;
          }
        }
      }
    }
    // add pre matrixMeta
    for (int i = 0; i < matrixMetas_pre.size(); i++){
      MatrixMeta matrixMeta_idle = matrixMetas_pre.get(i);
      int matrixIndex = matrixMeta_idle.getId();
      MatrixMeta matrixMeta_curr = matrixMetaManager.getMatrixMeta(matrixIndex);
      for (Map.Entry<Integer, PartitionMeta> entry : matrixMeta_idle.getPartitionMetas().entrySet()){
        matrixMeta_curr.getPartitionMetas().put(entry.getKey(), entry.getValue());
        PartitionKey partitionKey_pre = entry.getValue().getPartitionKey();
        matrixIdToPartsMap.get(matrixIndex).add(partitionKey_pre);
        for (int rowIndex = partitionKey_pre.getStartRow(); rowIndex < partitionKey_pre.getEndRow(); rowIndex++){
          rowIndexToPartsMap.get(matrixIndex).get(rowIndex).add(partitionKey_pre);
        }
      }
    }
  }

  public HashSet<Integer> rmPartitions_pre_PSAgentMatrixMetaManager(List<MatrixMeta> matrixMetas_pre){
    LOG.info("rmPartitions_PSAgentMatrixMetaManager");
    HashSet<Integer> rmPartitionIds = new HashSet<Integer>();
    // 1
    for (int i = 0; i < matrixMetas_pre.size(); i++){
      MatrixMeta matrixMeta_pre = matrixMetas_pre.get(i);
      MatrixMeta matrixMeta = matrixMetaManager.getMatrixMetas().get(matrixMeta_pre.getId());
      for (Map.Entry<Integer, Map<Integer, PartitionMeta>> entry : matrixMeta_pre.partitionMetas_repartition.entrySet()){
        for(Map.Entry<Integer, PartitionMeta> entry2 : entry.getValue().entrySet()){
          int partitionId = entry2.getKey();
          rmPartitionIds.add(partitionId);
          if (matrixMeta.getPartitionMetas().containsKey(partitionId)){
            matrixMeta.getPartitionMetas().get(partitionId).getPartitionKey().status = false;
          }
        }
      }
    }
    // 2
    for (Map.Entry<Integer, List<PartitionKey>> entry : matrixIdToPartsMap.entrySet()){
      if (entry.getValue() != null) {
        for (int i = 0; i < entry.getValue().size(); i++) {
          if (rmPartitionIds.contains(entry.getValue().get(i).getPartitionId())){
            entry.getValue().get(i).status = false;
          }
        }
      }
    }

    // 3
    for (Map.Entry<Integer, Map<Integer, List<PartitionKey>>> entry : rowIndexToPartsMap.entrySet()) {
      if (entry.getValue() != null){
        for (Map.Entry<Integer, List<PartitionKey>> entry2 : entry.getValue().entrySet()) {
          if (entry2.getValue() != null) {
            for (int i = 0; i < entry2.getValue().size(); i++) {
              if (rmPartitionIds.contains(entry2.getValue().get(i).getPartitionId())){
                entry2.getValue().get(i).status = false;
              }
            }
          }
        }
      }
    }
    return rmPartitionIds;
  }

  public void rmOneParameterServer_PSAgentMatrixMetaManager(int removedParameterServerIndex){
    LOG.info("rmOneParameterServer_PSAgentMatrixMetaManager");
    // 1
    for (Map.Entry<Integer, MatrixMeta>  entry : matrixMetaManager.getMatrixMetas().entrySet()) {
      for (Map.Entry<Integer, PartitionMeta> entry2: entry.getValue().getPartitionMetas().entrySet()){
        if (getMasterPS(entry2.getValue().getPartitionKey()).getIndex() == removedParameterServerIndex){
          entry2.getValue().getPartitionKey().status = false;
        }
      }
    }

    // 2
    for (Map.Entry<Integer, List<PartitionKey>> entry : matrixIdToPartsMap.entrySet()){
      if (entry.getValue() != null) {
        for (int i = 0; i < entry.getValue().size(); i++) {
          if (getMasterPS(entry.getValue().get(i)).getIndex() == removedParameterServerIndex){
            entry.getValue().get(i).status = false;
          }
        }
      }
    }
    // 3
    for (Map.Entry<Integer, Map<Integer, List<PartitionKey>>> entry : rowIndexToPartsMap.entrySet()) {
      if (entry.getValue() != null){
        for (Map.Entry<Integer, List<PartitionKey>> entry2 : entry.getValue().entrySet()) {
          if (entry2.getValue() != null) {
            for (int i = 0; i < entry2.getValue().size(); i++) {
              if (getMasterPS(entry2.getValue().get(i)).getIndex() == removedParameterServerIndex){
                entry2.getValue().get(i).status = false;
              }
            }
          }
        }
      }
    }
  }


  public void print_PSAgentMatrixMetaManager(){
    LOG.info("");
    LOG.info("");
    matrixMetaManager.print_MatrixMetaManager();
    LOG.info("");
    LOG.info("");
    print_matrixIdToPartsMap();
    LOG.info("");
    LOG.info("");
    print_rowIndexToPartsMap();
    LOG.info("");
    LOG.info("");
    partClockCache.print_ClockCache();
    LOG.info("");
    LOG.info("");
  }

  public void print_rowIndexToPartsMap(){
    LOG.info("print_rowIndexToPartsMap");
    for (Map.Entry<Integer, Map<Integer, List<PartitionKey>>> entry : rowIndexToPartsMap.entrySet()) {
      LOG.info("matrixId = " + entry.getKey());
      for (Map.Entry<Integer, List<PartitionKey>> entry2: entry.getValue().entrySet()) {
        LOG.info("  rowIndex = " + entry2.getKey());
        List<PartitionKey> PartitionKeyList= entry2.getValue();
        if (PartitionKeyList != null) {
          for (int i = 0; i< PartitionKeyList.size(); i++) {
            LOG.info("    partitionId = " + PartitionKeyList.get(i).getPartitionId());
            LOG.info("    partitionKey = " + PartitionKeyList.get(i).toString());
          }
        }
      }
    }
  }


  public void print_matrixIdToPartsMap(){
    LOG.info("print_matrixIdToPartsMap");
    for(Map.Entry<Integer, List<PartitionKey>> entry: matrixIdToPartsMap.entrySet()){
      LOG.info("matrixId = " + entry.getKey());
      List<PartitionKey> partitionKeyList = entry.getValue();
      if (partitionKeyList != null){
        for (int i = 0; i < partitionKeyList.size(); i++){
          LOG.info("  partitionId = " + partitionKeyList.get(i).getPartitionId());
          LOG.info("  partitionKey = " + partitionKeyList.get(i).toString());
        }
      }
    }
  }
  /* code end */

  /**
   * Add matrices meta
   *
   * @param matrixMetas matrices meta
   */
  public void addMatrices(List<MatrixMeta> matrixMetas) {
    int size = matrixMetas.size();
    for (int i = 0; i < size; i++) {
      addMatrix(matrixMetas.get(i));
    }
  }

  /**
   * Add matrix meta
   *
   * @param matrixMeta matrix meta
   */
  public void addMatrix(MatrixMeta matrixMeta) {
    matrixMeta.PartitionIdStart = matrixMeta.getPartitionMetas().size(); //////
    matrixMetaManager.addMatrix(matrixMeta);
    List<PartitionKey> partitions = getPartitions(matrixMeta.getId());
    partClockCache.addMatrix(matrixMeta.getId(), partitions);
  }

  /**
   * Remove matrix meta
   *
   * @param matrixId matrix id
   */
  public void removeMatrix(int matrixId) {
    matrixMetaManager.removeMatrix(matrixId);
    partClockCache.removeMatrix(matrixId);
  }

  /**
   * Get the server that hold the partition.
   *
   * @param partitionKey matrix partition key
   * @param sync         true means get from master, false means get from local cache
   * @return ParameterServerId server id
   */
  public List<ParameterServerId> getPss(PartitionKey partitionKey, boolean sync)
    throws ServiceException {
    if (!sync) {
      return getPss(partitionKey);
    } else {
      List<ParameterServerId> psIds = PSAgentContext.get().getMasterClient()
        .getStoredPss(partitionKey.getMatrixId(), partitionKey.getPartitionId());
      matrixMetaManager.setPss(partitionKey.getMatrixId(), partitionKey.getPartitionId(), psIds);
      return psIds;
    }
  }

  /**
   * Get partition location: includes stored pss and the location of the pss
   *
   * @param partitionKey partition information
   * @param sync         true means get from master; false means get from cache
   * @return partition location
   * @throws ServiceException
   */
  public PartitionLocation getPartLocation(PartitionKey partitionKey, boolean sync)
    throws ServiceException {
    if (!sync) {
      return getPartLocation(partitionKey);
    } else {
      return PSAgentContext.get().getMasterClient()
        .getPartLocation(partitionKey.getMatrixId(), partitionKey.getPartitionId());
    }
  }

  /**
   * Get partition location: includes stored pss and the location of the pss
   *
   * @param partitionKey partition information
   * @return partition location
   * @throws ServiceException
   */
  public PartitionLocation getPartLocation(PartitionKey partitionKey) {
    List<ParameterServerId> psIds = getPss(partitionKey);
    if (psIds == null) {
      return new PartitionLocation(new ArrayList<>());
    }

    int size = psIds.size();
    List<PSLocation> psLocs = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      psLocs.add(new PSLocation(psIds.get(i),
        PSAgentContext.get().getLocationManager().getPsLocation(psIds.get(i))));
    }
    return new PartitionLocation(psLocs);
  }

  /**
   * Get the server that hold the partition.
   *
   * @param partitionKey matrix partition key
   * @return ParameterServerId server id
   */
  public List<ParameterServerId> getPss(PartitionKey partitionKey) {
    return matrixMetaManager.getPss(partitionKey.getMatrixId(), partitionKey.getPartitionId());
  }

  /**
   * Get list of partitionKeys for matrixId and rowId.
   *
   * @param matrixId, matrixId
   * @param rowIndex, rowId
   * @return list of partitionKeys
   */
  public List<PartitionKey> getPartitions(int matrixId, int rowIndex) {
    Map<Integer, List<PartitionKey>> rowPartKeysCache = rowIndexToPartsMap.get(matrixId);
    if (rowPartKeysCache == null) {
      rowPartKeysCache = new ConcurrentHashMap<>();
      rowIndexToPartsMap.put(matrixId, rowPartKeysCache);
    }
    List<PartitionKey> rowParts = rowPartKeysCache.get(rowIndex);
    if (rowParts == null) {
      rowParts = getPartitionsFromMeta(matrixId, rowIndex);

      rowPartKeysCache.put(rowIndex, rowParts);
    }

    return rowParts;
  }

  private List<PartitionKey> getPartitionsFromMeta(int matrixId, int rowIndex) {
    List<PartitionKey> partitionKeys = new ArrayList<>();
    Iterator<PartitionMeta> iter =
      matrixMetaManager.getMatrixMeta(matrixId).getPartitionMetas().values().iterator();
    while (iter.hasNext()) {
      PartitionKey partitionKey = iter.next().getPartitionKey();
      if (partitionKey.getMatrixId() == matrixId && partitionKey.getStartRow() <= rowIndex
        && partitionKey.getEndRow() > rowIndex)
        partitionKeys.add(partitionKey);
    }
    partitionKeys.sort(new Comparator<PartitionKey>() {
      @Override public int compare(PartitionKey p1, PartitionKey p2) {
        if (p1.getStartCol() < p2.getStartCol()) {
          return -1;
        } else if (p1.getStartCol() > p2.getStartCol()) {
          return 1;
        } else {
          return 0;
        }
      }
    });
    return partitionKeys;
  }

  /**
   * Get list of partitionkeys belong to matrixId.
   *
   * @param matrixId, identity number of matrix requested
   * @return list of partitionKeys
   */
  public List<PartitionKey> getPartitions(int matrixId) {
    List<PartitionKey> partitions = matrixIdToPartsMap.get(matrixId);
    if (partitions == null) {
      partitions = getPartitionsFromMeta(matrixId);
      matrixIdToPartsMap.put(matrixId, partitions);
    }

    return partitions;
  }

  private List<PartitionKey> getPartitionsFromMeta(int matrixId) {
    List<PartitionKey> partitionKeys = new ArrayList<>();
    Iterator<PartitionMeta> iter =
      matrixMetaManager.getMatrixMeta(matrixId).getPartitionMetas().values().iterator();
    while (iter.hasNext()) {
      partitionKeys.add(iter.next().getPartitionKey());
    }
    partitionKeys.sort((PartitionKey p1, PartitionKey p2) -> {
      if (p1.getStartCol() < p2.getStartCol()) {
        return -1;
      } else if (p1.getStartCol() > p2.getStartCol()) {
        return 1;
      } else {
        return 0;
      }
    });
    return partitionKeys;
  }

  /**
   * Get the partitions the rows in.
   *
   * @param matrixId   matrix id
   * @param rowIndexes row indexes
   * @return Map<PartitionKey, List<Integer>> partitions to the rows contained in the partition map
   */
  public Map<PartitionKey, List<Integer>> getPartitionToRowsMap(int matrixId,
    List<Integer> rowIndexes) {
    Map<PartitionKey, List<Integer>> partToRowsMap = new HashMap<PartitionKey, List<Integer>>();

    int rowNum = rowIndexes.size();
    int partNum;
    for (int i = 0; i < rowNum; i++) {
      List<PartitionKey> partKeys = getPartitions(matrixId, rowIndexes.get(i));
      partNum = partKeys.size();
      for (int j = 0; j < partNum; j++) {
        List<Integer> rows = partToRowsMap.get(partKeys.get(j));
        if (rows == null) {
          rows = new ArrayList<>();
          partToRowsMap.put(partKeys.get(j), rows);
        }
        rows.add(rowIndexes.get(i));
      }
    }
    return partToRowsMap;
  }

  /**
   * Get the partitions the rows in.
   *
   * @param matrixId matrix id
   * @param rowIds   row indexes
   * @return Map<PartitionKey, List<Integer>> partitions to the rows contained in the partition map
   */
  public Map<PartitionKey, List<Integer>> getPartitionToRowsMap(int matrixId, int[] rowIds) {
    Map<PartitionKey, List<Integer>> partToRowsMap = new HashMap<PartitionKey, List<Integer>>();

    int rowNum = rowIds.length;
    int partNum;
    for (int i = 0; i < rowNum; i++) {
      List<PartitionKey> partKeys = getPartitions(matrixId, rowIds[i]);
      partNum = partKeys.size();
      for (int j = 0; j < partNum; j++) {
        List<Integer> rows = partToRowsMap.get(partKeys.get(j));
        if (rows == null) {
          rows = new ArrayList<>();
          partToRowsMap.put(partKeys.get(j), rows);
        }
        rows.add(rowIds[i]);
      }
    }
    return partToRowsMap;
  }

  /**
   * Get the partitions the rows in.
   *
   * @param rowIndex    matrix id and row indexes
   * @param batchNumber the split batch size.
   * @return Map<PartitionKey, List<Integer>> partitions to the rows contained in the partition map
   */
  public Map<PartitionKey, List<RowIndex>> getPartitionToRowIndexMap(RowIndex rowIndex,
    int batchNumber) {
    Map<PartitionKey, List<RowIndex>> partToRowIndexMap =
      new TreeMap<PartitionKey, List<RowIndex>>();
    if (rowIndex.getRowIds() == null) {
      return partToRowIndexMap;
    }

    IntOpenHashSet rowIdSet = rowIndex.getRowIds();
    IntOpenHashSet filtedRowIdSet = rowIndex.getFiltedIdSet();
    int[] rowIds = new int[rowIdSet.size() - filtedRowIdSet.size()];
    int count = 0;
    for (int rowId : rowIdSet) {
      if (!filtedRowIdSet.contains(rowId)) {
        rowIds[count++] = rowId;
      }
    }

    Arrays.sort(rowIds);

    int partNum = 0;
    for (int i = 0; i < rowIds.length; i++) {
      List<PartitionKey> partKeys = getPartitions(rowIndex.getMatrixId(), rowIds[i]);
      partNum = partKeys.size();
      for (int j = 0; j < partNum; j++) {
        PartitionKey partitionKey = partKeys.get(j);
        List<RowIndex> indexList = partToRowIndexMap.get(partitionKey);
        if (indexList == null) {
          indexList = new ArrayList<RowIndex>();
          partToRowIndexMap.put(partitionKey, indexList);
          indexList.add(new RowIndex(rowIndex.getMatrixId(), rowIndex));
        }

        RowIndex index = indexList.get(indexList.size() - 1);
        if (index.getRowsNumber() >= batchNumber) {
          index = new RowIndex(rowIndex.getMatrixId(), rowIndex);
          indexList.add(index);
        }
        index.addRowId(rowIds[i]);
      }
    }

    return partToRowIndexMap;
  }

  /**
   * Get the number of the partitions that contains the row.
   *
   * @param matrixId matrix id
   * @param rowIndex row index
   * @return the number of the partitions that contains the row
   */
  public int getRowPartitionSize(int matrixId, int rowIndex) {
    return getPartitions(matrixId, rowIndex).size();
  }

  /**
   * Clear the router table.
   */
  public void clear() {
    matrixIdToPartsMap.clear();
    rowIndexToPartsMap.clear();
    matrixMetaManager.clear();
  }

  /**
   * Get matrix id
   *
   * @param matrixName matrix name
   * @return matrix id
   */
  public int getMatrixId(String matrixName) {
    if (matrixMetaManager.exists(matrixName)) {
      return matrixMetaManager.getMatrixId(matrixName);
    } else {
      synchronized (this) {
        if (!matrixMetaManager.exists(matrixName)) {
          try {
            PSAgentContext.get().getPsAgent().refreshMatrixInfo();
          } catch (Throwable e) {
            LOG.error("get matrix information from master failed. ", e);
          }
        }
        if (matrixMetaManager.exists(matrixName)) {
          return matrixMetaManager.getMatrixId(matrixName);
        } else {
          return -1;
        }
      }
    }
  }

  /**
   * Get matrix meta
   *
   * @param matrixId matrix id
   * @return matrix meta
   */
  public MatrixMeta getMatrixMeta(int matrixId) {
    return matrixMetaManager.getMatrixMeta(matrixId);
  }

  /**
   * Get matrix meta
   *
   * @param matrixName matrix name
   * @return matrix meta
   */
  public MatrixMeta getMatrixMeta(String matrixName) {
    return matrixMetaManager.getMatrixMeta(matrixName);
  }

  /**
   * Get the master ps of a partition
   *
   * @param partKey partition key
   * @return the master ps of a partition
   */
  public ParameterServerId getMasterPS(PartitionKey partKey) {
    return matrixMetaManager.getMasterPs(partKey.getMatrixId(), partKey.getPartitionId());
  }

  /**
   * Get all matrices meta
   *
   * @return all matrices meta
   */
  public Map<Integer, MatrixMeta> getMatrixMetas() {
    return matrixMetaManager.getMatrixMetas();
  }

  /**
   * Is a matrix exist
   *
   * @param matrixId matrix id
   * @return true means exist
   */
  public boolean exist(int matrixId) {
    if (!matrixMetaManager.exists(matrixId)) {
      synchronized (this) {
        if (!matrixMetaManager.exists(matrixId)) {
          try {
            PSAgentContext.get().getPsAgent().refreshMatrixInfo();
          } catch (Throwable e) {
            LOG.error("get matrix information from master failed. ", e);
          }
        }
      }
    }
    return matrixMetaManager.exists(matrixId);
  }
}
