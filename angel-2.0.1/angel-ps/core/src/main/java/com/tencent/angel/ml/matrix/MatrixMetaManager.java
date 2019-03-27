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


package com.tencent.angel.ml.matrix;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ps.ParameterServerId;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The matrix meta manager.
 */
public class MatrixMetaManager {
  private static final Log LOG = LogFactory.getLog(MatrixMetaManager.class); //////
  /**
   * Matrix id to matrix meta map
   */
  private final ConcurrentHashMap<Integer, MatrixMeta> matrixIdToMetaMap;

  /**
   * Matrix name to matrix id map
   */
  private final ConcurrentHashMap<String, Integer> matrixNameToIdMap;

  /**
   * Creates a new matrix meta manager.
   */
  public MatrixMetaManager() {
    this.matrixIdToMetaMap = new ConcurrentHashMap<>();
    this.matrixNameToIdMap = new ConcurrentHashMap<>();
  }


  /* new code */
  public void resetParameterServers_idle_MatrixMetaManager(){
    for (Map.Entry<Integer, MatrixMeta> entry : matrixIdToMetaMap.entrySet()){
      entry.getValue().reassign_partitionMetas_idle();
    }
  }

  public void resetParameterServers_pre_MatrixMetaManager(Map<Integer, MatrixMeta> matrixMetas_pre){
    for (Map.Entry<Integer, MatrixMeta> entry : matrixIdToMetaMap.entrySet()){
      int matrixId = entry.getKey();
      if (matrixMetas_pre.containsKey(matrixId)){
        MatrixMeta matrixMeta_curr = entry.getValue();
        MatrixMeta matrixMeta_pre = matrixMetas_pre.get(matrixId);
        matrixMeta_curr.resetParameterServers_pre_MatrixMeta(matrixMeta_pre);
      }
    }
  }


  public void print_MatrixMetaManager(){
    LOG.info("print_MatrixMetaManager");
    // LOG.info("MatrixMetaManager_toString = " + matrixMetaManager.toString());
    for (Map.Entry<Integer, MatrixMeta> entry: matrixIdToMetaMap.entrySet()){
      LOG.info("matrixId = " + entry.getKey());
      entry.getValue().print_MatrixMeta();
    }
  }
  /* code end */
  /**
   * Add matrixes.
   *
   * @param matrixMetas the matrix metas
   */
  public void addMatrices(List<MatrixMeta> matrixMetas) {
    int size = matrixMetas.size();
    for (int i = 0; i < size; i++) {
      addMatrix(matrixMetas.get(i));
    }
  }

  /* new code */
  public Set<PartitionKey> removePartitions_pre(Map<Integer, Set<Integer>> matrixId2PartitionKeys_pre){
    Set<PartitionKey> partitionKeys_pre = new HashSet<PartitionKey>();
    for(Map.Entry<Integer, Set<Integer>> entry: matrixId2PartitionKeys_pre.entrySet()){
      int matrixId = entry.getKey();
      if (matrixIdToMetaMap.containsKey(matrixId)){
        matrixIdToMetaMap.get(matrixId).removePartitions_pre(entry.getValue(), partitionKeys_pre);
        if (matrixIdToMetaMap.get(matrixId).getPartitionMetas().size() == 0){
          matrixNameToIdMap.remove(matrixIdToMetaMap.get(matrixId).getName());
          matrixIdToMetaMap.remove(matrixId);
        }
      }
    }
    return partitionKeys_pre;
  }

  public void addMatrices_pre(List<MatrixMeta> matrixMetas_pre) {
    int size = matrixMetas_pre.size();
    for (int i = 0; i < size; i++){
      addMatrix_pre(matrixMetas_pre.get(i));
    }
  }

  public void addMatrices_idle(List<MatrixMeta> matrixMetas_idle) {
    int size = matrixMetas_idle.size();
    for (int i = 0; i < size; i++) {
      addMatrix_idle(matrixMetas_idle.get(i));
    }
  }

  public void adjustMatrices_idle(){
    for (Map.Entry<Integer, MatrixMeta> entry : matrixIdToMetaMap.entrySet()){
      entry.getValue().reassign_partitionMetas_idle();
    }
  }

  public void adjustMatrices_pre(){
    for (Map.Entry<Integer, MatrixMeta> entry : matrixIdToMetaMap.entrySet()){
      entry.getValue().clear_partitionMetas_repartition();
    }
  }
  /* code end */


  /**
   * Add matrix.
   *
   * @param matrixMeta the matrix meta
   */
  public void addMatrix(MatrixMeta matrixMeta) {
    this.matrixIdToMetaMap.putIfAbsent(matrixMeta.getId(), matrixMeta);
    this.matrixNameToIdMap.putIfAbsent(matrixMeta.getName(), matrixMeta.getId());
  }

  /* new code */
  public void addMatrix_idle(MatrixMeta matrixMeta_idle) {
    LOG.info("matrixMeta_idle.getName() = " + matrixMeta_idle.getName());
    LOG.info("matrixMeta_idle.getId() = " + matrixMeta_idle.getId());
    this.matrixNameToIdMap.putIfAbsent(matrixMeta_idle.getName(), matrixMeta_idle.getId());
    if (!this.matrixIdToMetaMap.containsKey(matrixMeta_idle.getId())){
      this.matrixIdToMetaMap.putIfAbsent(matrixMeta_idle.getId(), matrixMeta_idle);
    }
    this.matrixIdToMetaMap.get(matrixMeta_idle.getId()).partitionMetas_idle = matrixMeta_idle.partitionMetas_idle;
  }

  public void addMatrix_pre(MatrixMeta matrixMeta_pre) {
    LOG.info("matrixMeta_pre.getName() = " + matrixMeta_pre.getName());
    LOG.info("matrixMeta_pre.getId() = " + matrixMeta_pre.getId());
    this.matrixNameToIdMap.putIfAbsent(matrixMeta_pre.getName(), matrixMeta_pre.getId());
    this.matrixIdToMetaMap.putIfAbsent(matrixMeta_pre.getId(), matrixMeta_pre);
  }

  /* code end */

  /**
   * Remove a matrix
   *
   * @param matrixId matrix id
   */
  public void removeMatrix(int matrixId) {
    MatrixMeta meta = matrixIdToMetaMap.remove(matrixId);
    if (meta != null) {
      matrixNameToIdMap.remove(meta.getName());
    }
  }

  /**
   * Gets matrix id.
   *
   * @param matrixName the matrix name
   * @return the matrix id
   */
  public int getMatrixId(String matrixName) {
    if (matrixNameToIdMap.containsKey(matrixName)) {
      return matrixNameToIdMap.get(matrixName);
    } else {
      return -1;
    }
  }

  /**
   * Gets matrix meta.
   *
   * @param matrixId the matrix id
   * @return the matrix meta
   */
  public MatrixMeta getMatrixMeta(int matrixId) {
    return matrixIdToMetaMap.get(matrixId);
  }

  /**
   * Gets matrix meta.
   *
   * @param matrixName the matrix name
   * @return the matrix meta
   */
  public MatrixMeta getMatrixMeta(String matrixName) {
    int matrixId = getMatrixId(matrixName);
    if (matrixId == -1) {
      return null;
    } else {
      return matrixIdToMetaMap.get(matrixId);
    }
  }

  /**
   * Gets matrix ids.
   *
   * @return the matrix ids
   */
  public Set<Integer> getMatrixIds() {
    return matrixIdToMetaMap.keySet();
  }

  /**
   * Gets matrix names.
   *
   * @return the matrix names
   */
  public Set<String> getMatrixNames() {
    return matrixNameToIdMap.keySet();
  }

  /**
   * Is a matrix exist
   *
   * @param matrixName matrix name
   * @return true means exist
   */
  public boolean exists(String matrixName) {
    return matrixNameToIdMap.containsKey(matrixName);
  }

  /**
   * Is a matrix exist
   *
   * @param matrixId matrix id
   * @return true means exist
   */
  public boolean exists(int matrixId) {
    return matrixIdToMetaMap.containsKey(matrixId);
  }

  /**
   * Get all matrices meta
   *
   * @return all matrices meta
   */
  public Map<Integer, MatrixMeta> getMatrixMetas() {
    return matrixIdToMetaMap;
  }

  /**
   * Get the stored pss for a matrix partition
   *
   * @param matrixId matrix id
   * @param partId   partition id
   * @return the stored pss
   */
  public List<ParameterServerId> getPss(int matrixId, int partId) {
    MatrixMeta matrixMeta = matrixIdToMetaMap.get(matrixId);
    if (matrixMeta == null) {
      return null;
    }

    return matrixMeta.getPss(partId);
  }

  /**
   * Remove the matrix
   *
   * @param matrixName matrix name
   */
  public void removeMatrix(String matrixName) {
    if (matrixNameToIdMap.containsKey(matrixName)) {
      int matrixId = matrixNameToIdMap.remove(matrixName);
      matrixIdToMetaMap.remove(matrixId);
    }
  }

  /**
   * Remove all matrices
   */
  public void clear() {
    matrixIdToMetaMap.clear();
    matrixNameToIdMap.clear();
  }

  /**
   * Get the master stored ps for a matrix partition
   *
   * @param matrixId matrix id
   * @param partId   partition id
   * @return the master stored ps
   */
  public ParameterServerId getMasterPs(int matrixId, int partId) {
    MatrixMeta matrixMeta = matrixIdToMetaMap.get(matrixId);
    if (matrixMeta == null) {
      return null;
    }

    return matrixMeta.getMasterPs(partId);
  }

  /**
   * Remove matrices
   *
   * @param matrixIds matrix ids
   */
  public void removeMatrices(List<Integer> matrixIds) {
    int size = matrixIds.size();
    for (int i = 0; i < size; i++) {
      removeMatrix(matrixIds.get(i));
    }
  }

  /**
   * Remove the stored ps for all matrix partitions
   *
   * @param psId ps id
   */
  public void removePs(ParameterServerId psId) {
    for (MatrixMeta matrixMeta : matrixIdToMetaMap.values()) {
      matrixMeta.removePs(psId);
    }
  }

  /**
   * Set the stored pss for a matrix partition
   *
   * @param matrixId matrix id
   * @param partId   partition id
   * @param psIds    the stored pss
   */
  public void setPss(int matrixId, int partId, List<ParameterServerId> psIds) {
    MatrixMeta matrixMeta = matrixIdToMetaMap.get(matrixId);
    if (matrixMeta == null) {
      return;
    }

    matrixMeta.setPss(partId, psIds);
  }

  /**
   * Add a the stored ps for a matrix partition
   *
   * @param matrixId matrix id
   * @param partId
   * @param psId     ps id
   */
  public void addPs(int matrixId, int partId, ParameterServerId psId) {
    MatrixMeta matrixMeta = matrixIdToMetaMap.get(matrixId);
    if (matrixMeta == null) {
      return;
    }

    matrixMeta.addPs(partId, psId);
  }

  /**
   * Get estimate sparsity
   *
   * @param matrixId matrix id
   * @return estimate sparsity
   */
  public double getEstSparsity(int matrixId) {
    MatrixMeta meta = matrixIdToMetaMap.get(matrixId);
    if (meta == null) {
      return 1.0;
    } else {
      return meta.getEstSparsity();
    }
  }
}
