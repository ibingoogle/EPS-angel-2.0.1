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
import com.tencent.angel.conf.MatrixConf;
import com.tencent.angel.ps.PSContext;
import com.tencent.angel.ps.ParameterServerId;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;

/**
 * The meta of matrix.
 */
public class MatrixMeta {
  private static final Log LOG = LogFactory.getLog(MatrixMeta.class); //////
  /**
   * Matrix basic parameters
   */
  private final MatrixContext matrixContext;

  /**
   * Matrix partitions parameters
   * parititionId to partitionMeta
   */
  private final Map<Integer, PartitionMeta> partitionMetas;

  /* new code */
  // told workers and non-removed servers
  public Map<Integer, PartitionMeta> partitionMetas_idle = new HashMap<Integer, PartitionMeta>();
  public int PartitionIdStart = 0;
  // told removed servers
  public Map<Integer, Map<Integer, PartitionMeta>> partitionMetas_repartition = new HashMap<Integer, Map<Integer, PartitionMeta>>();
  /* code end */

  /**
   * Create a MatrixMeta
   *
   * @param mContext matrix context
   */
  public MatrixMeta(MatrixContext mContext) {
    this(mContext, new HashMap<>());
  }

  /**
   * Create a MatrixMeta
   *
   * @param matrixContext  matrix context
   * @param partitionMetas matrix partitions meta
   */
  public MatrixMeta(MatrixContext matrixContext, Map<Integer, PartitionMeta> partitionMetas) {
    this.matrixContext = matrixContext;
    this.partitionMetas = partitionMetas;
  }

  /* new code */


  public void removePartitions_pre(Set<Integer> partitions_pre, Set<PartitionKey> partitionKeys_pre){
    for(Integer partition_pre: partitions_pre){
      if (partitionMetas.containsKey(partition_pre)) {
        PartitionKey partitionKey_pre = partitionMetas.get(partition_pre).partitionKey;
        partitionKeys_pre.add(partitionKey_pre);
        partitionMetas.remove(partition_pre);
      }
    }
  }

  public void reassign_partitionMetas_idle(){
    for (Map.Entry<Integer, PartitionMeta> entry: partitionMetas_idle.entrySet()){
      partitionMetas.put(entry.getKey(), entry.getValue());
    }
    partitionMetas_idle.clear();
  }

  public void resetParameterServers_pre_MatrixMeta(MatrixMeta matrixMeta_pre, Set<Integer> rmPartitionIds){
    for(Integer rmPartitionId: rmPartitionIds){
      partitionMetas.remove(rmPartitionId);
    }
    for(Map.Entry<Integer, PartitionMeta> entry: matrixMeta_pre.getPartitionMetas().entrySet()){
      partitionMetas.put(entry.getKey(), entry.getValue());
    }
    refreshPartitionIdStart();
  }

  public void refreshPartitionIdStart(){
    int maxPartitionId = Integer.MIN_VALUE;
    for(Map.Entry<Integer, PartitionMeta> entry: partitionMetas.entrySet()){
      maxPartitionId = Math.max(maxPartitionId, entry.getKey());
    }
    PartitionIdStart = maxPartitionId + 1;
  }

  public void clear_partitionMetas_repartition(){
    partitionMetas_repartition.clear();
  }

  public void print_MatrixMeta() {
    LOG.info("print_MatrixMeta");
    LOG.info("PartitionIdStart = " + PartitionIdStart);
    // LOG.info("    MatrixMeta_toString = " + toString());
    // 1
    LOG.info("partitionMetas => ");
    if (partitionMetas != null) {
      for (Map.Entry<Integer, PartitionMeta> entry : partitionMetas.entrySet()) {
        LOG.info("    partitionId = " + entry.getKey());
        LOG.info("    PartitionMeta = " + entry.getValue().toString());
      }
    }
    // 2
    LOG.info("partitionMetas_idle => ");
    if (partitionMetas_idle != null) {
      for (Map.Entry<Integer, PartitionMeta> entry : partitionMetas_idle.entrySet()) {
        LOG.info("  partitionId_idle = " + entry.getKey());
        LOG.info("  PartitionMeta_idle = " + entry.getValue().toString());
      }
    }
    // 3
    LOG.info("partitionMetas_repartition => ");
    if (partitionMetas_repartition != null) {
      for (Map.Entry<Integer, Map<Integer, PartitionMeta>> entry : partitionMetas_repartition.entrySet()) {
        LOG.info("  pre partitionId = " + entry.getKey());
        for (Map.Entry<Integer, PartitionMeta> entry2 : entry.getValue().entrySet()) {
          LOG.info("  repartitioned partitionId = " + entry2.getKey());
          LOG.info("  repartitioned partitionMeta = " + entry2.getValue().toString());
        }
      }
    }
  }
  /* code end */

  /**
   * Get matrix id
   *
   * @return the id
   */
  public int getId() {
    return matrixContext.getMatrixId();
  }

  /**
   * Gets row num.
   *
   * @return the row num
   */
  public int getRowNum() {
    return matrixContext.getRowNum();
  }

  /**
   * Gets col num.
   *
   * @return the col num
   */
  public long getColNum() {
    return matrixContext.getColNum();
  }

  /**
   * Get number of non-zero elements
   *
   * @return number of non-zero elements
   */
  public long getValidIndexNum() {
    return matrixContext.getValidIndexNum();
  }

  /**
   * Gets name.
   *
   * @return the name
   */
  public String getName() {
    return matrixContext.getName();
  }

  /**
   * Gets row type.
   *
   * @return the row type
   */
  public RowType getRowType() {
    return matrixContext.getRowType();
  }

  /**
   * Gets attribute.
   *
   * @param key   the key
   * @param value the default value
   * @return the attribute
   */
  public String getAttribute(String key, String value) {
    if (!matrixContext.getAttributes().containsKey(key))
      return value;
    return matrixContext.getAttributes().get(key);
  }

  /**
   * Gets attribute.
   *
   * @param key the key
   * @return the attribute
   */
  public String getAttribute(String key) {
    return matrixContext.getAttributes().get(key);
  }

  /**
   * Is average.
   *
   * @return the result
   */
  public boolean isAverage() {
    String average = getAttribute(MatrixConf.MATRIX_AVERAGE, MatrixConf.DEFAULT_MATRIX_AVERAGE);
    return Boolean.parseBoolean(average);
  }

  /**
   * Is hogwild.
   *
   * @return the result
   */
  public boolean isHogwild() {
    String hogwild = getAttribute(MatrixConf.MATRIX_HOGWILD, MatrixConf.DEFAULT_MATRIX_HOGWILD);
    return Boolean.parseBoolean(hogwild);
  }

  /**
   * Gets staleness.
   *
   * @return the staleness
   */
  public int getStaleness() {
    return Integer.parseInt(getAttribute(MatrixConf.MATRIX_STALENESS, "0"));
  }

  /**
   * Get partitions meta
   *
   * @return all partitions meta
   */
  public Map<Integer, PartitionMeta> getPartitionMetas() {
    return partitionMetas;
  }

  /* new code */
  public Map<Integer, PartitionMeta> getPartitionMetas_idle() {
    return partitionMetas_idle;
  }
  /* code end */

  /**
   * Get matrix context
   *
   * @return matrix context
   */
  public MatrixContext getMatrixContext() {
    return matrixContext;
  }

  /**
   * Add meta for a partition
   *
   * @param id   partition id
   * @param meta partition meta
   */
  public void addPartitionMeta(int id, PartitionMeta meta) {
    partitionMetas.put(id, meta);
  }

  /* new code */
  /**
   * Add meta for a partition for partition_idle
   *
   * @param id   partition id
   * @param meta partition meta
   */
  public void addPartitionMeta_idle(int id, PartitionMeta meta) {
    partitionMetas_idle.put(id, meta);
  }
  /**
   * Add meta for a partition for partition_repartition
   *
   * @param id   partition id
   * @param meta partition meta
   */
  public void addPartitionMeta_repartition(int id, PartitionMeta meta) {
    int prePartitionId = meta.prePartitionId;
    if (!partitionMetas_repartition.containsKey(prePartitionId)){
      partitionMetas_repartition.put(prePartitionId, new HashMap<Integer, PartitionMeta>());
    }
    partitionMetas_repartition.get(prePartitionId).put(id, meta);
  }
  /* code end */

  /**
   * Get meta for a partition
   *
   * @param partId partition id
   * @return partition meta
   */
  public PartitionMeta getPartitionMeta(int partId) {
    return partitionMetas.get(partId);
  }

  /**
   * Get the stored pss for a partition
   *
   * @param partId partition id
   * @return the stored pss
   */
  public List<ParameterServerId> getPss(int partId) {
    PartitionMeta partitionMeta = partitionMetas.get(partId);
    if (partitionMeta == null) {
      return null;
    }
    return partitionMeta.getPss();
  }

  /**
   * Set the stored pss for a partition
   *
   * @param partId partition id
   * @param psIds  the stored pss
   */
  public void setPss(int partId, List<ParameterServerId> psIds) {
    PartitionMeta partitionMeta = partitionMetas.get(partId);
    if (partitionMeta == null) {
      return;
    }
    partitionMeta.setPss(psIds);
  }

  /**
   * Get the master stored ps for the partition
   *
   * @param partId partition id
   * @return the master stored ps
   */
  public ParameterServerId getMasterPs(int partId) {
    PartitionMeta partitionMeta = partitionMetas.get(partId);
    if (partitionMeta == null) {
      return null;
    }
    return partitionMeta.getMasterPs();
  }

  /**
   * Get matrix attributes
   *
   * @return matrix attributes
   */
  public Map<String, String> getAttributes() {
    return matrixContext.getAttributes();
  }

  /**
   * Get the block row number for the matrix
   *
   * @return the block row number for the matrix
   */
  public int getBlockRowNum() {
    return matrixContext.getMaxRowNumInBlock();
  }

  /**
   * Get the block column number for the matrix
   *
   * @return the block column number for the matrix
   */
  public long getBlockColNum() {
    return matrixContext.getMaxColNumInBlock();
  }

  @Override public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("MatrixContext:").append(matrixContext).append("\n");
    sb.append("partitions:").append("\n");
    List<PartitionMeta> parts = new ArrayList<>(partitionMetas.values());
    parts.sort((PartitionMeta p1, PartitionMeta p2) -> p1.getPartId() - p2.getPartId());
    int size = parts.size();
    sb.append("total partitoin number:" + size).append("\n");
    for (int i = 0; i < size; i++) {
      sb.append("partition ").append(parts.get(i).getPartId()).append(":").append(parts.get(i))
        .append("\n");
    }

    return sb.toString();
  }

  /**
   * Remove the stored ps for all partitions
   *
   * @param psId ps id
   */
  public void removePs(ParameterServerId psId) {
    for (PartitionMeta partMeta : partitionMetas.values()) {
      partMeta.removePs(psId);
    }
  }

  /**
   * Add the stored ps for the partition
   *
   * @param partId partition id
   * @param psId   ps id
   */
  public void addPs(int partId, ParameterServerId psId) {
    PartitionMeta partitionMeta = partitionMetas.get(partId);
    if (partitionMeta == null) {
      return;
    }
    partitionMeta.addReplicationPS(psId);
  }

  /**
   * Get estimate sparsity
   *
   * @return estimate sparsity
   */
  public double getEstSparsity() {
    return matrixContext.getEstSparsity();
  }
}
