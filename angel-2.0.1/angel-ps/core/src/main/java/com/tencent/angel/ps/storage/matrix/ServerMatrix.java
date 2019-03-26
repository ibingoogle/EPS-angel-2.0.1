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


package com.tencent.angel.ps.storage.matrix;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.PartitionMeta;
import com.tencent.angel.ps.PSContext;
import com.tencent.angel.ps.storage.vector.ServerRow;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;

/**
 * The Server matrix on parameter server,assigned by {@link com.tencent.angel.master.AngelApplicationMaster},which represents a set of partitions of matrix
 */
public class ServerMatrix {

  private final static Log LOG = LogFactory.getLog(ServerMatrix.class);

  /**
   * Mapping from matrix PartitionKey.partitionId to ServerPartition.
   */
  private final HashMap<Integer, ServerPartition> partitionMaps;

  /* new code */
  private final HashMap<Integer, ServerPartition> partitionMaps_idle;
  /* code end */

  private final int matrixId;

  private final String matrixName;

  private final PSContext context;

  /**
   * The partitions in this PS
   */
  //private final List<PartitionKey> partitionKeys;

  /**
   * Create a new Server matrix by matrix partition.
   *
   * @param matrixMeta the matrix partition contains a set of partitions, which need to load on Parameter Server
   */
  public ServerMatrix(MatrixMeta matrixMeta, PSContext context) {
    this.context = context;

    LOG.info(
      "Creating a Server Matrix, id: " + matrixMeta.getId() + ", name: " + matrixMeta.getName());
    partitionMaps = new HashMap<>(matrixMeta.getPartitionMetas().size());
    partitionMaps_idle = new HashMap<>(); //////
    matrixId = matrixMeta.getId();
    matrixName = matrixMeta.getName();
  }

  /* new code */
  public void saveRemovedPartitions_pre(Set<Integer> partitions_pre, PSContext context){
    for(Integer partition_pre: partitions_pre){
      if (partitionMaps.containsKey(partition_pre)){
        ServerPartition part = partitionMaps.get(partition_pre);
        LOG.info("ServerPartition in saveRemovedPartitions_pre => " + part.toString());
        part.save_removedValues(context);
        partitionMaps.remove(partition_pre);
      }
    }
  }


  public void print_ServerMatrix(){
    LOG.info("print_ServerMatrix");
    LOG.info("partitionMaps =>");
    for (Map.Entry<Integer, ServerPartition> entry : partitionMaps.entrySet()) {
      LOG.info("partitionId = " + entry.getKey());
      entry.getValue().print_ServerPartition();
    }
    LOG.info("partitionMaps_idle =>");
    for (Map.Entry<Integer, ServerPartition> entry : partitionMaps_idle.entrySet()) {
      LOG.info("partitionId_idle = " + entry.getKey());
      entry.getValue().print_ServerPartition();
    }
  }


  public void init_idle(PSContext context) {
    LOG.info("init_idle() ServerMatirx.java, MatrixId = " + matrixId + ", name = " + matrixName);
    MatrixMeta matrixMeta = context.getMatrixMetaManager().getMatrixMeta(matrixId);
    Map<Integer, PartitionMeta> partMetas_idle = matrixMeta.getPartitionMetas_idle();
    LOG.info("partMetas_idle size = " + partMetas_idle.size());
    String sourceClass = matrixMeta.getAttribute(AngelConf.ANGEL_PS_PARTITION_SOURCE_CLASS,
            AngelConf.DEFAULT_ANGEL_PS_PARTITION_SOURCE_CLASS);
    LOG.info("sourceClass in init_idle() = " + sourceClass);
    for (PartitionMeta partMeta_idle : partMetas_idle.values()) {
      ServerPartition part = new ServerPartition(partMeta_idle.getPartitionKey(), matrixMeta.getRowType(),
              matrixMeta.getEstSparsity(), sourceClass);
      part.loadAndSavePath.put(0, partMeta_idle.savePath);
      partitionMaps.put(partMeta_idle.getPartId(), part);
      part.init();
      part.setState(PartitionState.READ_AND_WRITE);
      part.load_values(context);
    }
  }

  public void init_pre(PSContext context) {
    LOG.info("init_pre() ServerMatirx.java, MatrixId = " + matrixId + ", name = " + matrixName);
    MatrixMeta matrixMeta = context.getMatrixMetaManager().getMatrixMeta(matrixId);
    Map<Integer, PartitionMeta> partMetas = matrixMeta.getPartitionMetas();
    LOG.info("partMetas size = " + partMetas.size());
    String sourceClass = matrixMeta.getAttribute(AngelConf.ANGEL_PS_PARTITION_SOURCE_CLASS,
            AngelConf.DEFAULT_ANGEL_PS_PARTITION_SOURCE_CLASS);
    LOG.info("sourceClass in init_pre() = " + sourceClass);
    for (PartitionMeta partMeta : partMetas.values()) {
      ServerPartition part = new ServerPartition(partMeta.getPartitionKey(), matrixMeta.getRowType(),
              matrixMeta.getEstSparsity(), sourceClass);
      List<String> loadPaths = new ArrayList<>();
      for(Map.Entry<Integer, PartitionMeta> entry: matrixMeta.partitionMetas_repartition.get(partMeta.getPartId()).entrySet()){
        loadPaths.add(entry.getValue().savePath);
      }
      part.loadPaths.put(0, loadPaths);
      partitionMaps.put(partMeta.getPartId(), part);
      part.init();
      part.setState(PartitionState.READ_AND_WRITE);
      part.load_values_fromPaths(context);
    }
  }
  /* code end */

  public void init() {
    LOG.info("init() ServerMatirx.java, MatrixId = " + matrixId + ", name = " + matrixName); //////
    MatrixMeta matrixMeta = context.getMatrixMetaManager().getMatrixMeta(matrixId);
    Map<Integer, PartitionMeta> partMetas = matrixMeta.getPartitionMetas();

    String sourceClass = matrixMeta.getAttribute(AngelConf.ANGEL_PS_PARTITION_SOURCE_CLASS,
        AngelConf.DEFAULT_ANGEL_PS_PARTITION_SOURCE_CLASS);

    LOG.info("sourceClass in idle() = " + sourceClass); //////

    for (PartitionMeta partMeta : partMetas.values()) {
      ServerPartition part = new ServerPartition(partMeta.getPartitionKey(), matrixMeta.getRowType(),
          matrixMeta.getEstSparsity(), sourceClass);
      partitionMaps.put(partMeta.getPartId(), part);
      part.init();
      part.setState(PartitionState.READ_AND_WRITE);
    }
  }

  /**
   * Gets partition specified by partition id
   *
   * @param partId the part id
   * @return the partition
   */
  public ServerPartition getPartition(int partId) {
    return partitionMaps.get(partId);
  }

  /**
   * Gets the matrix name.
   *
   * @return the name
   */
  public String getName() {
    return matrixName;
  }


  /**
   * Gets the matrix id.
   *
   * @return the id
   */
  public int getId() {
    return matrixId;
  }

  /**
   * Get row split
   *
   * @param partId partition id
   * @param rowId  row index
   * @return
   */
  public ServerRow getRow(int partId, int rowId) {
    ServerPartition part = getPartition(partId);
    if (part == null) {
      return null;
    }
    return part.getRow(rowId);
  }

  /**
   * Get all partitions in this ServerMatrix
   * @return all partitions in this ServerMatrix
   */
  public Map<Integer, ServerPartition> getPartitions() {
    return partitionMaps;
  }


  public void startServering() {
    for (ServerPartition part : partitionMaps.values()) {
      part.setState(PartitionState.READ_AND_WRITE);
    }
  }
}
