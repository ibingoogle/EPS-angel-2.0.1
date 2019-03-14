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


package com.tencent.angel.common.location;

import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.psagent.PSAgentId;
import com.tencent.angel.worker.WorkerId;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Location manager
 */
public class LocationManager {
  private static final Log LOG = LogFactory.getLog(LocationManager.class); //////
  /**
   * Master location
   */
  private volatile Location masterLocation;

  /**
   * PS locations
   */
  /* old code
  private final Map<ParameterServerId, Location> psIdToLocMap;
  /* new code */
  public final Map<ParameterServerId, Location> psIdToLocMap;
  /* code end */

  /**
   * Worker locations
   */
  private final Map<WorkerId, Location> workerIdToLocMap;

  /**
   * All ps ids
   */
  /* old code
  private volatile ParameterServerId[] psIds;
  /* new code */
  public volatile ParameterServerId[] psIds;
  /* code end */

  /**
   * Create a location manager
   */
  public LocationManager() {
    this.masterLocation = null;
    this.psIdToLocMap = new ConcurrentHashMap<>();
    this.workerIdToLocMap = new ConcurrentHashMap<>();
  }

  /* new code */

  public void rmOneParameterServer_LocationManager(int removedParameterServerIndex){
    LOG.info("rmOneParameterServer_LocationManager");
    for (Map.Entry<ParameterServerId, Location> entry : psIdToLocMap.entrySet()){
      if (entry.getKey().getIndex() == removedParameterServerIndex) {
        psIdToLocMap.remove(entry.getKey());
        break;
      }
    }
    ParameterServerId[] newpsIds = new ParameterServerId[psIds.length - 1];
    int index = 0;
    for (int i = 0; i < newpsIds.length; i++){
      if (psIds[index].getIndex() == removedParameterServerIndex) {
        index++;
      }
      newpsIds[i] = psIds[index];
      index++;
    }
    psIds = null;
    psIds = newpsIds;
  }


  public void print_LocationManager(){
    LOG.info("print_LocationManager");
    LOG.info("");
    LOG.info("");
    if (masterLocation != null) LOG.info("masterLocation.toString()" + masterLocation.toString());
    LOG.info("");
    LOG.info("");
    print_psIdToLocMap();
    LOG.info("");
    LOG.info("");
    print_workerIdToLocMap();
    LOG.info("");
    LOG.info("");
    print_psIds();
    LOG.info("");
    LOG.info("");

  }

  public void print_psIdToLocMap(){
    LOG.info("print_psIdToLocMap");
    if (psIdToLocMap != null) {
      for(Map.Entry<ParameterServerId, Location> entry: psIdToLocMap.entrySet()) {
        LOG.info("  ParameterServerIndex = " + entry.getKey().getIndex());
        LOG.info("  ParameterServerId.toString() = " + entry.getKey().toString());
        LOG.info("  location = " + entry.getValue().toString());
      }
    }
  }

  public void print_workerIdToLocMap(){
    LOG.info("print_workerIdToLocMap");
    if (workerIdToLocMap != null) {
      for(Map.Entry<WorkerId, Location> entry: workerIdToLocMap.entrySet()) {
        LOG.info("  WorkerIndex = " + entry.getKey().getIndex());
        LOG.info("  WorkerId.toString() = " + entry.getKey().toString());
        LOG.info("  location = " + entry.getValue().toString());
      }
    }
  }

  public void print_psIds(){
    LOG.info("print_psIds");
    if (psIds != null && psIds.length > 0){
      for (int i = 0; i< psIds.length; i++){
        LOG.info("  ParameterServerIndex = " + psIds[i].getIndex());
        LOG.info("  ParameterServerId.toString() = " + psIds[i].toString());
      }
    }
  }
  /* code end */

  /**
   * Get master location
   *
   * @return master location
   */
  public Location getMasterLocation() {
    return masterLocation;
  }

  /**
   * Get a ps location
   *
   * @param psId ps id
   * @return ps location
   */
  public Location getPsLocation(ParameterServerId psId) {
    return psIdToLocMap.get(psId);
  }

  /**
   * Get a worker location
   *
   * @param workerId worker id
   * @return worker location
   */
  public Location getWorkerLocation(WorkerId workerId) {
    return workerIdToLocMap.get(workerId);
  }

  /**
   * Set master location
   *
   * @param masterLocation master location
   */
  public void setMasterLocation(Location masterLocation) {
    this.masterLocation = masterLocation;
  }

  /**
   * Set a ps location
   *
   * @param psId ps id
   * @param loc  ps location
   */
  public void setPsLocation(ParameterServerId psId, Location loc) {
    if (loc == null) {
      psIdToLocMap.remove(psId);
    } else {
      psIdToLocMap.put(psId, loc);
    }
  }

  /**
   * Set worker location
   *
   * @param workerId worker id
   * @param loc      worker location
   */
  public void setWorkerLocation(WorkerId workerId, Location loc) {
    workerIdToLocMap.put(workerId, loc);
  }

  /**
   * Set all ps ids
   *
   * @param psIds all ps ids
   */
  public void setPsIds(ParameterServerId[] psIds) {
    this.psIds = psIds;
  }

  /**
   * Get all ps ids
   *
   * @return all ps ids
   */
  public ParameterServerId[] getPsIds() {
    return psIds;
  }

  public void setPSAgentLocation(PSAgentId psAgentId, Location location) {
  }

  /**
   * Are all pss registered
   *
   * @return true mean all pss have registered to master
   */
  public boolean isAllPsRegisted() {
    return (psIds != null) && (psIds.length == psIdToLocMap.size());
  }

  /**
   * Get all ps locations
   *
   * @return all ps locations
   */
  public Map<ParameterServerId, Location> getPsLocations() {
    return psIdToLocMap;
  }
}
