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


package com.tencent.angel.ml.core.network.layers.verge

import java.util.concurrent.Future

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.core.conf.{MLConf, SharedConf}
import com.tencent.angel.ml.core.network.layers._
import com.tencent.angel.ml.core.network.transfunc.TransFunc
import com.tencent.angel.ml.core.optimizer.{OptUtils, Optimizer}
import com.tencent.angel.ml.core.utils.{NetUtils, PSMatrixUtils}
import com.tencent.angel.ml.math2.matrix._
import com.tencent.angel.ml.math2.ufuncs.Ufuncs
import com.tencent.angel.ml.math2.utils.VectorUtils
import com.tencent.angel.ml.math2.vector._
import com.tencent.angel.ml.math2.{MFactory, VFactory}
import com.tencent.angel.ml.matrix.psf.update.RandomNormal
import com.tencent.angel.ml.matrix.psf.update.base.VoidResult
import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.model.{MatrixLoadContext, MatrixSaveContext, ModelLoadContext, ModelSaveContext}
import com.tencent.angel.psagent.PSAgentContext
import org.apache.commons.logging.LogFactory


class SimpleInputLayer(name: String, outputDim: Int, transFunc: TransFunc, override val optimizer: Optimizer)(implicit graph: AngelGraph)
  extends InputLayer(name, outputDim)(graph) with Trainable with Serializable {
  val LOG = LogFactory.getLog(classOf[SimpleInputLayer])

  graph.addTrainable(this)

  val sharedConf: SharedConf = graph.conf

  val parallel = sharedConf.get(MLConf.ML_MATRIX_DOT_USE_PARALLEL_EXECUTOR).toBoolean
  val modelType: RowType = SharedConf.modelType
  val valueType: String = SharedConf.valueType()
  val inputDataFormat: String = SharedConf.inputDataFormat
  val mode = SharedConf.runningMode()
  val modelsize = SharedConf.modelSize


  private val numSlot = OptUtils.getSlotNum(optimizer)

  private val weightCtx: MatrixContext = (inputDataFormat, NetUtils.storageType(modelType)) match {
    case ("dense", "dense" | "component_dense") => // dense data, dense model
      // in this condition, all the parameters are stored in one row
      LOG.info("1"); //////
      val psRows: Int = numSlot + 1
      val psCols = SharedConf.indexRange * outputDim
      LOG.info("outputDim = " + outputDim); //////
      LOG.info("SlotNum = " + numSlot); //////
      LOG.info("psRows = " + psRows); //////
      LOG.info("psCols = " + psCols); //////
      PSMatrixUtils.createPSMatrixCtx(s"${name}_weight", psRows, psCols, modelType)
    // in this condition, the shape of weight matrix is (inputDim, outputDim)
    // and inputDim = SharedConf.indexRange
    case ("libsvm" | "dummy", "dense" | "component_dense") => // sparse data, dense model
      LOG.info("2"); //////
      val psRows: Int = outputDim * (numSlot + 1)
      val psCols = SharedConf.indexRange
      LOG.info("outputDim = " + outputDim); //////
      LOG.info("SlotNum = " + numSlot); //////
      LOG.info("psRows = " + psRows); //////
      LOG.info("psCols = " + psCols); //////
      PSMatrixUtils.createPSMatrixCtx(s"${name}_weight", psRows, psCols, modelType)
    // in this condition, the shape of weight matrix is (outputDim, inputDim)
    // and inputDim = SharedConf.indexRange
    case ("libsvm" | "dummy", "sparse" | "component_sparse") => // sparse data, sparse model
      LOG.info("3"); //////
      val psRows: Int = outputDim * (numSlot + 1)
      val psCols = SharedConf.indexRange
      LOG.info("outputDim = " + outputDim); //////
      LOG.info("SlotNum = " + numSlot); //////
      LOG.info("psRows = " + psRows); //////
      LOG.info("psCols = " + psCols); //////
      val wCtx = PSMatrixUtils.createPSMatrixCtx(s"${name}_weight", psRows, psCols, modelType)
      // in this condition, the shape of weight matrix is (outputDim, inputDim)
      // and inputDim = SharedConf.indexRange
      wCtx.setValidIndexNum(modelsize)
      wCtx
    case _ => // dense data, sparse model
      throw new AngelException("Dense data, sparse model, pls. change model to dense")
  }

  private val biasCtx = PSMatrixUtils.createPSMatrixCtx(s"${name}_bias", 1, outputDim, SharedConf.denseModelType)
  graph.addMatrixCtx(weightCtx)
  graph.addMatrixCtx(biasCtx)

  lazy val weightId: Int = PSMatrixUtils.getMatrixId(s"${name}_weight")
  lazy val biasId: Int = PSMatrixUtils.getMatrixId(s"${name}_bias")

  @transient var forward: Matrix = _ // dense
  // dense
  @transient var backward: Matrix = _ // dense
  // dense
  @transient var output: Matrix = _ // dense

  @transient var weight: Matrix = _
  // ??
  @transient var bias: Vector = _ // dense

  override def calOutput(): Matrix = {
    val start = System.currentTimeMillis()
    status match {
      case STATUS.Null =>
        // println(s"the status in SparseInputLayer($name)-calOutput is ${status.toString}")
        (inputDataFormat, valueType) match {
          case ("dense", "double" | "float") => // the shape of weight matrix is (inputDim, outputDim)
            forward = graph.placeHolder.getFeats.dot(weight, parallel).iadd(bias)
          case ("libsvm" | "dummy", "double") => // the shape of weight matrix is (outputDim, inputDim)
            forward = MFactory.denseDoubleMatrix(graph.placeHolder.getBatchSize, outputDim)
            (0 until outputDim).foreach { colId => // the shape of weight matrix is (outputDim, inputDim)
              val col = graph.placeHolder.getFeats.dot(weight.getRow(colId)).iadd(VectorUtils.getDouble(bias, colId))
              forward.asInstanceOf[BlasDoubleMatrix].setCol(colId, col)
            }
          case ("libsvm" | "dummy", "float") =>
            forward = MFactory.denseFloatMatrix(graph.placeHolder.getBatchSize, outputDim)
            /* new code */
            LOG.info("outputDim = " + outputDim)
            LOG.info("BatchSize = " + graph.placeHolder.getBatchSize)
            LOG.info("forward.getNumRows => " + forward.getNumRows)
            /* code end */
            (0 until outputDim).foreach { colId =>
              /* old code
              val col = graph.placeHolder.getFeats.dot(weight.getRow(colId)).iadd(VectorUtils.getFloat(bias, colId))
              forward.asInstanceOf[BlasFloatMatrix].setCol(colId, col)
              code end */
              /* new code */
              LOG.info("colId = " + colId)
              val Feats = graph.placeHolder.getFeats // Matrix
              LOG.info("Feats.getNumRows => " + Feats.getNumRows)
              LOG.info("Feats.class = " + Feats.getClass)
              for (i <- 0 until 10){
                  LOG.info("row[" + i + "] => " + Feats.getRow(i).getSize + ", type = " + Feats.getRow(i).getType + ", RowId = " + Feats.getRow(i).getRowId)
              }
              LOG.info("NumRows in weight => " + weight.getNumRows)
              // intFloatMatrix
              LOG.info("weight getClass => " + weight.getClass)
              for (i <- 0 until weight.getNumRows){
                LOG.info("row[" + i + "] => " + weight.getRow(i).getSize + ", type = "+ weight.getRow(i).getType + ", RowId = " + weight.getRow(i).getRowId)
              }
              val getRow_colId = weight.getRow(colId) // Vector
              LOG.info("getRow_colId getSize= " + getRow_colId.getSize)
              LOG.info("getRow_colId getRowId= " + getRow_colId.getRowId)
              LOG.info("getRow_colId get class= " + getRow_colId.getClass)
              val dot = Feats.dot(getRow_colId)
              LOG.info("dot getSize= " + dot.getSize)
              LOG.info("dot getRowId= " + dot.getRowId)
              LOG.info("dot get class= " + dot.getClass)
              val col = dot.iadd(VectorUtils.getFloat(bias, colId))
              LOG.info("col getSize= " + col.getSize)
              LOG.info("col getRowId= " + col.getRowId)
              LOG.info("col get class= " + col.getClass)
              forward.asInstanceOf[BlasFloatMatrix].setCol(colId, col)
              /* code end */
            }
        }

        output = transFunc(forward)
        /* new code */
        LOG.info("output class = " + output.getClass)
        LOG.info("output numRows = " + output.getNumRows)
        for (i <- 0 until 10){
          LOG.info("row[" + i + "] => " + output.getRow(i).getSize + ", type = " + output.getRow(i).getType + ", RowId = " + output.getRow(i).getRowId)
        }
        /* code end */
        status = STATUS.Forward
      case _ =>
    }
    val end = System.currentTimeMillis()
    // println(s"SparseInputLayer($name) calOutput Time=${end - start} ms")

    output
  }

  def calBackward(): Matrix = {
    val start = System.currentTimeMillis()
    status match {
      case STATUS.Forward =>
        // println(s"the status in SparseInputLayer($name)-calBackward is ${status.toString}")
        val gradTemp = gatherGrad()
        backward = transFunc.calGrad(output, gradTemp)
        /* new code */
        LOG.info("backward class = " + backward.getClass)
        LOG.info("rowsNum in backward = " + backward.getNumRows)
        for (i <- 0 until 10) {
          LOG.info("row[" + i + "].size = " + backward.getRow(i).getSize + ", type = " + backward.getRow(i).getType + ", storage Type = " + backward.getRow(i).getStorage.getType)
        }
        /* code end */

        status = STATUS.Backward
      case _ =>
    }
    val end = System.currentTimeMillis()
    // println(s"SparseInputLayer($name) calBackward Time=${end - start} ms")

    backward
  }

  override def pullParams(epoch: Int): Unit = {
    // Note: weight is a row based matrix
    (inputDataFormat, NetUtils.storageType(modelType)) match {
      case ("dense", "dense" | "component_dense") => // dense data, dense model
        // the shape of weight matrix is (inputDim, outputDim)
        weight = PSMatrixUtils.getRowAsMatrix(epoch, weightId, 0, SharedConf.indexRange.toInt, outputDim)
      case ("libsvm" | "dummy", "dense" | "component_dense") => // sparse data, dense model
        val indices = graph.placeHolder.getIndices
        /* new code */
        LOG.info("indices get Type = " + indices.getType)
        LOG.info("indices size = " + indices.getSize)
        LOG.info("weightId = " + weightId)
        LOG.info("outputDim = " + outputDim)
        PSAgentContext.get.getUserRequestAdapter.currentEpoch = epoch
        /* code end */
        // the shape of weight matrix is (outputDim, inputDim)
        weight = PSMatrixUtils.getMatrixWithIndex(1, weightId, 0, outputDim, indices)
        /* new code */
        LOG.info("NumRows in weight => " + weight.getNumRows)
        // intFloatMatrix
        LOG.info("weight getClass => " + weight.getClass)
        for (i <- 0 until weight.getNumRows){
          LOG.info("row[" + i + "] => " + weight.getRow(i).getSize + ", type = "+ weight.getRow(i).getType + ", RowId = " + weight.getRow(i).getRowId)
        }
        /* code end */
      case ("libsvm" | "dummy", "sparse" | "component_sparse") => // sparse data, sparse model
        val indices = graph.placeHolder.getIndices
        // the shape of weight matrix is (outputDim, inputDim)
        // if epoch = 0, initAndGet(), else get()
        weight = PSMatrixUtils.getMatrixWithIndex(epoch, weightId, 0, outputDim, indices)
      case _ => // dense data, sparse model
        throw new AngelException("Dense data, sparse model, pls. change model to dense")
    }
    bias = PSMatrixUtils.getRow(epoch, biasId, 0)
    LOG.info("weight and bias pull over~~~~~~~~~~~") //////
  }

  override def pushGradient(): Unit = {
    val start = System.currentTimeMillis()
    val normal = 1.0 / OptUtils.getNormal(mode, graph)

    status match {
      case STATUS.Backward =>
        (inputDataFormat, NetUtils.storageType(modelType)) match {
          case ("dense", "dense" | "component_dense") => // dense data, dense model
            val weightGrad: Matrix = Ufuncs.dot(graph.placeHolder.getFeats, true, backward, false, parallel)
              .imul(normal)
            PSMatrixUtils.incrementRowByMatrix(weightId, numSlot, weightGrad)
          case _ => // sparse data, dense or sparse model, note: dense data, sparse model is not allowed
            LOG.info("outputDim in pushGradient = " + outputDim) //////
            val vectors = (0 until outputDim).toArray.map { colId =>
              val weightRowGrad = valueType match {
                case "double" =>
                  graph.placeHolder.getFeats.transDot(backward.asInstanceOf[BlasDoubleMatrix].getCol(colId))
                    .imul(normal)
                case "float" =>
                  // old code
                  //graph.placeHolder.getFeats.transDot(backward.asInstanceOf[BlasFloatMatrix].getCol(colId))
                    //.imul(normal)
                  /* new code */
                  LOG.info("float in pushGradient")
                  val Feats = graph.placeHolder.getFeats // Matrix
                  LOG.info("Feats.getNumRows in pushGradient => " + Feats.getNumRows)
                  LOG.info("Feats.class in pushGradient = " + Feats.getClass)
                  for (i <- 0 until 10){
                    LOG.info("row[" + i + "] => " + Feats.getRow(i).getSize + ", type = " + Feats.getRow(i).getType + ", RowId = " + Feats.getRow(i).getRowId)
                  }
                  val getCol_colId = backward.asInstanceOf[BlasFloatMatrix].getCol(colId) // Vector
                  LOG.info("getCol_colId getSize= " + getCol_colId.getSize)
                  LOG.info("getCol_colId getRowId= " + getCol_colId.getRowId)
                  LOG.info("ggetCol_colId get class= " + getCol_colId.getClass)
                  val dot = Feats.transDot(getCol_colId) // Vector
                  LOG.info("dot getSize in pushGradient= " + dot.getSize)
                  LOG.info("dot getRowId in pushGradient= " + dot.getRowId)
                  LOG.info("dot get class in pushGradient= " + dot.getClass)
                  val col = dot.imul(normal) // Vector
                  LOG.info("col getSize in pushGradient= " + col.getSize)
                  LOG.info("col getRowId in pushGradient= " + col.getRowId)
                  LOG.info("col get class in pushGradient= " + col.getClass)
                  col
                  /* code end */
              }

              weightRowGrad.setMatrixId(weight.getMatrixId)
              weightRowGrad.setRowId(outputDim * numSlot + colId)
              weightRowGrad.setClock(weight.getClock)
              /* new code */
              LOG.info("weightRowGrad getSize in pushGradient= " + weightRowGrad.getSize)
              LOG.info("weightRowGrad getRowId in pushGradient= " + weightRowGrad.getRowId)
              LOG.info("weightRowGrad get class in pushGradient= " + weightRowGrad.getClass)
              val realweight = weightRowGrad.asInstanceOf[IntFloatVector]
              LOG.info("map in realweight size = " + realweight.getStorage.size())
              for (i <- 0 until realweight.size()){
                if (i%5000 == 0) {
                  LOG.info("weight at " + i + " = " + realweight.get(i))
                }
              }
              LOG.info("weight average = " + realweight.average())

              LOG.info("weight argmin = " + realweight.argmin())
              LOG.info("weight argmax = " + realweight.argmax())

              LOG.info("weight min = " + realweight.min())
              LOG.info("weight max = " + realweight.max())

              LOG.info("weight isDense = " + realweight.isDense)
              LOG.info("weight isSparse = " + realweight.isSparse)
              val weight_indices = realweight.getStorage.getIndices
              val weight_values = realweight.getStorage.getValues
              if (weight_indices != null) {
                LOG.info("weight_indices.length = " + weight_indices.length)
                var i = 0
                while (i < weight_indices.length) {
                  if (i % 5000 == 0) LOG.info("weight_indices[" + i + "] = " + weight_indices(i))
                  i = i + 1;
                }
              }
              if (weight_values != null) {
                LOG.info("weight_values.length = " + weight_values.length)
                var i = 0
                while (i < weight_values.length) {
                  if (i % 5000 == 0) LOG.info("weight_values[" + i + "] = " + weight_values(i))
                  i = i + 1;
                }
              }
              /* code end */


              weightRowGrad
            }

            /*new code*/
            LOG.info("pushGradient() in SimpleInputLayer.scala")
            /*code end*/

            PSMatrixUtils.incrementRows(weightId, vectors.map(_.getRowId), vectors)
        }


        PSMatrixUtils.incrementRow(biasId, 0, backward.average(0).imul(-optimizer.getLR / graph.taskNum))

        status = STATUS.Gradient
      case _ =>
    }

    val end = System.currentTimeMillis()
    // println(s"pushGradient Time = ${end - start} ms")
  }

  override def pushGradient_partial(epoch: Int): Unit = {
    val start = System.currentTimeMillis()
    val normal = 1.0 / OptUtils.getNormal(mode, graph)

    status match {
      case STATUS.Backward =>
        (inputDataFormat, NetUtils.storageType(modelType)) match {
          case ("dense", "dense" | "component_dense") => // dense data, dense model
            val weightGrad: Matrix = Ufuncs.dot(graph.placeHolder.getFeats, true, backward, false, parallel)
              .imul(normal)
            PSMatrixUtils.incrementRowByMatrix(weightId, numSlot, weightGrad)
          case _ => // sparse data, dense or sparse model, note: dense data, sparse model is not allowed
            val vectors = (0 until outputDim).toArray.map { colId =>
              val weightRowGrad = valueType match {
                case "double" =>
                  graph.placeHolder.getFeats.transDot(backward.asInstanceOf[BlasDoubleMatrix].getCol(colId))
                    .imul(normal)
                case "float" =>
                  graph.placeHolder.getFeats.transDot(backward.asInstanceOf[BlasFloatMatrix].getCol(colId))
                    .imul(normal)
              }

              weightRowGrad.setMatrixId(weight.getMatrixId)
              weightRowGrad.setRowId(outputDim * numSlot + colId)
              weightRowGrad.setClock(weight.getClock)

              weightRowGrad
            }
            LOG.info("pushGradient() in SimpleInputLayer.scala")
            PSMatrixUtils.incrementRows_partial(weightId, vectors.map(_.getRowId), vectors, epoch)
        }


        PSMatrixUtils.incrementRow(biasId, 0, backward.average(0).imul(-optimizer.getLR / graph.taskNum))

        status = STATUS.Gradient
      case _ =>
    }

    val end = System.currentTimeMillis()
    // println(s"pushGradient Time = ${end - start} ms")
  }

  override def pushGradient_none(epoch: Int): Unit = {
    val start = System.currentTimeMillis()
    val normal = 1.0 / OptUtils.getNormal(mode, graph)

    status match {
      case STATUS.Backward =>
        (inputDataFormat, NetUtils.storageType(modelType)) match {
          case ("dense", "dense" | "component_dense") => // dense data, dense model
            val weightGrad: Matrix = Ufuncs.dot(graph.placeHolder.getFeats, true, backward, false, parallel)
              .imul(normal)
            PSMatrixUtils.incrementRowByMatrix(weightId, numSlot, weightGrad)
          case _ => // sparse data, dense or sparse model, note: dense data, sparse model is not allowed
            val vectors = (0 until outputDim).toArray.map { colId =>
              val weightRowGrad = valueType match {
                case "double" =>
                  graph.placeHolder.getFeats.transDot(backward.asInstanceOf[BlasDoubleMatrix].getCol(colId))
                    .imul(normal)
                case "float" =>
                  graph.placeHolder.getFeats.transDot(backward.asInstanceOf[BlasFloatMatrix].getCol(colId))
                    .imul(normal)
              }

              weightRowGrad.setMatrixId(weight.getMatrixId)
              weightRowGrad.setRowId(outputDim * numSlot + colId)
              weightRowGrad.setClock(weight.getClock)

              weightRowGrad
            }
            LOG.info("pushGradient() in SimpleInputLayer.scala")
            PSMatrixUtils.incrementRows_none(weightId, vectors.map(_.getRowId), vectors, epoch)
        }


        PSMatrixUtils.incrementRow(biasId, 0, backward.average(0).imul(-optimizer.getLR / graph.taskNum))

        status = STATUS.Gradient
      case _ =>
    }

    val end = System.currentTimeMillis()
    // println(s"pushGradient Time = ${end - start} ms")
  }


  override def pushGradient_null(): Unit = {
    val start = System.currentTimeMillis()
    val normal = 0.0
    status = STATUS.Backward
    status = STATUS.Gradient

    LOG.info("skip everything, no push");
    /*
    status match {
      case STATUS.Backward =>
        (inputDataFormat, NetUtils.storageType(modelType)) match {
          case ("dense", "dense" | "component_dense") => // dense data, dense model
            val weightGrad: Matrix = Ufuncs.dot(graph.placeHolder.getFeats, true, backward, false, parallel)
              .imul(normal)
            PSMatrixUtils.incrementRowByMatrix(weightId, numSlot, weightGrad)
          case _ => // sparse data, dense or sparse model, note: dense data, sparse model is not allowed
            val vectors = (0 until outputDim).toArray.map { colId =>
              val weightRowGrad = valueType match {
                case "double" =>
                  graph.placeHolder.getFeats.transDot(backward.asInstanceOf[BlasDoubleMatrix].getCol(colId))
                    .imul(normal)
                case "float" =>
                  graph.placeHolder.getFeats.transDot(backward.asInstanceOf[BlasFloatMatrix].getCol(colId))
                    .imul(normal)
              }

              weightRowGrad.setMatrixId(weight.getMatrixId)
              weightRowGrad.setRowId(outputDim * numSlot + colId)
              weightRowGrad.setClock(weight.getClock)

              weightRowGrad
            }

            PSMatrixUtils.incrementRows(weightId, vectors.map(_.getRowId), vectors)
        }


        PSMatrixUtils.incrementRow(biasId, 0, backward.average(0).imul(-optimizer.getLR / graph.taskNum))

        status = STATUS.Gradient
      case _ =>
    }

    val end = System.currentTimeMillis()
    // println(s"pushGradient Time = ${end - start} ms")
    */
  }

  override def update(epoch: Int, batchSize: Int): Future[VoidResult] = {
    val start = System.currentTimeMillis()
    var result: Future[VoidResult] = null
    status match {
      case STATUS.Gradient =>
        (inputDataFormat, NetUtils.storageType(modelType)) match {
          case ("dense", "dense" | "component_dense") => // dense data, dense model
            result = optimizer.update(weightId, 1, epoch, batchSize)
          case _ =>
            result = optimizer.update(weightId, outputDim, epoch, batchSize)
        }
        status = STATUS.Update
      case _ => throw new AngelException("STATUS Error, please calculate Gradient first!")
    }
    val end = System.currentTimeMillis()
    // println(s"update Time = ${end - start} ms")
    result
  }

  override def init(taskflag: Int): Unit = {
    if (taskflag == 0) {
      val bound = 0.0001
      (inputDataFormat, NetUtils.storageType(modelType)) match {
        case ("dense", "dense" | "component_dense") => // dense data, dense model
          val randFunc = new RandomNormal(weightId, 0, 1, 0.0, bound)
          PSAgentContext.get().getUserRequestAdapter.update(randFunc).get()
        case ("libsvm" | "dummy", "dense" | "component_dense") => // sparse data, dense model
          val randFunc = new RandomNormal(weightId, 0, outputDim, 0.0, bound)
          PSAgentContext.get().getUserRequestAdapter.update(randFunc).get()
        case _ => // sparse model, no need to initial, use iniAndGet instead
      }
    }
  }

  override def toString: String = {
    s"SimpleInputLayer name=$name outputDim=$outputDim optimizer=$optimizer"
  }

  override def loadParams(loadContext: ModelLoadContext): Unit = {
    loadContext.addMatrix(new MatrixLoadContext(weightCtx.getName))
    loadContext.addMatrix(new MatrixLoadContext(biasCtx.getName))
  }

  override def saveParams(saveContext: ModelSaveContext): Unit = {
    val outputFormat = SharedConf.sparseInputLayerMatrixOutputFormat
    val weightMCS: MatrixSaveContext = new MatrixSaveContext(weightCtx.getName, outputFormat)
    val biasMCS: MatrixSaveContext = new MatrixSaveContext(biasCtx.getName, outputFormat)
    weightMCS.addIndices((0 until outputDim).toArray)
    saveContext.addMatrix(weightMCS)
    saveContext.addMatrix(biasMCS)
  }
}
