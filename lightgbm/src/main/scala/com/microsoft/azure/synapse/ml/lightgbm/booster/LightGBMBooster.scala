1 // Copyright (C) Microsoft Corporation. All rights reserved.
2 // Licensed under the MIT License. See LICENSE in project root for information.
3 
4 package com.microsoft.azure.synapse.ml.lightgbm.booster
5 
6 import com.microsoft.azure.synapse.ml.lightgbm.dataset.LightGBMDataset
7 import com.microsoft.azure.synapse.ml.lightgbm.swig.SwigUtils
8 import com.microsoft.azure.synapse.ml.lightgbm.{LightGBMConstants, LightGBMUtils}
9 import com.microsoft.ml.lightgbm._
10 import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector}
11 import org.apache.spark.sql.{SaveMode, SparkSession}
12 
13 //scalastyle:off
14 protected abstract class NativePtrHandler[T](val ptr: T) {
15   protected def freeNativePtr(): Unit
16   override def finalize(): Unit = {
17     if (ptr != null) {
18       freeNativePtr()
19     }
20   }
21 }
22 
23 protected class DoubleNativePtrHandler(ptr: SWIGTYPE_p_double) extends NativePtrHandler[SWIGTYPE_p_double](ptr) {
24   override protected def freeNativePtr(): Unit = {
25     lightgbmlib.delete_doubleArray(ptr)
26   }
27 }
28 
29 protected class LongLongNativePtrHandler(ptr: SWIGTYPE_p_long_long) extends NativePtrHandler[SWIGTYPE_p_long_long](ptr) {
30   override protected def freeNativePtr(): Unit = {
31     lightgbmlib.delete_int64_tp(ptr)
32   }
33 }
34 
35 protected object BoosterHandler {
36   /**
37     * Creates the native booster from the given string representation by calling LGBM_BoosterLoadModelFromString.
38     * @param lgbModelString The string representation of the model.
39     * @return The SWIG pointer to the native representation of the booster.
40     */
41   private def createBoosterPtrFromModelString(lgbModelString: String): SWIGTYPE_p_void = {
42     val boosterOutPtr = lightgbmlib.voidpp_handle()
43     val numItersOut = lightgbmlib.new_intp()
44     LightGBMUtils.validate(
45       lightgbmlib.LGBM_BoosterLoadModelFromString(lgbModelString, numItersOut, boosterOutPtr),
46       "Booster LoadFromString")
47     lightgbmlib.delete_intp(numItersOut)
48     lightgbmlib.voidpp_value(boosterOutPtr)
49   }
50 }
51 
52 /** Wraps the boosterPtr and guarantees that Native library is initialized
53  * everytime it is needed
54  * @param boosterPtr The pointer to the native lightgbm booster
55  */
56 protected class BoosterHandler(var boosterPtr: SWIGTYPE_p_void) {
57 
58   /** Wraps the boosterPtr and guarantees that Native library is initialized
59     * everytime it is needed
60     *
61     * @param model The string serialized representation of the learner
62     */
63   def this(model: String) = {
64     this(BoosterHandler.createBoosterPtrFromModelString(model))
65   }
66 
67   val scoredDataOutPtr: ThreadLocal[DoubleNativePtrHandler] = {
68     new ThreadLocal[DoubleNativePtrHandler] {
69       override def initialValue(): DoubleNativePtrHandler = {
70         new DoubleNativePtrHandler(lightgbmlib.new_doubleArray(numClasses.toLong))
71       }
72     }
73   }
74 
75   val scoredDataLengthLongPtr: ThreadLocal[LongLongNativePtrHandler] = {
76     new ThreadLocal[LongLongNativePtrHandler] {
77       override def initialValue(): LongLongNativePtrHandler = {
78         val dataLongLengthPtr = lightgbmlib.new_int64_tp()
79         lightgbmlib.int64_tp_assign(dataLongLengthPtr, 1)
80         new LongLongNativePtrHandler(dataLongLengthPtr)
81       }
82     }
83   }
84 
85   val leafIndexDataOutPtr: ThreadLocal[DoubleNativePtrHandler] = {
86     new ThreadLocal[DoubleNativePtrHandler] {
87       override def initialValue(): DoubleNativePtrHandler = {
88         new DoubleNativePtrHandler(lightgbmlib.new_doubleArray(numTotalModel.toLong))
89       }
90     }
91   }
92 
93   val leafIndexDataLengthLongPtr: ThreadLocal[LongLongNativePtrHandler] = {
94     new ThreadLocal[LongLongNativePtrHandler] {
95       override def initialValue(): LongLongNativePtrHandler = {
96         val dataLongLengthPtr = lightgbmlib.new_int64_tp()
97         lightgbmlib.int64_tp_assign(dataLongLengthPtr, numTotalModel)
98         new LongLongNativePtrHandler(dataLongLengthPtr)
99       }
100     }
101   }
102 
103   // Note for binary case LightGBM only outputs the SHAP values for the positive class
104   val shapOutputShape: Long = if (numClasses > 2) (numFeatures + 1) * numClasses else numFeatures + 1
105 
106   val shapDataOutPtr: ThreadLocal[DoubleNativePtrHandler] = {
107     new ThreadLocal[DoubleNativePtrHandler] {
108       override def initialValue(): DoubleNativePtrHandler = {
109         new DoubleNativePtrHandler(lightgbmlib.new_doubleArray(shapOutputShape))
110       }
111     }
112   }
113 
114   val shapDataLengthLongPtr: ThreadLocal[LongLongNativePtrHandler] = {
115     new ThreadLocal[LongLongNativePtrHandler] {
116       override def initialValue(): LongLongNativePtrHandler = {
117         val dataLongLengthPtr = lightgbmlib.new_int64_tp()
118         lightgbmlib.int64_tp_assign(dataLongLengthPtr, shapOutputShape)
119         new LongLongNativePtrHandler(dataLongLengthPtr)
120       }
121     }
122   }
123 
124   val featureImportanceOutPtr: ThreadLocal[DoubleNativePtrHandler] = {
125     new ThreadLocal[DoubleNativePtrHandler] {
126       override def initialValue(): DoubleNativePtrHandler = {
127         new DoubleNativePtrHandler(lightgbmlib.new_doubleArray(numFeatures.toLong))
128       }
129     }
130   }
131 
132   val dumpModelOutPtr: ThreadLocal[LongLongNativePtrHandler] = {
133     new ThreadLocal[LongLongNativePtrHandler] {
134       override def initialValue(): LongLongNativePtrHandler = {
135         new LongLongNativePtrHandler(lightgbmlib.new_int64_tp())
136       }
137     }
138   }
139 
140   lazy val numClasses: Int = getNumClasses
141   lazy val numFeatures: Int = getNumFeatures
142   lazy val numTotalModel: Int = getNumTotalModel
143   lazy val numTotalModelPerIteration: Int = getNumModelPerIteration
144 
145   lazy val rawScoreConstant: Int = lightgbmlibConstants.C_API_PREDICT_RAW_SCORE
146   lazy val normalScoreConstant: Int = lightgbmlibConstants.C_API_PREDICT_NORMAL
147   lazy val leafIndexPredictConstant: Int = lightgbmlibConstants.C_API_PREDICT_LEAF_INDEX
148   lazy val contribPredictConstant: Int = lightgbmlibConstants.C_API_PREDICT_CONTRIB
149 
150   lazy val dataInt32bitType: Int = lightgbmlibConstants.C_API_DTYPE_INT32
151   lazy val data64bitType: Int = lightgbmlibConstants.C_API_DTYPE_FLOAT64
152 
153   def freeNativeMemory(): Unit = {
154     if (boosterPtr != null) {
155       LightGBMUtils.validate(lightgbmlib.LGBM_BoosterFree(boosterPtr), "Finalize Booster")
156       boosterPtr = null
157     }
158   }
159 
160   private def getNumClasses: Int = {
161     val numClassesOut = lightgbmlib.new_intp()
162     LightGBMUtils.validate(
163       lightgbmlib.LGBM_BoosterGetNumClasses(boosterPtr, numClassesOut),
164       "Booster NumClasses")
165     val out = lightgbmlib.intp_value(numClassesOut)
166     lightgbmlib.delete_intp(numClassesOut)
167     out
168   }
169 
170   private def getNumModelPerIteration: Int = {
171     val numModelPerIterationOut = lightgbmlib.new_intp()
172     LightGBMUtils.validate(
173       lightgbmlib.LGBM_BoosterNumModelPerIteration(boosterPtr, numModelPerIterationOut),
174       "Booster models per iteration")
175     val out = lightgbmlib.intp_value(numModelPerIterationOut)
176     lightgbmlib.delete_intp(numModelPerIterationOut)
177     out
178   }
179 
180   private def getNumTotalModel: Int = {
181     val numModelOut = lightgbmlib.new_intp()
182     LightGBMUtils.validate(
183       lightgbmlib.LGBM_BoosterNumberOfTotalModel(boosterPtr, numModelOut),
184       "Booster total models")
185     val out = lightgbmlib.intp_value(numModelOut)
186     lightgbmlib.delete_intp(numModelOut)
187     out
188   }
189 
190   private def getNumFeatures: Int = {
191     val numFeaturesOut = lightgbmlib.new_intp()
192     LightGBMUtils.validate(
193       lightgbmlib.LGBM_BoosterGetNumFeature(boosterPtr, numFeaturesOut),
194       "Booster NumFeature")
195     val out = lightgbmlib.intp_value(numFeaturesOut)
196     lightgbmlib.delete_intp(numFeaturesOut)
197     out
198   }
199 
200   override protected def finalize(): Unit = {
201     freeNativeMemory()
202     super.finalize()
203   }
204 }
205 
206 /** Represents a LightGBM Booster learner
207   * @param trainDataset The training dataset
208   * @param parameters The booster initialization parameters
209   * @param modelStr Optional parameter with the string serialized representation of the learner
210   */
211 @SerialVersionUID(777L)
212 class LightGBMBooster(val trainDataset: Option[LightGBMDataset] = None,
213                       val parameters: Option[String] = None,
214                       val modelStr: Option[String] = None) extends Serializable {
215 
216   /** Represents a LightGBM Booster learner
217     * @param trainDataset The training dataset
218     * @param parameters The booster initialization parameters
219     */
220   def this(trainDataset: LightGBMDataset, parameters: String) = {
221     this(Some(trainDataset), Some(parameters))
222   }
223 
224   /** Represents a LightGBM Booster learner
225     * @param model The string serialized representation of the learner
226     */
227   def this(model: String) = {
228     this(modelStr = Some(model))
229   }
230 
231   @transient
232   lazy val boosterHandler: BoosterHandler = {
233     LightGBMUtils.initializeNativeLibrary()
234     if (trainDataset.isEmpty && modelStr.isEmpty) {
235       throw new IllegalArgumentException("One of training dataset or serialized model parameters must be specified")
236     }
237     if (trainDataset.isEmpty) {
238       new BoosterHandler(modelStr.get)
239     } else {
240       val boosterOutPtr = lightgbmlib.voidpp_handle()
241       LightGBMUtils.validate(lightgbmlib.LGBM_BoosterCreate(trainDataset.map(_.datasetPtr).get,
242         parameters.get, boosterOutPtr), "Booster")
243       new BoosterHandler(lightgbmlib.voidpp_value(boosterOutPtr))
244     }
245   }
246 
247   var bestIteration: Int = -1
248   private var startIteration: Int = 0
249   private var numIterations: Int = -1
250 
251   /** Merges this Booster with the specified model.
252     * @param model The string serialized representation of the learner to merge.
253     */
254   def mergeBooster(model: String): Unit = {
255     val mergedBooster = new BoosterHandler(model)
256     LightGBMUtils.validate(lightgbmlib.LGBM_BoosterMerge(boosterHandler.boosterPtr, mergedBooster.boosterPtr),
257       "Booster Merge")
258   }
259 
260   /** Adds the specified LightGBMDataset to be the validation dataset.
261     * @param dataset The LightGBMDataset to add as the validation dataset.
262     */
263   def addValidationDataset(dataset: LightGBMDataset): Unit = {
264     LightGBMUtils.validate(lightgbmlib.LGBM_BoosterAddValidData(boosterHandler.boosterPtr,
265       dataset.datasetPtr), "Add Validation Dataset")
266   }
267 
268   /** Saves the booster to string representation.
269     * @param upToIteration The zero-based index of the iteration to save as the last one (ignoring the rest).
270     * @return The serialized string representation of the Booster.
271     */
272   def saveToString(upToIteration: Option[Int] = None): String = {
273       val bufferLength = LightGBMConstants.DefaultBufferLength
274       val bufferOutLengthPtr = lightgbmlib.new_int64_tp()
275       val iterationCount = if (upToIteration.isEmpty) -1 else upToIteration.get + 1
276       lightgbmlib.LGBM_BoosterSaveModelToStringSWIG(boosterHandler.boosterPtr,
277         0, iterationCount, 0, bufferLength, bufferOutLengthPtr)
278   }
279 
280   /** Get the evaluation dataset column names from the native booster.
281     * @return The evaluation dataset column names.
282     */
283   def getEvalNames: Array[String] = {
284     // Need to keep track of best scores for each metric, see callback.py in lightgbm for reference
285     // For debugging, can get metric names
286     val stringArrayHandle = lightgbmlib.LGBM_BoosterGetEvalNamesSWIG(boosterHandler.boosterPtr)
287     LightGBMUtils.validateArray(stringArrayHandle, "Booster Get Eval Names")
288     val evalNames = lightgbmlib.StringArrayHandle_get_strings(stringArrayHandle)
289     lightgbmlib.StringArrayHandle_free(stringArrayHandle)
290     evalNames
291   }
292 
293   /** Get the evaluation for the training data and validation data.
294     *
295     * @param evalNames      The names of the evaluation metrics.
296     * @param dataIndex Index of data, 0: training data, 1: 1st validation
297     *                  data, 2: 2nd validation data and so on.
298     * @return Array of tuples containing the evaluation metric name and metric value.
299     */
300   def getEvalResults(evalNames: Array[String], dataIndex: Int): Array[(String, Double)] = {
301     val evalResults = lightgbmlib.new_doubleArray(evalNames.length.toLong)
302     val dummyEvalCountsPtr = lightgbmlib.new_intp()
303     val resultEval = lightgbmlib.LGBM_BoosterGetEval(boosterHandler.boosterPtr, dataIndex,
304       dummyEvalCountsPtr, evalResults)
305     lightgbmlib.delete_intp(dummyEvalCountsPtr)
306     LightGBMUtils.validate(resultEval, s"Booster Get Eval Results for data index: $dataIndex")
307 
308     val results: Array[(String, Double)] = evalNames.zipWithIndex.map { case (evalName, index) =>
309       val score = lightgbmlib.doubleArray_getitem(evalResults, index.toLong)
310       (evalName, score)
311     }
312     lightgbmlib.delete_doubleArray(evalResults)
313     results
314   }
315 
316   /** Reset the specified parameters on the native booster.
317     * @param newParameters The new parameters to set.
318     */
319   def resetParameter(newParameters: String): Unit = {
320     LightGBMUtils.validate(lightgbmlib.LGBM_BoosterResetParameter(boosterHandler.boosterPtr,
321       newParameters), "Booster Reset learning_rate Param")
322   }
323 
324   /** Get predictions for the training and evaluation data on the booster.
325     * @param dataIndex Index of data, 0: training data, 1: 1st validation
326     *                  data, 2: 2nd validation data and so on.
327     * @param classification Whether this is a classification scenario or not.
328     * @return The predictions as a 2D array where first level is for row index
329     *         and second level is optional if there are classes.
330     */
331   def innerPredict(dataIndex: Int, classification: Boolean): Array[Array[Double]] = {
332     val numRows = this.trainDataset.get.numData()
333     val scoredDataOutPtr = lightgbmlib.new_doubleArray(numClasses.toLong * numRows)
334     val scoredDataLengthPtr = lightgbmlib.new_int64_tp()
335     lightgbmlib.int64_tp_assign(scoredDataLengthPtr, 1)
336     try {
337       lightgbmlib.LGBM_BoosterGetPredict(boosterHandler.boosterPtr, dataIndex,
338         scoredDataLengthPtr, scoredDataOutPtr)
339       val scoredDataLength = lightgbmlib.int64_tp_value(scoredDataLengthPtr)
340       if (classification && numClasses == 1) {
341         (0L until scoredDataLength).map(index =>
342           Array(lightgbmlib.doubleArray_getitem(scoredDataOutPtr, index))).toArray
343       } else {
344         val numRows = scoredDataLength / numClasses
345         (0L until numRows).map(rowIndex => {
346           val startIndex = rowIndex * numClasses
347           (0 until numClasses).map(classIndex =>
348             lightgbmlib.doubleArray_getitem(scoredDataOutPtr, startIndex + classIndex)).toArray
349         }).toArray
350       }
351     } finally {
352       lightgbmlib.delete_doubleArray(scoredDataOutPtr)
353       lightgbmlib.delete_int64_tp(scoredDataLengthPtr)
354     }
355   }
356 
357   /** Updates the booster for one iteration.
358     * @return True if terminated training early.
359     */
360   def updateOneIteration(): Boolean = {
361     val isFinishedPtr = lightgbmlib.new_intp()
362     try {
363       LightGBMUtils.validate(
364         lightgbmlib.LGBM_BoosterUpdateOneIter(boosterHandler.boosterPtr, isFinishedPtr),
365         "Booster Update One Iter")
366       lightgbmlib.intp_value(isFinishedPtr) == 1
367     } finally {
368       lightgbmlib.delete_intp(isFinishedPtr)
369     }
370   }
371 
372   /** Updates the booster with custom loss function for one iteration.
373     * @param gradient The gradient from custom loss function.
374     * @param hessian The hessian matrix from custom loss function.
375     * @return True if terminated training early.
376     */
377   def updateOneIterationCustom(gradient: Array[Float], hessian: Array[Float]): Boolean = {
378     var isFinishedPtrOpt: Option[SWIGTYPE_p_int] = None
379     var gradientPtrOpt: Option[SWIGTYPE_p_float] = None
380     var hessianPtrOpt: Option[SWIGTYPE_p_float] = None
381     try {
382       val gradPtr = SwigUtils.floatArrayToNative(gradient)
383       gradientPtrOpt = Some(gradPtr)
384       val hessPtr = SwigUtils.floatArrayToNative(hessian)
385       hessianPtrOpt = Some(hessPtr)
386       val isFinishedPtr = lightgbmlib.new_intp()
387       isFinishedPtrOpt = Some(isFinishedPtr)
388       LightGBMUtils.validate(
389         lightgbmlib.LGBM_BoosterUpdateOneIterCustom(boosterHandler.boosterPtr,
390           gradPtr, hessPtr, isFinishedPtr), "Booster Update One Iter Custom")
391       lightgbmlib.intp_value(isFinishedPtr) == 1
392     } finally {
393       isFinishedPtrOpt.foreach(lightgbmlib.delete_intp)
394       gradientPtrOpt.foreach(lightgbmlib.delete_floatArray)
395       hessianPtrOpt.foreach(lightgbmlib.delete_floatArray)
396     }
397   }
398 
399   def score(features: Vector, raw: Boolean, classification: Boolean, disableShapeCheck: Boolean): Array[Double] = {
400     val kind =
401       if (raw) boosterHandler.rawScoreConstant
402       else boosterHandler.normalScoreConstant
403     features match {
404       case dense: DenseVector => predictForMat(dense.toArray, kind, disableShapeCheck,
405         boosterHandler.scoredDataLengthLongPtr.get().ptr, boosterHandler.scoredDataOutPtr.get().ptr)
406       case sparse: SparseVector => predictForCSR(sparse, kind, disableShapeCheck,
407         boosterHandler.scoredDataLengthLongPtr.get().ptr, boosterHandler.scoredDataOutPtr.get().ptr)
408     }
409     predScoreToArray(classification, boosterHandler.scoredDataOutPtr.get().ptr, kind)
410   }
411 
412   def predictLeaf(features: Vector): Array[Double] = {
413     val kind = boosterHandler.leafIndexPredictConstant
414     features match {
415       case dense: DenseVector => predictForMat(dense.toArray, kind, disableShapeCheck = false,
416         boosterHandler.leafIndexDataLengthLongPtr.get().ptr, boosterHandler.leafIndexDataOutPtr.get().ptr)
417       case sparse: SparseVector => predictForCSR(sparse, kind, disableShapeCheck = false,
418         boosterHandler.leafIndexDataLengthLongPtr.get().ptr, boosterHandler.leafIndexDataOutPtr.get().ptr)
419     }
420     predLeafToArray(boosterHandler.leafIndexDataOutPtr.get().ptr)
421   }
422 
423   def featuresShap(features: Vector): Array[Double] = {
424     val kind = boosterHandler.contribPredictConstant
425     features match {
426       case dense: DenseVector => predictForMat(dense.toArray, kind, disableShapeCheck = false,
427         boosterHandler.shapDataLengthLongPtr.get().ptr, boosterHandler.shapDataOutPtr.get().ptr)
428       case sparse: SparseVector => predictForCSR(sparse, kind, disableShapeCheck = false,
429         boosterHandler.shapDataLengthLongPtr.get().ptr, boosterHandler.shapDataOutPtr.get().ptr)
430     }
431     shapToArray(boosterHandler.shapDataOutPtr.get().ptr)
432   }
433 
434   /** Sets the start index of the iteration to predict.
435     * If <= 0, starts from the first iteration.
436     * @param startIteration The start index of the iteration to predict.
437     */
438   def setStartIteration(startIteration: Int): Unit = {
439     this.startIteration = startIteration
440   }
441 
442   /** Sets the total number of iterations used in the prediction.
443     * If <= 0, all iterations from ``start_iteration`` are used (no limits).
444     * @param numIterations The total number of iterations used in the prediction.
445     */
446   def setNumIterations(numIterations: Int): Unit = {
447     this.numIterations = numIterations
448   }
449 
450   /** Sets the best iteration and also the numIterations to be the best iteration.
451     * @param bestIteration The best iteration computed by early stopping.
452     */
453   def setBestIteration(bestIteration: Int): Unit = {
454     this.bestIteration = bestIteration
455     this.numIterations = bestIteration
456   }
457 
458   /** Saves the native model serialized representation to file.
459     * @param session The spark session
460     * @param filename The name of the file to save the model to
461     * @param overwrite Whether to overwrite if the file already exists
462     */
463   def saveNativeModel(session: SparkSession, filename: String, overwrite: Boolean): Unit = {
464     if (filename == null || filename.isEmpty) {
465       throw new IllegalArgumentException("filename should not be empty or null.")
466     }
467     val rdd = session.sparkContext.parallelize(Seq(modelStr.get))
468     import session.sqlContext.implicits._
469     val dataset = session.sqlContext.createDataset(rdd)
470     val mode = if (overwrite) SaveMode.Overwrite else SaveMode.ErrorIfExists
471     dataset.coalesce(1).write.mode(mode).text(filename)
472   }
473 
474   /** Gets the native model serialized representation as a string.
475     */
476   def getNativeModel(): String = {
477     modelStr.get
478   }
479 
480   /** Dumps the native model pointer to file.
481     * @param session The spark session
482     * @param filename The name of the file to save the model to
483     * @param overwrite Whether to overwrite if the file already exists
484     */
485   def dumpModel(session: SparkSession, filename: String, overwrite: Boolean): Unit = {
486     val json = lightgbmlib.LGBM_BoosterDumpModelSWIG(boosterHandler.boosterPtr, 0, -1, 0, 1,
487       boosterHandler.dumpModelOutPtr.get().ptr)
488     val rdd = session.sparkContext.parallelize(Seq(json))
489     import session.sqlContext.implicits._
490     val dataset = session.sqlContext.createDataset(rdd)
491     val mode = if (overwrite) SaveMode.Overwrite else SaveMode.ErrorIfExists
492     dataset.coalesce(1).write.mode(mode).text(filename)
493   }
494 
495   /** Frees any native memory held by the underlying booster pointer.
496     */
497   def freeNativeMemory(): Unit = {
498     boosterHandler.freeNativeMemory()
499   }
500 
501   /**
502     * Calls into LightGBM to retrieve the feature importances.
503     * @param importanceType Can be "split" or "gain"
504     * @return The feature importance values as an array.
505     */
506   def getFeatureImportances(importanceType: String): Array[Double] = {
507     val importanceTypeNum = if (importanceType.toLowerCase.trim == "gain") 1 else 0
508     LightGBMUtils.validate(
509       lightgbmlib.LGBM_BoosterFeatureImportance(boosterHandler.boosterPtr, -1,
510         importanceTypeNum, boosterHandler.featureImportanceOutPtr.get().ptr),
511       "Booster FeatureImportance")
512     (0L until numFeatures.toLong).map(lightgbmlib.doubleArray_getitem(boosterHandler.featureImportanceOutPtr.get().ptr, _)).toArray
513   }
514 
515   lazy val numClasses: Int = boosterHandler.numClasses
516 
517   lazy val numFeatures: Int = boosterHandler.numFeatures
518 
519   lazy val numTotalModel: Int = boosterHandler.numTotalModel
520 
521   lazy val numModelPerIteration: Int = boosterHandler.numTotalModelPerIteration
522 
523   lazy val numTotalIterations: Int = numTotalModel / numModelPerIteration
524 
525   protected def predictForCSR(sparseVector: SparseVector, kind: Int,
526                               disableShapeCheck: Boolean,
527                               dataLengthLongPtr: SWIGTYPE_p_long_long,
528                               dataOutPtr: SWIGTYPE_p_double): Unit = {
529     val numCols = sparseVector.size
530 
531     val datasetParams = s"max_bin=255 predict_disable_shape_check=${disableShapeCheck.toString}"
532     val dataInt32bitType = boosterHandler.dataInt32bitType
533     val data64bitType = boosterHandler.data64bitType
534 
535     LightGBMUtils.validate(
536       lightgbmlib.LGBM_BoosterPredictForCSRSingle(
537         sparseVector.indices, sparseVector.values,
538         sparseVector.numNonzeros,
539         boosterHandler.boosterPtr, dataInt32bitType, data64bitType, 2, numCols,
540         kind, this.startIteration, this.numIterations, datasetParams,
541         dataLengthLongPtr, dataOutPtr), "Booster Predict")
542   }
543 
544   protected def predictForMat(row: Array[Double], kind: Int,
545                               disableShapeCheck: Boolean,
546                               dataLengthLongPtr: SWIGTYPE_p_long_long,
547                               dataOutPtr: SWIGTYPE_p_double): Unit = {
548     val data64bitType = boosterHandler.data64bitType
549 
550     val numCols = row.length
551     val isRowMajor = 1
552 
553     val datasetParams = s"max_bin=255 predict_disable_shape_check=${disableShapeCheck.toString}"
554 
555     LightGBMUtils.validate(
556       lightgbmlib.LGBM_BoosterPredictForMatSingle(
557         row, boosterHandler.boosterPtr, data64bitType,
558         numCols,
559         isRowMajor, kind,
560         this.startIteration, this.numIterations, datasetParams, dataLengthLongPtr, dataOutPtr),
561       "Booster Predict")
562   }
563 
564   private def predScoreToArray(classification: Boolean, scoredDataOutPtr: SWIGTYPE_p_double,
565                                kind: Int): Array[Double] = {
566     if (classification && numClasses == 1) {
567       // Binary classification scenario - LightGBM only returns the value for the positive class
568       val pred = lightgbmlib.doubleArray_getitem(scoredDataOutPtr, 0L)
569       if (kind == boosterHandler.rawScoreConstant) {
570         // Return the raw score for binary classification
571         Array(-pred, pred)
572       } else {
573         // Return the probability for binary classification
574         Array(1 - pred, pred)
575       }
576     } else {
577       (0 until numClasses).map(classNum =>
578         lightgbmlib.doubleArray_getitem(scoredDataOutPtr, classNum.toLong)).toArray
579     }
580   }
581 
582   private def predLeafToArray(leafIndexDataOutPtr: SWIGTYPE_p_double): Array[Double] = {
583     (0 until numTotalModel).map(modelNum =>
584       lightgbmlib.doubleArray_getitem(leafIndexDataOutPtr, modelNum.toLong)).toArray
585   }
586 
587   private def shapToArray(shapDataOutPtr: SWIGTYPE_p_double): Array[Double] = {
588     (0L until boosterHandler.shapOutputShape).map(featNum =>
589       lightgbmlib.doubleArray_getitem(shapDataOutPtr, featNum)).toArray
590   }
591 }
592 
