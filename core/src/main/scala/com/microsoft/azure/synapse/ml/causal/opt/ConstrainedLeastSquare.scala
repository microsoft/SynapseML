package com.microsoft.azure.synapse.ml.causal.opt

import breeze.optimize.DiffFunction
import com.microsoft.azure.synapse.ml.causal.CacheOps
import com.microsoft.azure.synapse.ml.causal.linalg.{MatrixOps, VectorOps}

/**
  * Solver for the following constrained least square problem:
  * minimize ||Ax-b||^2^ + λ||x||^2^, s.t. 1^T^x = 1, 0 ≤ x ≤ 1
  * @param step the initial step size
  * @param maxIter max number iterations allowed
  * @param numIterNoChange max number of iteration without change in loss function allowed before termination.
  * @param tol tolerance for loss function
  */
private[causal] class ConstrainedLeastSquare[TMat, TVec](step: Double,
                                         maxIter: Int,
                                         numIterNoChange: Option[Int] = None,
                                         tol: Double =1E-4
                                        )(implicit matrixOps: MatrixOps[TMat, TVec],
                                          vectorOps: VectorOps[TVec],
                                          cacheOps: CacheOps[TVec]) {

  if (step <= 0) throw new IllegalArgumentException("step must be positive")
  if (maxIter <= 0) throw new IllegalArgumentException("maxIter must be positive")
  if (tol <= 0) throw new IllegalArgumentException("tol must be positive")
  if (!numIterNoChange.forall(_ > 0)) {
    throw new IllegalArgumentException("numIterNoChange must be positive if defined.")
  }

  private[causal] def getLossFunc(A: TMat, b: TVec, lambda: Double)
                         (implicit matrixOps: MatrixOps[TMat, TVec], vectorOps: VectorOps[TVec])
  : DiffFunction[TVec] = {
    if (lambda < 0) throw new IllegalArgumentException("lambda must be positive.")

    (x: TVec) => {
      // gemv: alpha*A*x + beta*y

      val error = matrixOps.gemv(A, x, Some(b), beta = -1)
      val value = math.pow(vectorOps.nrm2(error), 2) +
        lambda * math.pow(vectorOps.nrm2(x), 2)

      val grad = matrixOps.gemv(
        A,
        x = error,
        yOpt = Some(x),
        alpha = 2,
        beta = 2 * lambda,
        aTranspose = true
      ) // 2 * A.t * (A*x - b) + 2 * lambda * x

      (value, grad)
    }
  }

  def solve(A: TMat, b: TVec,
            lambda: Double = 0d,
            fitIntercept: Boolean = false,
            seed: Long = util.Random.nextLong): (TVec, Double) = {

    val aCentered = if (fitIntercept) matrixOps.centerColumns(A) else A
    val bCentered = if (fitIntercept) vectorOps.center(b) else b

    val xSize = matrixOps.size(aCentered)._2

    val lossFunc = getLossFunc(aCentered, bCentered, lambda)
    val md = new MirrorDescent[TVec](lossFunc, step, maxIter, numIterNoChange, tol)

    val init = vectorOps.uniformRandom(xSize, seed)
    val x = md.solve(init)

    if (fitIntercept){
      val colMean = matrixOps.colMean(A)
      val bMean = vectorOps.mean(b)
      (x, bMean - vectorOps.dot(x, colMean))
    } else {
      (x, 0d)
    }
  }
}