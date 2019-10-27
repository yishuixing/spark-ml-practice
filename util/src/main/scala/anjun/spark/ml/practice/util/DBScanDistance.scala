package anjun.spark.ml.practice.util

import breeze.linalg.Vector

/**
  * Created by seawalker on 2016/11/27.
  */
object DBScanDistance {
  //地球半径 km
  val EARTH_RADIUS = 6378.137

  def rad(d:Double):Double = {
    d * Math.PI / 180.0D
  }

  /**
    * Compute rad distance Vector[latitude, longitude].
    */
  val radDistance = (p1: Vector[Double], p2: Vector[Double]) => {
    val lat1 = p1(0)
    val lng1 = p1(1)
    val lat2 = p2(0)
    val lng2 = p2(1)

    val radLat1 = rad(lat1)
    val radLat2 = rad(lat2)
    val deltaLat = rad(lat1) - rad(lat2)
    val deltaLng = rad(lng1) - rad(lng2)

    2 * EARTH_RADIUS * Math.asin(
      Math.sqrt(
        Math.pow(Math.sin( deltaLat / 2), 2)
          + Math.cos(radLat1) * Math.cos(radLat2) * Math.pow(Math.sin(deltaLng / 2), 2)
      )
    )
  }
}
