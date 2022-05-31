package de.kp.works.beats.osm.extract

/**
 * Copyright (c) 2019 - 2022 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 *
 */

import de.kp.works.beats.osm.OsmLogging
import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class OsmExtract(session:SparkSession) extends OsmLogging {

  private val sourceCfg = config.getSourceCfg

  protected val H3: String        = "h3"
  protected val ID: String        = "id"
  protected val JSON:String       = "json"
  protected val LATITUDE: String  = "latitude"
  protected val LONGITUDE: String = "longitude"
  protected val TAGS: String      = "tags"
  /**
   * A helper method to extract the specified
   * OSM nodes as Apache Spark DataFrame
   */
  protected def loadNodes:DataFrame = {

    val nodes = sourceCfg.getString("nodes")
    if (!nodes.endsWith(".parquet")) {
      val message = "OSM nodes are not specified as Parquet file."
      error(message)

      throw new Exception(message)

    }

    session.read.parquet(nodes)

  }
  /**
   * A helper method to extract the specified
   * OSM relations as Apache Spark DataFrame
   */
  protected def loadRelations:DataFrame = {

    val relations = sourceCfg.getString("relations")
    if (!relations.endsWith(".parquet")) {
      val message = "OSM relations are not specified as Parquet file."
      error(message)

      throw new Exception(message)

    }

    session.read.parquet(relations)

  }
  /**
   * A helper method to extract the specified
   * OSM ways as Apache Spark DataFrame
   */
  protected def loadWays:DataFrame = {

    val ways = sourceCfg.getString("ways")
    if (!ways.endsWith(".parquet")) {
      val message = "OSM ways are not specified as Parquet file."
      error(message)

      throw new Exception(message)

    }

    session.read.parquet(ways)

  }

}
