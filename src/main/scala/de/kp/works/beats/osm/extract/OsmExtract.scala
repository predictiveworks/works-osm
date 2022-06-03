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

import de.kp.works.beats.osm.OsmEntities.OsmEntity
import de.kp.works.beats.osm.extract.functions.query_match
import de.kp.works.beats.osm.h3.H3Utils
import de.kp.works.beats.osm.{BeatJob, BeatJobs, OsmLogging}
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

abstract class OsmExtract(session:SparkSession) extends OsmLogging with OsmPublisher {

  private val sourceCfg = config.getSourceCfg

  protected val H3: String        = "h3"
  protected val ID: String        = "id"
  protected val JSON:String       = "json"
  protected val LATITUDE: String  = "latitude"
  protected val LONGITUDE: String = "longitude"
  protected val TAGS: String      = "tags"

  protected val H3_RESOLUTION = 7

  protected val COLS = Seq(ID, LATITUDE, LONGITUDE, H3, TAGS)
  protected val SELCOLS: Seq[Column] = COLS.map(col)

  def extract(job:BeatJob):Unit = {
    /*
     * STEP #1: Register the provided extraction
     * job to support job related get requests
     */
    BeatJobs.register(job)
    /*
     * STEP #2: Retrieve entities from
     * the specified OSM dataset
     */
    val dataframe = build
    /*
     * STEP #3: Send extracted dataframe to
     * the configured output channels
     */
    publish(dataframe)
  }

  def build:DataFrame
  /**
   * A helper method to apply basic operations
   * to retrieve a certain OSM entity
   */
  def basicWithQuery(entity:OsmEntity, query:Map[String,String]):DataFrame = {
    /*
     * Load all nodes of the specified OSM dataset
     */
    val nodes = loadNodes
    /*
     * Apply provided query to filter the loaded
     * nodes to the specified ones
     */
    var dataset = nodes.filter(query_match(query)(col(TAGS)))
    dataset = dataset
      /*
       * Assign H3 Index to geospatial coordinate
       * to facilitate geo search and more
       */
      .withColumn(H3, H3Utils.pointToH3(H3_RESOLUTION)(col(LATITUDE), col(LONGITUDE)))
      /*
       * Transform metadata (tags) into unified
       * representation
       */
      .withColumn(TAGS, OsmTransform.attributes_udf(col(TAGS)))

    /*
     * Transform OSM nodes into an NGSI compliant entity
     * representation; the raw columns are finally removed
     */
    dataset = dataset
      .select(SELCOLS:_*)
      .withColumn(JSON, OsmTransform.transform(entity)(struct(SELCOLS: _*)))
      .drop(COLS:_*)

    dataset


  }
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
