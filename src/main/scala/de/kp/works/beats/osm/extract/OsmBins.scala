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

import com.google.gson.JsonObject
import de.kp.works.beats.osm.extract.functions.query_match
import org.apache.spark.sql.functions.{col, struct, udf}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable
/**
 * [OsmBins] is responsible for extracting waste bins
 * from the configured OSM dataset.
 *
 * A [Bin] is a certain OSM entity or sensor that
 * is supported by the OsmBeat
 */
class OsmBins(session:SparkSession) extends OsmExtract(session) {

  def build:DataFrame = {
    /*
     * Load all nodes of the specified OSM dataset
     */
    val nodes = loadNodes
    /*
     * Extract recycling container, and thereby leverage
     * the OSM tag specification
     */
    var query = Map("amenity" -> "recycling")
    var bins = nodes.filter(query_match(query)(col(TAGS)))

    query = Map("recycling_type" -> "container")
    bins = bins.filter(query_match(query)(col(TAGS)))
    /*
     * Curated list of materials that can be collected
     * by a certain waste bin; The list is extracted
     * from OSM node tags.
     */
    val materials = Seq(
      "recycling:batteries",
      "recycling:beverage_cartons",
      "recycling:books",
      "recycling:cans",
      "recycling:cardboard",
      "recycling:cartons",
      "recycling:clothes",
      "recycling:electrical_appliances",
      "recycling:glass",
      "recycling:glass_bottles",
      "recycling:green_waste",
      "recycling:magazines",
      "recycling:newspaper",
      "recycling:organic",
      "recycling:paper",
      "recycling:plastic",
      "recycling:plastic_packaging",
      "recycling:scrap_metal",
      "recycling:shoes",
      "recycling:waste")

    /*
     * Transform `tags` into a Map[String,String] and
     * thereby harmonize the respective attributes
     */
    def attributes_udf(materials:Seq[String]) = udf((tags: mutable.WrappedArray[Row]) => {
      /*
       * Prepare `tags` before harmonization
       * and thereby restrict to the materials
       * that can be gathered for recycling.
       */
      val mtags = tags
        .map(tag => {
          val k = new String(tag.getAs[Array[Byte]]("key"))
          val v = new String(tag.getAs[Array[Byte]]("value"))

          (k, v)
        })
        .filterNot{case(k, _) =>
          k == "amenity" || k == "recycling_type"
        }
        .filter{case(k,v) => k.startsWith("recycling:")}
        .toMap

      val attributes = materials.map(material => {

        val v = if (mtags.contains(material)) 1 else 0
        val k = material.replace("recycling:", "")

        (k,v)

      }).toMap

      attributes
    })

    val cols = Seq(ID, LATITUDE,LONGITUDE, TAGS)
    val selCols =cols.map(col)

    bins = bins
      .select(selCols:_*)
      .withColumn(TAGS, attributes_udf(materials)(col(TAGS)))

    /*
     * Prepare for NGSI entity representation
     */
    val json_def = udf((row:Row) => {

      val json = new JsonObject
      json.addProperty("id", s"beat:osm:bin:${row.getLong(0)}")
      json.addProperty("type", "bin")

      val lat = new JsonObject
      lat.addProperty("type", "Double")
      lat.addProperty("value", row.getDouble(1))

      json.add("latitude", lat)

      val lon = new JsonObject
      lon.addProperty("type", "Double")
      lon.addProperty("value", row.getDouble(2))

      json.add("longitude", lon)

      val tags = row.getAs[Map[String,Int]](3)
      tags.foreach{case(k,v) =>
        val tag = new JsonObject
        tag.addProperty("type", "Integer")
        tag.addProperty("value", v)

        json.add(k, tag)
      }

      json.toString

    })
    /*
     * Transform OSM nodes into an NGSI compliant entity
     * representation; the raw columns are finally removed
     */
    bins = bins
      .withColumn(JSON, json_def(struct(selCols: _*)))
      .drop(cols:_*)

    bins

  }

}
