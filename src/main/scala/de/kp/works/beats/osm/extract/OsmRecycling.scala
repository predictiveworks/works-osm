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

import de.kp.works.beats.osm.OsmEntities
import de.kp.works.beats.osm.extract.functions.query_match
import de.kp.works.beats.osm.h3.H3Utils
import org.apache.spark.sql.functions.{col, struct, udf}
import org.apache.spark.sql.{BeatSession, DataFrame, Row, SparkSession}

import scala.collection.mutable

object OsmRecyclingApi {

  private val session = BeatSession.getSession
  private var instance:Option[OsmRecycling] = None

  def getInstance:OsmRecycling = {

    if (instance.isEmpty)
      instance = Some(new OsmRecycling(session))

    instance.get

  }

}

/**
 * [OsmRecycling] is responsible for extracting waste bins
 * from the configured OSM dataset.
 *
 * A [Bin] is a certain OSM entity or sensor that
 * is supported by the OsmBeat
 */
class OsmRecycling(session:SparkSession) extends OsmExtract(session) {

  private val ENTITY = OsmEntities.RECYCLING

  override def build:DataFrame = {
    /*
     * Load all nodes of the specified OSM dataset
     */
    val nodes = loadNodes
    /*
     * Extract recycling container, and thereby leverage
     * the OSM tag specification
     */
    var query = Map("amenity" -> ENTITY.toString)
    var dataset = nodes.filter(query_match(query)(col(TAGS)))

    query = Map("recycling_type" -> "container")
    dataset = dataset.filter(query_match(query)(col(TAGS)))
    /*
     * Curated list of materials that can be collected
     * by a certain waste bin; The list is extracted
     * from OSM node tags.
     */
    val materials = Seq(
      "recycling:animal_waste",
      "recycling:aluminium",
      "recycling:batteries",
      "recycling:beverage_cartons",
      "recycling:bicycles",
      "recycling:books",
      "recycling:cans",
      "recycling:car_batteries",
      "recycling:cardboard",
      "recycling:cartons",
      "recycling:cds",
      "recycling:christmas_trees",
      "recycling:clothes",
      "recycling:computers",
      "recycling:cooking_oil",
      "recycling:cork",
      "recycling:drugs",
      "recycling:electrical_appliances",
      "recycling:engine_oil",
      "recycling:fluorescent_tubes",
      "recycling:foil",
      "recycling:gas_bottles",
      "recycling:garden_waste",
      "recycling:glass",
      "recycling:glass_bottles",
      "recycling:green_waste",
      "recycling:hardcore",
      "recycling:low_energy_bulbs",
      "recycling:magazines",
      "recycling:mobile_phones",
      "recycling:music",
      "recycling:newspaper",
      "recycling:organic",
      "recycling:paint",
      "recycling:paper",
      "recycling:paper_packaging",
      "recycling:plasterboard",
      "recycling:plastic",
      "recycling:plastic_bags",
      "recycling:plastic_bottles",
      "recycling:plastic_packaging",
      "recycling:polyester",
      "recycling:printer_cartridges",
      "recycling:rubble",
      "recycling:scrap_metal",
      "recycling:sheet_metal",
      "recycling:shoes",
      "recycling:small_appliances",
      "recycling:small_electrical_appliances",
      "recycling:styrofoam",
      "recycling:tyres",
      "recycling:waste",
      "recycling:waste_oil",
      "recycling:white_goods",
      "recycling:wood"
    )
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
      .withColumn(TAGS, attributes_udf(materials)(col(TAGS)))

    /*
     * Transform OSM nodes into an NGSI compliant entity
     * representation; the raw columns are finally removed
     */
    dataset = dataset
      .select(SELCOLS:_*)
      .withColumn(JSON, OsmTransform.transform(ENTITY)(struct(SELCOLS: _*)))
      .drop(COLS:_*)

    dataset

  }

  override def publish(dataset:DataFrame):Unit = ???
}
