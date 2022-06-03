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
import de.kp.works.beats.osm.OsmEntities._
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import scala.collection.mutable

object OsmTransform extends Serializable {

  private def buildGeo(row:Row, json:JsonObject):Unit = {

    // LATITUDE
    val lat = new JsonObject
    lat.addProperty("type", "Double")
    lat.addProperty("value", row.getDouble(1))

    json.add("latitude", lat)

    // LONGITUDE
    val lon = new JsonObject
    lon.addProperty("type", "Double")
    lon.addProperty("value", row.getDouble(2))

    json.add("longitude", lon)

    // H3 INDEX
    val h3 = new JsonObject
    h3.addProperty("type", "Long")
    h3.addProperty("value", row.getLong(3))

    json.add("index", h3)

  }
  /*
   * Transform `tags` into a Map[String,String]
   */
  def attributes_udf: UserDefinedFunction = udf((tags: mutable.WrappedArray[Row]) => {
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
      }).toMap

    mtags

  })

  def transform(entity:OsmEntity): UserDefinedFunction = udf((row:Row) => {

    val json = new JsonObject
    json.addProperty("id", s"beat:osm:${entity.toString}:${row.getLong(0)}")
    json.addProperty("type", s"${entity.toString}")

    buildGeo(row, json)

    val tags = row.getAs[Map[String,String]](4)
    entity match {
      case BUS_STATION =>
        bus_station(json, tags)

      case BUS_STOP =>
        bus_stop(json, tags)

      case CHARGING_STATION =>
        charging_station(json, tags)

      case RECYCLING =>
        recycling(json, tags)
    }

    json.toString

  })

  private def bus_station(json:JsonObject, tags:Map[String,String]):Unit = {
    /* DO NOTHING */
  }

  private def bus_stop(json:JsonObject, tags:Map[String,String]):Unit = {

    // BENCH
    val bench = buildStringAttr("bench", tags, "no")
    json.add("bench", bench)

    // NETWORK, OPERATOR
    val names = Seq("network", "operator")
    names.foreach(name => {
      val attr = buildStringAttr(name, tags)
      json.add(name, attr)
    })

    // SHELTER
    val shelter = buildStringAttr("shelter", tags, "no")
    json.add("shelter", shelter)

    // WHEELCHAIR
    val wheelchair = buildStringAttr("wheelchair", tags, "no")
    json.add("wheelchair", wheelchair)

  }

  /**
   * The current implementation of this method extracts
   * and harmonizes authentication, brand, capacity, operator
   * power output and supported vehicle types
   */
  private def charging_station(json:JsonObject, tags:Map[String,String]):Unit = {

    // AUTHENTICATION
    val authentication = {
      val attr = new JsonObject
      attr.addProperty("type", "String")
      val values = tags
        .filter{case(k, _) => k.startsWith("authentication")}
        .filter{case(_, v) => v == "yes"}
        .map{case(k, _) =>
          k.replace("authentication:", "")}
        .toSeq.sorted

      attr.addProperty("value", values.mkString(","))
      attr
    }
    json.add("authentication", authentication)

    // CAPACITY
    val capacity = {
      val attr = new JsonObject
      attr.addProperty("type", "Integer")
      attr.addProperty("value", if (tags.contains("capacity")) tags("capacity").toInt else 0)
      attr
    }
    json.add("capacity", capacity)

    // BRAND, NETWORK, OPERATOR
    val names = Seq("brand", "network", "operator")
    names.foreach(name => {
      val attr = buildStringAttr(name, tags)
      json.add(name, attr)
    })

    // POWER OUTPUT
    val output = buildStringAttr("charging_station:output", tags)
    json.add("output", output)

    // VEHICLES
    val vehicles = {
      val attr = new JsonObject
      attr.addProperty("type", "String")

      val names = Seq("bicycle", "bus", "hgv", "motorcar", "scooter")
      val values = names.filter(name => {
        tags.contains(name) && tags(name) == "yes"
      })

      attr.addProperty("value", values.mkString(","))
      attr
    }
    json.add("vehicles", vehicles)

    tags.foreach{case(k,v) =>
      /*
       * This transformation is built on top of
       * the OSM specification of charging stations
       */

      val tag = new JsonObject
      tag.addProperty("type", "Integer")
      tag.addProperty("value", v)

      json.add(k, tag)
    }

  }
  private def buildStringAttr(key:String, tags:Map[String,String], default:String = ""):JsonObject = {

    val attr = new JsonObject
    attr.addProperty("type", "String")
    attr.addProperty("value", if (tags.contains(key)) tags(key) else default)

    attr

  }

  private def recycling(json:JsonObject, tags:Map[String,String]):Unit = {

    tags.foreach{case(k,v) =>
      val tag = new JsonObject
      tag.addProperty("type", "Integer")
      tag.addProperty("value", v.toInt)

      json.add(k, tag)
    }

  }

}
