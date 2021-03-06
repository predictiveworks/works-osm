package de.kp.works.beats.osm.fiware

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

import de.kp.works.beats.osm.conf.BeatConf
import de.kp.works.beats.osm.http.HttpConnect
import de.kp.works.beats.osm.{BeatSensor, BeatSink}
/**
 * Base output channel to a FIWARE Context Broker;
 * this producer leverages the Broker REST API to
 * publish OSM Beat readings.
 *
 * This output channel is built to integrate OSM
 * Beats into larger and cross-domain information
 * management settings.
 */
abstract class Producer[T <: BeatConf](options:Options[T])
  extends HttpConnect with BeatSink {
  /**
   * The address of the Fiware Context Broker
   */
  private val brokerUrl = options.getBrokerUrl
  /**
   * The broker endpoint to create & update
   * entities (sensors)
   */
  private val entityCreateUrl = "/v2/entities"
  private val entityGetUrl    = "/v2/entities/{id}"
  private val entityUpdateUrl = "/v2/entities/{id}/attrs"
  /**
   * Make sure that the HTTP connection is secured,
   * if the respective configuration exists
   */
  private val httpsContext = options.getHttpsContext
  if (httpsContext.nonEmpty)
    setHttpsContext(httpsContext.get)

  override def execute(sensor: BeatSensor): Unit = {
    /*
     * STEP #1: Check whether the provided sensor
     * already exists; if this is the case, switch
     * to update request.
     */
    if (sensorExists(sensor))
      patchSensor(sensor)
    /*
     * STEP #2: Create a non-existing sensor
     */
    postSensor(sensor)
  }
  /**
   * Internal method to update a sensor without
   * existence checks
   */
  private def patchSensor(sensor:BeatSensor):Boolean = {

    val headers = Map.empty[String,String]
    val endpoint = brokerUrl + entityUpdateUrl.replace("{id}", sensor.sensorId)

    try {
      /*
       * Retrieve sensor in JSON format and restrict
       * to the respective attributes
       */
      val json = sensor.toJson
      json.remove("id")
      json.remove("type")
      /*
       * The expected response code = 204; in case of
       * another code, an exception is thrown
       */
      patch(endpoint, headers, json)
      true

    } catch {
      case _:Throwable => false
    }

  }
  /**
   * Internal method to create an OSM sensor
   * without existence checks
   */
  private def postSensor(sensor:BeatSensor):Boolean = {

    val headers = Map.empty[String,String]
    val endpoint = brokerUrl +  entityCreateUrl

    try {
      /*
       * The expected response code = 201; in case of
       * another code, an exception is thrown
       */
      post(endpoint, headers, sensor.toJson)
      true

      } catch {
      case _:Throwable => false
    }

  }

  def sensorExists(sensor:BeatSensor):Boolean = {

    val headers = Map.empty[String,String]
    val endpoint = brokerUrl + entityGetUrl.replace("{id}", sensor.sensorId)

    try {

      val bytes = get(endpoint, headers, pooled = true)
      extractJsonBody(bytes)

      true

    } catch {
      case _:Throwable => false
    }

  }
}
