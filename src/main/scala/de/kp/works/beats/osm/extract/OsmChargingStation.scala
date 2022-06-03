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
import org.apache.spark.sql.{BeatSession, DataFrame, SparkSession}

object OsmChargingStationApi {

  private val session = BeatSession.getSession
  private var instance:Option[OsmChargingStation] = None

  def getInstance:OsmChargingStation = {

    if (instance.isEmpty)
      instance = Some(new OsmChargingStation(session))

    instance.get

  }

}

class OsmChargingStation(session:SparkSession) extends OsmExtract(session) {

  private val ENTITY = OsmEntities.CHARGING_STATION

  override def build: DataFrame =
    basicWithQuery(ENTITY, Map("amenity" -> ENTITY.toString))

  override def publish(dataset: DataFrame): Unit = {

  }

}
