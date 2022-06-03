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

import de.kp.works.beats.osm.{BeatJob, OsmEntities}
import de.kp.works.beats.osm.OsmEntities.OsmEntity

object OsmFactory {

  def extract(entity:OsmEntity, job:BeatJob):Unit = {

    val api = entity match {
      case OsmEntities.BUS_STOP =>
        /*
         * Retrieve OSM highway bus stp interface
         * and execute the extraction job
         */
         OsmBusStopApi.getInstance

      case OsmEntities.BUS_STATION =>
        /*
         * Retrieve OSM bus station interface
         * and execute the extraction job
         */
        OsmBusStationApi.getInstance

      case OsmEntities.CHARGING_STATION =>
        /*
         * Retrieve OSM charging station interface
         * and execute the extraction job
         */
        OsmChargingStationApi.getInstance

      case OsmEntities.RECYCLING =>
        /*
         * Retrieve OSM recycling interface
         * and execute the extraction job
         */
        OsmRecyclingApi.getInstance

      case _ =>
        throw new Exception(s"Provided entity ${entity.toString} is not supported.")
    }

    api.extract(job)

  }
}
