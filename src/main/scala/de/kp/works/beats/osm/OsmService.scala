package de.kp.works.beats.osm

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

import akka.actor.ActorRef
import akka.http.scaladsl.server.Route
import de.kp.works.beats.osm.conf.OsmConf

/**
 * [OsmService] is built to manage the various micro
 * services used to provide the REST API and also
 * the multiple output channels
 */
class OsmService(config:OsmConf) extends BeatService[OsmConf](config) with OsmLogging {

  override protected var serviceName: String = "OsmService"
  /**
   * Public method to build the micro services (actors)
   * that refer to the REST API of the `OsmBeat`
   */
  override def buildApiActors(): Map[String, ActorRef] = ???
  /**
   * Public method to register the micro services that
   * provided the configured output channels
    */
  override def onStart(): Unit = ???

  /**
   * Public method to build sensor specific HTTP routes.
   */
  override def buildRoute(): Route = ???
}
