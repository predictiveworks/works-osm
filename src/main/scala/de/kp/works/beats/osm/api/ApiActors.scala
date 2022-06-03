package de.kp.works.beats.osm.api

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

import akka.actor.{Actor, Props}
import akka.http.scaladsl.model.HttpRequest
import akka.routing.RoundRobinPool
import de.kp.works.beats.osm.OsmEntities.OsmEntity
import de.kp.works.beats.osm.extract.OsmFactory
import de.kp.works.beats.osm._

class GetActor extends ApiActor {

  override def execute(request: HttpRequest): String = ???

}

class JobActor extends ApiActor {
  /**
   * OSM extraction tasks are executed by a
   * round robin tool of worker actors
   */
  private val jobWorker  =
    system
      .actorOf(RoundRobinPool(instances)
        .withResizer(resizer)
        .props(Props(new JobWorker())), "JobWorker")

  override def execute(request: HttpRequest): String = {
    /*
     * Validate whether this request returns
     * a valid JSON object
     */
    val json = getBodyAsJson(request)
    if (json == null) {
      val message = BeatMessages.invalidJson()

      warn(message)
      return buildErrorResp(message).toString

    }
    /*
     * Validate whether the requestor provided
     * a pre-defined OSM entity
     */
    val req = mapper.readValue(json.toString, classOf[BeatJobReq])
    val entity:Option[OsmEntity] = {
      try {
        Some(OsmEntities.withName(req.entity))
      } catch {
        case _:Throwable => None
      }
    }

    if (entity.isEmpty) {
      val message = BeatMessages.invalidEntity()

      warn(message)
      return buildErrorResp(message).toString

    }

    /* Send request to [JobWorker] */
    jobWorker ! req

    val message = BeatMessages.extractStarted()
    buildSuccessResp(message).toString

  }

}

class JobWorker extends Actor with OsmLogging {

  override def receive: Receive = {

    case req: BeatJobReq =>

      val now = System.currentTimeMillis()
      val job = BeatJob(
        id        = req.jid,
        entity    = req.entity,
        createdAt = now,
        updatedAt = 0L,
        status    = BeatStatuses.STARTED)

      val entity = OsmEntities.withName(req.entity)
      OsmFactory.extract(entity, job)

  }
}
