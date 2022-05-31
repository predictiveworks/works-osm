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

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import akka.util.Timeout
import de.kp.works.beats.osm.conf.BeatConf

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContextExecutor, Future}

abstract class BeatService[T <: BeatConf](config:T) {

  private var server:Option[Future[Http.ServerBinding]] = None
  private val uuid = java.util.UUID.randomUUID().toString
  /**
   * Akka 2.6 provides a default materializer out of the box, i.e., for Scala
   * an implicit materializer is provided if there is an implicit ActorSystem
   * available. This avoids leaking materializers and simplifies most stream
   * use cases somewhat.
   */
  implicit val system: ActorSystem = ActorSystem(s"osm-beat-$uuid")
  implicit lazy val context: ExecutionContextExecutor = system.dispatcher

  implicit val materializer: ActorMaterializer = ActorMaterializer()
  /**
   * Common timeout for all Akka connection
   */
  implicit val timeout: Timeout = Timeout(15.seconds)
  /**
   * Adjust subsequent variable to the specific service
   */
  protected var serviceName:String
  /**
   * Public method to build the micro services (actors)
   * that refer to the REST API of the `OsmBeat`
   */
  def buildApiActors():Map[String,ActorRef]
  /**
   * Public method to build sensor specific HTTP routes.
   */
  def buildRoute():Route
  /**
   * Method to build the HTTP(s) server that
   * defines the backbone of the SensorBeat
   */
  private def buildServer(route:Route):Unit = {

    val binding = config.getBindingCfg

    val host = binding.getString("host")
    val port = binding.getInt("port")

    val connectionContext = config.getConnectionContext
    server = connectionContext match {
      case Some(context) =>
        Http().setDefaultServerHttpContext(context)
        Some(Http().bindAndHandle(route, host, port, connectionContext = context))

      case None =>
        Some(Http().bindAndHandle(route , host, port))
    }

  }

  def start():Unit = {

    try {

      if (!config.isInit) config.init()
      if (!config.isInit) {
        throw new Exception(s"Loading configuration failed and service is not started.")
      }

      buildServer(buildRoute())
      /*
       * STEP #3: Invoke sensor specific after
       * start processing
       */
      onStart()

    } catch {
      case t:Throwable =>
        system.terminate()

        println(t.getLocalizedMessage)
        System.exit(0)
    }

  }
  /**
   * Method to invoke the sensor specific after
   * start processing
   */
  def onStart():Unit

  def stop():Unit = {

    if (server.isEmpty) {
      system.terminate()
      throw new Exception(s"Service was not launched.")
    }

    server.get
      /*
       * Trigger unbinding from port
       */
      .flatMap(_.unbind())
      /*
       * Shut down application
       */
      .onComplete(_ => {
        system.terminate()
        System.exit(0)
      })

  }

}
