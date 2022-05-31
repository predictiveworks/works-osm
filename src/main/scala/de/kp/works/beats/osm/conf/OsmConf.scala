package de.kp.works.beats.osm.conf

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

object OsmConf {

  private var instance:Option[OsmConf] = None

  def getInstance:OsmConf = {
    if (instance.isEmpty) instance = Some(new OsmConf())
    instance.get
  }

}

class OsmConf extends BeatConf {
  /**
   * The (internal) resource folder file name
   */
  override var path: String = "works-osm.conf"

}
