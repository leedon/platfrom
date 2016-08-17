package com.changtu.util.host

import java.io.File

import com.changtu.util.Logging
import org.apache.commons.configuration.PropertiesConfiguration


/**
  * Created by lubinsu on 8/15/2016.
  */
object Configuration extends Logging {

  def apply(fileName: String): PropertiesConfiguration = {
    logger.info("loading configuration ".concat(confHome.concat(File.separator.concat(fileName))))
    new PropertiesConfiguration(new File(confHome.concat(File.separator.concat(fileName))))
  }

}
