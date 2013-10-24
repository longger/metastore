package org.apache.hadoop.hive.metastore.metalog;

import org.apache.hadoop.hive.metastore.api.MetaException;



public class MetaLogException extends MetaException {

  public MetaLogException(String message) {
   super(message);
  }

  /**
   *
   */
  private static final long serialVersionUID = 1L;


}
