#!/bin/bash
 #for manually installed sbt
 sbt/bin/sbt clean
 sbt/bin/sbt package
 

 #mkdir lib
 #ln -s opt/spark/lib/*.jar lib
 #sbt clean
 #sbt package
 mv target/scala-2.10/*.jar WA.jar
