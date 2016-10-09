#!/bin/bash

lscpu

STORE_FRAC=$6
SHUFFLE_FRAC=$7
mkdir $1
echo "INPUT_ARGUMENTS: $@ \nDMEM $2 EMEM $3 PAR $5 ITER$4 \n STORE_FRAC $STORE_FRAC SHUFFLE_FRAC $SHUFFLE_FRAC" > $1/settings.txt
spark-submit --driver-memory $2 --executor-memory $3  --driver-cores 13 --executor-cores 4 \
WA.jar $8 $9 $4 $1/alignment.txt $5 $6 $7 > $1/stdout.txt 2>$1/spark.log 
mv runtime.txt $1/runtime.txt


#--driver-java-options "-XX:+UseCompressedOops"
#-XX:+UseCompressedOops"
