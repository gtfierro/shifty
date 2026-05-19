#!/bin/bash

for i in `find . -name '*.ttl'` ; do 
    i_noext=${i%.ttl}
    sed -i "s/<>/<urn:$(basename $i)>/g" $i ;
    sed -i "s/^<$(basename $i)>/<urn:$(basename $i)>/g" $i
    sed -i "s/<$(basename $i_noext)>/<urn:$(basename $i_noext)>/g" $i
done
