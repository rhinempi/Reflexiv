#!/usr/bin/env bash

#-----------------------------------------------------------------------------
 # Created by rhinempi on 02/01/18.
 #
 #      Reflexiv
 #
 # Copyright (c) 2015-2015
 #      Liren Huang      <huanglr at cebitec.uni-bielefeld.de>
 #
 # Reflexiv is free software: you can redistribute it and/or modify it
 # under the terms of the GNU General Public License as published by the Free
 # Software Foundation, either version 3 of the License, or (at your option)
 # any later version.
 #
 # This program is distributed in the hope that it will be useful, but WITHOUT
 # ANY WARRANTY; Without even the implied warranty of MERCHANTABILITY or
 # FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 # more detail.
 #
 # You should have received a copy of the GNU General Public License along
 # with this program. If not, see <http://www.gnu.org/licenses>.
 #-----------------------------------------------------------------------------


# path
sparkbin="/root/spark/bin"
reflexivlib="/mnt/software/reflexiv/lib/"

# spark metrics directory
mkdir -p /tmp/spark-events

# spark submit
time $sparkbin/spark-submit \
	--conf "spark.eventLog.enabled=true" \
	--conf "spark.task.cpus=1" \
	--conf "spark.memory.fraction=0.9" \
	--driver-memory 15G \
	--executor-memory 57G \
	--class uni.bielefeld.cmg.reflexiv.main.MainOfReAssembler \
	$reflexivlib/original-Reflexiv-0.3.jar \
		-fastq /mnt/HMP/line/part* \
		-frag Allgene.fa \
		-outfile /mnt/reflexiv \
		-kmer 31 \
		> reflexiv.log 2> reflexiv.err