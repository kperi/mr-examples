JAR=/usr/hdp/2.4.0.0-169/hadoop-mapreduce/hadoop-streaming.jar

hadoop jar $JAR \
-file /opt/kostas/python/mapper.py    -mapper /opt/kostas/python/mapper.py \
-file /opt/kostas/python/reducer.py   -reducer /opt/kostas/python/reducer.py \
-input /user/root/500* -output /user/root/gutenberg-output
