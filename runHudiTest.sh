cd ~/code/hudi/hudi-spark-datasource/hudi-spark
export JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-8.jdk/Contents/Home && mvn scalatest:test -Dsuites="org.apache.spark.sql.hudi.procedure.TestClusteringBinaryCopyStrategy" -Dcheckstyle.skip=true -Dscalastyle.skip=true -Drat.skip=true
