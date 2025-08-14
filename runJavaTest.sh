cd hudi-client/hudi-client-common && mvn -T 1C test \
  -Dtest="TestHoodieWriteConfigFileStitching,TestPartitionAwareClusteringPlanStrategy,TestHoodieBinaryCopyHandleSchemaEvolution" \
  -Dcheckstyle.skip=true \
  -Dscalastyle.skip=true \
  -Drat.skip=true

