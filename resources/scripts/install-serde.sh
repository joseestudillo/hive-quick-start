mkdir -p ~/.hiveJars
(cd ../../; mvn package -Dmaven.test.skip=true; cp target/*.jar ~/.hiveJars)
