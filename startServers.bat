START java -jar server/target/scala-2.12/server.jar -p 45679
timeout 4
START java -jar server/target/scala-2.12/server.jar -p 45680 -s localhost:45679
timeout 4
START java -jar server/target/scala-2.12/server.jar -p 45681 -s localhost:45679
timeout 4
START java -jar server/target/scala-2.12/server.jar -p 45682 -s localhost:45679
timeout 4
START java -jar server/target/scala-2.12/server.jar -p 45683 -s localhost:45679
timeout 4
START java -jar server/target/scala-2.12/server.jar -p 45684 -s localhost:45679