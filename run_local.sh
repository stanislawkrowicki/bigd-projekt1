/usr/bin/rm -rf output/
mvn compile exec:exec -Dexec.executable="java" -Dexec.args="-cp %classpath com.example.bigdata.ParkVisits input/datasource1 output/"
