# data-vis
Project using various tools to produce data visualizations

# Tools & Versions
* Scala 2.12.2
* SBT 0.13.15
* Groovy 2.5.2
* Baidu EChart 4.2.0-rc.1

# Sample Output
https://mhuckaby.github.io/data-vis/

## Data Collection
The `/data-collection` sub-project consists of a bash script and a Groovy script that support data acquisition and transormation into a format that can be used by the Spark job found in the `/analysis` sub-project.

Execute the bash script `crawl-bbs-list.sh` to acquire the data
 
Execute the Groovy script via Gradle (see default output for params):
* ./gradlew run

## Analysis
The `/analysis` sub-project consists of a Spark job written in Scala.

The packaged output (jar) can be run by Spark. 
 
Build and package the project with the following commands:
* sbt compile
* sbt package
