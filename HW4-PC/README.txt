                            CS455: Homework 4
                   Programming component for the Term Paper  
       Project(Post Recession Housing price vs Population vs Employment analysis)
                      2018 - Spring Semester 

Instruction To Compile Code:
        Compile code: mvn package
        Clean code:   mvn clean

Instruction to launch the program:
   "spark-submit --class cs455.spark.startup.StartUp ./target/HW4-PC-1.0.jar local <Input HDFS path(please ensure a '/' at the end)> <output HDFS path>"
   "spark-submit --master yarn --class cs455.spark.startup.StartUp ./target/HW4-PC-1.0.jar yarn <Input HDFS path(please ensure a '/' at the end)> <output HDFS path>"


Code Organization:
        src/cs455/spark: Top level source tree.
                common: common constant definition.
                employment: employment analyzer classes.
                housing: housing analyzer classes.
                model: linear regresser to find co-relations.
                population: Population analyzer
                startup: Driver program
                util: utility classes

        TOP LEVEL DIRECTORY
              |             
              |___src
              |   |
              |   |___cs455
              |         |
              |         |___spark 
              |               |
              |               |__common
              |               | 
              |               |__employment
              |               |
              |               |__housing
              |               |
              |               |__model
              |               |
              |               |__population
              |               |
              |               |__startup
              |               |
              |               |__util
              |
              |___instructions(setup and compile instructions)
              |
              |___pom.xml
              |
              |___README
     


 
 
