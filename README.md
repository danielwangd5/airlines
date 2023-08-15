## Run book
### Source/data structure

- data folder: contains original csv files: flightData.csv and passengers.csv. 
- results folder: contains q1/2/3/4.csv files as output from task 1 - 4
- src/main/scala: source code for tasks 1 - 4. SharedUtils contains common functions for all tasks. 
- src/test/scala: contains unit test classes for task 1 - 4


### Task description:
- task 1: Find the total number of flights for each month.
- task 2: Find the names of the 100 most frequent flyers.
- task 3: Find the greatest number of countries a passenger has been in without being in the UK. For example, if the countries a passenger was in were: UK -> FR -> US -> CN -> UK -> DE -> UK, the correct answer would be 3 countries.
- task 4: Find the passengers who have been on more than 3 flights together.

### Execution of code 
- method 1: run inside intellij. Please set up local scala environment (JDK 1.8, scala 2.12.10); for scala task, you can right-click on object file and click on "run main method". It will run the current main method, read source file from <project_home>/data, and write to <project_home>/results.
- method 2: run within sbt shell. In Intellij, go to sbt shell, and then run command like the following: runMain <class with main method>
- method 3: run using java after building project. Please add target/scala-2.12 to classpath, and use scala or java to run the main method for each task. 
