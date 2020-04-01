# CSC 369 Final Project

You project should perform some data analytics task. You can use either Map/Reduce or Spark. You can pick any programming language, but the project must be based on Hadoop Map/Reduce or Hadoop Spark. You must find your own data, perform some useful operation on the data, and get some meaningful results. Your data should be big enough to warrant the use of Hadoop. Please record the time for performing the data analytics task. You will be graded on:

- does your program find some useful pattern in the data
  - Using KNN Classifier algorithm to make an to predict classes
- use of Hadoop technology
  - Use of RDD & Spark and it's intersection with ML
- justification of why using Hadoop technology on the cluster is the right approach and performance results from running the program on the cluster
  - Time graphs of running program

Deliverables:

- source code and executables => code.scala
  - Turn in source code
- slides for presentation => slides.pdf or slides.ppt
  - okay
- log file: who did what and when, how much time was spent on each task => log.txt
  - okay
- a description of the internals of the program. How is the different functionality supported. Experimental results and justification for using Hadoop technology => report.pdf
  - so how our code works, its reproducible with any dataset as long as you change the data class, category, and distance method accordingly



Presentation should be 15 minutes. What to focus on:
- What did you do, what methodology did you use
  - Spark, RDD, KNN class, tried to mimic scikit library on Python
- How did Hadoop technology apply
  - Spark, RDD
- What did you learn?
  - ML & how spark can be utilized for ML
- Major obstacles.
  - Dealign with how RDD's are compiled 

You will be graded on how informative your presentation is to the other students. What did they learn from it.
