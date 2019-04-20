# User Behaviors Analysis Using Spark-SQL

###This is the term project of course COMP7305, Cluster and Cloud Computing, of Dept. of Computer Science, The University of Hong Kong. Supervised by [Prof. Cho-Li Wong](https://i.cs.hku.hk/~clwang/).

In this project, we analyse the user behaviours based on an online education website focused on programming techniques.

For project enviornment, we installed **Xen** virtual machines on our lab PCs, in order to create a 8-VM cluster. After that, we installed **Hadoop** and **Yarn** on this private cluster, and setup **Spark** on Yarn. Then we installed a **Mysql** Database on one of the VM machines to store the final cleaned data from **Spark SQL**, via **JDBC**.

Main programming language used in our project is **Scala**. We also wrote **Crawler** in Python to collect Course Labels and Catalogs from website, and then join the tabel with cleaned website log.

We debugged code using *--master*("local[2]") locally via IntelliJ Idea, and then assembled all the dependancies into *.jar* package using **Mvn**, pushing to the cluster. We used *./bin/spark-submit* to submit the task to Yarn.

We focused on **Parallel Programming** features in Spark using Scala, and parameters effect when submitting, as well as discovered **VCpus** and **Memory** configuration in Xen. 

After a series of improvements, we accelerate the running time form more than 3 minutes to less than 1.5 minutes.

### 