 # (Data)Stitchr Extensions

## What are (Data)Stitchr Extensions? ###

This code base contains additional tooling to support [Stitchr](https://github.com/nhachem/).

Most of this tooling is to consolidate adding transformation functions to support
data validation
dataframe transforms
data wrangling
udf functions

among other things.
 
## How to setup and demo the tool? ###

* Build

    First clone the repo
    ``` git clone https://github.com/nhachem/stitchr-extensions.git``` under for example  `....repo/`. The ```STITCHR_ROOT``` is ```...repo/stitchr-extensions``` 
  and  is the root of the code under stitchr. All environment variables for  stitchr are defined in ``` $STITCHR_ROOT/bash/stitchr_extensions_env.sh ```

    First time build
    ```
    source bash/stitchr_extesnions_env.sh
  ```
 
   ``` 
   cd $STITCHR_ROOT 
   mvn package -DskipTests 
    ```
   
  the output jar which is relevant will be @ ```$STITCHR_ROOT/app/target/stitchr-app-$VERSION-jar-with-dependencies.jar``` where ```VERSION``` is set in ```stitchr_env.sh```

* Configuration
    
[NH need to add reference to stop words files

* Dependencies
   
        needs Spark 3+ and Scala 2.12 installed. Not tested for spark 2.4 but probably works with minor adjustments

* How to invoke from spark-shell
    
  ```
          spark-shell --jars $STITCHR_ROOT/app/target/stitchr-extensions-$VERSION-jar-with-dependencies.jar


### Pyspark/python support

NH: future work

    
## Contribution guidelines ###
 
* Known Issues
    * mvn scala:doc fails if the dependent jar files are not added to the local repo. It is a build config issue. 
    This is not critical and the api docs actually are built. A work around is to add the jar after the build by running `./addJars2MavenRepo.sh $VERSION` found under the Stitchr code base.
      
* Pending Features and Fixes


* General
    * Writing unit tests
    * Code review
    * Other guidelines?!
    
### Send requests/comments  to ###
    
Repo owner/admin: Nabil Hachem (nabilihachem@gmail.com)

## Trademarks

Apache®, Apache Spark are either registered trademarks or trademarks of the Apache Software Foundation in the United States and/or other countries.
See guidance on use of Apache Spark trademarks. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

Copyright ©2019 The Apache Software Foundation. All rights reserved.
