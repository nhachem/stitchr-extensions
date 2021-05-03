export PROJECT_NAME=stitchr-extensions

## set the environments per your's
export USER_ROOT=$HOME ## change to your environment
export USER_PERSIST_ROOT=$USER_ROOT/data
## run it from the root code directory
export ROOT_DIR=`pwd`
export CONFIG_DIR=$USER_ROOT/
export DATA_DIR=$USER_PERSIST_ROOT/stitchr
export REGISTRY_DIR=$USER_ROOT/stitchr

export baseRegistryFolder=$REGISTRY_DIR/registry/
export baseConfigFolder=file://$CONFIG_DIR/config/
export baseConfigFolder=file://$ROOT_DIR/config/
## using tpcds generated and adjusted data
export baseDataFolder=$DATA_DIR/tpcds/ ## for the demo
export defaultOutputDir=/tmp
export propertiesFile=demoTransforms.properties

## spark
# export SPARK_HOME="<spark-home-if-not-set"
## set it up if JAVA_HOME is not set export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_211.jdk/Contents/Home
## export PATH=$SPARK_HOME/bin:$PATH

export PROJECT_NAME=stitchr-extensions
export VERSION=0.1-SNAPSHOT
export MASTER=local[4]
export buildDir=$HOME/devJars/$PROJECT_NAME/target

# export MOMA_JAR=$ROOT_DIR/target/momatx$PROJECT_NAME-$VERSION-jar-with-dependencies.jar
export STITCHR_EXTENSIONS_JAR=$buildDir/$PROJECT_NAME-$VERSION-jar-with-dependencies.jar

## used in bash scripts
export PROJECT_SCALA_VERSION=2.12
export PROJECT_SPARK_VERSION=3.0.0

## PYTHON SOURCES assumes that the root is root of src code
export PYTHONPATH=PYTHONPATH:$ROOT_DIR/python
