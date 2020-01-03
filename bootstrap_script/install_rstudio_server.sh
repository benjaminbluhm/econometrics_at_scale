#!/bin/bash

# These variables can be overwritten using the arguments below
VERSION="1.1.338"
# drwho is listed as user in YARN's Resource Manager UI.
USER="drwho"
# Depending on where the EMR cluster lives, you might have to change this to avoid security issues.
# To change the default password (and user), use the arguments bellow.
# If the cluster is not visible on the Internet, you can just leave the defaults for convenience.
PASS="tardis"
# A Spark version to install. sparklyr needs to have a "local" installed version of Spark to function.
# It should match the EMR cluster Spark version. Automatic detection at bootstrap time is
# unfortunately very difficult.
SPARK="2.1.1"

# To connect to Spark via YARN, after logging in the RStudio Server Web UI execute the following code:
#
# library(sparklyr)
# library(dplyr)
# sc <- spark_connect(master = "yarn-client")
#

grep -Fq "\"isMaster\": true" /mnt/var/lib/info/instance.json
if [ $? -eq 0 ];
then
    while [[ $# > 1 ]]; do
        key="$1"

        case $key in
            # RStudio Server version to install. Executing `aws s3 ls s3://rstudio-dailybuilds/rstudio-` will give you valid versions
            --sd-version)
                VERSION="$2"
                shift
                ;;
            # A user to create. It is going to be this user under which all RStudio Server actions will be executed
            --sd-user)
                USER="$2"
                shift
                ;;
            # The password for the above specified user
            --sd-user-password)
                PASS="$2"
                shift
                ;;
            # The version of Spark to install locally
            --spark-version)
                SPARK="$2"
                shift
                ;;
            *)
                echo "Unknown option: ${key}"
                exit 1;
        esac
        shift
    done
    echo "*****************************************"
    echo "  1. Download RStudio Server ${VERSION}   "
    echo "*****************************************"
    wget https://s3.amazonaws.com/rstudio-dailybuilds/rstudio-server-rhel-${VERSION}-x86_64.rpm
    echo "         2. Install dependencies         "
    echo "*****************************************"
    # This is needed for installing devtools
    sudo yum -y install libcurl libcurl-devel 1>&2
    echo "        3. Install RStudio Server        "
    echo "*****************************************"
    sudo yum -y install --nogpgcheck rstudio-server-rhel-${VERSION}-x86_64.rpm 1>&2
    echo "      4. Create R Studio Server user     "
    echo "*****************************************"
    epass=$(perl -e 'print crypt($ARGV[0], "password")' ${PASS})
    sudo useradd -m -p ${epass} ${USER}
    # This is to allow access to HDFS
    sudo usermod -a -G hadoop ${USER}
    echo "  5. Create environment variables file   "
    echo "*****************************************"
    # This file contains env variables that are loaded into RStudio. Using RStudio with Spark
    # is the main use case for installing it in EMR in the first place, so it only makes sense that
    # SPARK_HOME is added to the environment. The location is based on version ^5.0.0 of EMR.
    sudo runuser -l ${USER} -c "touch /home/${USER}/.Renviron"
    sudo runuser -l ${USER} -c "echo 'SPARK_HOME=/usr/lib/spark' >> /home/${USER}/.Renviron"
    echo "     6. Install devtools and sparkyr     "
    echo "*****************************************"
    # Create global install script and execute it
    touch /home/hadoop/install-global.R
    echo 'install.packages("devtools", "/usr/share/R/library/", repos="http://cran.rstudio.com/")' >> /home/hadoop/install-global.R
    echo 'devtools::install_github("rstudio/sparklyr")' >> /home/hadoop/install-global.R
    sudo R CMD BATCH /home/hadoop/install-global.R
    # Create user install script and execute it
    sudo runuser -l ${USER} -c 'touch /home/'${USER}'/install-user.R'
    sudo runuser -l ${USER} -c "echo 'library(sparklyr)' >> /home/${USER}/install-user.R"
    sudo runuser -l ${USER} -c "echo 'spark_install(version = \"${SPARK}\")' >> /home/${USER}/install-user.R"
    sudo runuser -l ${USER} -c 'R CMD BATCH /home/'${USER}'/install-user.R'
	echo "                  Done                   "
    echo "*****************************************"

else
    echo "RStudio Server is only installed on the master node. This is a slave."

sudo rstudio-server start;
exit 0;
fi