#!/bin/bash

if [[ $# -ne 1 ]]
 then
   echo 'Enter only one param, number row you want generate'
   exit 1
fi

# dict userId value
USER_ID=("guest"
			   "manager"
			   "admin"
			   "authorized_user"
			   "hacker")

# dict temperature value
TEMP=("HIGH"
      "MEDIUM"
      "LOW")

# function return one row and write in log file
getLine () {
        TIMESTAMP=$(date -d "$((RANDOM%1+2020))-$((RANDOM%12+1))-$((RANDOM%28+1)) $((RANDOM%23+1)):$((RANDOM%59+1)):$((RANDOM%59+1))" '+%d-%m-%Y %H:%M:%S')
        #TIMESTAMP=`date +%Y-%m-%d_%H-%M-%S`
        NUMBER_X=$((1 + RANDOM % 900))
        NUMBER_Y=$((1 + RANDOM % 600))

# define areas from coordinates X and Y
        if [[ $NUMBER_X -gt 0 && $NUMBER_X -le 300 && $NUMBER_Y -gt 0 && $NUMBER_Y -le 300 ]]
          then
            PLACE_ID="wrapper"

        elif [[ $NUMBER_X -gt 300 && $NUMBER_X -le 600 && $NUMBER_Y -gt 0 && $NUMBER_Y -le 300 ]]
          then
            PLACE_ID="header"

        elif [[ $NUMBER_X -gt 600 && $NUMBER_X -le 900 && $NUMBER_Y -gt 0 && $NUMBER_Y -le 300 ]]
          then
            PLACE_ID="main"

        elif [[ $NUMBER_X -gt 0 && $NUMBER_X -le 300 && $NUMBER_Y -gt 300 && $NUMBER_Y -le 600 ]]
          then
            PLACE_ID="sidebar"

        elif [[ $NUMBER_X -gt 300 && $NUMBER_X -le 600 && $NUMBER_Y -gt 300 && $NUMBER_Y -le 600 ]]
          then
            PLACE_ID="content"

        elif [[ $NUMBER_X -gt 600 && $NUMBER_X -le 900 && $NUMBER_Y -gt 300 && $NUMBER_Y -le 600 ]]
          then
            PLACE_ID="footer"

        else
          PLACE_ID= "unknown"
        fi

#write row in file
        echo "$TIMESTAMP $NUMBER_X $NUMBER_Y ${USER_ID[$((RANDOM % ${#USER_ID[*]}))]} $PLACE_ID ${TEMP[$((RANDOM % ${#TEMP[*]}))]}" >> input/genFile.txt
}

# delete and create dir
rm -rf input
mkdir input

for ((i=1; i<=$1; i++))
  do
    $(getLine)
  done

# Prepare hadoop file system
hdfs dfs -rm -r input output

# Send input data to the distributed fs
hdfs dfs -put input input

# Run the application
yarn jar /home/raidermh/lab1/target/lab1-1.0-SNAPSHOT-jar-with-dependencies.jar input output

# Read sequence file
echo '=====================JOB RESULT====================='
hadoop fs -text /user/root/output/part-r-00000