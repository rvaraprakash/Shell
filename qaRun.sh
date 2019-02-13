#!/bin/bash

########################################################################################################
# qaRun.sh - Process files on Mediation/CABS/ECS application and collect table statistics
# 
#  Author: Varaprakash Reddy 
# 
# Following will be supported  by this script
#   1.  Supports file processing for Mediation, CABS and ECS applications
#   2.  Starts Components/CTE
#   3.  Cleans the Database for list of files processing
#   4.  Cleans UNIX drop/pcfs folder for list of files processing
#   5.  Copies test data files to DROP location
#   6.  If file size is too big then test data files will be moved to DROP location and
#         Once processing done move files back to test data location to improve performance
#   7.  Executes Schedule jobs
#   8.  Verifies file process status
#   9.  Stop CTE component (if too many CTE’s running)
#   10. Capture process logs in log file
#   11. Creates Select SQL commands into log file
########################################################################################################

############### Configure Settings ####################################
### Max components up at same time = <NameServ + MedSev + JBOSS + CTE's>
maxCompUp=8

### CTE/component Max wait time(maxwait x 30) in sec. 
maxwait=60

### Backup processing files from $FUSIONWORKS_BASE/pcfs folder (Yes=1, No=0)
backupFiles=0

###########################################################
### List of DP stream specific tables which needs cleenup
###########################################################
aluTables="BL_ALU BL_ALU_SS BL_ALU_AGGR BL_ALU_DUP BL_ALU_ERR"
aluAuditTables="BL_ALU_AUDIT BL_ALU_AUDIT_DUP BL_ALU_AUDIT_ERR"
btsTables="BL_BTS BL_BTS_DUP BL_BTS_ERR"
sonusTables="BL_SONUS BL_SONUS_DUP BL_SONUS_ERR BL_SONUS_NONCORRELATABLE"
broadSoftTables="BL_BROADSOFT BL_BROADSOFT_DUP BL_BROADSOFT_ERR BL_BROADSOFT_UNCORRELATED BL_BROADSOFT_NONCORRELATABLE"
ncicTables="BL_NCIC BL_NCIC_DUP BL_NCIC_ERR"
infonxxTables="BL_INFONXX BL_INFONXX_DUP BL_INFONXX_ERR"
imsTables="BL_IMS BL_IMS_DUP BL_IMS_ERR"
hiqTables="BL_HIQ BL_HIQ_DUP BL_HIQ_ERR"
kgbTables="BL_KGB BL_KGB_DUP BL_KGB_ERR BL_KGBCREDIT BL_KGBCREDIT_DUP BL_KGBCREDIT_ERR"
verizonTables="BL_VERIZON BL_VERIZON_DUP BL_VERIZON_ERR"
commTables="BL_FLR BL_CDR BL_STATISTICS_FILE CFW_FILES_COLLECTED BL_CDR_UNENR BL_DISCARD_INFO"

###########################################################
### List of DP stream specific jobs
###########################################################
### CTE_ALU_1
alu1_coll="CTE_ALU_1 ALU_01 Start_Collect_PB_ALU_01"
alu1_aggr="CTE_ALU_1 ALU_AGG_01 Start_TP_ALU_AGGREGATION_01"
alu1_med="CTE_ALU_1 ALU_MED_1 Start_TP_ALU_MEDIATION_01"

### CTE_ALU_2
alu2_coll="CTE_ALU_2 ALU_02 Start_Collect_PB_ALU_02"
alu2_aggr="CTE_ALU_2 ALU_AGG_02 Start_TP_ALU_AGGREGATION_02"
alu2_med="CTE_ALU_2 ALU_MED_BHN_02 Start_TP_ALU_BHN_MEDIATION_02"

### CTE_CDRMS_1
audit1_coll="CTE_CDRMS_1 ALU_AUDIT Start_Collect_PB_ALU_AUDIT"

### CTE_BROADSOFT_1
brdSoft_coll="CTE_BROADSOFT_1 BROADSOFT_001 Start_Collect_PB_BROADSOFT_01"
brdSoft_aggr="CTE_BROADSOFT_1 BROADSOFT_AGG_001 Start_TP_BROADSOFT_AGGREGATION_01"

### CTE_BTS_1
bts1_coll="CTE_BTS_1 BTS_01 Start_Collect_PB_BTS_01"

### CTE_BTS_2
bts2_coll="CTE_BTS_2 BTS_02 Start_Collect_PB_BTS_02"

### CTE_IMS_1
ims1_coll="CTE_IMS_1 IMS_01 Start_Collect_PB_IMS_01"

### CTE_USAGE_1 -> IMS_02, INFONXX, NCIC
ims2_coll="CTE_USAGE_1 IMS_02 Start_Collect_PB_IMS_02"
infoNxx_coll="CTE_USAGE_1  INFONXX_01 Start_Collect_PB_INFONXX_01"
ncic_coll="CTE_USAGE_1 NCIC_01 Start_Collect_PB_NCIC_01"

### CTE_HIQ_1
hiq1_coll="CTE_HIQ_1 HIQ_01 Start_Collect_PB_HIQ_01"

### CTE_HIQ_2
hiq2_coll="CTE_HIQ_2 HIQ_02 Start_Collect_PB_HIQ_02"

### CTE_SONUS_1
sonus_coll="CTE_SONUS_1 SONUS_01 Start_Collect_PB_SONUS_01"

### CTE_CLEC_1 -> KGB, Sprint, Verizon
kgb_coll="CTE_CLEC_1 KGB_01 Start_Collect_PB_KGB_01"
sprint_coll="CTE_CLEC_1 SPRINT_01 Start_Collect_PB_SPRINT_01"
verizon_coll="CTE_CLEC_1 VERIZON_01 Start_Collect_PB_VERIZON_01"

###########################################################
### List of CABS stream specific tables which needs cleenup
###########################################################
cabsSonusTables="BL_SONUS BL_SONUS_DUP BL_SONUS_ERR BL_SONUS_NONCORRELATABLE BL_SONUS_UNCORRELATED"
cabsBTSTandemTables="BL_BTS_TANDEM BL_BTS_TANDEM_DUP BL_BTS_TANDEM_ERR BL_BTS_CORRELATED BL_BTS_UNCORRELATED BL_BTS_NONCORRELATABLE"
cabsIlecTables="BL_CABS_ILEC BL_CABS_ILEC_DUP BL_CABS_ILEC_ERR BL_CABS_AUDIT"
cabsCommTables="BL_FLR BL_CDR BL_STATISTICS_FILE CFW_FILES_COLLECTED BL_CDR_UNENR BL_DISCARD_INFO"

###########################################################
### List of CABS stream specific jobs
###########################################################
gsx_sonus_coll="CTE_SONUS_1 SONUS_GSX_01 Start_Collect_PB_SONUS_GSX_01"
sbr_sonus_coll="CTE_SONUS_1 SONUS_SBR_01 Start_Collect_PB_SONUS_SBR_01"
cabsBTS_Tandem_coll="CTE_BTS_1 BTS_01 Start_Collect_PB_BTS_01"
cabsIlec_coll="CTE_CDRMS_1 EMI_ILEC_01 Start_Collect_PB_EMI_ILEC_01"

###########################################################
### List of ECS specific tables which needs cleenup
###########################################################
ecsCommTables="CFW_FILES_COLLECTED BL_SPR_ACC BL_SPR_SVC BL_SPR_PKG BL_SPR_EVENT BL_SPR BL_SPR_ERR BL_SPR_CTRL BL_SUB_PROC_STAT SUB_SBR SUB_SBR_BHN BL_ADJ_REM BL_ADJUSTMENT"
ecsBL_Tables="BL_SPR_ACC BL_SPR_SVC BL_SPR_PKG BL_SPR_EVENT BL_SPR BL_SPR_ERR"
ecsCtrlTable="BL_SPR_CTRL BL_SUB_PROC_STAT"
ecsSubSbrTables="SUB_SBR SUB_SBR_BHN"
ecsOtherTables="CFW_FILES_SEQ_CTRL CFW_FILES_SEQUENCES CFW_FILES_SEQ_CTRL"

###########################################################
### List of ECS specific jobs
###########################################################
#### TWC
twc_subs_coll="CTE_SPR_1 ICOMS_1 Start_Collect_PB_ICOMS_1"
twc_adj_coll="CTE_SPR_1 ADJ_1 Start_Collect_PB_ADJ_1"
#### BHN
bhn_subs_coll="CTE_SPR_2 SPR_BHN_2 Start_Collect_PB_SPR_BHN_2"
bhn_adj_coll="CTE_SPR_2 ADJ_2 Start_Collect_PB_ADJ_2"
#### National Sales
ns_subs_coll="CTE_SPR_2 NBC_1 Start_Collect_PB_NBC_1"
ns_adj_coll="CTE_SPR_2 NBC_ADJ_1 Start_Collect_PB_NBC_ADJ_1"
#### Maintenance Jobs
usgcycl_mntnc="CTE_SPR_1 MAINTENANCE Start_TP_USGCYCL_MNTNC_1"



###################################################################
##### Check for required command line arguments
###################################################################
if [ $# -ne 2 ]; then
    echo "usage: $0 <MED|CABS|ECS> <Cfg File>" 1>&2
    exit 1
fi

system=`echo $1 | tr '[:lower:]' '[:upper:]'`
if [ "$system" != "MED" ] && [ "$system" != "CABS" ] && [ "$system" != "ECS" ]; then
    echo "Invalid argument $1, expecting MED or CABS or ECS"
    exit 1
fi

### DP Server check
if [ "$system" = "MED" ] && [ "$HOSTNAME" != "slpnqapp02" ]; then
    echo " "
    echo "Your running DP MED application on $HOSTNAME server instead of slpnqapp02"
    echo "Exiting "
    exit 1
fi

### CABS Server check
if [ "$system" = "CABS" ] && [ "$HOSTNAME" != "slpnqapp01" ]; then
    echo " "
    echo "Your running CABS application on $HOSTNAME server instead of slpnqapp01"
    echo "Exiting "
    exit 1
fi

### ECS Server check
if [ "$system" = "ECS" ] && [ "$HOSTNAME" != "slpnqapp03" ]; then
    echo " "
    echo "Your running ECS application on $HOSTNAME server instead of slpnqapp03"
    echo "Exiting "
    exit 1
fi

if [ ! -f $2 ]; then
    echo "$2 is not exits!" 1>&2
    exit 1
fi

bold="\x1b[1m"
norm="\x1b[m"
echo " "
echo "################################################################################### "
echo -e "     -------  Running for:\x1b[1m \"$1\"\x1b[m "
echo -e "     -------  Using configuration file:$bold \"$2\"$norm "
echo "################################################################################### "
echo " "

interactive=1

if [ "$interactive" = "1" ]; then
    echo -ne "   Do you want to Continue? (${bold}y/n${norm}) > "
    read response
    response=`echo $response | tr '[:lower:]' '[:upper:]'`
    if [ "$response" = "N" ]; then
        echo "Exiting program."
        exit 1
    fi
fi
echo " "

##### Constants
basepath="/export/home/fwtwc/Vara/Ref_Scripts"
cfgFile=$2

cfgPrefix=$(echo $cfgFile | awk -F'.' '{print $1}')
logFile="${basepath}/log/`date +%Y-%m-%d`_process_${cfgPrefix}.log"
selectSqlFile="${basepath}/log/`date +%Y-%m-%d`_selectSql_${cfgPrefix}.log"
:> ${selectSqlFile}
TITLE="System Information: $HOSTNAME"
RIGHT_NOW=$(date +"%x %r %Z")
TIME_STAMP="Execution on $RIGHT_NOW"
#echo $TIME_STAMP
echo $TIME_STAMP > $logFile 
echo $TITLE >> $logFile

### Local parapeters
printDebug=0
dropCfgFile="${basepath}/dropPath.cfg"
declare -A dropLocMap
declare -A cteDataMap
declare -A allFiles
moveFileList=()
bigSize=50000
isCompUp=0
testRes=""

############### Start capture information into log file
echo "System type:  $system" >> $logFile
echo "################################################################################### " >> $logFile
echo -e "     -------  Running for:$bold \"$1\"$norm " >> $logFile
echo -e "     -------  Using configuration file:$bold \"$2\"$norm " >> $logFile
echo "################################################################################### " >> $logFile
echo " " >> $logFile
echo " " >> $logFile

#######################################################
##### Functions ######
#######################################################
sqlq(){
    if [ $system = "MED" ]; then
        DB_USERNAME="FWMEDQA"
        DB_PASSWORD="rxvw436yaq"
    elif [ $system = "CABS" ]; then
        DB_USERNAME="FWCABSQA"
        DB_PASSWORD="truly482s"
    elif [ $system = "ECS" ]; then
        DB_USERNAME="FWSPRSQ"
        DB_PASSWORD="mzbh739fdk"
    fi
    #echo "Executing: exit|sqlplus -S $DB_USERNAME/$DB_PASSWORD@$ORACLE_SID @$1"
    #sqlplus -S $DB_USERNAME/$DB_PASSWORD@$ORACLE_SID < $1|grep -v '^$'
    #echo "executing:" `cat $1` >> $logFile
    
    exit|sqlplus -S $DB_USERNAME/$DB_PASSWORD@$ORACLE_SID @$1 >> $logFile 2>&1
}


addToMap(){
    mapName=$1
    input=$2
    str1=$(awk -F= '{print $1}' <<< $input)
    cte=`echo "$str1" | sed 's/[[:space:]]//g'`
    str2=$(awk -F= '{print $2}' <<< $input)
    path=`echo "$str2" | sed 's/[[:space:]]//g'`
    eval $mapName[$cte]=$path

}

buildSelectSQLWithFeilds(){
   local tableList=$1
   local fileList=$2
   local selectList=$3
   local grpList=$4
   echo " " >> $selectSqlFile
   echo "################################################################################" >> $selectSqlFile
   echo "******************************* Group by Select ********************************" >> $selectSqlFile
   echo "################################################################################" >> $selectSqlFile
   echo " " >> $selectSqlFile
   for table in $tableList; do
      echo "####### $table  #######" >> $selectSqlFile
      unset sqlStmnt
      local j=0
      for file in $fileList;do
          if [ $j -eq 0 ];then
              if [ $table == "CFW_FILES_COLLECTED" ]; then
                  sqlStmnt="Select ${selectList} from $table where file_name in(\n'$file'"
              else
                  sqlStmnt="Select ${selectList} from $table where fw_filename in(\n'$file'"
              fi
                    #echo "sqlStmnt:$sqlStmnt"
                    #echo "j=$j"
               j=`expr $j + 1`
           else
               sqlStmnt="$sqlStmnt,\n'$file'"
           fi
      done
      sqlStmnt="$sqlStmnt) $grpList;"
      echo -e "$sqlStmnt" >> $selectSqlFile
      echo " " >> $selectSqlFile
    done
}
buildSelectSQL(){
    local tableList=$1
    local dataFileList=$2
    #echo "Building Select SQL:" >> $logFile
    for table in $tableList; do
        echo "####### $table  #######" >> $selectSqlFile
        unset sqlStmnt
        local j=0
        for file in $dataFileList;do
            #echo "file: $file"
            allFiles["$file"]=1
            if [ $j -eq 0 ];then
                if [ $table == "CFW_FILES_COLLECTED" ]; then
                    sqlStmnt="Select * from $table where file_name in(\n'$file'"
                else
                    sqlStmnt="Select * from $table where fw_filename in(\n'$file'"
                fi
                #echo "sqlStmnt:$sqlStmnt"
                #echo "j=$j"
                j=`expr $j + 1`
            else
                sqlStmnt="$sqlStmnt,\n'$file'"
            fi
        done
        sqlStmnt="$sqlStmnt);"
        echo -e "$sqlStmnt" >> $selectSqlFile
        echo " " >> $selectSqlFile
    done
}

buildRunSQL(){
    tableList=$1
    dataFileList=$2
    #echo "tableList:$tableList" >> $logFile
    #echo "dataFileList:$dataFileList" >> $logFile
    for table in $tableList; do
        unset sqlStmnt
        unset j
        j=0
        echo " " >> $logFile
        echo " ========================= Deleting table $table " >> $logFile
        for file in $dataFileList;do
            #echo "file: $file"
            if [ $j -eq 0 ];then
                if [ $table == "CFW_FILES_COLLECTED" ]; then
                    sqlStmnt="delete from $table where file_name in \n('$file'"
                else
                    sqlStmnt="delete from $table where fw_filename in \n('$file'"
                fi
                #echo "sqlStmnt:$sqlStmnt"
                #echo "j=$j"
                j=`expr $j + 1`
            else
                sqlStmnt="$sqlStmnt, \n'$file'"
            fi
        done
        sqlStmnt="$sqlStmnt);"
       # sqlStmnt="$sqlStmnt \ncommit;"
        echo -e "$sqlStmnt" >> $logFile
        echo -e "${sqlStmnt}" > /tmp/tmp.sql 
        echo " " >> $logFile
        sqlq /tmp/tmp.sql 
    done
}

moveFilesBack(){
   local dataLoc=$1
   #echo "Inside moveFilesBack...." >> $logFile
   echo " " >> $logFile
   echo "dataLoc: $dataLoc" >> $logFile
   echo " " >> $logFile

   ####### move list of files back to data loc
   #echo "moveFileList.... ${moveFileList[@]}"
   baseDir="$FUSIONWORKS_BASE/pcfs"
   for file in ${moveFileList[@]};do
       #echo "file: $file"
       cmd="mv \`find $baseDir -name $file\` $dataLoc"
       #echo "Executing :$cmd"
       eval $cmd
   done
}
updateTable(){
   local table=$1
   local file=$2
   part1=`echo $file | awk '{print substr($0,0,6)}'`
   part2=`echo $file | awk '{print substr($0,7,8)}'`
   sqlStmnt="update $table set next_seq='$part2' where key='$part1';"
   sqlStmnt="$sqlStmnt \ncommit;"
   echo " " >> $logFile
   echo -e "$sqlStmnt" >> $logFile
   echo -e "${sqlStmnt}" > /tmp/tmp.sql
   sqlq /tmp/tmp.sql
}

processFileByFile(){
   local cte=$1
   local stream=$2
   local dataLoc=$3
   local dropLoc=$4
   local dataFiles=$(ls $dataLoc -p | grep -v /)
   unset moveFileList
   
   ####### If files are big in size then move files else copy
   for file in $dataFiles;do
       size=`du $dataLoc/$file | awk -F' ' '{print $1}'`
       #echo "size of file $file:$size"
       if [ $size -ge $bigSize ]; then
         #echo " " >> $logFile
         #echo "Moving file $file to $dropLoc"
         mv $dataLoc/$file $dropLoc
         moveFileList+=($file)
       else
         echo " " >> $logFile
         echo " "
         echo "Copying file $file to $dropLoc" >> $logFile
         echo "Copying file $file to $dropLoc"
         cp $dataLoc/$file $dropLoc
       fi
       #############################
       ##### Update ctrl table
       #############################
       ctlTable="CFW_FILES_SEQ_CTRL"
       updateTable $ctlTable $file
       ### Execute Schedule task
       executeScheduleTask $cte $stream "$dataLoc"
   done
   #echo "moveFileList.... ${moveFileList[@]}"
   echo "Done."
}

CopyFilesToDrop(){
   local dataLoc=$1
   local dropLoc=$2
   local dataFiles=$(ls $dataLoc -p | grep -v /)
   unset moveFileList
   
   echo " "
   echo "Copying files to Drop folder...."
   #echo " " >> $logFile
   #echo "dataLoc: $dataLoc" >> $logFile
   #echo " " >> $logFile
   #echo "dropLoc: $dropLoc" >> $logFile
   #echo " " >> $logFile
   #echo "dataFiles: $dataFiles" >> $logFile
   #echo " " >> $logFile

   ####### If files are big in size then move files else copy
   for file in $dataFiles;do
       size=`du $dataLoc/$file | awk -F' ' '{print $1}'`
       #echo "size of file $file:$size"
       if [ $size -ge $bigSize ]; then
         #echo " " >> $logFile
         #echo "Moving file $file to $dropLoc"
         mv $dataLoc/$file $dropLoc
         moveFileList+=($file)
       else
         #echo " " >> $logFile
         echo "Copying file $file to $dropLoc" >> $logFile
         cp $dataLoc/$file $dropLoc
       fi
   done
   #echo "moveFileList.... ${moveFileList[@]}"
   echo "Done."
}

CleanDropFolder(){
   local cteStrm=$1
   local cteName=`echo $1 | awk -F',' '{print $1}'`
   local dataLoc=$2
   local stream=`echo $1 | awk -F',' '{print $2}'`
   local dataFiles=$(ls $dataLoc -p | grep -v /)
   local p1=`echo $cteName | awk -F'_' '{print $2}' | tr '[:upper:]' '[:lower:]'`
   local p2=`echo $cteName | awk -F'_' '{print $3}'`
   #echo "p1:$p1"
  # echo "p2:$p2"
   local pcfsPath=""
   case "$cteStrm" in
       "CTE_CDRMS_1,ALU") pcfsPath="$FUSIONWORKS_BASE_PATH/pcfs/pb_alu_audit"
       ;;
       "CTE_USAGE_1,BHN_IMS") pcfsPath="$FUSIONWORKS_BASE_PATH/pcfs/pb_ims_02"
       ;;
       "CTE_USAGE_1,NCIC") pcfsPath="$FUSIONWORKS_BASE_PATH/pcfs/pb_ncic_01"
       ;;
       "CTE_CLEC_1,NCIC") pcfsPath="$FUSIONWORKS_BASE_PATH/pcfs/pb_ncic_01"
       ;;
       "CTE_USAGE_1,INFONXX") pcfsPath="$FUSIONWORKS_BASE_PATH/pcfs/pb_infonxx_01"
       ;;
       "CTE_BROADSOFT_1,BW") pcfsPath="$FUSIONWORKS_BASE_PATH/pcfs/pb_broadsoft_01"
       ;;
       "CTE_CLEC_1,VERIZON") pcfsPath="$FUSIONWORKS_BASE_PATH/pcfs/pb_verizon_01"
       ;;
       "CTE_CLEC_1,KGB") pcfsPath="$FUSIONWORKS_BASE_PATH/pcfs/pb_kgb_01"
       ;;
       "CTE_SONUS_1,GSX_SONUS") pcfsPath="$FUSIONWORKS_BASE_PATH/pcfs/pb_sonus_gsx_01"
       ;;
       "CTE_SONUS_1,SBR_SONUS") pcfsPath="$FUSIONWORKS_BASE_PATH/pcfs/pb_sonus_sbr_01"
       ;;
       "CTE_BTS_1,BTS_TANDEM") pcfsPath="$FUSIONWORKS_BASE_PATH/pcfs/pb_bts_01"
       ;;
       "CTE_CDRMS_1,EMI_ILEC") pcfsPath="$FUSIONWORKS_BASE_PATH/pcfs/pb_emi_ilec_01"
       ;;
       "CTE_SPR_1,CSG") pcfsPath="$FUSIONWORKS_BASE_PATH/pcfs/pb_csg_1"
       ;;
       "CTE_SPR_1,ICOMS") pcfsPath="$FUSIONWORKS_BASE_PATH/pcfs/pb_icoms_1"
       ;;
       "CTE_SPR_1,TWC_ADJ") pcfsPath="$FUSIONWORKS_BASE_PATH/pcfs/pb_adj_1"
       ;;
       "CTE_SPR_2,BHN") pcfsPath="$FUSIONWORKS_BASE_PATH/pcfs/pb_spr_bhn_2"
       ;;
       "CTE_SPR_2,BHN_ADJ") pcfsPath="$FUSIONWORKS_BASE_PATH/pcfs/pb_adj_2"
       ;;
       "CTE_SPR_2,NS") pcfsPath="$FUSIONWORKS_BASE_PATH/pcfs/pb_nbc_1"
       ;;
       "CTE_SPR_2,NS_ADJ") pcfsPath="$FUSIONWORKS_BASE_PATH/pcfs/pb_nbc_adj_1"
       ;;
       *) pcfsPath="$FUSIONWORKS_BASE_PATH/pcfs/pb_${p1}_0${p2}"
       ;;
   esac
   #echo "pcfsPath=$pcfsPath" >> $logFile
   echo " " >> $logFile
   echo "################################################################################" >> $logFile
   echo "Cleaning UNIX folder path: $pcfsPath " >> $logFile
   echo " " 
   echo "Cleaning UNIX folder path: $pcfsPath "
   for j in $dataFiles;do
       #local file=`find $pcfsPath -name $j`
       local file=$(find $pcfsPath -name *${j}*)
       if [ ! -z "$file" ]; then
           if [ "$backupFiles" -eq 1 ]; then
               local tmpPath="$dataLoc/tmp"
               if [ ! -d "$tmpPath" ]; then
                   mkdir -p "$tmpPath"
               fi
               #echo "Moving $file to $tmpPath" >> $logFile
               cmd="mv $file $tmpPath"
               #echo "cmd:$cmd" >> $logFile
               eval $cmd
           else
               cmd="rm $file"
               eval $cmd
           fi
       fi
   done
   echo "Done..."
   echo "Done..." >> $logFile
}

CleanDB(){
   local cteName=`echo $1 | awk -F',' '{print $1}'`
   local dataLoc=$2
   local stream=`echo $1 | awk -F',' '{print $2}'`
   local dataFiles=$(ls $dataLoc -p | grep -v /)
   echo " " >> $logFile
   echo " " >> $logFile
   echo "################################################################################" >> $logFile
   echo "                    Cleaning Database: Starts..." >> $logFile
   echo "################################################################################" >> $logFile
   #echo " " >> $logFile
   #echo "cteName: $cteName" >> $logFile
   #echo " " >> $logFile
   #echo "Stream: $stream" >> $logFile
   #echo " " >> $logFile
   #echo "dataLoc: $dataLoc" >> $logFile
   #echo " " >> $logFile

   ######################################################
   ###############    TWC/BHN-ALU ####################### 
   ######################################################
   if [ $stream = "ALU" ] || [ $stream = "BHN_ALU" ]; then
     ### check for ALU Audit file processing
     if [ "$cte" = "CTE_CDRMS_1" ]; then
         #echo " " >> $logFile
         #echo "ALU Audir table:$aluAuditTables" >> $logFile
         #echo " " >> $logFile
         #echo "dataFiles:$dataFiles" >> $logFile
         ### Clean ALU tables
         buildRunSQL "${aluAuditTables}" "${dataFiles}"
         echo " " >> $logFile
         buildSelectSQL "${aluAuditTables}" "${dataFiles}"
     else
         #echo " " >> $logFile
         #echo "ALU table:$aluTables" >> $logFile
         #echo " " >> $logFile
         #echo "dataFiles:$dataFiles" >> $logFile
         ### Clean ALU tables
         buildRunSQL "${aluTables}" "${dataFiles}"
         echo " " >> $logFile
         buildSelectSQL "${aluTables}" "${dataFiles}"
     fi

   ######################################################
   ####################    Broadsoft  ####################### 
   ######################################################
   elif [ $stream = "BW" ]; then
     #echo "Broadsoft STREAM table" >> $logFile
     #echo " " >> $logFile
     #echo "broadSoftTables:$broadSoftTables" >> $logFile
     #echo " " >> $logFile
     #echo "dataFiles:$dataFiles" >> $logFile
     buildRunSQL "${broadSoftTables}" "${dataFiles}"
     echo " " >> $logFile
     buildSelectSQL "${broadSoftTables}" "${dataFiles}"

   ######################################################
   ####################    BTS  ####################### 
   ######################################################
   elif [ $stream = "BTS" ] || [ $stream = "BHN_BTS" ]; then
     #echo "BTS STREAM table" >> $logFile
     #echo " " >> $logFile
     #echo "btsTables:$btsTables" >> $logFile
     #echo " " >> $logFile
     #echo "dataFiles:$dataFiles" >> $logFile
     buildRunSQL "${btsTables}" "${dataFiles}"
     echo " " >> $logFile
     buildSelectSQL "${btsTables}" "${dataFiles}"

   ######################################################
   ####################    NCIC  ####################### 
   ######################################################
   elif [ $stream = "NCIC" ]; then
     #echo "NCIC STREAM table" >> $logFile
     #echo " " >> $logFile
     #echo "ncicTables:$ncicTables" >> $logFile
     #echo " " >> $logFile
     #echo "dataFiles:$dataFiles" >> $logFile
     buildRunSQL "${ncicTables}" "${dataFiles}"
     echo " " >> $logFile
     buildSelectSQL "${ncicTables}" "${dataFiles}"

   ######################################################
   ####################    INFONXX  ####################### 
   ######################################################
   elif [ $stream = "INFONXX" ]; then
     #echo "INFONXX STREAM table" >> $logFile
     #echo " " >> $logFile
     #echo "infonxxTables:$infonxxTables" >> $logFile
     #echo " " >> $logFile
     #echo "dataFiles:$dataFiles" >> $logFile
     buildRunSQL "${infonxxTables}" "${dataFiles}"
     echo " " >> $logFile
     buildSelectSQL "${infonxxTables}" "${dataFiles}"

   ######################################################
   ####################    IMS  ####################### 
   ######################################################
   elif [ $stream = "IMS" ] || [ $stream = "BHN_IMS" ]; then
     #echo "IMS STREAM table" >> $logFile
     #echo " " >> $logFile
     #echo "imsTables:$imsTables" >> $logFile
     #echo " " >> $logFile
     #echo "dataFiles:$dataFiles" >> $logFile
     buildRunSQL "${imsTables}" "${dataFiles}"
     echo " " >> $logFile
     buildSelectSQL "${imsTables}" "${dataFiles}"

   ######################################################
   ####################    HIQ  ####################### 
   ######################################################
   elif [ $stream = "HIQ" ] || [ $stream = "BHN_HIQ" ]; then
     #echo "HIQ STREAM table" >> $logFile
     #echo " " >> $logFile
     #echo "hiqTables:$hiqTables" >> $logFile
     #echo " " >> $logFile
     #echo "dataFiles:$dataFiles" >> $logFile
     buildRunSQL "${hiqTables}" "${dataFiles}"
     echo " " >> $logFile
     buildSelectSQL "${hiqTables}" "${dataFiles}"

   ######################################################
   ####################    KGB  ####################### 
   ######################################################
   elif [ $stream = "KGB" ]; then
     #echo "KGB STREAM table" >> $logFile
     #echo " " >> $logFile
     #echo "kgbTables:$kgbTables" >> $logFile
     #echo " " >> $logFile
     #echo "dataFiles:$dataFiles" >> $logFile
     buildRunSQL "${kgbTables}" "${dataFiles}"
     echo " " >> $logFile
     buildSelectSQL "${kgbTables}" "${dataFiles}"

   ######################################################
   ####################    SONUS  ####################### 
   ######################################################
   elif [ $stream = "SONUS" ]; then
     #echo "SONUS STREAM table" >> $logFile
     #echo " " >> $logFile
     #echo "sonusTables:$sonusTables" >> $logFile
     #echo " " >> $logFile
     #echo "dataFiles:$dataFiles" >> $logFile
     buildRunSQL "${sonusTables}" "${dataFiles}"
     echo " " >> $logFile
     buildSelectSQL "${sonusTables}" "${dataFiles}"

   ######################################################
   ####################    VERIZON  ####################### 
   ######################################################
   elif [ $stream = "VERIZON" ]; then
     #echo "VERIZON STREAM table" >> $logFile
     #echo " " >> $logFile
     #echo "verizonTables:$verizonTables" >> $logFile
     #echo " " >> $logFile
     #echo "dataFiles:$dataFiles" >> $logFile
     buildRunSQL "${verizonTables}" "${dataFiles}"
     echo " " >> $logFile
     buildSelectSQL "${verizonTables}" "${dataFiles}"

   ######################################################
   ################  CABS GSX/SBR SONUS  ################ 
   ######################################################
   elif [ $stream = "GSX_SONUS" ] || [ $stream = "SBR_SONUS" ]; then
     buildRunSQL "${cabsSonusTables}" "${dataFiles}"
     echo " " >> $logFile
     buildSelectSQL "${cabsSonusTables}" "${dataFiles}"

   ######################################################
   ###################   BTS TANDEM  #################### 
   ######################################################
   elif [ $stream = "BTS_TANDEM" ]; then
     buildRunSQL "${cabsBTSTandemTables}" "${dataFiles}"
     echo " " >> $logFile
     buildSelectSQL "${cabsBTSTandemTables}" "${dataFiles}"

   ######################################################
   ###################   EMI ILEC   ##################### 
   ######################################################
   elif [ $stream = "EMI_ILEC" ]; then
     buildRunSQL "${cabsIlecTables}" "${dataFiles}"
     echo " " >> $logFile
     buildSelectSQL "${cabsIlecTables}" "${dataFiles}"
   fi


   ######################################################
   ####################    COMMON Tables  ####################### 
   ######################################################
    if [ $system = "MED" ]; then
        buildRunSQL "${commTables}" "${dataFiles}"
        buildSelectSQL "${commTables}" "${dataFiles}"
    elif [ $system = "CABS" ]; then
        buildRunSQL "${cabsCommTables}" "${dataFiles}"
        buildSelectSQL "${cabsCommTables}" "${dataFiles}"
    elif [ $system = "ECS" ]; then
        buildRunSQL "${ecsCommTables}" "${dataFiles}"
        buildSelectSQL "${ecsCommTables}" "${dataFiles}"
    fi

    #### Commit changes
    sqlStmnt="commit;"
    echo -e "$sqlStmnt" >> $logFile
    echo -e "${sqlStmnt}" > /tmp/tmp.sql 
    echo " " >> $logFile
    sqlq /tmp/tmp.sql 

   echo "################################################################################" >> $logFile
   echo "Cleaning Database: Completed." >> $logFile
   echo "################################################################################" >> $logFile
}


#### Start given component
StartComponent(){
    comp=$1
    isCompUp=0
    sleepTime=30
    status=$(IsFusionWorksRunning.sh | grep ${comp} | grep "is running")
    #echo " " >> $logFile
    #echo "Run status: $status" >> $logFile
    if [ "$status" ]; then
        pidVal=$(echo $status | awk -F' ' '{print $3}')
        echo "$comp is already running with PID=$pidVal" >> $logFile
        isCompUp=1
    else
        echo "component ->  $comp is not running" >> $logFile
        echo " " >> $logFile
        echo " "
        #echo "Starting component: $comp" >> $logFile
        #echo "Starting component: $comp"
        if [ $comp == "mediation" ]; then
          echo "Starting MediationServer" >> $logFile
          echo "Starting MediationServer" 
           $FUSIONWORKS_PROD/bin/StartMediationServer > /dev/null 2>&1
        elif [ "$comp" == "nameserv" ]; then
          echo "Starting NameServ" >> $logFile
          echo "Starting NameServ" 
           $FUSIONWORKS_PROD/bin/StartNameServ > /dev/null 2>&1
        elif [ "$comp" == "jboss" ]; then
           echo "Starting JBOSS" >> $logFile
           echo "Starting JBOSS" 
           $FUSIONWORKS_PROD/bin/StartJBossAppServer > /dev/null 2>&1
        else
           echo "Starting COR: $comp" >> $logFile
           echo "Starting COR: $comp" 
           $FUSIONWORKS_PROD/bin/StartCOR $comp 1000 > /dev/null 2>&1
        fi
        ### Wait for component to start
        waitCnt=$maxwait
        while [ $waitCnt -gt 0 ]; do
            status=$(IsFusionWorksRunning.sh | grep ${comp} | grep "is running")
            if [ "$status" ]; then
                pidVal=$(echo $status | awk -F' ' '{print $3}')
                echo "${comp} Started successfully with PID:$pidVal"
                echo "${comp} Started successfully with PID:$pidVal" >> $logFile
                waitCnt=0
                isCompUp=1
            else
                sleep $sleepTime
                waitCnt=`expr $waitCnt - 1`
                echo " "
                echo "Still waiting for ${comp} component to startup..." >> $logFile
                echo "Still waiting for ${comp} component to startup..."
            fi
       done
    fi
    
    if [ $isCompUp -eq 0 ]; then
        echo " *****---------------***********-----------************"
        echo "$comp is NOT starting even after `expr ${maxwait}*$sleepTime` sec...." >> $logFile
        echo "$comp is NOT starting even after `expr ${maxwait}*$sleepTime` sec...."
        echo " *****---------------***********-----------************"
        echo " " >> $logFile
        echo " "
        echo " Continuing on next..." >> $logFile
        testRes="FAIL"
    fi

}

#### Start Main server components
StartFWApplication(){
    ### StartMediationServer
    StartComponent "mediation"

    ### StartNameServ
    StartComponent "nameserv"

    ### StartJBossAppServer
    StartComponent "jboss"
}

#### Execute Schedule task
runJob(){
    local job="$FUSIONWORKS_BASE/fw_execScheduledTask.sh $1"
    echo "$job" >> $logFile
    #echo "$job"
    eval $job > /dev/null 2>&1
    echo "Done.. " >> $logFile
    echo "Done.. "
    echo " "
    echo " " >> $logFile
    echo "sleep 2 sec.." >> $logFile
    sleep 2
}

verifyFileProcess(){
    local fileList=$1
    local sourceLoc=$2
    #echo "Inside verifyFileProcess... $fileList"
    echo " " >> $logFile
    echo " " >> $logFile
    echo "###############################################################################" >> $logFile
    echo "                  Verify file process..." >> $logFile
    echo "###############################################################################" >> $logFile
    echo "###############################################################################"
    echo "                  Verify file process..."
    echo "###############################################################################"
    for k in $fileList;do
       #echo "file:$k";
       resp=`find $FUSIONWORKS_BASE/ -name *${k}* 2>/dev/null | grep -v ACK`
       #echo "resp:$resp"
       tmpSt=$( echo "$resp" | cut -d'/' -f5)
       #echo "tmpSt:" $tmpSt
       if [ "$tmpSt" == "drop" ];then
           echo "file: $k  ----- in drop, check file mask" >> $logFile
           echo "file: $k  ----- in drop, check file mask"
           continue
       fi
       local stillInProcess=1
       local firstTime=0
       while [ "$stillInProcess" -eq 1 ];do
           if [ ! -z "$resp" ]; then
               fieldCount=`expr $( echo $resp | awk -F'/' '{print NF}') - 1`
               local status=$( echo "$resp" | cut -d'/' -f"$fieldCount")
               #echo "status=$status"
               case "$status" in
                  PARSING) 
                      if [ "$firstTime" -eq 0 ]; then
                          echo "File: $k ----- in PARSING" >> $logFile
                          echo "File: $k ----- in PARSING"
                          echo "Waiting..." >> $logFile
                          echo "Waiting..."
                          firstTime=1
                      fi
                      sleep 2
                      resp=`find $FUSIONWORKS_BASE/pcfs -name *${k}*`
                  ;;
                  QUEUE) 
                      if [ "$firstTime" -eq 0 ]; then
                          #echo "File: $k ----- in QUEUE" >> $logFile
                          #echo "File: $k ----- in QUEUE"
                          echo "Waiting.." >> $logFile
                          echo "Waiting..."
                          firstTime=1
                      fi
                      sleep 2
                      resp=`find $FUSIONWORKS_BASE/pcfs -name *${k}*`
                  ;;
                 ARCHIVED) echo "File: $k ----- $status" >> $logFile
                    echo "File: $k ----- $status"
                    stillInProcess=0
                  ;;
                 *) echo "File: $k ----- $status" >> $logFile
                    echo "File: $k ----- $status"
                    if [ ${#moveFileList[@]} -ne 0 ]; then
                        delete=($k)
                        moveFileList=(${moveFileList[@]/$delete})
                        local tmpPath="$sourceLoc/tmp"
                        if [ ! -d "$tmpPath" ]; then
                            mkdir -p "$tmpPath" 
                        fi 
                        cmd="mv $file $tmpPath"
                        echo "cmd:$cmd" >> $logFile
                        eval $cmd
                    fi
                    stillInProcess=0
                    testRes="FAIL"
                  ;;
               esac
           fi
       done
    done
}

#### Execute Schedule task
executeScheduleTask(){
    local cte=$1
    local stream=$2
    local dloc=$3
    local fileList=$(ls $3 -p | grep -v /)
    fileList="$fileList ${moveFileList[@]}" 
    #echo "fileList: $fileList"
    echo " " >> $logFile
    echo " "
    case "$cte" in
       CTE_ALU_1*) echo "Executing Collection job:" >> $logFile
           echo "Executing Collection job:"
           runJob "$alu1_coll"
           verifyFileProcess "${fileList}" "$dloc"

           echo " " >> $logFile
           echo " "
           echo "Executing Aggregation job:"
           echo "Executing Aggregation job:" >> $logFile
           runJob "$alu1_aggr"

           echo " " >> $logFile
           echo " "
           echo "Executing Medaition job:" >> $logFile
           echo "Executing Medaition job:"
           runJob "$alu1_med"
        ;;
       CTE_ALU_2*) echo "Executing Collection job:" >> $logFile
           echo "Executing Collection job:"
           runJob "$alu2_coll"
           verifyFileProcess "${fileList}" "$dloc"

           echo " " >> $logFile
           echo " "
           echo "Executing Aggregation job:" >> $logFile
           echo "Executing Aggregation job:"
           runJob "$alu2_aggr"

           echo " " >> $logFile
           echo " "
           echo "Executing Medaition job:" >> $logFile
           echo "Executing Medaition job:"
           runJob "$alu2_med"
        ;;
       CTE_CDRMS_1*) echo "Executing Collection job:" >> $logFile
           echo "Executing Collection job:"
           if [ "$stream" = "ALU" ]; then
               runJob "$audit1_coll"
           elif [ "$stream" = "EMI_ILEC" ]; then
               runJob "$cabsIlec_coll"
           fi
           verifyFileProcess "${fileList}" "$dloc"
        ;;
       CTE_BROADSOFT_1*) echo "Executing Collection job:" >> $logFile
           echo "Executing Collection job:"
           runJob "$brdSoft_coll"
           verifyFileProcess "${fileList}" "$dloc"

           echo " " >> $logFile
           echo " "
           echo "Executing Aggregation job:" >> $logFile
           echo "Executing Aggregation job:"
           runJob "$brdSoft_aggr"
        ;;
       CTE_BTS_1*) echo "Executing Collection job:" >> $logFile
           echo "Executing Collection job:"
           if [ "$stream" = "BTS" ]; then
               runJob "$bts1_coll"
           elif [ "$stream" = "BTS_TANDEM" ]; then
               runJob "$cabsBTS_Tandem_coll"
           fi
           verifyFileProcess "${fileList}" "$dloc"
        ;;
       CTE_BTS_2*) echo "Executing Collection job:" >> $logFile
           echo "Executing Collection job:"
           runJob "$bts2_coll"
           verifyFileProcess "${fileList}" "$dloc"
        ;;
       CTE_IMS_1*) echo "Executing Collection job:" >> $logFile
           echo "Executing Collection job:"
           runJob "$ims1_coll"
           verifyFileProcess "${fileList}" "$dloc"
        ;;
       CTE_USAGE_1*) echo "Executing Collection job:" >> $logFile
           echo "Executing Collection job:"
           if [ "$stream" = "BHN_IMS" ]; then
               runJob "$ims2_coll"
           elif [ "$stream" = "INFONXX" ]; then
               runJob "$infoNxx_coll"
           elif [ "$stream" = "NCIC" ]; then
               runJob "$ncic_coll"
           fi
           verifyFileProcess "${fileList}" "$dloc"
        ;;
       CTE_HIQ_1*) echo "Executing Collection job:" >> $logFile
           echo "Executing Collection job:"
           runJob "$hiq1_coll"
           verifyFileProcess "${fileList}" "$dloc"
        ;;
       CTE_HIQ_2*) echo "Executing Collection job:" >> $logFile
           echo "Executing Collection job:"
           runJob "$hiq2_coll"
           verifyFileProcess "${fileList}" "$dloc"
        ;;
       CTE_SONUS_1*) echo "Executing Collection job:" >> $logFile
           echo "Executing Collection job:"
           if [ "$stream" = "SONUS" ]; then
               runJob "$sonus_coll"
           elif [ "$stream" = "GSX_SONUS" ]; then
               runJob "$gsx_sonus_coll"
           elif [ "$stream" = "SBR_SONUS" ]; then
               runJob "$sbr_sonus_coll"
           fi
           verifyFileProcess "${fileList}" "$dloc"
        ;;
       CTE_CLEC_1*) echo "Executing Collection job:" >> $logFile
           echo "Executing Collection job:"
           if [ "$stream" = "KGB" ]; then
               runJob "$kgb_coll"
           elif [ "$stream" = "SPRINT" ]; then
               runJob "$sprint_coll"
           elif [ "$stream" = "VERIZON" ]; then
               runJob "$verizon_coll"
           fi
           verifyFileProcess "${fileList}" "$dloc"
        ;;
       CTE_SPR_1*) echo "Executing Collection job:" >> $logFile
           echo "Executing Collection job:"
           if [ "$stream" = "CSG" ] || [ "$stream" = "ICOMS" ]; then
               runJob "$twc_subs_coll"
           elif [ "$stream" = "TWC_ADJ" ]; then
               runJob "$twc_adj_coll"
           fi
           verifyFileProcess "${fileList}" "$dloc"
           sleep 2
           subStr=`echo $stream | cut -d_ -f2`
           if [ "$subStr" != "ADJ" ]; then
               echo " "
               echo " " >> $logFile
               echo "Executing Maintenance job:"
               echo "Executing Maintenance job:" >> $logFile
               runJob "$usgcycl_mntnc"
           fi 
        ;;
       CTE_SPR_2*) echo "Executing Collection job:" >> $logFile
           echo "Executing Collection job:"
           if [ "$stream" = "BHN" ]; then
               runJob "$bhn_subs_coll"
           elif [ "$stream" = "NS" ]; then
               runJob "$ns_subs_coll"
           elif [ "$stream" = "BHN_ADJ" ]; then
               runJob "$bhn_adj_coll"
           elif [ "$stream" = "NS_ADJ" ]; then
               runJob "$ns_adj_coll"
           fi
           verifyFileProcess "${fileList}" "$dloc"
           sleep 2
           subStr=`echo $stream | cut -d_ -f2`
           if [ "$subStr" != "ADJ" ]; then
               echo " "
               echo " " >> $logFile
               echo "Executing Maintenance job:"
               echo "Executing Maintenance job:" >> $logFile
               runJob "$usgcycl_mntnc"
           fi 
        ;;
        *) echo "Default case...."
        ;;
    esac
}

###############################################
####### Main starts ###########################
###############################################


### Read config file
echo "###################  Configuration file enabled for: " >> $logFile

echo " "
echo "################### Configuration file enabled for: "
while read -r line
do
    [[ $line = \#* ]] && continue 
    [[ -z $line ]] && continue 
    # display $line or do somthing with $line
    #echo "$line" >> $logFile
    #addCteRunList "$line"
    echo "$line" >> $logFile
    echo "$line"
    addToMap cteDataMap "$line"
done <"$cfgFile"
echo "###################################################################################"

### Read drop config file
echo " "
#echo " ###################  Reading drop Config file ##### "
while read -r line
do
    [[ $line = \#* ]] && continue 
    [[ -z $line ]] && continue 
    # display $line or do somthing with $line
    #echo "$line"
   # addDropConfList "$line"
    addToMap dropLocMap "$line"
done <"$dropCfgFile"


if [ "$interactive" = "1" ]; then
    echo -ne "   Is configuration looks good and wish to Continue? (${bold}y/n${norm}) > "
    read response
    if [ "$response" = "n" ]; then
        echo "Exiting program."
        exit 1
    fi
fi

if [ "$printDebug" = "1" ]; then
        echo " **********************************" >> $logFile
        echo "CTE Data map " >> $logFile
	for k in "${!cteDataMap[@]}"; do echo "    $k --> ${cteDataMap[$k]}" >> $logFile; done
	echo "----------------------------" >> $logFile
        echo " " >> $logFile
        echo "Drop Location map " >> $logFile
        echo " " >> $logFile
	for k in "${!dropLocMap[@]}"; do echo "    $k --> ${dropLocMap[$k]}" >> $logFile; done
        echo " " >> $logFile
        echo " **********************************" >> $logFile
fi

echo " "
echo "Log file: $logFile"
echo "SQL Select file: $selectSqlFile"

#### Start Mediation/NameServer and JBOSS
echo " " >> $logFile
echo " " >> $logFile
echo "###############################################################################" >> $logFile
echo "                  Checking FW Components..." >> $logFile
echo "###############################################################################" >> $logFile
StartFWApplication
if [ "$isCompUp" -eq 0 ]; then
    echo "Failed to start FW main server components, exiting..."
    exit 1
fi

######################################################
#### Loop through CTE array list and work on each CTE
######################################################
for key in ${!cteDataMap[@]}; do 
    testDataLoc=${cteDataMap[$key]}
    if [ ${dropLocMap[$key]+_} ]; then
       dropLoc=${dropLocMap[$key]}
    else
       echo "Drop location is missing for: $key, exiting.." >> $logFile
       echo "Drop location is missing for: $key, exiting.."
       echo "exit 1" >> $logFile
       exit 1
    fi
    cte=$(echo $key | awk -F',' '{print $1}')
    stream=$(echo $key | awk -F',' '{print $2}')
    testRes="PASS"
    echo " "
    echo " "
    echo "******************************************************************************* "
    echo "                          Working on :'$cte'"
    echo "******************************************************************************** "
    echo " "
    echo "=====================================================================" >> $selectSqlFile
    echo "     *********** $cte ************" >> $selectSqlFile 
    echo "=====================================================================" >> $selectSqlFile
    echo " " >> $selectSqlFile
    echo " " >> $logFile
    echo " " >> $logFile
    echo "********************************************************************************" >> $logFile
    echo "                          Working on :'$cte'" >> $logFile
    echo "********************************************************************************" >> $logFile

    #### Start CTE component
    echo " " >> $logFile
    echo "                  Start $cte" >> $logFile
    echo "###############################################################################" >> $logFile
    StartComponent $cte
    if [ "$isCompUp" -eq 0 ]; then
        echo "Failed to start $cte components, continuing to next CTE..."
        continue 
    fi
    echo "###############################################################################" >> $logFile
    echo "Test data Loc:'$testDataLoc'"
    #echo " Drop Loc:'$dropLoc'"
    echo " "

    echo " " >> $logFile
    echo "Test data Loc:'$testDataLoc'" >> $logFile
    echo "Drop Loc:'$dropLoc'" >> $logFile
    echo " " >> $logFile
    dataFiles=$(ls $testDataLoc -p | grep -v /)
    echo "Files found: " >> $logFile
    echo "Files found: "
    for i in $dataFiles;do
      echo "    $i" >> $logFile
      echo "    $i"
    done

    ############## Clean FW pcfs folder before processing ########################
    CleanDropFolder $key $testDataLoc

    ############## Clean DB tables before processing ########################
    CleanDB $key $testDataLoc

    ##### If ECS copy and execute task one by one
    if [ "$system" = "ECS" ]; then
       processFileByFile $cte $stream $testDataLoc $dropLoc
    else
        ############## Copy files to drop directory ########################
        echo " " >> $logFile
        echo "###############################################################################" >> $logFile
        echo "                  Copying test data files to Drop directory.." >> $logFile
        echo "###############################################################################" >> $logFile
        CopyFilesToDrop $testDataLoc $dropLoc

        #### Execute Schedule task #################
        echo " " >> $logFile
        echo " " >> $logFile
        echo " "
        echo " "
        echo "###############################################################################" >> $logFile
        echo "     ************** Executing JOBS **************" >> $logFile
        echo "###############################################################################"
        echo "     ************** Executing JOBS **************"
        executeScheduleTask $cte $stream "$testDataLoc"
        echo "###############################################################################" >> $logFile
    fi

    ############## Move huge files back to test data directory ########################
    #echo "****Move file list: ${moveFileList[@]}:"
    if [ ${#moveFileList[@]} -gt 0 ]; then
        moveFilesBack $testDataLoc
    fi
    
    #echo "$dataFiles"
    echo " " >> $logFile
    echo " "
    echo "********************************************************************************"
    echo "Done with '$cte - $stream' --- Test Result => $testRes"
    echo "********************************************************************************"
    echo "********************************************************************************" >> $logFile
    echo "Done with '$cte - $stream' --- Test Result => $testRes" >> $logFile
    echo "********************************************************************************" >> $logFile
    numCompUp=`$FUSIONWORKS_PROD/bin/IsFusionWorksRunning.sh | grep "is running" | wc -l`
    if [ $numCompUp -gt $maxCompUp ]; then
        echo "Stopping $cte ..." >> $logFile
        echo "Stopping $cte .."
        $FUSIONWORKS_PROD/bin/StopComponent cor${cte} > /dev/null 2>&1
        echo "Done." >> $logFile
        echo "Done."
    fi
done

#### Build Select SQL for all files  ################
cteList=${!cteDataMap[@]}
count=`echo $cteList | awk -F' ' '{print NF}'`
tmpAllFiles=${!allFiles[@]}
#echo "count:$count"
if [ $count -gt 1 ]; then
    echo "=====================================================================" >> $selectSqlFile
    echo "       ************** Common tables with All Files **************" >> $selectSqlFile
    echo "=====================================================================" >> $selectSqlFile
    buildSelectSQL "$commTables" "${tmpAllFiles}"
fi

#### Build Group by Select SQL for all files  ################
tables="BL_CDR_UNENR"
sfeilds="FW_REASON_CODE, FW_STREAM_NAME, count(FW_REASON_CODE)"
gfeilds="group by FW_REASON_CODE, FW_STREAM_NAME order by FW_STREAM_NAME"
buildSelectSQLWithFeilds "$tables" "$tmpAllFiles" "$sfeilds" "$gfeilds"
    

echo " " >> $logFile
echo " " >> $logFile
echo "Execution Completed." >> $logFile

echo " "
echo " "
echo " Execution completed."
