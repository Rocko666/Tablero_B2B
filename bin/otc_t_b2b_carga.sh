set -e
#!/bin/bash
#############################################################################
# Proyecto:          Tablas finales tablero B2B                				#
# Cliente:           Telefonica                                 			#
# Elaborado por:     Softconsulting S.A.                        			#
# Script:            otc_t_b2b_carga.sh                         			#
# Fecha de creacion: 2020/07/18                                 			#
# Fecha de modificacion:                                        			#
# Fecha de modificacion:  2023/03/29                            			#
# User modificacion: Klever Amari                               			#
# Descripcion de modificacion: Se actualiza la sentencia hive por beeline   #
#########################################################################################################
# MODIFICACIONES																						#
# FECHA  		AUTOR     			DESCRIPCION MOTIVO												    #
# 2023-10-26	CRISTIAN ORTIZ		Migracion beeline->spark BIGD-368									#
#########################################################################################################
##############
# VARIABLES #
##############
ENTIDAD=D_OTC_T_B2B_PARQUE_FACTURACION
FECHAEJE=$1 # Formato yyyyMMdd

# PARAMETROS DEFINIDOS EN LA TABLA params_des
VAL_KINIT=$(mysql -N <<<"select valor from params_des where ENTIDAD = 'D_SPARK_GENERICO' AND parametro = 'VAL_KINIT';")
$VAL_KINIT
VAL_RUTA_SPARK=`mysql -N  <<<"select valor from params_des where ENTIDAD = 'D_SPARK_GENERICO' AND parametro = 'VAL_RUTA_SPARK';"`
VAL_RUTA=$(mysql -N <<<"select valor from params_des where entidad = '"$ENTIDAD"' and ambiente='"$AMBIENTE"' AND parametro = 'VAL_RUTA';")
VAL_ESQUEMA=$(mysql -N <<<"select valor from params_des where entidad = '"$ENTIDAD"' and ambiente='"$AMBIENTE"' AND parametro = 'VAL_ESQUEMA';")
VAL_QUEUE=$(mysql -N <<<"select valor from params_des where entidad = '"$ENTIDAD"' and ambiente='"$AMBIENTE"' AND parametro = 'VAL_QUEUE';")
VAL_MASTER=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_MASTER';"`
VAL_DRIVER_MEMORY=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_DRIVER_MEMORY';"`
VAL_EXECUTOR_MEMORY=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_EXECUTOR_MEMORY';"`
VAL_NUM_EXECUTORS=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NUM_EXECUTORS';"`
VAL_NUM_EXECUTOR_CORES=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NUM_EXECUTOR_CORES';"`

#PARAMETROS CALCULADOS Y AUTOGENERADOS
ini_fecha=$(date '+%Y%m%d%H%M%S')
eval year=$(echo $FECHAEJE | cut -c1-4)
eval month=$(echo $FECHAEJE | cut -c5-6)
day="01"
fechaIniMes=$year$month$day #Formato YYYYMMDD
fechaIniMesDate=$(date '+%Y%m%d' -d "$fechaIniMes")
fechaFinMesAnterior=$(date -d "$fechaIniMesDate-1 day" +%Y%m%d)
fecha_eje1=$(date '+%Y%m%d' -d "$fechaFinMesAnterior")
MesTresDate=$(date -d "$fechaIniMes-3 month" +%Y-%m-%d)
MesVeinteCuatro=$(date -d "$fechaFinMesAnterior-24 month" +%Y%m%d)
MesVeinteCuatroDate=$(date -d "$fechaFinMesAnterior-24 month" +%Y-%m-%d)
MesTres=$(date -d "$fechaIniMes-3 month" +%Y%m%d)
VAL_LOG=$VAL_RUTA/logs/carga_informacion_b2b_$ini_fecha.log

#VALIDACION DE PARAMETROS INICIALES
if  [ -z "$ENTIDAD" ] || 
	[ -z "$FECHAEJE" ] || 
	[ -z "$VAL_RUTA_SPARK" ] || 
	[ -z "$VAL_RUTA" ] || 
	[ -z "$VAL_ESQUEMA" ]|| 
	[ -z "$VAL_QUEUE" ] ||  
	[ -z "$VAL_MASTER" ] || 
	[ -z "$VAL_DRIVER_MEMORY" ] || 
	[ -z "$VAL_EXECUTOR_MEMORY" ] || 
	[ -z "$VAL_NUM_EXECUTORS" ] || 
	[ -z "$VAL_NUM_EXECUTOR_CORES" ] || 
	[ -z "$MesTres" ] || 
	[ -z "$MesTresDate" ] || 
	[ -z "$MesVeinteCuatro" ] || 
	[ -z "$MesVeinteCuatroDate" ] || 
	[ -z "$VAL_LOG" ]; then
	echo " ERROR - uno de los parametros esta vacio o nulo"
	exit 1
fi

#---------------------------------------------------
# Carga variables de configuracion desde table de parametros
#---------------------------------------------------
echo "****** Carga variables de configuracion ******"
echo "Ultimo dia mes anterior" $fechaFinMesAnterior
echo "fechaIniMes" $fechaIniMes
echo "fechaIniMesDate" $fechaIniMesDate
echo "fechaFinMesAnterior" $fechaFinMesAnterior
echo "fecha_eje1" $fecha_eje1
echo "MesTresDate" $MesTresDate
echo "MesVeinteCuatro" $MesVeinteCuatro
echo "MesVeinteCuatroDate" $MesVeinteCuatroDate
echo "MesTres" $MesTres

##############################################
# ETAPA 1: EJECUCION DEL PROCESO DE CARGA B2B
##############################################

#--------------------------------------------------
# Impresion de parametros
#---------------------------------------------------
echo "----------------------------------" 2>&1 &>> $VAL_LOG
echo "------*****INICIO ETAPA 1*****--------------" 2>&1 &>> $VAL_LOG
echo "COLA:" $VAL_QUEUE 2>&1 &>> $VAL_LOG
echo "ESQUEMA HIVE: " $VAL_ESQUEMA 2>&1 &>> $VAL_LOG
echo "DIR SERV: " $VAL_RUTA 2>&1 &>> $VAL_LOG
echo "FECHA EJECUCION: " $FECHAEJE 2>&1 &>> $VAL_LOG
echo "FECHA EJECUCION MENOS 24 MESES : " $MesVeinteCuatro 2>&1 &>> $VAL_LOG
echo "FECHA EJECUCION MENOS 3 MESES DATE: " $MesTresDate 2>&1 &>> $VAL_LOG
echo "FECHA EJECUCION MENOS 3 MESES : " $MesTres 2>&1 &>> $VAL_LOG
echo "****** Fin Carga variables de configuracion ******"
echo "----------------------------------" 2>&1 &>> $VAL_LOG

#---------------------------------------------------------------------------------------------------
# #REALIZA EL LLAMADO EL ARCHIVO SPARK QUE REALIZA LOS CRUCES DE INFORMACION
#---------------------------------------------------------------------------------------------------
$VAL_RUTA_SPARK \
--jars /opt/cloudera/parcels/CDH/jars/hive-warehouse-connector-assembly-1.0.0.7.1.7.1000-141.jar \
--conf spark.sql.hive.hwc.execution.mode=spark \
--conf spark.kryo.registrator=com.qubole.spark.hiveacid.util.HiveAcidKyroRegistrator \
--conf spark.sql.extensions=com.qubole.spark.hiveacid.HiveAcidAutoConvertExtension \
--py-files /opt/cloudera/parcels/CDH/lib/hive_warehouse_connector/pyspark_hwc-1.0.0.7.1.7.1000-141.zip \
--conf spark.datasource.hive.warehouse.read.mode=DIRECT_READER_V2 \
--conf spark.sql.hive.hiveserver2.jdbc.url="jdbc:hive2://quisrvbigdata1.otecel.com.ec:2181,quisrvbigdata2.otecel.com.ec:2181,quisrvbigdata10.otecel.com.ec:2181,quisrvbigdata11.otecel.com.ec:2181/default;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" \
--conf spark.hadoop.hive.metastore.uris="thrift://quisrvbigdata1.otecel.com.ec:9083,thrift://quisrvbigdata10.otecel.com.ec:9083" \
--conf spark.datasource.hive.warehouse.user.name="rgenerator" \
--conf spark.port.maxRetries=100 \
--queue $VAL_QUEUE \
--name $ENTIDAD \
--master $VAL_MASTER \
--driver-memory $VAL_DRIVER_MEMORY \
--executor-memory $VAL_EXECUTOR_MEMORY \
--num-executors $VAL_NUM_EXECUTORS \
--executor-cores $VAL_NUM_EXECUTOR_CORES \
$VAL_RUTA/python/carga_informacion_b2b.py \
--vSchmRep=$VAL_ESQUEMA \
--FechaProceso=$fechaFinMesAnterior \
--mesTres=$MesTres \
--mesVeinteCuatro=$MesVeinteCuatro \
--mesVeinteCuatroDate=$MesVeinteCuatroDate \
--mesTresDate=$MesTresDate 2>&1 &>> $VAL_LOG

#VALIDA EJECUCION DEL ARCHIVO SPARK
error_spark=`egrep 'invalid syntax|Traceback|An error occurred|Caused by:|pyspark.sql.utils.ParseException|AnalysisException:|NameError:|IndentationError:|Permission denied:|ValueError:|ERROR:|error:|unrecognized arguments:|No such file or directory|Failed to connect|Could not open client' $VAL_LOG | wc -l`
if [ $error_spark -eq 0 ];then
echo "==== OK - La ejecucion del archivo spark carga_informacion_b2b.py es EXITOSO ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG
else
echo "==== ERROR: - En la ejecucion del archivo spark carga_informacion_b2b.py ====" 2>&1 &>> $VAL_LOG
exit 1
fi

echo "==== Finaliza ejecucion del proceso OTC_T_B2B_CARGA ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
