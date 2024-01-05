###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Parametros definidos en la tabla $TABLA" 2>&1 &>> $VAL_LOG
###########################################################################################################################################################
VAL_RUTA_OUT=`mysql -N <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_RUTA_OUT';"` 
VAL_HORA_INI_1=`mysql -N  <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_HORA_INI_1';"`
VAL_HORA_FIN_1=`mysql -N  <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_HORA_FIN_1';"`
VAL_HORA_INI_2=`mysql -N  <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_HORA_INI_2';"`
VAL_HORA_FIN_2=`mysql -N  <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_HORA_FIN_2';"`
VAL_HORA_INI_3=`mysql -N  <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_HORA_INI_3';"`
VAL_HORA_FIN_3=`mysql -N  <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_HORA_FIN_3';"`
VAL_TABLA_FINAL=`mysql -N  <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TABLA_FINAL';"`
VAL_TABLA_TMP=`mysql -N  <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TABLA_TMP';"`
VAL_TIPO_CARGA=`mysql -N  <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TIPO_CARGA';"`
VAL_REPARTITION=`mysql -N  <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_REPARTITION';"`
VAL_FETCH_SIZE=`mysql -N  <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_FETCH_SIZE';"`
VAL_MASTER=`mysql -N  <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_MASTER';"`
VAL_DRIVER_MEMORY=`mysql -N  <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_DRIVER_MEMORY';"`
VAL_EXECUTOR_MEMORY=`mysql -N  <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_EXECUTOR_MEMORY';"`
VAL_NUM_EXECUTORS=`mysql -N  <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NUM_EXECUTORS';"`
VAL_NUM_EXECUTOR_CORES=`mysql -N  <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NUM_EXECUTOR_CORES';"`
VAL_QUEUE=`mysql -N  <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'QUEUE';"`
VAL_CORREO_ASUNTO=`mysql -N  <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_CORREO_ASUNTO';"`
VAL_CORREO_EMISOR=`mysql -N  <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_CORREO_EMISOR';"`
VAL_CORREOS_RECEPTORES=`mysql -N  <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_CORREOS_RECEPTORES';"`
ETAPA=`mysql -N  <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'ETAPA';"`
EVENTO=`mysql -N  <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'EVENTO';"`
SHELL=`mysql -N  <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'SHELL';"`
