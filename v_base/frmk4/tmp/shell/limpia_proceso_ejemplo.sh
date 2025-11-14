#------------------------------------------------------------------------------------------------------------------------------------------
#-- RESUMEN
#-- Squad              : PD CROSS
#-- Descripcion        : Shell de limpia_CCD_MP_PERSONAL.sh Ingesta Optimus
#-- Fecha de Creacion  : 05/09/2025
#-- Responsable IBK	   : David Silva
#-- Autor              : Joycer Cayllahua L.
#-- ---------------------------------------------------------------------------------------------------------------------------------------
#-- MODIFICACIONES
#-- Nro. (SRT/SRI)  		   Fecha         Desarrollador        Lider Tecnico	           Descripcion
#-- ---------------------------------------------------------------------------------------------------------------------------------------
#-- SRT_2025-15773             05/09/2025    Joycer Cayllahua L.  David Silva              Limpieza de caracteres
#-- ---------------------------------------------------------------------------------------------------------------------------------------

# Limpieza de caracteres especiales
file=$1
fecproceso=$2
RutaInput=$3
RutaProcessed=$4

sed -e "s/\o47//g;s/\o342\o200\o223/\o055/g;s/\o302\o240/\o112/g;s/\o303\o203\o305\o223/\o303\o234/g;s/\o226/-/g;s/\o222/\o047/g;s/\o224//g;s/\o205//g;s/\o223//g;s/\o220//g;s/\o221//g;s/\o225//g;s/\o227//g;s/\o212/\o123/g" "${RutaInput}/${file}" | \
awk -v OFS="~|~" '{
    print  substr($0,1,3),    \
           substr($0,4,50),   \
           substr($0,54,6),   \
           substr($0,60,50),  \
           substr($0,110,6),  \
           substr($0,116,50)
}' > "${RutaProcessed}/${file}"
