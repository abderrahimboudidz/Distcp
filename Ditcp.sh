#!/bin/bash

set -euo pipefail

############################
# FONCTIONS LOGS
############################

timestamp() {
  date "+%Y-%m-%d %H:%M:%S"
}

log() {
  echo "$(timestamp) [INFO ] $*" | tee -a "$LOG_FILE"
}

log_warn() {
  echo "$(timestamp) [WARN ] $*" | tee -a "$LOG_FILE"
}

log_error() {
  echo "$(timestamp) [ERROR] $*" | tee -a "$LOG_FILE"
}

fatal() {
  log_error "$*"
  exit 1
}

############################
# VERIFICATION PARAMETRES
############################

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <liste_fichiers>"
  exit 1
fi

FILES_LIST="$1"
CONF_FILE="./distcp.conf"

[ -f "$FILES_LIST" ] || fatal "Liste de fichiers introuvable: $FILES_LIST"
[ -f "$CONF_FILE" ] || fatal "Fichier de configuration manquant: $CONF_FILE"

############################
# CHARGEMENT CONFIG
############################

source "$CONF_FILE"

mkdir -p "$LOG_DIR"

RUN_ID=$(date "+%Y%m%d_%H%M%S")
LOG_FILE="$LOG_DIR/distcp_${RUN_ID}.log"

log "==== DEMARRAGE DU SCRIPT DISTCP ===="
log "Liste des fichiers : $FILES_LIST"
log "Run ID            : $RUN_ID"

############################
# INITIALISATION KERBEROS
############################

log "Initialisation Kerberos"

KEYTAB=$(find "$KEYTAB_BASE_DIR" -name "hdfs.keytab" 2>/dev/null | head -n 1)

[ -n "$KEYTAB" ] || fatal "Keytab hdfs introuvable dans $KEYTAB_BASE_DIR"

kinit -kt "$KEYTAB" "$KERBEROS_PRINCIPAL" \
  || fatal "Echec kinit Kerberos"

klist -s || fatal "Ticket Kerberos invalide"

log "Kerberos OK (keytab: $KEYTAB)"

############################
# CHECKS PREALABLES
############################

log "Début des checks préalables"

TOTAL_SIZE=0
TOTAL_FILES=0

log "Liste des chemins à copier :"
while read -r FILE; do
  log " - $FILE"
done < "$FILES_LIST"

while read -r FILE; do
  SRC_PATH="${HDP_NAMENODE}${FILE}"

  hdfs dfs -test -e "$SRC_PATH" \
    || fatal "Fichier inexistant sur HDP: $FILE"

  DU_OUTPUT=$(hdfs dfs -du -s "$SRC_PATH")
  SIZE=$(echo "$DU_OUTPUT" | awk '{print $1}')

  TOTAL_SIZE=$((TOTAL_SIZE + SIZE))
  TOTAL_FILES=$((TOTAL_FILES + 1))

done < "$FILES_LIST"

log "Volume total à copier : $(numfmt --to=iec $TOTAL_SIZE)"
log "Nombre total de fichiers : $TOTAL_FILES"

############################
# EXECUTION DISTCP
############################

log "Lancement DistCp"
log "Source      : $HDP_NAMENODE"
log "Destination : $CDP_NAMENODE"
log "Mappers     : $DISTCP_MAPPERS"
log "Bandwidth   : $DISTCP_BANDWIDTH MB"
log "Options     : -pugpxt"

START_TIME=$(date +%s)

hadoop distcp \
  -m "$DISTCP_MAPPERS" \
  -bandwidth "$DISTCP_BANDWIDTH" \
  -pugpxt \
  -f "$FILES_LIST" \
  "$HDP_NAMENODE" \
  "$CDP_NAMENODE" \
  >> "$LOG_FILE" 2>&1 \
  || fatal "Echec DistCp"

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

log "DistCp terminé avec succès"

############################
# VERIFICATION INTEGRITE
############################

log "Début de la vérification d'intégrité (checksum)"

while read -r FILE; do
  SRC_PATH="${HDP_NAMENODE}${FILE}"
  DST_PATH="${CDP_NAMENODE}${FILE}"

  SRC_SUM=$(hdfs dfs -checksum "$SRC_PATH" | awk '{print $NF}')
  DST_SUM=$(hdfs dfs -checksum "$DST_PATH" | awk '{print $NF}')

  if [ "$SRC_SUM" != "$DST_SUM" ]; then
    fatal "Checksum différent pour $FILE"
  fi

  log "Checksum OK : $FILE"

done < "$FILES_LIST"

log "Vérification d'intégrité terminée"

############################
# RESUME FINAL
############################

log "==== RESUME ===="
log "Fichiers copiés : $TOTAL_FILES"
log "Volume copié    : $(numfmt --to=iec $TOTAL_SIZE)"
log "Durée totale    : ${DURATION} secondes"
log "Log complet     : $LOG_FILE"
log "==== FIN DU SCRIPT ===="
