############################
# INITIALISATION KERBEROS
############################

log "Initialisation Kerberos (keytab hdfs NAMENODE)"

KEYTAB=$(find /var/run/cloudera-scm-agent/process \
  -type f \
  -wholename "*-hdfs-NAMENODE/hdfs.keytab" \
  | sort -V \
  | tail -1)

if [ -z "$KEYTAB" ]; then
  fatal "Keytab hdfs NAMENODE introuvable dans /var/run/cloudera-scm-agent/process"
fi

log "Keytab utilisée : $KEYTAB"

kinit -kt "$KEYTAB" hdfs \
  || fatal "Echec kinit avec la keytab $KEYTAB"

klist -s || fatal "Ticket Kerberos invalide"

log "Kerberos initialisé avec succès"
