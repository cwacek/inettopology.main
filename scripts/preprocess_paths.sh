# 2238  | 3320  |  DE | DTAG Deutsche Telekom AG
# 672   | 3209  |  DE | Arcor IP-Network
# 576   | 3269  |  IT | ASN-IBSNAZ TELECOM ITALIA
# 566   | 13184 |  DE | HANSENET HanseNet Telekommunikation GmbH
# 429   | 6805  |  DE | TDDE-ASN1 Telefonica Deutschland Autonomous System

if [[ $# -lt 4 ]]; then
  echo "Usage: run_scripts.sh <outdir> <filestart> <fileend> <label> <asn> [<asn> ...]"
  exit 1
fi

outdir=$1
shift
filestart=$1
shift
endfile=$1
shift
label=$1
shift

declare -a asns=()

echo "Planning to process files:  $(seq -s ' ' $filestart $endfile)..."
echo "Will write to $outdir/${label}.[asn].samples${filestart}_$endfile.aspaths.out"
read -p "Are you sure? " -n 1 -r
if [[ $REPLY =~ ^[Yy]$ ]]
then
  echo
      # do dangerous stuff
  while [[ "$#" -gt 0 ]]; do
    asns=( ${asns[@]} $1 )
    files=$(for f in $(seq $filestart $endfile); do echo simulate.typical.*-samples.$f.out; done)
    (inettopology extra torps.preprocess --client_as ${1} ribs_20130330 $files 2> $outdir/${label}.${1}.samples${filestart}_$endfile.aspaths.log > $outdir/${label}.${1}.samples${filestart}_$endfile.aspaths.out )& 
    shift
  done

  wait 

  read -r -d '' msg <<MSG
  ASNs: ${asns[@]}
  Files: $( seq -s ' ' $filestart $endfile )
  Directory: $(pwd)
MSG

  echo "$msg"

  echo "$msg" | mail -s  "Done" cwacek@cs.georgetown.edu

fi
